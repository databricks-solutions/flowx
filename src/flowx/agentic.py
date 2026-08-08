"""Fingerprint-bound agentic resolution for source-reconciled migration gaps.

Flowx remains the owner of source parsing, task identity, graph structure, policy, IR, and
packaging. A provider may reason about one captured leaf gap and return only a constrained payload;
this module validates that payload and applies it to an immutable deterministic baseline.
"""

from __future__ import annotations

import ast
import copy
import hashlib
import json
import re
import shutil
import tempfile
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flowx.ir_serde import pipeline_to_dict
from flowx.sources.airflow.loader import discover_dags, load_pipelines

CONTRACT_VERSION = "1"
PROVIDER_NAME = "airflow-to-dabs"
PROVIDER_VERSION = "0.1.0"
PROVIDER_REPOSITORY = "https://github.com/park-peter/airflow-to-dabs"

_ALLOWED_REPLACEMENT_KINDS = ("notebook", "sql")
_RESOLUTION_STATUSES = {"resolved", "needs_input", "deferred"}
_DISPOSITIONS = {"consumed", "preserved_by_flowx", "ignored"}
_COMMON_TASK_FIELDS = (
    "name",
    "task_key",
    "description",
    "timeout_seconds",
    "max_retries",
    "min_retry_interval_millis",
    "depends_on",
    "cluster",
    "existing_cluster_id",
    "libraries",
    "parameter_approximations",
    "required_parameters",
    "compute_mode",
    "notifications",
)
_FLOWX_OWNED_ARGUMENTS = {
    "task_id",
    "retries",
    "retry_delay",
    "execution_timeout",
    "trigger_rule",
    "depends_on_past",
    "wait_for_downstream",
    "pool",
    "pool_slots",
    "priority_weight",
    "queue",
}
_NESTED_TASK_FIELDS = ("inner_activities", "if_true_activities", "if_false_activities", "default_activities")
_AIRFLOW_TEMPLATE = re.compile(r"{{\s*([^{}]+?)\s*}}|{%\s*([^{}]+?)\s*%}")
_DAB_TEMPLATE_PREFIXES = ("job.", "tasks.", "input", "backfill.")


class AgenticContractError(ValueError):
    """Raised when an agentic workspace or resolution violates the contract."""


@dataclass(frozen=True, slots=True, kw_only=True)
class GapEnvelope:
    """Versioned context for one source-reconciled leaf placeholder."""

    gap_id: str
    pipeline_name: str
    task_key: str
    task_path: list[str | int]
    operator: str
    source_file: str
    source_sha256: str
    baseline_report_sha256: str
    source_span: dict[str, int]
    raw_definition: dict[str, Any]
    arguments: list[dict[str, Any]]
    upstream_task_keys: list[str]
    downstream_task_keys: list[str]
    dag_settings: dict[str, Any]
    reason: dict[str, str]

    def as_dict(self) -> dict[str, Any]:
        """Returns the public GapEnvelope v1 representation."""
        return {
            "contract_version": CONTRACT_VERSION,
            "gap_id": self.gap_id,
            "source": "airflow",
            "pipeline_name": self.pipeline_name,
            "capture_identity": self.task_key,
            "task_key": self.task_key,
            "task_path": self.task_path,
            "operator": self.operator,
            "operator_fqn": self.operator,
            "source_file": self.source_file,
            "source_sha256": self.source_sha256,
            "baseline_report_sha256": self.baseline_report_sha256,
            "source_span": self.source_span,
            "raw_definition": self.raw_definition,
            "arguments": self.arguments,
            "upstream_task_keys": self.upstream_task_keys,
            "downstream_task_keys": self.downstream_task_keys,
            "dag_settings": self.dag_settings,
            "reason": self.reason,
            "allowed_replacement_kinds": list(_ALLOWED_REPLACEMENT_KINDS),
            "knowledge_provider": {
                "name": PROVIDER_NAME,
                "version": PROVIDER_VERSION,
                "repository": PROVIDER_REPOSITORY,
            },
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class StagedResolution:
    """A schema-validated resolution bound to one GapEnvelope."""

    gap: dict[str, Any]
    candidate: dict[str, Any]
    sha256: str


def prepare_airflow_resolutions(
    *,
    source_path: Path,
    report_path: Path,
    output_dir: Path,
    dbt_mode: str = "static",
) -> dict[str, Any]:
    """Snapshots source and an exactly reproducible deterministic report, then emits GapEnvelope v1."""
    source_path = source_path.resolve()
    report_path = report_path.resolve()
    output_dir = output_dir.resolve()
    if not source_path.exists():
        raise AgenticContractError(f"Airflow source path does not exist: {source_path}")
    try:
        baseline_bytes = report_path.read_bytes()
        baseline = json.loads(baseline_bytes)
    except OSError as error:
        raise AgenticContractError(f"Could not read deterministic report: {error}") from error
    except json.JSONDecodeError as error:
        raise AgenticContractError(f"Deterministic report contains invalid JSON: {error}") from error
    _require_airflow_baseline(baseline)

    source_files = _source_files(source_path)
    if not source_files:
        raise AgenticContractError(f"No Airflow DAG files found under {source_path}")
    source_hashes = {relative: _sha256_file(path) for relative, path in source_files}
    baseline_hash = _sha256_bytes(baseline_bytes)

    work_dir = output_dir / ".work"
    work_dir.mkdir(parents=True, exist_ok=True)
    target = work_dir / "agentic"
    with tempfile.TemporaryDirectory(prefix=".agentic-prepare-", dir=work_dir) as temporary:
        staging = Path(temporary)
        snapshot = staging / "source"
        for relative, path in source_files:
            destination = snapshot / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
        if {relative: _sha256_file(snapshot / relative) for relative in source_hashes} != source_hashes:
            raise AgenticContractError("Airflow source changed while the agentic snapshot was being created")

        snapshot_source = snapshot / source_files[0][0] if source_path.is_file() else snapshot
        rebuilt = _rebuild_airflow_report(snapshot_source, baseline, dbt_mode=dbt_mode)
        if rebuilt != baseline:
            raise AgenticContractError(
                "Airflow source no longer reproduces the deterministic report; rerun convert before prepare"
            )

        gaps = _build_gap_envelopes(baseline, baseline_hash=baseline_hash, source_hashes=source_hashes)
        if not gaps:
            raise AgenticContractError("The deterministic report contains no eligible Airflow leaf gaps")

        (staging / "baseline.json").write_bytes(baseline_bytes)
        gaps_bytes = _json_bytes(gaps)
        (staging / "gaps.json").write_bytes(gaps_bytes)
        (staging / "candidates").mkdir()
        _write_json(staging / "candidate_index.json", {})
        manifest = {
            "contract_version": CONTRACT_VERSION,
            "source": "airflow",
            "provider": {
                "name": PROVIDER_NAME,
                "version": PROVIDER_VERSION,
                "repository": PROVIDER_REPOSITORY,
            },
            "source_path": str(source_path),
            "source_kind": "file" if source_path.is_file() else "directory",
            "source_files": [
                {"path": relative, "sha256": source_hashes[relative]} for relative in sorted(source_hashes)
            ],
            "dbt_mode": dbt_mode,
            "baseline_report_sha256": baseline_hash,
            "gaps_sha256": _sha256_bytes(gaps_bytes),
        }
        _write_json(staging / "manifest.json", manifest)
        if target.exists():
            shutil.rmtree(target)
        shutil.move(str(staging), target)

    return {
        "status": "prepared",
        "contract_version": CONTRACT_VERSION,
        "provider_version": PROVIDER_VERSION,
        "gap_count": len(gaps),
        "workspace": str(target),
    }


def stage_airflow_resolutions(*, output_dir: Path, candidate_paths: list[Path]) -> dict[str, Any]:
    """Validates provider candidates and records their immutable hashes in the agentic workspace."""
    workspace = _workspace(output_dir)
    manifest, gaps = _load_workspace(workspace)
    if not candidate_paths:
        raise AgenticContractError("At least one --candidate path is required")
    gap_by_id = {gap["gap_id"]: gap for gap in gaps}
    staged: dict[str, StagedResolution] = {}
    for path in candidate_paths:
        try:
            candidate = json.loads(path.read_text(encoding="utf-8"))
        except OSError as error:
            raise AgenticContractError(f"Could not read candidate {path}: {error}") from error
        except json.JSONDecodeError as error:
            raise AgenticContractError(f"Candidate {path} contains invalid JSON: {error}") from error
        resolution = _validate_candidate(candidate, gap_by_id=gap_by_id, manifest=manifest)
        staged[resolution.gap["gap_id"]] = resolution

    candidates_dir = workspace / "candidates"
    candidates_dir.mkdir(exist_ok=True)
    index = _load_candidate_index(workspace, valid_gap_ids=set(gap_by_id))
    for gap_id, resolution in staged.items():
        destination = candidates_dir / f"{gap_id}.json"
        destination.write_bytes(_json_bytes(resolution.candidate))
        index[gap_id] = {
            "sha256": resolution.sha256,
            "status": resolution.candidate["status"],
        }
    _write_json(workspace / "candidate_index.json", index)
    return {"status": "staged", "staged": sorted(staged), "candidate_count": len(index)}


def apply_airflow_resolutions(
    *,
    output_dir: Path,
    accepted_gap_ids: list[str] | None = None,
    accept_all: bool = False,
    reset: bool = False,
    source_path: Path | None = None,
) -> dict[str, Any]:
    """Rebuilds an agentic report from the immutable baseline and the declarative acceptance set."""
    if sum(bool(option) for option in (accepted_gap_ids, accept_all, reset)) != 1:
        raise AgenticContractError("Choose exactly one of --accept-gap, --accept-all, or --reset")
    output_dir = output_dir.resolve()
    workspace = _workspace(output_dir)
    manifest, gaps = _load_workspace(workspace)
    baseline_path = workspace / "baseline.json"
    baseline_bytes = baseline_path.read_bytes()
    if _sha256_bytes(baseline_bytes) != manifest["baseline_report_sha256"]:
        raise AgenticContractError("The immutable deterministic baseline was modified after prepare")
    baseline = json.loads(baseline_bytes)

    _verify_snapshot(workspace, manifest)
    live_source = (source_path or Path(manifest["source_path"])).resolve()
    if _current_source_hashes(live_source) != _manifest_source_hashes(manifest):
        raise AgenticContractError("source changed since prepare; re-run prepare")
    snapshot_source = _snapshot_source(workspace, manifest)
    if _rebuild_airflow_report(snapshot_source, baseline, dbt_mode=manifest["dbt_mode"]) != baseline:
        raise AgenticContractError("The prepared source snapshot no longer reproduces the deterministic report")

    gap_by_id = {gap["gap_id"]: gap for gap in gaps}
    index = _load_candidate_index(workspace, valid_gap_ids=set(gap_by_id))
    selected_ids = [] if reset else sorted(index) if accept_all else list(dict.fromkeys(accepted_gap_ids or []))
    missing = sorted(set(selected_ids) - set(index))
    if missing:
        raise AgenticContractError(f"No staged candidate exists for gap(s): {', '.join(missing)}")

    selected: list[StagedResolution] = []
    for gap_id in selected_ids:
        candidate_path = workspace / "candidates" / f"{gap_id}.json"
        candidate_bytes = candidate_path.read_bytes()
        if _sha256_bytes(candidate_bytes) != index[gap_id]["sha256"]:
            raise AgenticContractError(f"staged candidate was modified after validation: {gap_id}")
        candidate = json.loads(candidate_bytes)
        selected.append(_validate_candidate(candidate, gap_by_id=gap_by_id, manifest=manifest))

    applied = _apply_to_baseline(baseline, selected)
    report_path = output_dir / ".work" / "translation_report.agentic.json"
    _write_json_atomic(report_path, applied)

    evidence = output_dir / "metadata" / "agentic"
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / "baseline.json").write_bytes(baseline_bytes)
    (evidence / "gaps.json").write_bytes((workspace / "gaps.json").read_bytes())
    (evidence / "manifest.json").write_bytes((workspace / "manifest.json").read_bytes())
    accepted_payload = {
        "contract_version": CONTRACT_VERSION,
        "candidates": [resolution.candidate for resolution in selected],
    }
    _write_json(evidence / "accepted_resolutions.json", accepted_payload)
    return {
        "status": "reset" if reset else "applied",
        "accepted_gap_ids": selected_ids,
        "report_path": str(report_path),
    }


def validate_persisted_agentic_report(report: dict[str, Any], *, evidence_dir: Path) -> list[str]:
    """Replays accepted candidates from kept evidence and compares the exact expected report."""
    try:
        baseline_bytes = (evidence_dir / "baseline.json").read_bytes()
        baseline = json.loads(baseline_bytes)
        gaps_bytes = (evidence_dir / "gaps.json").read_bytes()
        gaps = json.loads(gaps_bytes)
        manifest = _read_json_object(evidence_dir / "manifest.json")
        accepted = _read_json_object(evidence_dir / "accepted_resolutions.json")
    except (OSError, json.JSONDecodeError, AgenticContractError) as error:
        return [f"agentic resolution evidence is missing or invalid: {error}"]
    if _sha256_bytes(baseline_bytes) != manifest.get("baseline_report_sha256"):
        return ["agentic resolution baseline hash does not match its manifest"]
    if _sha256_bytes(gaps_bytes) != manifest.get("gaps_sha256"):
        return ["agentic gap-envelope hash does not match its manifest"]
    expected_provider = {
        "name": PROVIDER_NAME,
        "version": PROVIDER_VERSION,
        "repository": PROVIDER_REPOSITORY,
    }
    if (
        manifest.get("contract_version") != CONTRACT_VERSION
        or manifest.get("source") != "airflow"
        or manifest.get("provider") != expected_provider
    ):
        return ["agentic resolution manifest has an unsupported contract, source, or provider"]
    gap_by_id = {
        str(gap["gap_id"]): gap for gap in gaps if isinstance(gap, dict) and isinstance(gap.get("gap_id"), str)
    }
    resolutions: list[StagedResolution] = []
    try:
        candidates = accepted.get("candidates")
        if not isinstance(candidates, list):
            raise AgenticContractError("accepted_resolutions.json must contain a candidates list")
        for candidate in candidates:
            resolutions.append(_validate_candidate(candidate, gap_by_id=gap_by_id, manifest=manifest))
        expected = _apply_to_baseline(baseline, resolutions)
    except AgenticContractError as error:
        return [f"agentic resolution evidence failed validation: {error}"]
    if expected != report:
        return ["agentic report does not match replay from its immutable baseline and accepted resolutions"]
    return []


def _require_airflow_baseline(payload: Any) -> None:
    pipelines = _pipeline_list(payload)
    for pipeline in pipelines:
        if not isinstance(pipeline, dict) or (pipeline.get("tags") or {}).get("source") != "airflow":
            raise AgenticContractError("resolve-agentic requires a canonical Airflow translation report")
        if pipeline.get("reconciliation_status") == "failed":
            raise AgenticContractError("Agentic resolution cannot repair a failed source-reconciliation report")


def _rebuild_airflow_report(source_path: Path, baseline: dict[str, Any], *, dbt_mode: str) -> dict[str, Any]:
    expected = _pipeline_list(baseline)
    expected_names = [pipeline["name"] for pipeline in expected]
    excluded = {pipeline["name"] for pipeline in expected if pipeline.get("migration_status") == "excluded"}
    loaded = load_pipelines(source_path, dbt_mode=dbt_mode, exclude_dags=excluded)
    by_name = {pipeline.name: pipeline for pipeline in loaded}
    if any(name not in by_name for name in expected_names):
        raise AgenticContractError("Source snapshot does not contain every DAG in the deterministic report")
    rebuilt = [pipeline_to_dict(by_name[name]) for name in expected_names]
    return {"pipelines": rebuilt} if "pipelines" in baseline else rebuilt[0]


def _build_gap_envelopes(
    baseline: dict[str, Any],
    *,
    baseline_hash: str,
    source_hashes: dict[str, str],
) -> list[dict[str, Any]]:
    envelopes: list[dict[str, Any]] = []
    for pipeline in _pipeline_list(baseline):
        if pipeline.get("migration_status") == "excluded":
            continue
        source_file = (pipeline.get("audit") or {}).get("source_file", "")
        source_hash = source_hashes.get(source_file)
        if source_hash is None and len(source_hashes) == 1:
            source_hash = next(iter(source_hashes.values()))
        if source_hash is None:
            raise AgenticContractError(f"No source snapshot hash matches pipeline {pipeline.get('name')!r}")
        findings = {
            (finding.get("details") or {}).get("task_key"): finding
            for finding in pipeline.get("not_translatable") or []
            if isinstance(finding, dict) and finding.get("code") == "operator_placeholder"
        }
        tasks = list(_walk_tasks(pipeline.get("tasks") or []))
        downstream = _downstream_index([task for _, task in tasks])
        for task_path, task in tasks:
            if task.get("type") != "PlaceholderActivity" or str(task.get("task_key", "")).startswith("__flowx_"):
                continue
            finding = findings.get(task.get("task_key"))
            if not finding or not finding.get("fingerprint"):
                continue
            raw_definition = dict(task.get("raw_definition") or {})
            operator = str(raw_definition.get("operator") or task.get("original_type") or "UnknownOperator")
            envelope = GapEnvelope(
                gap_id=str(finding["fingerprint"]),
                pipeline_name=str(pipeline["name"]),
                task_key=str(task["task_key"]),
                task_path=list(task_path),
                operator=operator,
                source_file=str(source_file),
                source_sha256=source_hash,
                baseline_report_sha256=baseline_hash,
                source_span={key: int(finding.get(key, 0)) for key in ("line", "column", "end_line", "end_column")},
                raw_definition=raw_definition,
                arguments=_extract_arguments(raw_definition, operator=operator),
                upstream_task_keys=[str(item.get("task_key")) for item in task.get("depends_on") or []],
                downstream_task_keys=downstream.get(str(task["task_key"]), []),
                dag_settings={
                    "schedule": pipeline.get("schedule"),
                    "parameters": pipeline.get("parameters"),
                    "tags": pipeline.get("tags"),
                },
                reason={
                    "code": str(finding.get("code", "operator_placeholder")),
                    "message": str(finding.get("message", "")),
                },
            )
            envelopes.append(envelope.as_dict())
    return sorted(envelopes, key=lambda item: (item["pipeline_name"], item["task_path"]))


def _extract_arguments(raw_definition: dict[str, Any], *, operator: str) -> list[dict[str, Any]]:
    source = raw_definition.get("source")
    arguments: list[dict[str, Any]] = []
    if isinstance(source, str) and source.strip():
        try:
            module = ast.parse(textwrap.dedent(source))
        except SyntaxError:
            module = None
        if module is not None:
            calls = [node for node in ast.walk(module) if isinstance(node, ast.Call)]
            matching = [call for call in calls if _call_name(call.func) == operator]
            call = matching[0] if matching else calls[0] if calls else None
            if call is not None:
                for index, value in enumerate(call.args):
                    name = f"$star{index}" if isinstance(value, ast.Starred) else f"$arg{index}"
                    expression = ast.unparse(value.value if isinstance(value, ast.Starred) else value)
                    arguments.append(_argument(name, expression))
                kwargs_index = 0
                for keyword in call.keywords:
                    keyword_name = keyword.arg
                    if keyword_name is None:
                        keyword_name = f"$kwargs{kwargs_index}"
                        kwargs_index += 1
                    arguments.append(_argument(keyword_name, ast.unparse(keyword.value)))
    mapping = raw_definition.get("mapping")
    if isinstance(mapping, str) and mapping:
        arguments.append(_argument("$mapping", mapping))
    return arguments


def _argument(name: str, expression: str) -> dict[str, Any]:
    return {
        "name": name,
        "source_expression": expression,
        "preserved_by_flowx": name in _FLOWX_OWNED_ARGUMENTS,
    }


def _validate_candidate(
    candidate: Any,
    *,
    gap_by_id: dict[str, dict[str, Any]],
    manifest: dict[str, Any],
) -> StagedResolution:
    if not isinstance(candidate, dict):
        raise AgenticContractError("Candidate must be a JSON object")
    allowed_top = {
        "contract_version",
        "gap_id",
        "status",
        "baseline_report_sha256",
        "source_sha256",
        "provider",
        "model",
        "argument_disposition",
        "prerequisites",
        "warnings",
        "semantic_deltas",
        "replacement",
        "generated_files",
        "reason",
    }
    extra = sorted(set(candidate) - allowed_top)
    if extra:
        raise AgenticContractError(f"Candidate contains unsupported fields: {', '.join(extra)}")
    if candidate.get("contract_version") != CONTRACT_VERSION:
        raise AgenticContractError(f"Unsupported agentic contract_version: {candidate.get('contract_version')!r}")
    gap_id = candidate.get("gap_id")
    if not isinstance(gap_id, str):
        raise AgenticContractError("Candidate gap_id must be a string")
    gap = gap_by_id.get(gap_id)
    if gap is None:
        raise AgenticContractError(f"Candidate gap_id does not match a prepared gap: {gap_id!r}")
    if candidate.get("baseline_report_sha256") != manifest.get("baseline_report_sha256"):
        raise AgenticContractError("Candidate baseline_report_sha256 does not match the prepared baseline")
    if candidate.get("source_sha256") != gap.get("source_sha256"):
        raise AgenticContractError("Candidate source_sha256 does not match its GapEnvelope")
    provider = candidate.get("provider")
    expected_provider = {
        "name": PROVIDER_NAME,
        "version": PROVIDER_VERSION,
        "repository": PROVIDER_REPOSITORY,
    }
    if provider != expected_provider:
        raise AgenticContractError(f"Candidate provider must match pinned {PROVIDER_NAME} v{PROVIDER_VERSION}")
    model = candidate.get("model")
    if not isinstance(model, dict) or not isinstance(model.get("name"), str) or not model["name"].strip():
        raise AgenticContractError("Candidate model provenance requires a non-empty model.name")
    status = candidate.get("status")
    if status not in _RESOLUTION_STATUSES:
        raise AgenticContractError(f"Candidate status must be one of: {', '.join(sorted(_RESOLUTION_STATUSES))}")
    for field in ("prerequisites", "warnings", "semantic_deltas"):
        if not isinstance(candidate.get(field), list) or not all(isinstance(item, str) for item in candidate[field]):
            raise AgenticContractError(f"Candidate {field} must be a list of strings")
    _validate_argument_disposition(candidate.get("argument_disposition"), gap)
    if status == "resolved":
        _validate_replacement(candidate, gap)
    else:
        if not isinstance(candidate.get("reason"), str) or not candidate["reason"].strip():
            raise AgenticContractError(f"{status} candidate requires a non-empty reason")
        if "replacement" in candidate or "generated_files" in candidate:
            raise AgenticContractError(f"{status} candidate must not contain a replacement or generated files")
    normalized = json.loads(_json_bytes(candidate))
    return StagedResolution(gap=gap, candidate=normalized, sha256=_sha256_bytes(_json_bytes(normalized)))


def _validate_argument_disposition(value: Any, gap: dict[str, Any]) -> None:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise AgenticContractError("argument_disposition must be a list of objects")
    expected = {argument["name"]: argument for argument in gap.get("arguments") or []}
    actual_names = [item.get("name") for item in value]
    if len(actual_names) != len(set(actual_names)) or set(actual_names) != set(expected):
        raise AgenticContractError("argument_disposition must cover every source argument exactly once")
    for item in value:
        disposition = item.get("disposition")
        if disposition not in _DISPOSITIONS:
            raise AgenticContractError(f"Unknown argument disposition for {item.get('name')!r}: {disposition!r}")
        rationale = item.get("rationale")
        if not isinstance(rationale, str) or not rationale.strip():
            qualifier = "ignored argument" if disposition == "ignored" else "argument disposition"
            raise AgenticContractError(f"{qualifier} requires a rationale: {item.get('name')}")
        if expected[item["name"]]["preserved_by_flowx"] and disposition != "preserved_by_flowx":
            raise AgenticContractError(f"Flowx-owned argument must be preserved_by_flowx: {item['name']}")
        if not expected[item["name"]]["preserved_by_flowx"] and disposition == "preserved_by_flowx":
            raise AgenticContractError(f"Provider argument is not preserved by Flowx: {item['name']}")


def _validate_replacement(candidate: dict[str, Any], gap: dict[str, Any]) -> None:
    replacement = candidate.get("replacement")
    if not isinstance(replacement, dict):
        raise AgenticContractError("Resolved candidate requires a replacement object")
    kind = replacement.get("kind")
    if kind not in gap.get("allowed_replacement_kinds", []):
        raise AgenticContractError(f"Replacement kind is not allowed for this gap: {kind!r}")
    allowed = {"kind", "file", "base_parameters"} if kind == "notebook" else {"kind", "file", "parameters"}
    extra = sorted(set(replacement) - allowed)
    if extra:
        raise AgenticContractError(f"replacement contains unsupported fields: {', '.join(extra)}")
    file_name = replacement.get("file")
    if not isinstance(file_name, str) or not _safe_relative_path(file_name):
        raise AgenticContractError("Replacement file must be a safe relative path")
    parameters_field = "base_parameters" if kind == "notebook" else "parameters"
    parameters = replacement.get(parameters_field, {})
    if not isinstance(parameters, dict) or not all(
        isinstance(key, str) and isinstance(val, str) for key, val in parameters.items()
    ):
        raise AgenticContractError(f"Replacement {parameters_field} must be a string-to-string object")
    files = candidate.get("generated_files")
    if not isinstance(files, list) or len(files) != 1 or not isinstance(files[0], dict):
        raise AgenticContractError("Resolved v1 candidate requires exactly one inline generated file")
    generated = files[0]
    allowed_file_fields = {"path", "language", "content", "sha256"}
    if set(generated) - allowed_file_fields:
        raise AgenticContractError("Generated file contains unsupported fields")
    if generated.get("path") != file_name:
        raise AgenticContractError("Replacement file does not match generated_files.path")
    expected_language = "python" if kind == "notebook" else "sql"
    if generated.get("language") != expected_language:
        raise AgenticContractError(f"Generated file language must be {expected_language!r}")
    content = generated.get("content")
    if not isinstance(content, str) or not content.strip():
        raise AgenticContractError("Generated file content must be non-empty")
    if generated.get("sha256") != _sha256_bytes(content.encode("utf-8")):
        raise AgenticContractError("Generated file sha256 does not match its content")
    _reject_unresolved_templates({"replacement": replacement, "generated_files": files})
    if kind == "notebook":
        try:
            module = ast.parse(content)
        except SyntaxError as error:
            raise AgenticContractError(f"Generated notebook is not valid Python: {error}") from error
        for node in ast.walk(module):
            if isinstance(node, ast.Import) and any(
                alias.name == "airflow" or alias.name.startswith("airflow.") for alias in node.names
            ):
                raise AgenticContractError("Generated notebook must not import Airflow")
            if isinstance(node, ast.ImportFrom) and (
                node.module == "airflow" or str(node.module).startswith("airflow.")
            ):
                raise AgenticContractError("Generated notebook must not import Airflow")


def _apply_to_baseline(baseline: dict[str, Any], resolutions: list[StagedResolution]) -> dict[str, Any]:
    applied = copy.deepcopy(baseline)
    baseline_pipelines = {pipeline["name"]: pipeline for pipeline in _pipeline_list(baseline)}
    applied_pipelines = {pipeline["name"]: pipeline for pipeline in _pipeline_list(applied)}
    by_pipeline: dict[str, list[StagedResolution]] = {}
    for resolution in resolutions:
        by_pipeline.setdefault(resolution.gap["pipeline_name"], []).append(resolution)

    for pipeline_name, selected in by_pipeline.items():
        original_pipeline = baseline_pipelines[pipeline_name]
        pipeline = applied_pipelines[pipeline_name]
        resolved_count = 0
        accepted_proof: list[dict[str, Any]] = []
        resolved_paths: set[tuple[str | int, ...]] = set()
        for resolution in selected:
            path = tuple(resolution.gap["task_path"])
            task = _get_path(pipeline, path)
            if not isinstance(task, dict) or task.get("type") != "PlaceholderActivity":
                raise AgenticContractError(f"Gap no longer points to a placeholder: {resolution.gap['gap_id']}")
            status = resolution.candidate["status"]
            if status == "resolved":
                _set_path(pipeline, path, _build_replacement(task, resolution.candidate))
                resolved_count += 1
                resolved_paths.add(path)
            _annotate_finding(pipeline, resolution)
            accepted_proof.append(
                {
                    "gap_id": resolution.gap["gap_id"],
                    "task_key": resolution.gap["task_key"],
                    "status": status,
                    "candidate_sha256": resolution.sha256,
                }
            )
        _assert_task_invariants(original_pipeline, pipeline, resolved_paths=resolved_paths)
        baseline_graph = _graph_hash(original_pipeline)
        merged_graph = _graph_hash(pipeline)
        if baseline_graph != merged_graph:
            raise AgenticContractError(
                f"Agentic resolution changed graph or task policy for pipeline {pipeline_name!r}"
            )
        pipeline.setdefault("audit", {})["agentic_resolution"] = {
            "contract_version": CONTRACT_VERSION,
            "provider_version": PROVIDER_VERSION,
            "validation_status": "verified",
            "baseline_graph_sha256": baseline_graph,
            "merged_graph_sha256": merged_graph,
            "accepted": sorted(accepted_proof, key=lambda item: item["gap_id"]),
            "resolved_count": resolved_count,
        }
        if resolved_count:
            pipeline["reconciliation_status"] = "verified_with_reviewed_resolutions"
    return applied


def _build_replacement(placeholder: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    replacement = candidate["replacement"]
    generated = candidate["generated_files"][0]
    task = {field: copy.deepcopy(placeholder[field]) for field in _COMMON_TASK_FIELDS if field in placeholder}
    if replacement["kind"] == "notebook":
        task.update(
            {
                "type": "NotebookActivity",
                "notebook_path": f"notebooks/{placeholder['task_key']}.py",
                "generated_source": generated["content"],
            }
        )
        if replacement.get("base_parameters"):
            task["base_parameters"] = dict(replacement["base_parameters"])
    else:
        task.update({"type": "SqlActivity", "sql": generated["content"], "warehouse_ref": "${var.warehouse_id}"})
        if replacement.get("parameters"):
            task["parameters"] = dict(replacement["parameters"])
    return task


def _annotate_finding(pipeline: dict[str, Any], resolution: StagedResolution) -> None:
    for finding in pipeline.get("not_translatable") or []:
        if isinstance(finding, dict) and finding.get("fingerprint") == resolution.gap["gap_id"]:
            finding["resolution"] = {
                "status": resolution.candidate["status"],
                "provider": resolution.candidate["provider"],
                "model": resolution.candidate["model"],
                "argument_disposition": resolution.candidate["argument_disposition"],
                "prerequisites": resolution.candidate["prerequisites"],
                "warnings": resolution.candidate["warnings"],
                "semantic_deltas": resolution.candidate["semantic_deltas"],
                "candidate_sha256": resolution.sha256,
            }
            if resolution.candidate["status"] == "resolved":
                finding["severity"] = "resolved"
            elif resolution.candidate.get("reason"):
                finding["resolution"]["reason"] = resolution.candidate["reason"]
            return
    raise AgenticContractError(f"Gap finding is missing from pipeline report: {resolution.gap['gap_id']}")


def _assert_task_invariants(
    baseline_pipeline: dict[str, Any],
    applied_pipeline: dict[str, Any],
    *,
    resolved_paths: set[tuple[str | int, ...]],
) -> None:
    baseline_tasks = {path: task for path, task in _walk_tasks(baseline_pipeline.get("tasks") or [])}
    applied_tasks = {path: task for path, task in _walk_tasks(applied_pipeline.get("tasks") or [])}
    if set(baseline_tasks) != set(applied_tasks):
        raise AgenticContractError("Agentic resolution changed task count or enclosing control-flow structure")
    for path, baseline_task in baseline_tasks.items():
        applied_task = applied_tasks[path]
        if path not in resolved_paths and _task_shell(baseline_task) != _task_shell(applied_task):
            raise AgenticContractError(f"Agentic resolution changed an unaccepted task at path {list(path)}")
        if path in resolved_paths and _task_policy(baseline_task) != _task_policy(applied_task):
            raise AgenticContractError(
                f"Agentic resolution changed task identity, dependencies, or policy at {list(path)}"
            )


def _graph_hash(pipeline: dict[str, Any]) -> str:
    projection = [{"path": list(path), **_task_policy(task)} for path, task in _walk_tasks(pipeline.get("tasks") or [])]
    return _sha256_bytes(json.dumps(projection, sort_keys=True, separators=(",", ":")).encode("utf-8"))


def _task_policy(task: dict[str, Any]) -> dict[str, Any]:
    return {field: copy.deepcopy(task.get(field)) for field in _COMMON_TASK_FIELDS}


def _task_shell(task: dict[str, Any]) -> dict[str, Any]:
    """Returns one task without descendant lists so a nested leaf may change independently."""
    shell = {key: copy.deepcopy(value) for key, value in task.items() if key not in _NESTED_TASK_FIELDS}
    cases = shell.get("cases")
    if isinstance(cases, list):
        shell["cases"] = [
            {key: value for key, value in case.items() if key != "activities"} if isinstance(case, dict) else case
            for case in cases
        ]
    return shell


def _walk_tasks(
    tasks: list[Any], path: tuple[str | int, ...] = ("tasks",)
) -> list[tuple[tuple[str | int, ...], dict[str, Any]]]:
    walked: list[tuple[tuple[str | int, ...], dict[str, Any]]] = []
    for index, task in enumerate(tasks):
        if not isinstance(task, dict):
            continue
        task_path = (*path, index)
        walked.append((task_path, task))
        for field in _NESTED_TASK_FIELDS:
            child = task.get(field)
            if isinstance(child, list):
                walked.extend(_walk_tasks(child, (*task_path, field)))
        cases = task.get("cases")
        if isinstance(cases, list):
            for case_index, case in enumerate(cases):
                if isinstance(case, dict) and isinstance(case.get("activities"), list):
                    walked.extend(_walk_tasks(case["activities"], (*task_path, "cases", case_index, "activities")))
    return walked


def _get_path(root: dict[str, Any], path: tuple[str | int, ...]) -> Any:
    current: Any = root
    for part in path:
        current = current[part]
    return current


def _set_path(root: dict[str, Any], path: tuple[str | int, ...], value: Any) -> None:
    parent = _get_path(root, path[:-1])
    parent[path[-1]] = value


def _downstream_index(tasks: list[dict[str, Any]]) -> dict[str, list[str]]:
    downstream: dict[str, list[str]] = {}
    for task in tasks:
        for dependency in task.get("depends_on") or []:
            if isinstance(dependency, dict) and dependency.get("task_key"):
                downstream.setdefault(str(dependency["task_key"]), []).append(str(task.get("task_key")))
    return {key: sorted(value) for key, value in downstream.items()}


def _reject_unresolved_templates(value: Any) -> None:
    if isinstance(value, str):
        for match in _AIRFLOW_TEMPLATE.finditer(value):
            expression = (match.group(1) or match.group(2) or "").strip()
            if not expression.startswith(_DAB_TEMPLATE_PREFIXES):
                raise AgenticContractError(f"Generated payload contains unresolved Airflow Jinja: {match.group(0)}")
    elif isinstance(value, dict):
        for item in value.values():
            _reject_unresolved_templates(item)
    elif isinstance(value, list):
        for item in value:
            _reject_unresolved_templates(item)


def _pipeline_list(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise AgenticContractError("Translation report must be a JSON object")
    pipelines = payload.get("pipelines") if "pipelines" in payload else [payload]
    if not isinstance(pipelines, list) or not pipelines or not all(isinstance(item, dict) for item in pipelines):
        raise AgenticContractError("Translation report does not contain canonical pipelines")
    return pipelines


def _source_files(source_path: Path) -> list[tuple[str, Path]]:
    paths = discover_dags(source_path)
    root = source_path.parent if source_path.is_file() else source_path
    return sorted((path.resolve().relative_to(root.resolve()).as_posix(), path.resolve()) for path in paths)


def _current_source_hashes(source_path: Path) -> dict[str, str]:
    if not source_path.exists():
        return {}
    return {relative: _sha256_file(path) for relative, path in _source_files(source_path)}


def _manifest_source_hashes(manifest: dict[str, Any]) -> dict[str, str]:
    return {str(item["path"]): str(item["sha256"]) for item in manifest.get("source_files") or []}


def _snapshot_source(workspace: Path, manifest: dict[str, Any]) -> Path:
    snapshot = workspace / "source"
    if manifest.get("source_kind") == "file":
        files = manifest.get("source_files") or []
        if len(files) != 1:
            raise AgenticContractError("Prepared single-file source manifest is invalid")
        return snapshot / files[0]["path"]
    return snapshot


def _verify_snapshot(workspace: Path, manifest: dict[str, Any]) -> None:
    snapshot = workspace / "source"
    actual = {
        str(item["path"]): _sha256_file(snapshot / str(item["path"])) for item in manifest.get("source_files") or []
    }
    if actual != _manifest_source_hashes(manifest):
        raise AgenticContractError("The prepared Airflow source snapshot was modified after prepare")


def _load_workspace(workspace: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not workspace.is_dir():
        raise AgenticContractError(f"Agentic workspace not found: {workspace}; run prepare first")
    manifest = _read_json_object(workspace / "manifest.json")
    if manifest.get("contract_version") != CONTRACT_VERSION or manifest.get("source") != "airflow":
        raise AgenticContractError("Agentic workspace has an unsupported contract or source")
    expected_provider = {
        "name": PROVIDER_NAME,
        "version": PROVIDER_VERSION,
        "repository": PROVIDER_REPOSITORY,
    }
    if manifest.get("provider") != expected_provider:
        raise AgenticContractError(f"Agentic workspace provider must be pinned to {PROVIDER_NAME} v{PROVIDER_VERSION}")
    gaps_bytes = (workspace / "gaps.json").read_bytes()
    if _sha256_bytes(gaps_bytes) != manifest.get("gaps_sha256"):
        raise AgenticContractError("Prepared GapEnvelope file was modified after prepare")
    gaps = json.loads(gaps_bytes)
    if not isinstance(gaps, list):
        raise AgenticContractError("Prepared gaps.json must be a list")
    return manifest, gaps


def _load_candidate_index(workspace: Path, *, valid_gap_ids: set[str]) -> dict[str, dict[str, str]]:
    index = _read_json_object(workspace / "candidate_index.json")
    validated: dict[str, dict[str, str]] = {}
    for gap_id, entry in index.items():
        if gap_id not in valid_gap_ids:
            raise AgenticContractError(f"Candidate index contains an unknown gap_id: {gap_id!r}")
        if not isinstance(entry, dict) or set(entry) != {"sha256", "status"}:
            raise AgenticContractError(f"Candidate index entry is invalid for gap: {gap_id}")
        digest = entry.get("sha256")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise AgenticContractError(f"Candidate index sha256 is invalid for gap: {gap_id}")
        status = entry.get("status")
        if not isinstance(status, str) or status not in _RESOLUTION_STATUSES:
            raise AgenticContractError(f"Candidate index status is invalid for gap: {gap_id}")
        validated[gap_id] = {"sha256": digest, "status": status}
    return validated


def _workspace(output_dir: Path) -> Path:
    return output_dir.resolve() / ".work" / "agentic"


def _safe_relative_path(value: str) -> bool:
    path = Path(value)
    return bool(value) and not path.is_absolute() and ".." not in path.parts and value == path.as_posix()


def _call_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_json_bytes(value))


def _write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(_json_bytes(value))
    temporary.replace(path)


def _read_json_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AgenticContractError(f"Expected a JSON object in {path}")
    return value


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()
