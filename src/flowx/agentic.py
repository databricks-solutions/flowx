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
PROVIDER_VERSION = "0.2.2"
PROVIDER_REPOSITORY = "https://github.com/park-peter/airflow-to-dabs"

_ALLOWED_REPLACEMENT_KINDS = ("notebook", "sql", "spark_python")
_RESOLUTION_STATUSES = {"resolved", "needs_input", "deferred"}
_DISPOSITIONS = {"consumed", "preserved_by_flowx", "ignored", "needs_input"}
_MAX_GENERATED_FILE_BYTES = 1024 * 1024
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
}
_NESTED_TASK_FIELDS = ("inner_activities", "if_true_activities", "if_false_activities", "default_activities")
_AIRFLOW_TEMPLATE = re.compile(r"{{\s*([^{}]+?)\s*}}|{%\s*([^{}]+?)\s*%}")
_DAB_TEMPLATE_PREFIXES = ("job.", "tasks.", "input.", "backfill.")
_NOTEBOOK_PARAMETER_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]*\Z")
_RESERVED_NOTEBOOK_PARAMETER_KEYS = frozenset(
    {
        *_COMMON_TASK_FIELDS,
        "condition_task",
        "dbt_task",
        "disable_auto_optimization",
        "email_notifications",
        "environment_key",
        "for_each_task",
        "job_cluster_key",
        "new_cluster",
        "notebook_task",
        "notification_settings",
        "pipeline_task",
        "python_wheel_task",
        "retry_on_timeout",
        "run_if",
        "run_job_task",
        "spark_jar_task",
        "spark_python_task",
        "sql_task",
        "webhook_notifications",
    }
)
_UNSET = object()


class AgenticContractError(ValueError):
    """Raised when an agentic workspace or resolution violates the contract."""


@dataclass(frozen=True, slots=True, kw_only=True)
class GapEnvelope:
    """Versioned context for one source-reconciled leaf placeholder."""

    gap_id: str
    pipeline_name: str
    dag_capture_identity: str
    capture_identity: str
    task_key: str
    task_path: list[str | int]
    operator: str
    source_file: str
    source_sha256: str
    baseline_report_sha256: str
    task_sha256: str
    graph_sha256: str
    provider_sha256: str
    finding_fingerprints: list[str]
    source_span: dict[str, int]
    raw_definition: dict[str, Any]
    arguments: list[dict[str, Any]]
    upstream_task_keys: list[str]
    downstream_task_keys: list[str]
    dag_settings: dict[str, Any]
    reason: dict[str, str]

    def as_dict(self) -> dict[str, Any]:
        """Returns the public GapEnvelope v1 representation."""
        provider = {
            "name": PROVIDER_NAME,
            "version": PROVIDER_VERSION,
            "repository": PROVIDER_REPOSITORY,
        }
        payload = {
            "contract_version": CONTRACT_VERSION,
            "gap_id": self.gap_id,
            "source": "airflow",
            "pipeline_name": self.pipeline_name,
            "dag_capture_identity": self.dag_capture_identity,
            "capture_identity": self.capture_identity,
            "task_key": self.task_key,
            "task_path": self.task_path,
            "operator": self.operator,
            "operator_fqn": self.operator,
            "source_file": self.source_file,
            "source_sha256": self.source_sha256,
            "baseline_report_sha256": self.baseline_report_sha256,
            "task_sha256": self.task_sha256,
            "graph_sha256": self.graph_sha256,
            "provider_sha256": self.provider_sha256,
            "finding_fingerprints": self.finding_fingerprints,
            "source_span": self.source_span,
            "raw_definition": self.raw_definition,
            "arguments": self.arguments,
            "upstream_task_keys": self.upstream_task_keys,
            "downstream_task_keys": self.downstream_task_keys,
            "dag_settings": self.dag_settings,
            "reason": self.reason,
            "allowed_replacement_kinds": list(_ALLOWED_REPLACEMENT_KINDS),
            "knowledge_provider": provider,
        }
        payload["request_sha256"] = _sha256_bytes(_json_bytes(payload))
        return payload


@dataclass(frozen=True, slots=True, kw_only=True)
class StagedResolution:
    """A schema-validated resolution bound to one GapEnvelope."""

    gap: dict[str, Any]
    candidate: dict[str, Any]
    sha256: str


@dataclass(frozen=True, slots=True, kw_only=True)
class PersistedResolutionEvidence:
    """Validated kept evidence for one reviewed Airflow resolution run."""

    provider_version: str
    gaps: list[dict[str, Any]]
    resolutions: list[StagedResolution]
    reviewed_resolutions: list[StagedResolution]
    decisions: list[dict[str, str]]
    expected_report: dict[str, Any]


def prepare_airflow_resolutions(
    *,
    source_path: Path,
    report_path: Path,
    output_dir: Path,
    dbt_mode: str = "static",
    gap_id: str | None = None,
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
    provider_source = _provider_context_path()
    provider_sha256 = _directory_sha256(provider_source)

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

        gaps = _build_gap_envelopes(
            baseline,
            baseline_hash=baseline_hash,
            source_hashes=source_hashes,
            provider_sha256=provider_sha256,
        )
        if not gaps:
            raise AgenticContractError("The deterministic report contains no eligible Airflow leaf gaps")
        if gap_id is not None and gap_id not in {gap["gap_id"] for gap in gaps}:
            raise AgenticContractError(f"No eligible Airflow gap matches --gap-id {gap_id!r}")

        (staging / "baseline.json").write_bytes(baseline_bytes)
        gaps_bytes = _json_bytes(gaps)
        (staging / "gaps.json").write_bytes(gaps_bytes)
        (staging / "candidates").mkdir()
        _copy_provider_context(staging / "provider")
        if _directory_sha256(staging / "provider") != provider_sha256:
            raise AgenticContractError("Pinned provider context changed while the agentic workspace was prepared")
        _write_json(staging / "candidate_index.json", {})
        manifest = {
            "contract_version": CONTRACT_VERSION,
            "source": "airflow",
            "provider": {**_provider_identity(), "sha256": provider_sha256},
            "source_path": str(source_path),
            "source_kind": "file" if source_path.is_file() else "directory",
            "source_files": [
                {"path": relative, "sha256": source_hashes[relative]} for relative in sorted(source_hashes)
            ],
            "dbt_mode": dbt_mode,
            "baseline_report_sha256": baseline_hash,
            "gaps_sha256": _sha256_bytes(gaps_bytes),
            "requested_gap_id": gap_id,
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
        "requested_gap_id": gap_id,
        "workspace": str(target),
    }


def stage_airflow_resolutions(
    *,
    output_dir: Path,
    candidate_paths: list[Path],
    replace: bool = False,
) -> dict[str, Any]:
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
        gap_id = str(resolution.gap["gap_id"])
        if gap_id in staged:
            raise AgenticContractError(f"Duplicate candidate supplied for gap_id: {gap_id}")
        staged[gap_id] = resolution

    candidates_dir = workspace / "candidates"
    candidates_dir.mkdir(exist_ok=True)
    index = _load_candidate_index(workspace, valid_gap_ids=set(gap_by_id))
    for gap_id, resolution in staged.items():
        destination = candidates_dir / f"{gap_id}.json"
        existing = index.get(gap_id)
        if existing is not None and existing["sha256"] != resolution.sha256 and not replace:
            raise AgenticContractError(
                f"A different candidate is already staged for gap {gap_id}; use --replace to replace it"
            )
        if existing is not None and destination.exists() and _sha256_file(destination) != existing["sha256"]:
            raise AgenticContractError(f"staged candidate was modified after validation: {gap_id}")
        _write_json_atomic(destination, resolution.candidate)
        index[gap_id] = {
            "sha256": resolution.sha256,
            "status": resolution.candidate["status"],
        }
    _write_json(workspace / "candidate_index.json", index)
    review_manifest_path = _write_review_manifest(workspace, manifest=manifest, index=index)
    return {
        "status": "staged",
        "staged": sorted(staged),
        "candidate_count": len(index),
        "review_manifest": str(review_manifest_path),
        "review": [
            {
                "gap_id": gap_id,
                "status": resolution.candidate["status"],
                "provider": resolution.candidate["provider"],
                "model": resolution.candidate["model"],
                "argument_disposition": resolution.candidate["argument_disposition"],
                "prerequisites": resolution.candidate["prerequisites"],
                "warnings": resolution.candidate["warnings"],
                "semantic_deltas": resolution.candidate["semantic_deltas"],
                "replacement": resolution.candidate.get("replacement"),
                "generated_files": resolution.candidate.get("generated_files", []),
                "reason": resolution.candidate.get("reason"),
                "candidate_sha256": resolution.sha256,
            }
            for gap_id, resolution in sorted(staged.items())
        ],
    }


def apply_airflow_resolutions(
    *,
    output_dir: Path,
    accepted_gap_ids: list[str] | None = None,
    accept_all: bool = False,
    review_complete: bool = False,
    review_manifest_path: Path | None = None,
    reset: bool = False,
    source_path: Path | None = None,
) -> dict[str, Any]:
    """Rebuilds an agentic report from the immutable baseline and the declarative acceptance set."""
    if sum(bool(option) for option in (accepted_gap_ids, accept_all, review_complete, reset)) != 1:
        raise AgenticContractError("Choose exactly one of --accept-gap, --accept-all, --review-complete, or --reset")
    output_dir = output_dir.resolve()
    if reset:
        return _reset_airflow_resolutions(output_dir)

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
    review_manifest: dict[str, Any] | None = None
    review_manifest_bytes: bytes | None = None
    if accept_all or review_complete:
        if review_manifest_path is None:
            raise AgenticContractError("--accept-all and --review-complete require --review-manifest")
        review_manifest, review_manifest_bytes = _load_review_manifest(
            review_manifest_path,
            manifest=manifest,
            index=index,
        )
        if not index:
            raise AgenticContractError("A reviewed operation requires at least one staged candidate")
    elif review_manifest_path is not None:
        raise AgenticContractError("--review-manifest is only valid with --accept-all or --review-complete")

    selected_ids = (
        sorted(index) if accept_all else [] if review_complete else list(dict.fromkeys(accepted_gap_ids or []))
    )
    missing = sorted(set(selected_ids) - set(index))
    if missing:
        raise AgenticContractError(f"No staged candidate exists for gap(s): {', '.join(missing)}")

    reviewed_ids = sorted(index) if review_manifest is not None else selected_ids
    reviewed: dict[str, StagedResolution] = {}
    for gap_id in reviewed_ids:
        candidate_path = workspace / "candidates" / f"{gap_id}.json"
        candidate_bytes = candidate_path.read_bytes()
        if _sha256_bytes(candidate_bytes) != index[gap_id]["sha256"]:
            raise AgenticContractError(f"staged candidate was modified after validation: {gap_id}")
        candidate = json.loads(candidate_bytes)
        reviewed[gap_id] = _validate_candidate(candidate, gap_by_id=gap_by_id, manifest=manifest)

    selected = [reviewed[gap_id] for gap_id in selected_ids]
    decisions = [
        {
            "gap_id": gap_id,
            "candidate_sha256": reviewed[gap_id].sha256,
            "decision": "accepted" if gap_id in selected_ids else "declined",
        }
        for gap_id in reviewed_ids
    ]

    applied = _apply_to_baseline(baseline, selected)
    report_path = output_dir / ".work" / "translation_report.agentic.json"
    _persist_agentic_evidence(
        output_dir=output_dir,
        material_dir=workspace,
        baseline_bytes=baseline_bytes,
        reviewed=list(reviewed.values()),
        selected=selected,
        decisions=decisions,
        review_manifest_bytes=review_manifest_bytes,
    )
    _write_json_atomic(report_path, applied)
    return {
        "status": "review_complete" if review_complete else "applied",
        "accepted_gap_ids": selected_ids,
        "declined_gap_ids": [decision["gap_id"] for decision in decisions if decision["decision"] == "declined"],
        "report_path": str(report_path),
    }


def _reset_airflow_resolutions(output_dir: Path) -> dict[str, Any]:
    """Restores the durable deterministic baseline without consulting mutable live source."""
    workspace = _workspace(output_dir)
    evidence = output_dir / "metadata" / "agentic"
    if workspace.is_dir():
        manifest, _ = _load_workspace(workspace)
        material_dir = workspace
        _verify_snapshot(workspace, manifest)
    elif evidence.is_dir():
        _load_persisted_agentic_evidence(evidence)
        manifest = _read_json_object(evidence / "manifest.json")
        material_dir = evidence
    else:
        raise AgenticContractError("No durable agentic baseline exists; run prepare first")

    baseline_bytes = (material_dir / "baseline.json").read_bytes()
    if _sha256_bytes(baseline_bytes) != manifest.get("baseline_report_sha256"):
        raise AgenticContractError("The immutable deterministic baseline was modified after prepare")
    baseline = json.loads(baseline_bytes)
    _require_airflow_baseline(baseline)
    _persist_agentic_evidence(
        output_dir=output_dir,
        material_dir=material_dir,
        baseline_bytes=baseline_bytes,
        reviewed=[],
        selected=[],
        decisions=[],
        review_manifest_bytes=None,
    )

    if workspace.is_dir():
        candidates = workspace / "candidates"
        if candidates.exists():
            shutil.rmtree(candidates)
        candidates.mkdir()
        _write_json(workspace / "candidate_index.json", {})
        review_manifests = workspace / "review_manifests"
        if review_manifests.exists():
            shutil.rmtree(review_manifests)

    report_path = output_dir / ".work" / "translation_report.agentic.json"
    _write_json_atomic(report_path, baseline)
    return {"status": "reset", "accepted_gap_ids": [], "declined_gap_ids": [], "report_path": str(report_path)}


def _persist_agentic_evidence(
    *,
    output_dir: Path,
    material_dir: Path,
    baseline_bytes: bytes,
    reviewed: list[StagedResolution],
    selected: list[StagedResolution],
    decisions: list[dict[str, str]],
    review_manifest_bytes: bytes | None,
) -> None:
    """Keeps replayable source, baseline, candidates, and review decisions outside transient work state."""
    metadata_dir = output_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    target = metadata_dir / "agentic"
    with tempfile.TemporaryDirectory(prefix=".agentic-evidence-", dir=metadata_dir) as temporary:
        staging = Path(temporary) / "agentic"
        staging.mkdir()
        (staging / "baseline.json").write_bytes(baseline_bytes)
        (staging / "gaps.json").write_bytes((material_dir / "gaps.json").read_bytes())
        (staging / "manifest.json").write_bytes((material_dir / "manifest.json").read_bytes())
        shutil.copytree(material_dir / "source", staging / "source")
        _write_json(
            staging / "reviewed_candidates.json",
            {"contract_version": CONTRACT_VERSION, "candidates": [item.candidate for item in reviewed]},
        )
        _write_json(
            staging / "accepted_resolutions.json",
            {"contract_version": CONTRACT_VERSION, "candidates": [item.candidate for item in selected]},
        )
        _write_json(
            staging / "review_decisions.json",
            {"contract_version": CONTRACT_VERSION, "decisions": decisions},
        )
        if review_manifest_bytes is not None:
            (staging / "review_manifest.json").write_bytes(review_manifest_bytes)
        if target.exists():
            shutil.rmtree(target)
        shutil.move(str(staging), target)


def _review_manifest_payload(manifest: dict[str, Any], index: dict[str, dict[str, str]]) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "source": "airflow",
        "baseline_report_sha256": manifest["baseline_report_sha256"],
        "gaps_sha256": manifest["gaps_sha256"],
        "provider": manifest["provider"],
        "candidates": [
            {"gap_id": gap_id, "sha256": index[gap_id]["sha256"], "status": index[gap_id]["status"]}
            for gap_id in sorted(index)
        ],
    }


def _write_review_manifest(
    workspace: Path,
    *,
    manifest: dict[str, Any],
    index: dict[str, dict[str, str]],
) -> Path:
    payload = _review_manifest_payload(manifest, index)
    payload_bytes = _json_bytes(payload)
    digest = _sha256_bytes(payload_bytes)
    path = workspace / "review_manifests" / f"{digest}.json"
    if path.exists() and path.read_bytes() != payload_bytes:
        raise AgenticContractError("Hash-addressed review manifest contains different content")
    path.parent.mkdir(exist_ok=True)
    path.write_bytes(payload_bytes)
    return path


def _load_review_manifest(
    path: Path,
    *,
    manifest: dict[str, Any],
    index: dict[str, dict[str, str]],
) -> tuple[dict[str, Any], bytes]:
    try:
        payload_bytes = path.read_bytes()
        payload = json.loads(payload_bytes)
    except OSError as error:
        raise AgenticContractError(f"Could not read review manifest: {error}") from error
    except json.JSONDecodeError as error:
        raise AgenticContractError(f"Review manifest contains invalid JSON: {error}") from error
    expected = _review_manifest_payload(manifest, index)
    if payload != expected:
        raise AgenticContractError("Review manifest does not exactly match the currently staged candidate set")
    return expected, payload_bytes


def validate_persisted_agentic_report(report: dict[str, Any], *, evidence_dir: Path) -> list[str]:
    """Replays accepted candidates from kept evidence and compares the exact expected report."""
    try:
        evidence = _load_persisted_agentic_evidence(evidence_dir)
    except AgenticContractError as error:
        return [f"agentic resolution evidence is missing or invalid: {error}"]
    if evidence.expected_report != report:
        return ["agentic report does not match replay from its immutable baseline and accepted resolutions"]
    return []


def summarize_persisted_agentic_resolutions(evidence_dir: Path) -> dict[str, Any]:
    """Returns validated per-pipeline outcomes from kept Airflow resolution evidence.

    An absent evidence directory represents a deterministic-only run. Once evidence exists, every
    file is contract- and hash-validated before any reporting metric may consume it.
    """
    if not evidence_dir.exists():
        return {}
    evidence = _load_persisted_agentic_evidence(evidence_dir)
    pipeline_outcomes: dict[str, dict[str, int]] = {}
    for pipeline in _pipeline_list(evidence.expected_report):
        name = str(pipeline["name"])
        audit = pipeline.get("audit") or {}
        outcomes = _empty_resolution_outcomes()
        outcomes["unreviewed"] = int(audit.get("agentic_count", 0))
        pipeline_outcomes[name] = outcomes
    for resolution in evidence.resolutions:
        pipeline_name = str(resolution.gap["pipeline_name"])
        outcomes = pipeline_outcomes.setdefault(pipeline_name, _empty_resolution_outcomes())
        if outcomes["unreviewed"] <= 0:
            raise AgenticContractError(f"agentic resolution over-accounts pipeline {pipeline_name!r}")
        outcomes["unreviewed"] -= 1
        outcomes[str(resolution.candidate["status"])] += 1
    accepted_ids = {resolution.gap["gap_id"] for resolution in evidence.resolutions}
    for decision in evidence.decisions:
        if decision["decision"] != "declined" or decision["gap_id"] in accepted_ids:
            continue
        resolution = next(item for item in evidence.reviewed_resolutions if item.gap["gap_id"] == decision["gap_id"])
        pipeline_name = str(resolution.gap["pipeline_name"])
        outcomes = pipeline_outcomes.setdefault(pipeline_name, _empty_resolution_outcomes())
        if outcomes["unreviewed"] <= 0:
            raise AgenticContractError(f"agentic review over-accounts pipeline {pipeline_name!r}")
        outcomes["unreviewed"] -= 1
        outcomes["declined"] += 1
    return {
        "provider_version": evidence.provider_version,
        "pipelines": pipeline_outcomes,
    }


def _load_persisted_agentic_evidence(evidence_dir: Path) -> PersistedResolutionEvidence:
    try:
        baseline_bytes = (evidence_dir / "baseline.json").read_bytes()
        baseline = json.loads(baseline_bytes)
        gaps_bytes = (evidence_dir / "gaps.json").read_bytes()
        gaps = json.loads(gaps_bytes)
        manifest = _read_json_object(evidence_dir / "manifest.json")
        accepted = _read_json_object(evidence_dir / "accepted_resolutions.json")
        reviewed = _read_json_object(evidence_dir / "reviewed_candidates.json")
        decision_manifest = _read_json_object(evidence_dir / "review_decisions.json")
    except (OSError, json.JSONDecodeError, AgenticContractError) as error:
        raise AgenticContractError(str(error)) from error

    _require_airflow_baseline(baseline)
    if _sha256_bytes(baseline_bytes) != manifest.get("baseline_report_sha256"):
        raise AgenticContractError("agentic resolution baseline hash does not match its manifest")
    if _sha256_bytes(gaps_bytes) != manifest.get("gaps_sha256"):
        raise AgenticContractError("agentic gap-envelope hash does not match its manifest")
    if manifest.get("contract_version") != CONTRACT_VERSION or manifest.get("source") != "airflow":
        raise AgenticContractError("agentic resolution manifest has an unsupported contract or source")
    provider_sha256 = _validate_manifest_provider(manifest.get("provider"))
    if accepted.get("contract_version") != CONTRACT_VERSION:
        raise AgenticContractError("accepted_resolutions.json has an unsupported contract_version")
    if reviewed.get("contract_version") != CONTRACT_VERSION:
        raise AgenticContractError("reviewed_candidates.json has an unsupported contract_version")
    if decision_manifest.get("contract_version") != CONTRACT_VERSION:
        raise AgenticContractError("review_decisions.json has an unsupported contract_version")
    if not isinstance(gaps, list):
        raise AgenticContractError("gaps.json must contain a list")

    gap_by_id: dict[str, dict[str, Any]] = {}
    for gap in gaps:
        if not isinstance(gap, dict):
            raise AgenticContractError("every persisted gap must be an object")
        gap_id = gap.get("gap_id")
        pipeline_name = gap.get("pipeline_name")
        if not isinstance(gap_id, str) or not gap_id:
            raise AgenticContractError("every persisted gap requires a non-empty gap_id")
        if not isinstance(pipeline_name, str) or not pipeline_name:
            raise AgenticContractError(f"persisted gap {gap_id!r} requires a non-empty pipeline_name")
        if gap_id in gap_by_id:
            raise AgenticContractError(f"duplicate persisted gap_id: {gap_id}")
        gap_by_id[gap_id] = gap

    try:
        source_hashes = _manifest_source_hashes(manifest)
    except (KeyError, TypeError) as error:
        raise AgenticContractError("agentic resolution manifest has invalid source_files") from error
    if not source_hashes:
        raise AgenticContractError("agentic resolution manifest has no source_files")
    try:
        durable_source_hashes = {
            relative: _sha256_file(evidence_dir / "source" / relative) for relative in source_hashes
        }
    except OSError as error:
        raise AgenticContractError(f"durable Airflow source snapshot is missing: {error}") from error
    if durable_source_hashes != source_hashes:
        raise AgenticContractError("durable Airflow source snapshot does not match its manifest")
    expected_gaps = _build_gap_envelopes(
        baseline,
        baseline_hash=str(manifest["baseline_report_sha256"]),
        source_hashes=source_hashes,
        provider_sha256=provider_sha256,
    )
    if gaps != expected_gaps:
        raise AgenticContractError("persisted gap envelopes do not match the immutable baseline")

    reviewed_candidates = reviewed.get("candidates")
    candidates = accepted.get("candidates")
    if not isinstance(reviewed_candidates, list):
        raise AgenticContractError("reviewed_candidates.json must contain a candidates list")
    if not isinstance(candidates, list):
        raise AgenticContractError("accepted_resolutions.json must contain a candidates list")
    reviewed_resolutions: list[StagedResolution] = []
    reviewed_by_id: dict[str, StagedResolution] = {}
    for candidate in reviewed_candidates:
        resolution = _validate_candidate(candidate, gap_by_id=gap_by_id, manifest=manifest)
        gap_id = str(resolution.gap["gap_id"])
        if gap_id in reviewed_by_id:
            raise AgenticContractError(f"duplicate reviewed resolution for gap_id: {gap_id}")
        reviewed_by_id[gap_id] = resolution
        reviewed_resolutions.append(resolution)

    resolutions: list[StagedResolution] = []
    accepted_ids: set[str] = set()
    for candidate in candidates:
        resolution = _validate_candidate(candidate, gap_by_id=gap_by_id, manifest=manifest)
        gap_id = str(resolution.gap["gap_id"])
        if gap_id in accepted_ids:
            raise AgenticContractError(f"duplicate accepted resolution for gap_id: {gap_id}")
        accepted_ids.add(gap_id)
        resolutions.append(resolution)

    raw_decisions = decision_manifest.get("decisions")
    if not isinstance(raw_decisions, list) or not all(isinstance(item, dict) for item in raw_decisions):
        raise AgenticContractError("review_decisions.json must contain a decisions list")
    decisions: list[dict[str, str]] = []
    decision_ids: set[str] = set()
    for item in raw_decisions:
        if set(item) != {"gap_id", "candidate_sha256", "decision"}:
            raise AgenticContractError("review decision contains unsupported fields")
        gap_id = item.get("gap_id")
        digest = item.get("candidate_sha256")
        decision = item.get("decision")
        if not isinstance(gap_id, str) or gap_id not in reviewed_by_id or gap_id in decision_ids:
            raise AgenticContractError(f"review decision has an invalid gap_id: {gap_id!r}")
        if digest != reviewed_by_id[gap_id].sha256:
            raise AgenticContractError(f"review decision hash does not match candidate: {gap_id}")
        if decision not in {"accepted", "declined"}:
            raise AgenticContractError(f"review decision is invalid for gap: {gap_id}")
        decision_ids.add(gap_id)
        decisions.append({"gap_id": gap_id, "candidate_sha256": str(digest), "decision": str(decision)})
    accepted_decision_ids = {item["gap_id"] for item in decisions if item["decision"] == "accepted"}
    if accepted_ids != accepted_decision_ids:
        raise AgenticContractError("accepted resolutions do not match the durable review decisions")
    if set(reviewed_by_id) != decision_ids:
        raise AgenticContractError("review decisions do not cover every persisted reviewed candidate")

    try:
        expected_report = _apply_to_baseline(baseline, resolutions)
    except AgenticContractError as error:
        raise AgenticContractError(f"agentic resolution evidence failed validation: {error}") from error
    return PersistedResolutionEvidence(
        provider_version=PROVIDER_VERSION,
        gaps=gaps,
        resolutions=resolutions,
        reviewed_resolutions=reviewed_resolutions,
        decisions=decisions,
        expected_report=expected_report,
    )


def _empty_resolution_outcomes() -> dict[str, int]:
    return {"resolved": 0, "needs_input": 0, "deferred": 0, "declined": 0, "unreviewed": 0}


def _require_airflow_baseline(payload: Any) -> None:
    pipelines = _pipeline_list(payload)
    for pipeline in pipelines:
        if not isinstance(pipeline, dict) or (pipeline.get("tags") or {}).get("source") != "airflow":
            raise AgenticContractError("resolve-agentic requires a canonical Airflow translation report")
        status = pipeline.get("reconciliation_status")
        if status not in {"verified", "verified_with_gaps", "excluded"}:
            raise AgenticContractError(
                f"Agentic resolution requires a successfully reconciled deterministic report, got {status!r}"
            )
        audit = pipeline.get("audit")
        if not isinstance(audit, dict) or "audited_activity_count" not in audit or "transformations" not in audit:
            raise AgenticContractError("Airflow deterministic report is missing source-audit metadata")


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
    provider_sha256: str,
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
        pipeline_findings = [finding for finding in pipeline.get("not_translatable") or [] if isinstance(finding, dict)]
        findings: dict[tuple[str | int, ...], dict[str, Any]] = {}
        for finding in pipeline_findings:
            if finding.get("code") != "operator_placeholder":
                continue
            task_path = (finding.get("details") or {}).get("task_path")
            if not isinstance(task_path, list) or not all(isinstance(item, (str, int)) for item in task_path):
                raise AgenticContractError("Operator placeholder finding is missing its captured task path")
            path_key = tuple(task_path)
            if path_key in findings:
                raise AgenticContractError(f"Duplicate operator placeholder finding at task path: {task_path}")
            findings[path_key] = finding
        tasks = list(_walk_tasks(pipeline.get("tasks") or []))
        downstream = _downstream_index([task for _, task in tasks])
        for task_path, task in tasks:
            if task.get("type") != "PlaceholderActivity" or task.get("original_type") == "AirflowSourceSemantics":
                continue
            matched_finding = findings.get(task_path)
            if not matched_finding or not matched_finding.get("fingerprint"):
                raise AgenticContractError(f"Placeholder at task path {list(task_path)} has no bound finding")
            raw_definition = dict(task.get("raw_definition") or {})
            operator = str(raw_definition.get("operator") or task.get("original_type") or "UnknownOperator")
            finding_details = matched_finding.get("details") or {}
            capture_identity = finding_details.get("capture_id")
            if not isinstance(capture_identity, str) or not capture_identity:
                raise AgenticContractError(f"Placeholder at task path {list(task_path)} has no capture identity")
            flowx_owned_arguments = set(_FLOWX_OWNED_ARGUMENTS)
            if task.get("max_retries") is not None:
                flowx_owned_arguments.add("retries")
            if task.get("min_retry_interval_millis") is not None:
                flowx_owned_arguments.add("retry_delay")
            if task.get("timeout_seconds") is not None:
                flowx_owned_arguments.add("execution_timeout")
            related_findings = [
                item for item in pipeline_findings if (item.get("details") or {}).get("capture_id") == capture_identity
            ]
            if not any(item.get("code") == "unsupported_trigger_rule" for item in related_findings):
                flowx_owned_arguments.add("trigger_rule")
            sanitized_definition = _sanitize_raw_definition(raw_definition)
            envelope = GapEnvelope(
                gap_id=str(matched_finding["fingerprint"]),
                pipeline_name=str(pipeline["name"]),
                dag_capture_identity=f"dag:{source_file}:{pipeline['name']}",
                capture_identity=capture_identity,
                task_key=str(task["task_key"]),
                task_path=list(task_path),
                operator=operator,
                source_file=str(source_file),
                source_sha256=source_hash,
                baseline_report_sha256=baseline_hash,
                task_sha256=_sha256_bytes(_json_bytes(task)),
                graph_sha256=_graph_hash(pipeline),
                provider_sha256=provider_sha256,
                finding_fingerprints=sorted(
                    {str(item["fingerprint"]) for item in related_findings if isinstance(item.get("fingerprint"), str)}
                ),
                source_span={
                    key: int(matched_finding.get(key, 0)) for key in ("line", "column", "end_line", "end_column")
                },
                raw_definition=sanitized_definition,
                arguments=_extract_arguments(
                    sanitized_definition,
                    operator=operator,
                    flowx_owned_arguments=flowx_owned_arguments,
                ),
                upstream_task_keys=[str(item.get("task_key")) for item in task.get("depends_on") or []],
                downstream_task_keys=downstream.get(str(task["task_key"]), []),
                dag_settings={
                    "schedule": pipeline.get("schedule"),
                    "timeout_seconds": pipeline.get("timeout_seconds"),
                    "email_notifications": pipeline.get("email_notifications"),
                    "parameters": pipeline.get("parameters"),
                    "tags": pipeline.get("tags"),
                    "description": pipeline.get("description"),
                },
                reason={
                    "code": str(matched_finding.get("code", "operator_placeholder")),
                    "message": str(task.get("comment") or matched_finding.get("message", "")),
                },
            )
            envelopes.append(envelope.as_dict())
    ordered = sorted(envelopes, key=lambda item: (item["pipeline_name"], item["task_path"]))
    gap_ids = [item["gap_id"] for item in ordered]
    if len(gap_ids) != len(set(gap_ids)):
        raise AgenticContractError("Prepared Airflow gaps contain duplicate fingerprints")
    return ordered


def _extract_arguments(
    raw_definition: dict[str, Any],
    *,
    operator: str,
    flowx_owned_arguments: set[str] | None = None,
) -> list[dict[str, Any]]:
    owned = _FLOWX_OWNED_ARGUMENTS if flowx_owned_arguments is None else flowx_owned_arguments
    bound_source = raw_definition.get("bound_source")
    invocation = raw_definition.get("invocation")
    source = (
        bound_source
        if isinstance(bound_source, str) and bound_source.strip()
        else invocation
        if isinstance(invocation, str) and invocation.strip()
        else raw_definition.get("source")
    )
    if operator.startswith("@") and not (isinstance(invocation, str) and invocation.strip()):
        source = None
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
                    arguments.append(_argument(name, expression, owned))
                kwargs_index = 0
                for keyword in call.keywords:
                    keyword_name = keyword.arg
                    if keyword_name is None:
                        keyword_name = f"$kwargs{kwargs_index}"
                        kwargs_index += 1
                    arguments.append(_argument(keyword_name, ast.unparse(keyword.value), owned))
    mapping = raw_definition.get("mapping")
    if isinstance(mapping, str) and mapping:
        arguments.append(_argument("$mapping", mapping, owned))
    return arguments


def _argument(name: str, expression: str, flowx_owned_arguments: set[str]) -> dict[str, Any]:
    preserved = name in flowx_owned_arguments
    argument = {
        "name": name,
        "source_expression": expression,
        "owner": "flowx" if preserved else "provider",
        "preserved_by_flowx": preserved,
    }
    try:
        literal = ast.literal_eval(expression)
    except (ValueError, SyntaxError):
        literal = _UNSET
    if literal is not _UNSET and _is_json_literal(literal):
        argument["normalized_value"] = literal
    return argument


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
        "task_sha256",
        "graph_sha256",
        "provider_sha256",
        "request_sha256",
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
    for field in ("task_sha256", "graph_sha256", "provider_sha256", "request_sha256"):
        if candidate.get(field) != gap.get(field):
            raise AgenticContractError(f"Candidate {field} does not match its GapEnvelope")
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
        if any(item.get("disposition") == "needs_input" for item in candidate["argument_disposition"]):
            raise AgenticContractError("Resolved candidate cannot retain a needs_input argument disposition")
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
    parameters = replacement.get(parameters_field, [] if kind == "spark_python" else {})
    if kind == "spark_python":
        if not isinstance(parameters, list) or not all(isinstance(value, str) for value in parameters):
            raise AgenticContractError("Replacement parameters must be a list of strings for spark_python")
    elif not isinstance(parameters, dict) or not all(
        isinstance(key, str) and isinstance(val, str) for key, val in parameters.items()
    ):
        raise AgenticContractError(f"Replacement {parameters_field} must be a string-to-string object")
    if kind == "notebook":
        _validate_notebook_parameter_keys(parameters)
    files = candidate.get("generated_files")
    if not isinstance(files, list) or len(files) != 1 or not isinstance(files[0], dict):
        raise AgenticContractError("Resolved v1 candidate requires exactly one inline generated file")
    generated = files[0]
    allowed_file_fields = {"path", "language", "content", "sha256"}
    if set(generated) - allowed_file_fields:
        raise AgenticContractError("Generated file contains unsupported fields")
    if generated.get("path") != file_name:
        raise AgenticContractError("Replacement file does not match generated_files.path")
    expected_language = "sql" if kind == "sql" else "python"
    if generated.get("language") != expected_language:
        raise AgenticContractError(f"Generated file language must be {expected_language!r}")
    content = generated.get("content")
    if not isinstance(content, str) or not content.strip():
        raise AgenticContractError("Generated file content must be non-empty")
    if len(content.encode("utf-8")) > _MAX_GENERATED_FILE_BYTES:
        raise AgenticContractError(f"Generated file exceeds the {_MAX_GENERATED_FILE_BYTES}-byte contract limit")
    if generated.get("sha256") != _sha256_bytes(content.encode("utf-8")):
        raise AgenticContractError("Generated file sha256 does not match its content")
    _reject_unresolved_templates(replacement)
    _reject_generated_file_templates(content)
    if kind in {"notebook", "spark_python"}:
        lines = content.splitlines()
        first_line = lines[0] if lines else ""
        if kind == "notebook" and first_line != "# Databricks notebook source":
            raise AgenticContractError("Generated notebook requires the Databricks notebook source marker")
        try:
            module = ast.parse(content)
        except SyntaxError as error:
            raise AgenticContractError(f"Generated notebook is not valid Python: {error}") from error
        _reject_airflow_imports(module)


def _validate_notebook_parameter_keys(parameters: dict[str, str]) -> None:
    for key in parameters:
        if not _NOTEBOOK_PARAMETER_KEY.fullmatch(key):
            raise AgenticContractError(f"Unsafe notebook base_parameters key: {key!r}")
        normalized = key.casefold()
        if normalized.startswith("__flowx") or normalized in _RESERVED_NOTEBOOK_PARAMETER_KEYS:
            raise AgenticContractError(f"Reserved notebook base_parameters key: {key!r}")


def _reject_airflow_imports(module: ast.AST) -> None:
    pending = [module]
    while pending:
        tree = pending.pop()
        nodes = list(ast.walk(tree))
        importlib_names = {"importlib"}
        import_module_names: set[str] = set()
        builtins_names = {"builtins"}
        builtin_import_names = {"__import__"}
        execution_names = {"exec", "eval"}

        for node in nodes:
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if _is_airflow_module(alias.name):
                        raise AgenticContractError("Generated notebook must not import Airflow")
                    if alias.name == "importlib":
                        importlib_names.add(alias.asname or alias.name)
                    elif alias.name == "builtins":
                        builtins_names.add(alias.asname or alias.name)
            elif isinstance(node, ast.ImportFrom):
                if _is_airflow_module(node.module):
                    raise AgenticContractError("Generated notebook must not import Airflow")
                for alias in node.names:
                    bound_name = alias.asname or alias.name
                    if node.module == "importlib" and alias.name == "import_module":
                        import_module_names.add(bound_name)
                    elif node.module == "builtins" and alias.name == "__import__":
                        builtin_import_names.add(bound_name)
                    elif node.module == "builtins" and alias.name in {"exec", "eval"}:
                        execution_names.add(bound_name)

        for node in nodes:
            if not isinstance(node, ast.Call):
                continue
            target = node.func
            literal = _literal_call_argument(node)
            if literal is None:
                continue
            if _is_module_import_call(
                target,
                importlib_names,
                import_module_names,
                builtins_names,
                builtin_import_names,
            ):
                if _is_airflow_module(literal):
                    raise AgenticContractError("Generated notebook must not import Airflow")
                continue
            if _is_execution_call(target, execution_names, builtins_names):
                try:
                    pending.append(ast.parse(literal, mode="exec"))
                except SyntaxError:
                    continue


def _is_airflow_module(value: str | None) -> bool:
    return value == "airflow" or bool(value and value.startswith("airflow."))


def _literal_call_argument(node: ast.Call) -> str | None:
    value: ast.AST | None = node.args[0] if node.args else None
    if value is None:
        for keyword in node.keywords:
            if keyword.arg in {"name", "source", "object"}:
                value = keyword.value
                break
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return value.value
    return None


def _is_module_import_call(
    target: ast.expr,
    importlib_names: set[str],
    import_module_names: set[str],
    builtins_names: set[str],
    builtin_import_names: set[str],
) -> bool:
    if isinstance(target, ast.Name):
        return target.id in import_module_names or target.id in builtin_import_names
    return (
        isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and (
            (target.attr == "import_module" and target.value.id in importlib_names)
            or (target.attr == "__import__" and target.value.id in builtins_names)
        )
    )


def _is_execution_call(target: ast.expr, execution_names: set[str], builtins_names: set[str]) -> bool:
    if isinstance(target, ast.Name):
        return target.id in execution_names
    return (
        isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.value.id in builtins_names
        and target.attr in {"exec", "eval"}
    )


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
    elif replacement["kind"] == "sql":
        task.update({"type": "SqlActivity", "sql": generated["content"], "warehouse_ref": "${var.warehouse_id}"})
        if replacement.get("parameters"):
            task["parameters"] = dict(replacement["parameters"])
    else:
        task.update(
            {
                "type": "SparkPythonActivity",
                "python_file": f"scripts/{placeholder['task_key']}.py",
                "generated_source": generated["content"],
            }
        )
        if replacement.get("parameters"):
            task["parameters"] = list(replacement["parameters"])
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
            if expression != "input" and not expression.startswith(_DAB_TEMPLATE_PREFIXES):
                raise AgenticContractError(f"Generated payload contains unresolved Airflow Jinja: {match.group(0)}")
    elif isinstance(value, dict):
        for item in value.values():
            _reject_unresolved_templates(item)
    elif isinstance(value, list):
        for item in value:
            _reject_unresolved_templates(item)


def _reject_generated_file_templates(content: str) -> None:
    """Rejects templates in uploaded source files, where Jobs cannot interpolate them."""
    for match in _AIRFLOW_TEMPLATE.finditer(content):
        expression = (match.group(1) or match.group(2) or "").strip()
        if expression == "input" or expression.startswith(_DAB_TEMPLATE_PREFIXES):
            raise AgenticContractError(
                "Generated file content cannot contain Databricks dynamic references; "
                "pass them through replacement parameters"
            )
        raise AgenticContractError(f"Generated payload contains unresolved Airflow Jinja: {match.group(0)}")


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
    provider_sha256 = _validate_manifest_provider(manifest.get("provider"))
    if _directory_sha256(workspace / "provider") != provider_sha256:
        raise AgenticContractError("Pinned provider context was modified after prepare")
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


def _copy_provider_context(destination: Path) -> None:
    shutil.copytree(_provider_context_path(), destination)


def _provider_context_path() -> Path:
    source = (
        Path(__file__).resolve().parents[2] / "skills" / "flowx-resolve-airflow-gaps" / "references" / "airflow-to-dabs"
    )
    if not source.is_dir():
        raise AgenticContractError(
            f"provider_unavailable: pinned {PROVIDER_NAME} v{PROVIDER_VERSION} context is missing"
        )
    return source


def _provider_identity() -> dict[str, str]:
    return {"name": PROVIDER_NAME, "version": PROVIDER_VERSION, "repository": PROVIDER_REPOSITORY}


def _validate_manifest_provider(value: Any) -> str:
    if not isinstance(value, dict) or {key: value.get(key) for key in _provider_identity()} != _provider_identity():
        raise AgenticContractError(f"Agentic workspace provider must be pinned to {PROVIDER_NAME} v{PROVIDER_VERSION}")
    sha256 = value.get("sha256")
    if (
        set(value) != {*_provider_identity(), "sha256"}
        or not isinstance(sha256, str)
        or not re.fullmatch(r"[0-9a-f]{64}", sha256)
    ):
        raise AgenticContractError("Agentic workspace provider pin is invalid")
    return sha256


def _directory_sha256(directory: Path) -> str:
    if not directory.is_dir():
        raise AgenticContractError(f"Pinned provider context is missing: {directory}")
    files: list[Path] = []
    for path in directory.rglob("*"):
        if path.is_symlink():
            raise AgenticContractError(f"Pinned provider context cannot contain symlinks: {path}")
        if path.is_file():
            files.append(path)
    if not files:
        raise AgenticContractError("Pinned provider context contains no files")
    digest = hashlib.sha256()
    for path in sorted(files, key=lambda item: item.relative_to(directory).as_posix()):
        relative = path.relative_to(directory).as_posix().encode()
        content = path.read_bytes()
        digest.update(relative)
        digest.update(b"\0")
        digest.update(str(len(content)).encode())
        digest.update(b"\0")
        digest.update(content)
    return digest.hexdigest()


def _safe_relative_path(value: str) -> bool:
    path = Path(value)
    return bool(value) and not path.is_absolute() and ".." not in path.parts and value == path.as_posix()


def _is_json_literal(value: Any) -> bool:
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, list):
        return all(_is_json_literal(item) for item in value)
    if isinstance(value, tuple):
        return all(_is_json_literal(item) for item in value)
    if isinstance(value, dict):
        return all(isinstance(key, str) and _is_json_literal(item) for key, item in value.items())
    return False


_SENSITIVE_ARGUMENT = re.compile(
    r"(?:password|passwd|token|secret|credential|private[_-]?key|access[_-]?key)",
    re.IGNORECASE,
)


class _SecretLiteralRedactor(ast.NodeTransformer):
    def visit_Call(self, node: ast.Call) -> ast.AST:
        self.generic_visit(node)
        for keyword in node.keywords:
            if keyword.arg is not None and _SENSITIVE_ARGUMENT.search(keyword.arg):
                keyword.value = ast.Constant(value="<redacted>")
        return node


def _sanitize_python_source(value: str) -> str:
    try:
        module = ast.parse(textwrap.dedent(value))
    except SyntaxError:
        return re.sub(
            r"(?i)((?:password|passwd|token|secret|credential|private[_-]?key|access[_-]?key)\s*=\s*)"
            r"(['\"]).*?\2",
            r"\1'<redacted>'",
            value,
        )
    redacted = _SecretLiteralRedactor().visit(module)
    ast.fix_missing_locations(redacted)
    return ast.unparse(redacted)


def _sanitize_raw_definition(value: Any, *, key: str = "") -> Any:
    if _SENSITIVE_ARGUMENT.search(key):
        return "<redacted>"
    if isinstance(value, dict):
        return {item_key: _sanitize_raw_definition(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_raw_definition(item) for item in value]
    if isinstance(value, str) and key in {"source", "bound_source", "invocation", "mapping"}:
        return _sanitize_python_source(value)
    return value


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
