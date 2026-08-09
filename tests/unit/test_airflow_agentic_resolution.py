"""Tests for the fingerprint-bound Airflow agentic resolution workflow."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from flowx.adapter.__main__ import main as adapter_main
from flowx.agentic import AgenticContractError, _validate_candidate, summarize_persisted_agentic_resolutions
from flowx.bundler.dab_writer import main as package_main
from flowx.reporting.coverage import build_coverage_rows
from flowx.sources.airflow.convert import main as airflow_convert


def _write_source(tmp_path: Path, *, two_tasks: bool = False) -> Path:
    source = tmp_path / "dag.py"
    second = (
        "    second = KubernetesPodOperator(task_id='second', image='python:3.12')\n    pod >> second\n"
        if two_tasks
        else ""
    )
    source.write_text(
        "from airflow import DAG\n"
        "with DAG(dag_id='agentic') as dag:\n"
        "    pod = KubernetesPodOperator(task_id='pod', image='python:3.11', retries=2)\n"
        f"{second}",
        encoding="utf-8",
    )
    return source


def _prepare(tmp_path: Path, *, two_tasks: bool = False) -> tuple[Path, Path, dict]:
    source = _write_source(tmp_path, two_tasks=two_tasks)
    output = tmp_path / "output"
    assert airflow_convert(["--source-dir", str(source), "--output-dir", str(output)]) == 0
    report = output / ".work" / "translation_report.json"
    original = report.read_bytes()

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "prepare",
                "--source",
                "airflow",
                "--source-path",
                str(source),
                "--report",
                str(report),
                "--output-dir",
                str(output),
            ]
        )
        == 0
    )
    assert report.read_bytes() == original
    gaps = json.loads((output / ".work" / "agentic" / "gaps.json").read_text(encoding="utf-8"))
    return source, output, gaps


def _candidate(gap: dict, *, source: str = "print('Migrated from Airflow')\n", status: str = "resolved") -> dict:
    generated_file = {
        "path": "task.py",
        "language": "python",
        "content": source,
        "sha256": hashlib.sha256(source.encode("utf-8")).hexdigest(),
    }
    dispositions = [
        {
            "name": argument["name"],
            "disposition": "preserved_by_flowx" if argument["preserved_by_flowx"] else "consumed",
            "rationale": "Flowx preserves task policy." if argument["preserved_by_flowx"] else "Used by notebook code.",
        }
        for argument in gap["arguments"]
    ]
    candidate = {
        "contract_version": "1",
        "gap_id": gap["gap_id"],
        "status": status,
        "baseline_report_sha256": gap["baseline_report_sha256"],
        "source_sha256": gap["source_sha256"],
        "provider": {
            "name": "airflow-to-dabs",
            "version": "0.2.0",
            "repository": "https://github.com/park-peter/airflow-to-dabs",
        },
        "model": {"name": "test-model"},
        "argument_disposition": dispositions,
        "prerequisites": [],
        "warnings": [],
        "semantic_deltas": [],
    }
    if status == "resolved":
        candidate["replacement"] = {"kind": "notebook", "file": "task.py"}
        candidate["generated_files"] = [generated_file]
    else:
        candidate["reason"] = "More deployment information is required."
    return candidate


def _stage(output: Path, candidate: dict, *, name: str = "candidate.json") -> int:
    candidate_path = output / name
    candidate_path.write_text(json.dumps(candidate, indent=2), encoding="utf-8")
    return adapter_main(
        [
            "resolve-agentic",
            "stage",
            "--source",
            "airflow",
            "--output-dir",
            str(output),
            "--candidate",
            str(candidate_path),
        ]
    )


def _load_tasks(report: Path) -> dict[str, dict]:
    payload = json.loads(report.read_text(encoding="utf-8"))
    pipeline = payload["pipelines"][0] if "pipelines" in payload else payload
    return {task["task_key"]: task for task in pipeline["tasks"]}


def test_prepare_writes_versioned_fingerprint_bound_gap_without_changing_report(tmp_path: Path):
    source, output, gaps = _prepare(tmp_path)

    assert len(gaps) == 1
    gap = gaps[0]
    assert gap["contract_version"] == "1"
    assert gap["gap_id"]
    assert gap["pipeline_name"] == "agentic"
    assert gap["task_key"] == "pod"
    assert gap["operator"] == "KubernetesPodOperator"
    assert gap["source_sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert {argument["name"] for argument in gap["arguments"]} == {"task_id", "image", "retries"}
    assert (output / ".work" / "agentic" / "baseline.json").exists()
    assert (output / ".work" / "agentic" / "source" / "dag.py").read_bytes() == source.read_bytes()


def test_resolve_agentic_is_explicitly_airflow_only(tmp_path: Path, capsys):
    exit_code = adapter_main(
        [
            "resolve-agentic",
            "prepare",
            "--source",
            "adf",
            "--source-path",
            str(tmp_path),
            "--report",
            str(tmp_path / "report.json"),
            "--output-dir",
            str(tmp_path / "output"),
        ]
    )

    assert exit_code == 2
    assert "not enabled for ADF; ADF uses the legacy merge path" in capsys.readouterr().err


def test_stage_rejects_graph_identity_fields(tmp_path: Path, capsys):
    _, output, gaps = _prepare(tmp_path)
    candidate = _candidate(gaps[0])
    candidate["replacement"]["task_key"] = "HIJACKED"

    assert _stage(output, candidate) == 1
    assert "replacement contains unsupported fields" in capsys.readouterr().err
    assert not list((output / ".work" / "agentic" / "candidates").glob("*.json"))


def test_stage_rejects_airflow_import_but_allows_airflow_in_comments(tmp_path: Path, capsys):
    _, output, gaps = _prepare(tmp_path)
    bad = _candidate(gaps[0], source="# Airflow provenance\nfrom airflow import DAG\n")

    assert _stage(output, bad) == 1
    assert "must not import Airflow" in capsys.readouterr().err

    good = _candidate(gaps[0], source="# Migrated from Airflow\nprint('ok')\n")
    assert _stage(output, good) == 0


def test_stage_requires_complete_argument_disposition_and_ignored_rationale(tmp_path: Path, capsys):
    _, output, gaps = _prepare(tmp_path)
    missing = _candidate(gaps[0])
    missing["argument_disposition"].pop()

    assert _stage(output, missing) == 1
    assert "argument_disposition must cover every source argument" in capsys.readouterr().err

    ignored = _candidate(gaps[0])
    ignored["argument_disposition"][1] = {
        "name": ignored["argument_disposition"][1]["name"],
        "disposition": "ignored",
        "rationale": "",
    }
    assert _stage(output, ignored) == 1
    assert "ignored argument requires a rationale" in capsys.readouterr().err


def test_stage_rejects_unresolved_jinja_provider_drift_and_file_hash_mismatch(tmp_path: Path, capsys):
    _, output, gaps = _prepare(tmp_path)

    unresolved = _candidate(gaps[0], source="print('{{ ds }}')\n")
    assert _stage(output, unresolved) == 1
    assert "unresolved Airflow Jinja" in capsys.readouterr().err

    wrong_provider = _candidate(gaps[0])
    wrong_provider["provider"]["version"] = "0.1.0"
    assert _stage(output, wrong_provider) == 1
    assert "pinned airflow-to-dabs v0.2.0" in capsys.readouterr().err

    bad_hash = _candidate(gaps[0])
    bad_hash["generated_files"][0]["sha256"] = "0" * 64
    assert _stage(output, bad_hash) == 1
    assert "sha256 does not match" in capsys.readouterr().err


def test_pinned_v020_provider_fixtures_satisfy_the_flowx_contract() -> None:
    root = Path(__file__).parents[2] / "skills" / "flowx-resolve-airflow-gaps" / "references" / "airflow-to-dabs-v0.2.0"
    provider = json.loads((root / "provider.json").read_text(encoding="utf-8"))

    assert provider["provider"]["version"] == "0.2.0"
    for outcome in ("notebook", "sql", "needs-input", "deferred"):
        gap = json.loads((root / "fixtures" / f"gap-{outcome}.json").read_text(encoding="utf-8"))
        candidate = json.loads((root / "fixtures" / f"resolution-{outcome}.json").read_text(encoding="utf-8"))
        manifest = {"baseline_report_sha256": gap["baseline_report_sha256"]}

        resolution = _validate_candidate(candidate, gap_by_id={gap["gap_id"]: gap}, manifest=manifest)

        assert resolution.gap["gap_id"] == gap["gap_id"]


def test_apply_rebuilds_from_baseline_and_preserves_graph_policy(tmp_path: Path):
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0
    baseline_report = output / ".work" / "agentic" / "baseline.json"
    baseline_bytes = baseline_report.read_bytes()
    baseline_task = _load_tasks(baseline_report)["pod"]

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )

    applied_report = output / ".work" / "translation_report.agentic.json"
    applied = json.loads(applied_report.read_text(encoding="utf-8"))
    applied_task = _load_tasks(applied_report)["pod"]
    for field in ("name", "task_key", "depends_on", "max_retries", "timeout_seconds", "min_retry_interval_millis"):
        assert applied_task.get(field) == baseline_task.get(field)
    assert applied_task["type"] == "NotebookActivity"
    assert "Migrated from Airflow" in applied_task["generated_source"]
    assert applied["reconciliation_status"] == "verified_with_reviewed_resolutions"
    assert applied["audit"]["agentic_resolution"]["validation_status"] == "verified"
    assert baseline_report.read_bytes() == baseline_bytes
    assert (output / "metadata" / "agentic" / "accepted_resolutions.json").exists()


def test_apply_supports_sql_leaf_payload(tmp_path: Path):
    _, output, gaps = _prepare(tmp_path)
    candidate = _candidate(gaps[0])
    sql = "SELECT 1 AS resolved\n"
    candidate["replacement"] = {"kind": "sql", "file": "task.sql", "parameters": {}}
    candidate["generated_files"] = [
        {
            "path": "task.sql",
            "language": "sql",
            "content": sql,
            "sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        }
    ]
    assert _stage(output, candidate) == 0

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )

    task = _load_tasks(output / ".work" / "translation_report.agentic.json")["pod"]
    assert task["type"] == "SqlActivity"
    assert task["sql"] == sql
    assert task["warehouse_ref"] == "${var.warehouse_id}"


def test_nested_for_each_resolution_preserves_enclosing_control_flow(tmp_path: Path):
    fixture = Path(__file__).resolve().parents[1] / "resources" / "airflow" / "review_repros" / "a8_classic_mapping.py"
    source = tmp_path / "a8_classic_mapping.py"
    source.write_bytes(fixture.read_bytes())
    output = tmp_path / "output"
    assert airflow_convert(["--source-dir", str(source), "--output-dir", str(output)]) == 0
    report = output / ".work" / "translation_report.json"
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "prepare",
                "--source",
                "airflow",
                "--source-path",
                str(source),
                "--report",
                str(report),
                "--output-dir",
                str(output),
            ]
        )
        == 0
    )
    gaps = json.loads((output / ".work" / "agentic" / "gaps.json").read_text(encoding="utf-8"))
    assert len(gaps) == 1
    assert _stage(output, _candidate(gaps[0], source="print(dbutils.widgets.get('env'))\n")) == 0
    baseline = json.loads((output / ".work" / "agentic" / "baseline.json").read_text(encoding="utf-8"))
    baseline_outer = baseline["tasks"][0]

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )

    applied = json.loads((output / ".work" / "translation_report.agentic.json").read_text(encoding="utf-8"))
    applied_outer = applied["tasks"][0]
    assert applied_outer["type"] == baseline_outer["type"] == "ForEachActivity"
    assert applied_outer["task_key"] == baseline_outer["task_key"]
    assert applied_outer["items_expression"] == baseline_outer["items_expression"]
    assert applied_outer["inner_activities"][0]["type"] == "NotebookActivity"


def test_apply_rejects_staged_candidate_tampering(tmp_path: Path, capsys):
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0
    staged = output / ".work" / "agentic" / "candidates" / f"{gaps[0]['gap_id']}.json"
    payload = json.loads(staged.read_text(encoding="utf-8"))
    payload["replacement"]["kind"] = "sql"
    staged.write_text(json.dumps(payload), encoding="utf-8")

    exit_code = adapter_main(
        [
            "resolve-agentic",
            "apply",
            "--source",
            "airflow",
            "--output-dir",
            str(output),
            "--accept-gap",
            gaps[0]["gap_id"],
        ]
    )

    assert exit_code == 1
    assert "staged candidate was modified after validation" in capsys.readouterr().err
    assert not (output / ".work" / "translation_report.agentic.json").exists()


def test_apply_rejects_malformed_candidate_index(tmp_path: Path, capsys):
    _, output, _ = _prepare(tmp_path)
    index = output / ".work" / "agentic" / "candidate_index.json"
    index.write_text(
        json.dumps({"../../outside": {"sha256": "0" * 64, "status": "resolved"}}),
        encoding="utf-8",
    )

    exit_code = adapter_main(
        ["resolve-agentic", "apply", "--source", "airflow", "--output-dir", str(output), "--reset"]
    )

    assert exit_code == 1
    assert "Candidate index contains an unknown gap_id" in capsys.readouterr().err
    assert not (output / ".work" / "translation_report.agentic.json").exists()


def test_apply_rejects_live_source_changes(tmp_path: Path, capsys):
    source, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0
    source.write_text(source.read_text(encoding="utf-8") + "# changed\n", encoding="utf-8")

    exit_code = adapter_main(
        [
            "resolve-agentic",
            "apply",
            "--source",
            "airflow",
            "--output-dir",
            str(output),
            "--accept-gap",
            gaps[0]["gap_id"],
        ]
    )

    assert exit_code == 1
    assert "source changed since prepare; re-run prepare" in capsys.readouterr().err


def test_reduced_allowlist_restores_unaccepted_placeholder_and_reset_restores_all(tmp_path: Path):
    _, output, gaps = _prepare(tmp_path, two_tasks=True)
    for index, gap in enumerate(gaps):
        assert _stage(output, _candidate(gap), name=f"candidate-{index}.json") == 0

    assert (
        adapter_main(["resolve-agentic", "apply", "--source", "airflow", "--output-dir", str(output), "--accept-all"])
        == 0
    )
    applied_report = output / ".work" / "translation_report.agentic.json"
    assert {task["type"] for task in _load_tasks(applied_report).values()} == {"NotebookActivity"}

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )
    types = {task_key: task["type"] for task_key, task in _load_tasks(applied_report).items()}
    assert types[gaps[0]["task_key"]] == "NotebookActivity"
    assert types[gaps[1]["task_key"]] == "PlaceholderActivity"

    assert (
        adapter_main(["resolve-agentic", "apply", "--source", "airflow", "--output-dir", str(output), "--reset"]) == 0
    )
    assert {task["type"] for task in _load_tasks(applied_report).values()} == {"PlaceholderActivity"}


@pytest.mark.parametrize("status", ["needs_input", "deferred"])
def test_unresolved_outcome_is_terminal_and_keeps_the_linked_placeholder(tmp_path: Path, status: str):
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0], status=status)) == 0

    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )

    report = output / ".work" / "translation_report.agentic.json"
    assert _load_tasks(report)["pod"]["type"] == "PlaceholderActivity"
    payload = json.loads(report.read_text(encoding="utf-8"))
    finding = next(item for item in payload["not_translatable"] if item["fingerprint"] == gaps[0]["gap_id"])
    assert finding["resolution"]["status"] == status


def test_package_accepts_only_flowx_verified_agentic_report(tmp_path: Path):
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )

    report = output / ".work" / "translation_report.agentic.json"
    assert (
        package_main(
            [
                "--report",
                str(report),
                "--output-dir",
                str(output),
                "--no-download-workspace-files",
            ]
        )
        == 0
    )
    assert not (output / ".work").exists()
    assert (output / "metadata" / "agentic" / "accepted_resolutions.json").exists()


def test_package_replays_terminal_needs_input_evidence(tmp_path: Path):
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0], status="needs_input")) == 0
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )
    report = output / ".work" / "translation_report.agentic.json"
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["reconciliation_status"] == "verified_with_gaps"
    accepted = output / "metadata" / "agentic" / "accepted_resolutions.json"
    accepted.write_text(json.dumps({"contract_version": "1", "candidates": []}), encoding="utf-8")
    bundle = tmp_path / "bundle"

    assert package_main(["--report", str(report), "--output-dir", str(bundle)]) == 1
    assert not (bundle / "databricks.yml").exists()


def test_package_rejects_agentic_report_tampering_before_bundle_writes(tmp_path: Path):
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )
    report = output / ".work" / "translation_report.agentic.json"
    payload = json.loads(report.read_text(encoding="utf-8"))
    payload["tasks"][0]["task_key"] = "HIJACKED"
    report.write_text(json.dumps(payload), encoding="utf-8")
    bundle = tmp_path / "bundle"

    assert package_main(["--report", str(report), "--output-dir", str(bundle)]) == 1
    assert not (bundle / "databricks.yml").exists()


def test_reviewed_resolution_evidence_drives_honest_runnable_coverage(tmp_path: Path) -> None:
    _, output, gaps = _prepare(tmp_path, two_tasks=True)
    assert _stage(output, _candidate(gaps[0]), name="resolved.json") == 0
    assert _stage(output, _candidate(gaps[1], status="needs_input"), name="needs-input.json") == 0
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
                "--accept-gap",
                gaps[1]["gap_id"],
            ]
        )
        == 0
    )
    metadata = output / "metadata"
    (metadata / "inventory.json").write_text(
        json.dumps(
            {
                "source": "airflow",
                "pipelines": [
                    {
                        "name": "agentic",
                        "activities": [],
                        "audited_activity_count": 2,
                        "deterministic_count": 0,
                        "agentic_count": 2,
                        "failed_count": 0,
                        "excluded_count": 0,
                        "reconciliation_status": "verified_with_gaps",
                        "migration_status": "included",
                        "findings": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    summary = summarize_persisted_agentic_resolutions(metadata / "agentic")
    row = build_coverage_rows(metadata)[0]

    assert summary == {
        "provider_version": "0.2.0",
        "pipelines": {"agentic": {"resolved": 1, "needs_input": 1, "deferred": 0, "unreviewed": 0}},
    }
    assert row["coverage_pct"] == 100.0
    assert row["deterministic_coverage_pct"] == 0.0
    assert row["runnable_coverage_pct"] == 50.0
    assert row["unresolved_agentic_activities"] == 1
    assert row["agentic_provider_version"] == "0.2.0"
    assert row["reconciliation_status"] == "verified_with_reviewed_resolutions"


def test_reporting_rejects_duplicate_hash_valid_agentic_evidence(tmp_path: Path) -> None:
    _, output, gaps = _prepare(tmp_path)
    assert _stage(output, _candidate(gaps[0])) == 0
    assert (
        adapter_main(
            [
                "resolve-agentic",
                "apply",
                "--source",
                "airflow",
                "--output-dir",
                str(output),
                "--accept-gap",
                gaps[0]["gap_id"],
            ]
        )
        == 0
    )
    evidence = output / "metadata" / "agentic"
    duplicated_gaps = [gaps[0], gaps[0]]
    gaps_bytes = (json.dumps(duplicated_gaps, sort_keys=True, separators=(",", ":")) + "\n").encode()
    (evidence / "gaps.json").write_bytes(gaps_bytes)
    manifest_path = evidence / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["gaps_sha256"] = hashlib.sha256(gaps_bytes).hexdigest()
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    with pytest.raises(AgenticContractError, match="duplicate persisted gap_id"):
        summarize_persisted_agentic_resolutions(evidence)
