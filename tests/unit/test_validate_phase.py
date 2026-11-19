"""Tests for the validate phase: scoring, the loop and its stop reasons, remediation, reports, and the UC table."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from flowx.adapter.__main__ import main as adapter_cli_main
from flowx.models.validation import (
    ValidationFinding,
    ValidationStamp,
    iteration_from_dict,
    report_from_dict,
    to_dict,
)
from flowx.validate.agentic import (
    apply_remediation,
    iteration_path,
    load_iterations,
    load_report,
    prepare_validation,
    record_validation,
    report_path,
    run_dir,
)
from flowx.validate.results_table import (
    VALIDATION_RESULTS_COLUMNS,
    build_insert_sql,
    build_rows,
    write_validation_results,
)
from flowx.validate.scoring import approval_rate, roll_up_job, score_findings, verdict_for

JOB = "pl_orders"
JOB_FILE = f"{JOB}/resources/{JOB}.yml"


def _job(concurrency: int = 20) -> dict[str, Any]:
    return {
        "name": JOB,
        "tasks": [
            {"task_key": "load", "notebook_task": {"notebook_path": "../src/notebooks/load.py"}},
            {
                "task_key": "fan_out",
                "depends_on": [{"task_key": "load"}],
                "for_each_task": {
                    "inputs": "[1, 2]",
                    "concurrency": concurrency,
                    "task": {"task_key": "fan_out_item", "notebook_task": {"notebook_path": "../src/x.py"}},
                },
            },
        ],
    }


def _write_job(output_dir: Path, job: dict[str, Any]) -> None:
    (output_dir / JOB_FILE).write_text(yaml.safe_dump({"resources": {"jobs": {JOB: job}}}), encoding="utf-8")


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    bundle = tmp_path / JOB
    (bundle / "resources").mkdir(parents=True)
    (bundle / "databricks.yml").write_text(f"bundle:\n  name: {JOB}\ninclude:\n- resources/*.yml\n", encoding="utf-8")
    _write_job(tmp_path, _job())
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / f"{JOB}.arm.json").write_text(json.dumps({"name": JOB, "properties": {"activities": []}}))
    (metadata / "configuration.json").write_text(json.dumps({"copy_activity_paradigm": "sdp"}))
    return tmp_path


def _finding(severity: str = "major", **extra: str) -> dict[str, str]:
    return {
        "code": "ORCH-CONCURRENCY",
        "severity": severity,
        "message": "Source batchCount is 5.",
        "suggested_fix": "Set concurrency to 5.",
        **extra,
    }


def _reviews(prepared: dict[str, Any], findings: dict[str, list[dict[str, str]]] | None = None) -> dict[str, Any]:
    findings = findings or {}
    return {
        "reviewer": "claude-opus-5",
        "reviews": [
            {"object_id": target["object_id"], "findings": findings.get(target["object_id"], [])}
            for target in prepared["targets"]
        ],
    }


def _stamp(object_kind: str, verdict: str, score: int) -> ValidationStamp:
    return ValidationStamp(
        object_id="x",
        object_kind=object_kind,
        verdict=verdict,
        score=score,
        reviewer="r",
        ruleset_version="1",
        reviewed_at="t",
        iteration=1,
        content_hash="h",
    )


def test_score_subtracts_weighted_penalties_and_ignores_customizations() -> None:
    findings = [
        ValidationFinding(code="A", severity="major", message="m"),
        ValidationFinding(code="B", severity="minor", message="m"),
        ValidationFinding(code="C", severity="advisory", message="m"),
        ValidationFinding(code="D", severity="major", message="m", customization="copy_activity_paradigm"),
    ]
    assert score_findings(findings) == 75
    assert score_findings([ValidationFinding(code="A", severity="major", message="m")] * 6) == 0


def test_verdict_rejects_blockers_and_low_scores() -> None:
    blocker = ValidationFinding(code="A", severity="blocker", message="m")
    minor = ValidationFinding(code="B", severity="minor", message="m")
    assert verdict_for([blocker], score_findings([blocker])) == "rejected"
    assert verdict_for([minor], 55) == "rejected"
    assert verdict_for([minor], 95) == "approved_with_findings"
    assert verdict_for([], 100) == "approved"


def test_job_rollup_takes_the_lower_score_and_any_rejected_task() -> None:
    assert roll_up_job([], [_stamp("task", "approved", 100), _stamp("task", "approved_with_findings", 80)]) == (
        90,
        "approved_with_findings",
    )
    assert roll_up_job([], [_stamp("task", "rejected", 0), _stamp("task", "approved", 100)]) == (50, "rejected")
    minor = ValidationFinding(code="B", severity="minor", message="m")
    assert roll_up_job([minor], [_stamp("task", "approved", 100)]) == (95, "approved_with_findings")


def test_approval_rate_counts_only_job_stamps_and_rejected_jobs_as_zero() -> None:
    stamps = [_stamp("job", "approved", 100), _stamp("job", "rejected", 70), _stamp("task", "approved", 10)]
    assert approval_rate(stamps) == 0.5
    assert approval_rate([_stamp("task", "approved", 100)]) == 0.0


def test_prepare_returns_job_and_tasks_with_source_and_customizations(output_dir: Path) -> None:
    prepared = prepare_validation(output_dir)
    assert prepared["status"] == "review"
    assert prepared["iteration"] == 1
    assert [target["object_id"] for target in prepared["targets"]] == [JOB, f"{JOB}/load", f"{JOB}/fan_out"]
    assert prepared["sources"][JOB]["name"] == JOB
    assert prepared["customizations"] == {"copy_activity_paradigm": "sdp"}


def test_prepare_reads_the_translation_report_without_task_ir(output_dir: Path) -> None:
    work = output_dir / ".work"
    work.mkdir()
    report = {"pipelines": [{"name": JOB, "migration_status": "complete", "tasks": [{}]}, {"name": "other"}]}
    (work / "translation_report.json").write_text(json.dumps(report))
    assert prepare_validation(output_dir)["translation"] == {JOB: {"name": JOB, "migration_status": "complete"}}


def test_static_violations_short_circuit_review(output_dir: Path) -> None:
    job = _job()
    job["tasks"][1]["task_key"] = "load"
    _write_job(output_dir, job)
    prepared = prepare_validation(output_dir)
    assert prepared["status"] == "static_failed"
    assert prepared["approval_rate"] == 0.0
    assert "targets" not in prepared
    assert not (output_dir / "metadata" / "validation" / "pending.json").exists()
    stamp = load_report(output_dir).stamps[0]
    assert (stamp.object_kind, stamp.verdict, stamp.reviewer) == ("pipeline", "rejected", "flowx-static")
    assert all(finding.severity == "blocker" for finding in stamp.findings)


def test_unparseable_yaml_is_a_static_failure(output_dir: Path) -> None:
    (output_dir / JOB_FILE).write_text("resources: [unclosed", encoding="utf-8")
    prepared = prepare_validation(output_dir)
    assert prepared["status"] == "static_failed"
    assert prepared["static"][0]["code"] == "STATIC-INVALID-YAML"


def test_record_rejects_invalid_reviews_and_writes_nothing(output_dir: Path) -> None:
    prepared = prepare_validation(output_dir)
    reviews = _reviews(prepared)
    reviews["reviews"] = reviews["reviews"][1:] + [{"object_id": "nope", "findings": []}]
    reviews["reviews"][0]["findings"] = [
        _finding(severity="fatal"),
        _finding(suggested_fix=""),
        _finding(customization="unknown_option"),
        _finding(severity="blocker", customization="copy_activity_paradigm"),
    ]
    result = record_validation(output_dir, reviews)
    assert result["ok"] is False
    text = "\n".join(result["violations"])
    for expected in ("severity must be one of", "suggested_fix must be", "unknown_option", "blocker cannot", "nope"):
        assert expected in text
    assert f"{JOB} has no review." in text
    assert load_report(output_dir).iterations == 0


def test_record_refuses_a_bundle_changed_after_prepare(output_dir: Path) -> None:
    prepared = prepare_validation(output_dir)
    _write_job(output_dir, _job(concurrency=3))
    result = record_validation(output_dir, _reviews(prepared))
    assert result == {"ok": False, "violations": ["The bundle changed after prepare; run validate prepare again."]}


def test_report_matches_the_prd_shape_and_round_trips(output_dir: Path) -> None:
    prepared = prepare_validation(output_dir, threshold=0.95)
    record_validation(output_dir, _reviews(prepared, {f"{JOB}/fan_out": [_finding()]}))
    raw = json.loads(report_path(output_dir).read_text())
    for key in ("run_id", "run_by", "ruleset_version", "reviewer", "terminated", "iterations", "approval_rate"):
        assert key in raw
    assert raw["content_hash"].startswith("sha256:")
    stamp = next(stamp for stamp in raw["stamps"] if stamp["object_id"] == f"{JOB}/fan_out")
    assert stamp["verdict"] == "approved_with_findings"
    assert stamp["score"] == 80
    assert stamp["findings"][0]["suggested_fix"] == "Set concurrency to 5."
    for key in ("object_kind", "reviewer", "ruleset_version", "reviewed_at", "iteration", "content_hash"):
        assert key in stamp
    assert to_dict(report_from_dict(raw)) == raw
    iteration = json.loads(iteration_path(output_dir, raw["run_id"], 1).read_text())
    assert to_dict(iteration_from_dict(iteration)) == iteration
    assert raw["approval_rate"] == 0.9
    assert raw["trajectory"] == [
        {"iteration": 1, "approval_rate": 0.9, "content_hash": raw["content_hash"], "terminated": ""}
    ]


def test_threshold_met_stops_the_loop(output_dir: Path) -> None:
    result = record_validation(output_dir, _reviews(prepare_validation(output_dir)))
    assert (result["approval_rate"], result["terminated"]) == (1.0, "threshold_met")
    assert prepare_validation(output_dir)["status"] == "terminated"


def test_budget_exhausted_stops_the_loop(output_dir: Path) -> None:
    prepared = prepare_validation(output_dir, max_iterations=1)
    result = record_validation(output_dir, _reviews(prepared, {f"{JOB}/fan_out": [_finding()] * 2}))
    assert result["terminated"] == "budget_exhausted"


def test_repeated_bundle_hash_stops_with_oscillation(output_dir: Path) -> None:
    findings = {f"{JOB}/fan_out": [_finding()] * 2}
    record_validation(output_dir, _reviews(prepare_validation(output_dir), findings))
    result = record_validation(output_dir, _reviews(prepare_validation(output_dir), findings))
    assert result["terminated"] == "oscillation"


def test_unchanged_approved_objects_are_skipped_and_diffs_are_emitted(output_dir: Path) -> None:
    prepared = prepare_validation(output_dir)
    record_validation(output_dir, _reviews(prepared, {f"{JOB}/fan_out": [_finding()] * 2}))
    fixed = (output_dir / JOB_FILE).read_text().replace("concurrency: 20", "concurrency: 5")
    assert apply_remediation(output_dir, [{"path": JOB_FILE, "content": fixed}])["applied"] is True
    second = prepare_validation(output_dir)
    assert [target["object_id"] for target in second["targets"]] == [JOB, f"{JOB}/fan_out"]
    assert second["skipped"] == [f"{JOB}/load"]
    result = record_validation(output_dir, _reviews(second))
    assert result["terminated"] == "threshold_met"
    assert result["changed_objects"] == [JOB, f"{JOB}/fan_out"]
    first, latest = load_iterations(output_dir)
    assert first.remediation == [JOB_FILE]
    assert "-  concurrency: 20" in latest.diffs[f"{JOB}/fan_out"]
    carried = next(stamp for stamp in latest.stamps if stamp.object_id == f"{JOB}/load")
    assert carried.iteration == 1


def test_dry_run_never_changes_the_bundle(output_dir: Path) -> None:
    before = (output_dir / JOB_FILE).read_bytes()
    prepared = prepare_validation(output_dir, dry_run=True)
    result = record_validation(output_dir, _reviews(prepared, {f"{JOB}/fan_out": [_finding()] * 2}))
    assert result["terminated"] == "dry_run"
    proposed = apply_remediation(output_dir, [{"path": JOB_FILE, "content": "resources: {}\n"}])
    assert (proposed["status"], proposed["applied"]) == ("dry_run", False)
    assert proposed["diffs"][JOB_FILE]
    assert (output_dir / JOB_FILE).read_bytes() == before


def test_approval_gate_holds_edits_until_approved(output_dir: Path) -> None:
    prepared = prepare_validation(output_dir, require_approval=True)
    record_validation(output_dir, _reviews(prepared, {f"{JOB}/fan_out": [_finding()] * 2}))
    edits = [{"path": JOB_FILE, "content": "resources: {}\n"}]
    assert apply_remediation(output_dir, edits)["status"] == "needs_approval"
    assert "fan_out" in (output_dir / JOB_FILE).read_text()
    assert apply_remediation(output_dir, edits, approve=True)["status"] == "applied"
    assert (output_dir / JOB_FILE).read_text() == "resources: {}\n"


@pytest.mark.parametrize("path", [f"{JOB}/src/notebooks/load.py", "metadata/inventory.yml", "../escape.yml"])
def test_apply_only_edits_yaml_inside_a_bundle(output_dir: Path, path: str) -> None:
    record_validation(output_dir, _reviews(prepare_validation(output_dir), {f"{JOB}/fan_out": [_finding()] * 2}))
    result = apply_remediation(output_dir, [{"path": path, "content": "x"}])
    assert result["ok"] is False
    assert "is not a YAML file inside a bundle" in result["error"]


def test_run_resumes_and_reset_keeps_history_and_approvals(output_dir: Path) -> None:
    first = prepare_validation(output_dir, threshold=0.99)
    again = prepare_validation(output_dir, threshold=0.5)
    assert (again["run_id"], again["iteration"], again["settings"]["threshold"]) == (first["run_id"], 1, 0.99)
    record_validation(output_dir, _reviews(again, {f"{JOB}/fan_out": [_finding()]}))
    resumed = prepare_validation(output_dir)
    assert (resumed["run_id"], resumed["iteration"]) == (first["run_id"], 2)
    restarted = prepare_validation(output_dir, reset=True)
    assert restarted["run_id"] != first["run_id"]
    assert restarted["iteration"] == 1
    assert restarted["skipped"] == [f"{JOB}/load"]
    assert (run_dir(output_dir, first["run_id"]) / "validation_report.json").is_file()
    assert iteration_path(output_dir, first["run_id"], 1).is_file()


def test_results_rows_follow_the_prd_columns(output_dir: Path) -> None:
    prepared = prepare_validation(output_dir, threshold=0.99)
    record_validation(output_dir, _reviews(prepared, {f"{JOB}/fan_out": [_finding(), _finding("minor")]}))
    rows = build_rows(load_iterations(output_dir))
    row = next(row for row in rows if row["object_id"] == f"{JOB}/fan_out")
    assert (row["iteration"], row["finding_count"], row["major_count"], row["blocker_count"]) == (1, 2, 1, 0)
    assert len(rows) == 3
    sql = build_insert_sql("c.s.t", rows)
    assert sql.startswith(f"INSERT INTO c.s.t ({', '.join(name for name, _ in VALIDATION_RESULTS_COLUMNS)})")
    assert "CURRENT_TIMESTAMP(), CURRENT_USER()" in sql


def test_write_validation_results_replaces_the_runs_rows(output_dir: Path) -> None:
    record_validation(output_dir, _reviews(prepare_validation(output_dir)))
    statements: list[str] = []

    def execute_statement(statement: str, warehouse_id: str, wait_timeout: str) -> Any:
        statements.append(statement)
        return SimpleNamespace(status=SimpleNamespace(state="SUCCEEDED"))

    client = SimpleNamespace(statement_execution=SimpleNamespace(execute_statement=execute_statement))
    run_id, count = write_validation_results(output_dir, "c.s.t", warehouse_id="w", client=client)
    assert count == 3
    assert statements[0].startswith("CREATE TABLE IF NOT EXISTS c.s.t")
    assert statements[1] == f"DELETE FROM c.s.t WHERE run_id = '{run_id}'"
    assert statements[2].startswith("INSERT INTO c.s.t")


def test_cli_runs_prepare_and_reports_record_violations(output_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert adapter_cli_main(["validate", "prepare", "--output-dir", str(output_dir), "--max-iterations", "2"]) == 0
    assert json.loads(capsys.readouterr().out)["settings"]["max_iterations"] == 2
    reviews = output_dir / "reviews.json"
    reviews.write_text(json.dumps({"reviewer": "m", "reviews": []}))
    assert (
        adapter_cli_main(["validate", "record", "--output-dir", str(output_dir), "--reviews-path", str(reviews)]) == 1
    )
    assert json.loads(capsys.readouterr().out)["ok"] is False
