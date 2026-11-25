"""Tests for the MCP server's agent-driven interactive ``migrate`` flow.

The server returns the full option schema once (``needs_input``); the agent walks the chain locally
and re-calls ``migrate`` once with the complete answers, which applies and packages. These tests
guard that one-shot contract. Skipped where the optional ``mcp`` dependency is absent.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

pytest.importorskip("mcp")

import flowx.agentic as agentic_contract  # noqa: E402
from flowx.mcp import runner, server  # noqa: E402


class _FakeResult:
    def __init__(
        self,
        *,
        ok: bool = True,
        stdout: str = "",
        stderr: str = "",
        returncode: int | None = None,
    ) -> None:
        self.ok = ok
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = (0 if ok else 1) if returncode is None else returncode

    def as_dict(self) -> dict[str, object]:
        return {"returncode": self.returncode, "stdout": self.stdout, "stderr": self.stderr}


_SCHEMA = {
    "pipelines": [
        {
            "pipeline_name": "p",
            "options": [
                {
                    "option_id": "notify_destination",
                    "prompt": "Route notifications?",
                    "rationale": "...",
                    "choices": [{"value": "keep", "label": "Keep", "description": ""}],
                    "free_text": False,
                    "default": "keep",
                    "affected_task_keys": ["load"],
                    "show_when": [],
                },
                {
                    "option_id": "notify_slack_url",
                    "prompt": "Slack URL?",
                    "rationale": "...",
                    "choices": [],
                    "free_text": True,
                    "default": "",
                    "affected_task_keys": ["load"],
                    "show_when": [{"option_id": "notify_destination", "in": ["slack"]}],
                },
            ],
        }
    ]
}


@pytest.fixture
def stub_adapter(monkeypatch):
    """Stubs the adapter subprocess + artifact readers; records which subcommands ran."""
    calls: list[str] = []

    def fake_run_adapter(args):
        calls.append(args[0])
        if args[0] == "inspect":
            return _FakeResult(stdout=json.dumps(_SCHEMA))
        return _FakeResult()

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    monkeypatch.setattr(runner, "summarize_inventory", lambda out: {"pipeline_count": 1})
    monkeypatch.setattr(runner, "summarize_translation", lambda out: {"translated": 1})
    monkeypatch.setattr(runner, "list_tree", lambda out: ["databricks.yml"])
    monkeypatch.setattr(runner, "read_tree", lambda out: {"files": {}, "truncated": []})
    return calls


def test_first_call_returns_full_schema_without_packaging(stub_adapter, tmp_path: Path):
    result = server._cmd_migrate(
        {"source": "adf", "adf_source_path": str(tmp_path / "adf"), "output_dir": str(tmp_path / "out")}
    )
    assert result["status"] == "needs_input"
    # The whole tree (including the conditional slack follow-up) is returned up front.
    option_ids = {o["option_id"] for pipe in result["pending_options"] for o in pipe["options"]}
    assert {"notify_destination", "notify_slack_url"} <= option_ids
    all_options = [o for pipe in result["pending_options"] for o in pipe["options"]]
    slack = next(o for o in all_options if o["option_id"] == "notify_slack_url")
    assert slack["show_when"] == [{"option_id": "notify_destination", "in": ["slack"]}]
    # discover + convert ran, but NOT package (we paused for input).
    assert stub_adapter == ["discover", "convert", "inspect"]


def test_resume_with_answers_applies_and_packages_once(stub_adapter, tmp_path: Path):
    out = tmp_path / "out"
    (out / ".work").mkdir(parents=True)
    (out / ".work" / "translation_report.json").write_text("{}")  # prior convert output -> resume path

    result = server._cmd_migrate(
        {
            "source": "adf",
            "adf_source_path": str(tmp_path / "adf"),
            "output_dir": str(out),
            "answers": ["notify_destination=slack", "notify_slack_url=https://hooks.slack.com/x"],
        }
    )
    assert result["status"] == "completed"
    # Resume skips discover/convert and does not re-inspect; it applies the answers then packages.
    assert stub_adapter == ["modify", "package"]
    assert "apply_answers" in result["steps"] and "package" in result["steps"]


def test_interactive_false_skips_prompt_and_packages(stub_adapter, tmp_path: Path):
    result = server._cmd_migrate(
        {
            "source": "adf",
            "adf_source_path": str(tmp_path / "adf"),
            "output_dir": str(tmp_path / "out"),
            "interactive": False,
        }
    )
    assert result["status"] == "completed"
    assert stub_adapter == ["discover", "convert", "package"]  # no inspect, no pause


def test_failed_inspect_stops_before_package(stub_adapter, monkeypatch, tmp_path: Path):
    def fake_run_adapter(args):
        stub_adapter.append(args[0])
        if args[0] == "inspect":
            return runner.AdapterResult(command=[], returncode=None, stdout="", stderr="Timed out after 1 s.")
        return _FakeResult()

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    result = server._cmd_migrate(
        {"source": "adf", "adf_source_path": str(tmp_path / "adf"), "output_dir": str(tmp_path / "out")}
    )
    assert result["ok"] is False
    assert result["status"] == "failed"
    assert result["failed_phase"] == "inspect"
    assert result["steps"]["inspect"]["process"]["returncode"] is None
    assert stub_adapter == ["discover", "convert", "inspect"]


def _airflow_report(*, operator: str | None = "KubernetesPodOperator") -> dict:
    findings = []
    tasks = []
    if operator is not None:
        findings.append(
            {
                "fingerprint": "gap-1",
                "code": "operator_placeholder",
                "severity": "gap",
                "message": f"{operator} requires explicit migration.",
                "line": 3,
                "column": 4,
                "end_line": 3,
                "end_column": 70,
                "details": {"operator": operator, "task_path": ["tasks", 0]},
            }
        )
        tasks.append({"type": "PlaceholderActivity", "task_key": "work", "original_type": operator})
    else:
        findings.append(
            {
                "fingerprint": "gap-setting",
                "code": "unsupported_dag_setting",
                "severity": "gap",
                "message": "The DAG timetable requires explicit migration.",
                "line": 2,
                "column": 0,
                "end_line": 2,
                "end_column": 40,
                "details": {"name": "timetable"},
            }
        )
        tasks.append(
            {
                "type": "PlaceholderActivity",
                "task_key": "__flowx_source_gaps",
                "original_type": "AirflowSourceSemantics",
            }
        )
    return {
        "pipelines": [
            {
                "name": "example",
                "tags": {"source": "airflow"},
                "reconciliation_status": "verified_with_gaps",
                "tasks": tasks,
                "not_translatable": findings,
                "audit": {"audited_activity_count": 1, "transformations": []},
            }
        ]
    }


def _gap(operator: str) -> dict:
    normalized_operator = operator.casefold().replace("_", "")
    is_structural = any(marker in normalized_operator for marker in ("branch", "shortcircuit", "taskgroup", "subdag"))
    return {
        "contract_version": "1",
        "gap_id": "gap-1",
        "pipeline_name": "example",
        "operator": operator,
        "finding_fingerprints": ["gap-1"],
        "required_capability": "graph_patch" if is_structural else None,
        "allowed_replacement_kinds": [] if is_structural else ["notebook", "sql", "spark_python"],
    }


def test_airflow_gap_state_never_advertises_contract_restricted_gap_as_leaf_eligible() -> None:
    gap = {**_gap("CustomOperator"), "allowed_replacement_kinds": []}

    state = agentic_contract.classify_airflow_gaps(_airflow_report(operator="CustomOperator"), [gap])

    assert state["eligible_gaps"] == []
    assert state["structural_gaps"][0]["required_capability"] == "non_leaf_patch"


def test_airflow_gap_state_trusts_hash_bound_leaf_eligibility_over_operator_heuristics() -> None:
    gap = {**_gap("BranchPythonOperator"), "allowed_replacement_kinds": ["notebook"]}

    state = agentic_contract.classify_airflow_gaps(_airflow_report(operator="BranchPythonOperator"), [gap])

    assert state["eligible_gaps"] == [gap]
    assert state["structural_gaps"] == []


def test_unprepared_branch_finding_has_no_stageable_envelope():
    state = agentic_contract.classify_airflow_gaps(
        _airflow_report(operator="BranchPythonOperator"), [], prepare_unavailable="file limit"
    )
    assert not state["eligible_gaps"]
    (gap,) = state["structural_gaps"]
    assert gap["required_capability"] == "graph_patch"
    assert gap["prepare_unavailable"] == "file limit" and "request_sha256" not in gap


def test_airflow_gap_state_fails_closed_when_leaf_eligibility_is_missing() -> None:
    gap = _gap("CustomOperator")
    del gap["allowed_replacement_kinds"]

    state = agentic_contract.classify_airflow_gaps(_airflow_report(operator="CustomOperator"), [gap])

    assert state["eligible_gaps"] == []
    assert state["structural_gaps"][0]["required_capability"] == "non_leaf_patch"


def test_resolve_prepare_gap_id_classifies_against_the_complete_prepared_set(monkeypatch, tmp_path: Path) -> None:
    output = tmp_path / "out"
    agentic_dir = output / ".work" / "agentic"
    agentic_dir.mkdir(parents=True)
    report = _airflow_report(operator="KubernetesPodOperator")
    second_finding = {
        **report["pipelines"][0]["not_translatable"][0],
        "fingerprint": "gap-2",
        "details": {
            **report["pipelines"][0]["not_translatable"][0]["details"],
            "task_path": ["tasks", 1],
        },
    }
    report["pipelines"][0]["not_translatable"].append(second_finding)
    report["pipelines"][0]["tasks"].append(
        {
            "type": "PlaceholderActivity",
            "task_key": "second",
            "original_type": "KubernetesPodOperator",
        }
    )
    report_path = output / ".work" / "translation_report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    gaps = [
        _gap("KubernetesPodOperator"),
        {
            **_gap("KubernetesPodOperator"),
            "gap_id": "gap-2",
            "finding_fingerprints": ["gap-2"],
        },
    ]
    (agentic_dir / "gaps.json").write_text(json.dumps(gaps), encoding="utf-8")
    source = tmp_path / "dags"
    source.mkdir()
    monkeypatch.setattr(runner, "run_adapter", lambda _args: _FakeResult(stdout="{}"))

    result = server._cmd_resolve_agentic(
        {
            "source": "airflow",
            "action": "prepare",
            "output_dir": str(output),
            "airflow_source_path": str(source),
            "report_path": str(report_path),
            "gap_id": "gap-1",
        }
    )

    assert [gap["gap_id"] for gap in result["gaps"]] == ["gap-1"]
    assert [gap["gap_id"] for gap in result["eligible_gaps"]] == ["gap-1", "gap-2"]
    assert result["unsupported_gaps"] == []


def _stub_airflow_migrate(monkeypatch, output: Path, *, operator: str | None) -> list[list[str]]:
    calls: list[list[str]] = []

    def fake_run_adapter(args):
        argv = [str(value) for value in args]
        calls.append(argv)
        if argv[0] == "convert":
            report_path = output / ".work" / "translation_report.json"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(_airflow_report(operator=operator)), encoding="utf-8")
        elif argv[:2] == ["resolve-agentic", "prepare"]:
            agentic_dir = output / ".work" / "agentic"
            agentic_dir.mkdir(parents=True, exist_ok=True)
            (agentic_dir / "gaps.json").write_text(json.dumps([_gap(str(operator))]), encoding="utf-8")
        elif argv[0] == "inspect":
            return _FakeResult(stdout=json.dumps({"pipelines": []}))
        return _FakeResult()

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    monkeypatch.setattr(runner, "summarize_inventory", lambda _out: {"pipeline_count": 1})
    monkeypatch.setattr(runner, "summarize_translation", lambda _out: {"pipelines": 1})
    monkeypatch.setattr(runner, "list_tree", lambda _out: ["databricks.yml"])
    monkeypatch.setattr(runner, "read_tree", lambda _out: {"files": {}})
    return calls


def test_airflow_migrate_pauses_for_leaf_agentic_gaps_even_when_noninteractive(monkeypatch, tmp_path: Path):
    output = tmp_path / "out"
    calls = _stub_airflow_migrate(monkeypatch, output, operator="KubernetesPodOperator")

    result = server._cmd_migrate(
        {
            "source": "airflow",
            "airflow_source_path": str(tmp_path / "dags"),
            "output_dir": str(output),
            "interactive": False,
        }
    )

    assert result["status"] == "needs_agentic_resolution"
    assert [gap["gap_id"] for gap in result["eligible_gaps"]] == ["gap-1"]
    assert result["structural_gaps"] == []
    assert result["unsupported_gaps"] == []
    assert "airflow_volume_path" in result["next_action"]
    assert [argv[0] for argv in calls] == ["discover", "convert", "resolve-agentic"]


@pytest.mark.parametrize(
    ("returncode", "expected_ok", "expected_status"),
    [
        (agentic_contract.AGENTIC_SOURCE_REPRODUCIBILITY_EXIT_CODE, True, "needs_agentic_resolution"),
        (1, False, "failed"),
    ],
)
def test_airflow_migrate_only_degrades_source_reproducibility_failures(
    monkeypatch,
    tmp_path: Path,
    returncode: int,
    expected_ok: bool,
    expected_status: str,
) -> None:
    output = tmp_path / "out"
    calls: list[list[str]] = []

    def fake_run_adapter(args):
        argv = [str(value) for value in args]
        calls.append(argv)
        if argv[0] == "convert":
            report_path = output / ".work" / "translation_report.json"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                json.dumps(_airflow_report(operator="KubernetesPodOperator")),
                encoding="utf-8",
            )
        if argv[:2] == ["resolve-agentic", "prepare"]:
            return _FakeResult(
                ok=False,
                stderr="Agentic resolution unavailable: Airflow source snapshot exceeds 10000 files",
                returncode=returncode,
            )
        return _FakeResult()

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    monkeypatch.setattr(runner, "summarize_inventory", lambda _out: {"pipeline_count": 1})
    monkeypatch.setattr(runner, "summarize_translation", lambda _out: {"pipelines": 1})

    result = server._cmd_migrate(
        {
            "source": "airflow",
            "airflow_source_path": str(tmp_path / "dags"),
            "output_dir": str(output),
            "interactive": False,
        }
    )

    assert result["ok"] is expected_ok
    assert result["status"] == expected_status
    assert not any(argv[0] == "package" for argv in calls)
    if expected_ok:
        assert result["prepare_unavailable"] == (
            "Agentic resolution unavailable: Airflow source snapshot exceeds 10000 files"
        )
        assert "snapshot exceeds 10000 files" in result["next_action"]
        assert "source changed" not in result["next_action"]
        assert result["eligible_gaps"][0]["prepare_unavailable"] == result["prepare_unavailable"]
        assert "request_sha256" not in result["eligible_gaps"][0]
        assert not result["unsupported_gaps"]
    else:
        assert result["failed_phase"] == "prepare_agentic"


def test_airflow_migrate_skips_adf_configuration_prompt_before_agentic_review(monkeypatch, tmp_path: Path):
    output = tmp_path / "out"
    calls = _stub_airflow_migrate(monkeypatch, output, operator="KubernetesPodOperator")

    result = server._cmd_migrate(
        {
            "source": "airflow",
            "airflow_source_path": str(tmp_path / "dags"),
            "output_dir": str(output),
        }
    )

    assert result["status"] == "needs_agentic_resolution"
    assert [argv[0] for argv in calls] == ["discover", "convert", "resolve-agentic"]


def test_airflow_migrate_rejects_adf_configuration_answers(tmp_path: Path):
    result = server._cmd_migrate(
        {
            "source": "airflow",
            "output_dir": str(tmp_path / "out"),
            "answers": ["notify_destination=keep"],
        }
    )

    assert result["ok"] is False
    assert result["status"] == "failed"
    assert "answers" in result["error"]
    assert "ADF" in result["error"]


@pytest.mark.parametrize("operator", ["BranchPythonOperator", "@task.branch", "@task_group"])
def test_airflow_migrate_reports_graph_structural_gaps_as_unsupported(monkeypatch, tmp_path: Path, operator: str):
    output = tmp_path / "out"
    calls = _stub_airflow_migrate(monkeypatch, output, operator=operator)

    result = server._cmd_migrate(
        {
            "source": "airflow",
            "airflow_source_path": str(tmp_path / "dags"),
            "output_dir": str(output),
            "interactive": False,
        }
    )

    assert result["status"] == "needs_agentic_resolution"
    assert result["eligible_gaps"] == []
    assert result["structural_gaps"][0]["gap_id"] == "gap-1"
    assert result["structural_gaps"][0]["required_capability"] == "graph_patch"
    assert not any(argv[0] == "package" for argv in calls)


def test_airflow_migrate_reports_unbound_source_semantics_as_unsupported(monkeypatch, tmp_path: Path):
    output = tmp_path / "out"
    calls = _stub_airflow_migrate(monkeypatch, output, operator=None)

    result = server._cmd_migrate(
        {
            "source": "airflow",
            "airflow_source_path": str(tmp_path / "dags"),
            "output_dir": str(output),
            "interactive": False,
        }
    )

    assert result["status"] == "needs_agentic_resolution"
    assert result["eligible_gaps"] == []
    assert result["structural_gaps"] == []
    assert result["unsupported_gaps"][0]["gap_id"] == "gap-setting"
    assert result["unsupported_gaps"][0]["required_capability"] == "source_semantics"
    assert "package" in result["next_action"]
    assert "allow_unresolved_gaps=true" in result["next_action"]
    assert result["report_path"] in result["next_action"]
    assert not any(argv[0] == "resolve-agentic" for argv in calls)
    assert not any(argv[0] == "package" for argv in calls)


def test_airflow_migrate_without_gaps_packages_normally(monkeypatch, tmp_path: Path):
    output = tmp_path / "out"
    calls: list[list[str]] = []

    def fake_run_adapter(args):
        argv = [str(value) for value in args]
        calls.append(argv)
        if argv[0] == "convert":
            report_path = output / ".work" / "translation_report.json"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                json.dumps(
                    {
                        "pipelines": [
                            {
                                "name": "deterministic",
                                "tags": {"source": "airflow"},
                                "reconciliation_status": "verified",
                                "tasks": [],
                                "not_translatable": [],
                                "audit": {"audited_activity_count": 0, "transformations": []},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
        return _FakeResult()

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    monkeypatch.setattr(runner, "summarize_inventory", lambda _out: {"pipeline_count": 1})
    monkeypatch.setattr(runner, "summarize_translation", lambda _out: {"pipelines": 1})
    monkeypatch.setattr(runner, "list_tree", lambda _out: ["databricks.yml"])
    monkeypatch.setattr(runner, "read_tree", lambda _out: {"files": {}})

    result = server._cmd_migrate(
        {
            "source": "airflow",
            "airflow_source_path": str(tmp_path / "dags"),
            "output_dir": str(output),
        }
    )

    assert result["status"] == "completed"
    assert [argv[0] for argv in calls] == ["discover", "convert", "package"]


def test_package_reports_explicitly_acknowledged_unresolved_airflow_gaps(monkeypatch, tmp_path: Path):
    output = tmp_path / "out"
    report = output / ".work" / "translation_report.json"
    report.parent.mkdir(parents=True)
    report.write_text(json.dumps(_airflow_report(operator="KubernetesPodOperator")), encoding="utf-8")
    calls: list[list[str]] = []

    def package_and_prune(args):
        calls.append([str(value) for value in args])
        report.unlink()
        return _FakeResult(
            stdout='Airflow gap review: {"accepted_count": 0, "reviewed_count": 0, "unreviewed_count": 1}'
        )

    monkeypatch.setattr(runner, "run_adapter", package_and_prune)
    monkeypatch.setattr(runner, "list_tree", lambda _out: ["databricks.yml"])
    monkeypatch.setattr(runner, "read_tree", lambda _out: {"files": {}})

    result = server._cmd_package(
        {
            "output_dir": str(output),
            "report_path": str(report),
            "allow_unresolved_gaps": True,
        }
    )

    assert result["ok"] is True
    assert result["status"] == "packaged_with_unresolved_gaps"
    assert "--allow-unresolved-gaps" in calls[0]


def test_airflow_migrate_resumes_from_reviewed_agentic_report(monkeypatch, tmp_path: Path):
    output = tmp_path / "out"
    report = output / ".work" / "translation_report.agentic.json"
    report.parent.mkdir(parents=True)
    report.write_text(json.dumps(_airflow_report(operator="KubernetesPodOperator")), encoding="utf-8")
    calls: list[list[str]] = []

    def fake_run_adapter(args):
        calls.append([str(value) for value in args])
        report.unlink()
        return _FakeResult(
            stdout='Airflow gap review: {"accepted_count": 1, "reviewed_count": 1, "unreviewed_count": 1}'
        )

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)
    monkeypatch.setattr(runner, "list_tree", lambda _out: ["databricks.yml"])
    monkeypatch.setattr(runner, "read_tree", lambda _out: {"files": {}})
    result = server._cmd_migrate(
        {
            "source": "airflow",
            "output_dir": str(output),
            "resume_agentic": True,
        }
    )

    assert result["status"] == "completed_with_reviewed_gaps"
    assert len(calls) == 1 and calls[0][0] == "package"
    assert calls[0][calls[0].index("--report") + 1] == str(report)


def test_airflow_migrate_resume_is_completed_only_when_reviewed_report_has_no_gaps(monkeypatch, tmp_path: Path):
    output = tmp_path / "out"
    report = output / ".work" / "translation_report.agentic.json"
    report.parent.mkdir(parents=True)
    payload = _airflow_report(operator="KubernetesPodOperator")
    payload["pipelines"][0]["not_translatable"][0]["severity"] = "resolved"
    payload["pipelines"][0]["reconciliation_status"] = "verified_with_reviewed_resolutions"
    report.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(runner, "run_adapter", lambda _args: _FakeResult())
    monkeypatch.setattr(runner, "list_tree", lambda _out: ["databricks.yml"])
    monkeypatch.setattr(runner, "read_tree", lambda _out: {"files": {}})
    result = server._cmd_migrate(
        {
            "source": "airflow",
            "output_dir": str(output),
            "resume_agentic": True,
        }
    )

    assert result["status"] == "completed"


def test_airflow_migrate_resume_requires_reviewed_agentic_report(tmp_path: Path):
    result = server._cmd_migrate(
        {
            "source": "airflow",
            "output_dir": str(tmp_path / "out"),
            "resume_agentic": True,
        }
    )

    assert result["ok"] is False
    assert result["status"] == "failed"
    assert "translation_report.agentic.json" in result["error"]


def test_airflow_migrate_resume_rejects_custom_agentic_report_path(monkeypatch, tmp_path: Path):
    output = tmp_path / "out"
    custom_report = tmp_path / "untrusted.json"
    custom_report.write_text(json.dumps(_airflow_report(operator="KubernetesPodOperator")), encoding="utf-8")
    calls: list[list[str]] = []
    monkeypatch.setattr(runner, "run_adapter", lambda args: calls.append([str(value) for value in args]))

    result = server._cmd_migrate(
        {
            "source": "airflow",
            "output_dir": str(output),
            "resume_agentic": True,
            "agentic_report_path": str(custom_report),
        }
    )

    assert result["ok"] is False
    assert result["status"] == "failed"
    assert "agentic_report_path" in result["error"]
    assert calls == []


def test_airflow_migrate_resume_delegates_agentic_evidence_validation_to_package(monkeypatch, tmp_path: Path):
    output = tmp_path / "out"
    report = output / ".work" / "translation_report.agentic.json"
    report.parent.mkdir(parents=True)
    report.write_text(json.dumps(_airflow_report(operator="KubernetesPodOperator")), encoding="utf-8")
    calls: list[list[str]] = []

    def fail_package(args):
        calls.append([str(value) for value in args])
        return _FakeResult(ok=False, stderr="agentic resolution evidence is invalid")

    monkeypatch.setattr(runner, "run_adapter", fail_package)
    monkeypatch.setattr(runner, "list_tree", lambda _out: [])

    result = server._cmd_migrate(
        {
            "source": "airflow",
            "output_dir": str(output),
            "resume_agentic": True,
        }
    )

    assert result["ok"] is False
    assert result["status"] == "failed"
    assert result["failed_phase"] == "package"
    assert len(calls) == 1 and calls[0][0] == "package"


def test_hosted_inline_airflow_prepare_and_apply_rematerialize_the_same_source(tmp_path: Path):
    definitions = {
        "dag.py": (
            "from airflow import DAG\n"
            "from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator\n"
            "from helpers import ProjectOperator\n"
            "with DAG(dag_id='hosted_apply') as dag:\n"
            "    pod = KubernetesPodOperator(task_id='pod', image='python:3.12')\n"
            "    second = ProjectOperator(task_id='second')\n"
        ),
        "helpers.py": "class ProjectOperator:\n    pass\n",
    }
    output = tmp_path / "out"
    convert_result = server._cmd_convert(
        {
            "source": "airflow",
            "airflow_definitions": definitions,
            "output_dir": str(output),
        }
    )
    assert convert_result["ok"] is True
    report = output / ".work" / "translation_report.json"
    prepare_result = server._cmd_resolve_agentic(
        {
            "source": "airflow",
            "action": "prepare",
            "airflow_definitions": definitions,
            "report_path": str(report),
            "output_dir": str(output),
        }
    )
    assert prepare_result["ok"] is True
    gap = prepare_result["gaps"][0]
    generated_source = "# Databricks notebook source\nprint('resolved')\n"
    candidate = {
        "contract_version": "1",
        "gap_id": gap["gap_id"],
        "status": "resolved",
        "baseline_report_sha256": gap["baseline_report_sha256"],
        "source_sha256": gap["source_sha256"],
        "task_sha256": gap["task_sha256"],
        "graph_sha256": gap["graph_sha256"],
        "provider_sha256": gap["provider_sha256"],
        "request_sha256": gap["request_sha256"],
        "provider": agentic_contract._provider_identity(),
        "model": {"name": "test-model"},
        "argument_disposition": [
            {
                "name": argument["name"],
                "disposition": "preserved_by_flowx" if argument["preserved_by_flowx"] else "consumed",
                "rationale": "Preserved by Flowx." if argument["preserved_by_flowx"] else "Used by the notebook.",
            }
            for argument in gap["arguments"]
        ],
        "prerequisites": [],
        "warnings": [],
        "semantic_deltas": [],
        "replacement": {"kind": "notebook", "file": "task.py"},
        "generated_files": [
            {
                "path": "task.py",
                "language": "python",
                "content": generated_source,
                "sha256": hashlib.sha256(generated_source.encode("utf-8")).hexdigest(),
            }
        ],
    }
    stage_result = server._cmd_resolve_agentic(
        {
            "source": "airflow",
            "action": "stage",
            "candidates": [candidate],
            "output_dir": str(output),
        }
    )
    assert stage_result["ok"] is True
    apply_result = server._cmd_resolve_agentic(
        {
            "source": "airflow",
            "action": "apply",
            "airflow_definitions": definitions,
            "accept_gap": gap["gap_id"],
            "output_dir": str(output),
        }
    )
    assert apply_result["ok"] is True
    assert apply_result["reviewed_count"] == 1
    assert apply_result["unreviewed_count"] == 1
    assert (output / ".work" / "translation_report.agentic.json").is_file()

    packaged = server._cmd_migrate(
        {"source": "airflow", "output_dir": str(output), "resume_agentic": True, "keep_intermediates": True}
    )
    assert (packaged["status"], packaged["reviewed_count"], packaged["unreviewed_count"]) == (
        "completed_with_reviewed_gaps",
        1,
        1,
    )
    assert server._cmd_resolve_agentic(
        {"source": "airflow", "action": "apply", "reset": True, "output_dir": str(output)}
    )["ok"]
    resume = {"source": "airflow", "resume_agentic": True, "output_dir": str(output)}
    rejected = server._cmd_migrate(resume)
    assert (rejected["status"], rejected["reviewed_count"], rejected["unreviewed_count"]) == ("failed", 0, 2)
    acknowledged = server._cmd_migrate({**resume, "allow_unresolved_gaps": True})
    assert (acknowledged["status"], acknowledged["reviewed_count"], acknowledged["unreviewed_count"]) == (
        "packaged_with_unresolved_gaps",
        0,
        2,
    )


def test_hosted_airflow_apply_without_source_reports_missing_input(tmp_path: Path) -> None:
    definitions = {
        "dag.py": (
            "from airflow import DAG\n"
            "from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator\n"
            "with DAG(dag_id='hosted_apply') as dag:\n"
            "    pod = KubernetesPodOperator(task_id='pod', image='python:3.12')\n"
        )
    }
    output = tmp_path / "out"
    assert server._cmd_convert(
        {
            "source": "airflow",
            "airflow_definitions": definitions,
            "output_dir": str(output),
        }
    )["ok"]
    prepare = server._cmd_resolve_agentic(
        {
            "source": "airflow",
            "action": "prepare",
            "airflow_definitions": definitions,
            "report_path": str(output / ".work" / "translation_report.json"),
            "output_dir": str(output),
        }
    )
    assert prepare["ok"] is True

    apply = server._cmd_resolve_agentic(
        {
            "source": "airflow",
            "action": "apply",
            "accept_gap": prepare["gaps"][0]["gap_id"],
            "output_dir": str(output),
        }
    )

    assert apply["ok"] is False
    assert "source input is missing" in apply["process"]["stderr"]
    assert "provide --source-path" in apply["process"]["stderr"]


def test_hosted_migrate_then_prepare_selected_missing_script(tmp_path: Path):
    params = {
        "source": "airflow",
        "output_dir": str(tmp_path / "out"),
        "airflow_definitions": {
            "dag.py": (
                "from airflow import DAG\nfrom airflow.operators.bash import BashOperator\n"
                "with DAG(dag_id='missing'):\n    one = BashOperator(task_id='one', bash_command='./missing.sh')\n"
            )
        },
    }
    paused = server._cmd_migrate(params)
    gap = paused["eligible_gaps"][0]
    prepared = server._cmd_resolve_agentic(
        {**params, "action": "prepare", "report_path": paused["report_path"], "gap_id": gap["gap_id"]}
    )
    assert prepared["ok"] and prepared["gaps"] == [gap]


def test_airflow_migrate_end_to_end_prepares_gap_without_writing_bundle(tmp_path: Path):
    source = tmp_path / "dag.py"
    source.write_text(
        "from airflow import DAG\n"
        "from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator\n"
        "with DAG(dag_id='hosted_agentic') as dag:\n"
        "    pod = KubernetesPodOperator(task_id='pod', image='python:3.12')\n",
        encoding="utf-8",
    )
    output = tmp_path / "out"

    result = server._cmd_migrate(
        {
            "source": "airflow",
            "airflow_source_path": str(source),
            "output_dir": str(output),
            "interactive": False,
        }
    )

    assert result["ok"] is True
    assert result["status"] == "needs_agentic_resolution"
    assert [gap["operator"] for gap in result["eligible_gaps"]] == ["KubernetesPodOperator"]
    assert (output / ".work" / "agentic" / "gaps.json").is_file()
    assert not (output / "databricks.yml").exists()


def test_airflow_migrate_prepares_project_local_custom_operator_gap(tmp_path: Path) -> None:
    source = tmp_path / "dags"
    helper = source / "libs" / "operators.py"
    helper.parent.mkdir(parents=True)
    helper.write_text("class CopyOperator:\n    pass\n", encoding="utf-8")
    (source / "dag.py").write_text(
        "from airflow import DAG\n"
        "from libs.operators import CopyOperator\n"
        "with DAG(dag_id='custom') as dag:\n"
        "    copy = CopyOperator(task_id='copy')\n",
        encoding="utf-8",
    )
    output = tmp_path / "out"

    result = server._cmd_migrate(
        {
            "source": "airflow",
            "airflow_source_path": str(source),
            "output_dir": str(output),
            "interactive": False,
        }
    )

    assert result["ok"] is True
    assert result["status"] == "needs_agentic_resolution"
    assert [gap["operator"] for gap in result["eligible_gaps"]] == ["CopyOperator"]
    assert result["unsupported_gaps"] == []


def test_airflow_migrate_keeps_ssh_gap_eligible_beside_missing_dbt_project(tmp_path: Path) -> None:
    source = tmp_path / "dags"
    source.mkdir()
    (source / "dag.py").write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.providers.ssh.operators.ssh import SSHOperator\n"
        "with DAG(dag_id='dbt_and_ssh') as dag:\n"
        "    dbt = BashOperator(task_id='dbt', bash_command='cd missing_project && dbt run')\n"
        "    ssh = SSHOperator(task_id='ssh', command='run-remote-tool')\n",
        encoding="utf-8",
    )

    result = server._cmd_migrate(
        {
            "source": "airflow",
            "airflow_source_path": str(source),
            "output_dir": str(tmp_path / "out"),
            "interactive": False,
        }
    )

    assert result["ok"] is True
    assert result["status"] == "needs_agentic_resolution"
    assert "prepare_unavailable" not in result
    assert "SSHOperator" in {gap["operator"] for gap in result["eligible_gaps"]}


def test_airflow_migrate_ignores_gaps_from_excluded_dags(tmp_path: Path) -> None:
    source = tmp_path / "dags"
    source.mkdir()
    (source / "clean.py").write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='clean') as dag:\n"
        "    run = BashOperator(task_id='run', bash_command='echo ok')\n",
        encoding="utf-8",
    )
    (source / "excluded.py").write_text(
        "from airflow import DAG\n"
        "from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator\n"
        "with DAG(dag_id='excluded') as dag:\n"
        "    pod = KubernetesPodOperator(task_id='pod', image='python:3.12')\n",
        encoding="utf-8",
    )

    result = server._cmd_migrate(
        {
            "source": "airflow",
            "airflow_source_path": str(source),
            "output_dir": str(tmp_path / "out"),
            "exclude_dag": ["excluded"],
            "interactive": False,
        }
    )

    assert result["ok"] is True
    assert result["status"] == "completed"
    assert "prepare_agentic" not in result["steps"]


def _build_candidate_for_gap(gap: dict[str, object], *, status: str = "resolved") -> dict[str, object]:
    arguments = gap["arguments"]
    assert isinstance(arguments, list)
    dispositions = [
        {
            "name": argument["name"],
            "disposition": "preserved_by_flowx" if argument["preserved_by_flowx"] else "consumed",
            "rationale": "handled in test",
        }
        for argument in arguments
    ]
    candidate: dict[str, object] = {
        "contract_version": "1",
        "gap_id": gap["gap_id"],
        "status": status,
        "baseline_report_sha256": gap["baseline_report_sha256"],
        "source_sha256": gap["source_sha256"],
        "task_sha256": gap["task_sha256"],
        "graph_sha256": gap["graph_sha256"],
        "provider_sha256": gap["provider_sha256"],
        "request_sha256": gap["request_sha256"],
        "provider": gap["knowledge_provider"],
        "model": {"name": "test-model"},
        "argument_disposition": dispositions,
        "prerequisites": [],
        "warnings": [],
        "semantic_deltas": [],
    }
    if status == "resolved":
        content = "# Databricks notebook source\nprint('migrated')\n"
        file_path = f"notebooks/{gap['task_key']}.py"
        candidate["replacement"] = {"kind": "notebook", "file": file_path, "base_parameters": {}}
        candidate["generated_files"] = [
            {
                "path": file_path,
                "language": "python",
                "content": content,
                "sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
            }
        ]
    else:
        candidate["reason"] = "awaiting remote host mapping"
    return candidate


def test_airflow_migrate_accepting_only_ssh_gap_resumes_as_completed(tmp_path: Path) -> None:
    source = tmp_path / "ssh_dag.py"
    source.write_text(
        "from airflow import DAG\n"
        "from airflow.providers.ssh.operators.ssh import SSHOperator\n"
        "with DAG(dag_id='ssh_only') as dag:\n"
        "    run_ssh = SSHOperator(task_id='run_ssh', ssh_conn_id='prod', command='echo hi')\n",
        encoding="utf-8",
    )
    output = tmp_path / "out"

    paused = server._cmd_migrate({"source": "airflow", "airflow_source_path": str(source), "output_dir": str(output)})
    assert paused["status"] == "needs_agentic_resolution"
    (gap,) = paused["eligible_gaps"]

    assert server._cmd_resolve_agentic(
        {
            "source": "airflow",
            "action": "stage",
            "candidates": [_build_candidate_for_gap(gap, status="needs_input")],
            "output_dir": str(output),
        }
    )["ok"]
    assert server._cmd_resolve_agentic(
        {
            "source": "airflow",
            "action": "apply",
            "accept_gap": gap["gap_id"],
            "output_dir": str(output),
        }
    )["ok"]
    unresolved_resume = server._cmd_migrate(
        {"source": "airflow", "output_dir": str(output), "resume_agentic": True, "keep_intermediates": True}
    )
    assert (
        unresolved_resume["status"],
        unresolved_resume["reviewed_count"],
        unresolved_resume["unreviewed_count"],
    ) == ("completed_with_reviewed_gaps", 1, 0)

    assert server._cmd_resolve_agentic(
        {"source": "airflow", "action": "apply", "reset": True, "output_dir": str(output)}
    )["ok"]
    assert server._cmd_resolve_agentic(
        {
            "source": "airflow",
            "action": "stage",
            "candidates": [_build_candidate_for_gap(gap, status="resolved")],
            "output_dir": str(output),
        }
    )["ok"]
    applied = server._cmd_resolve_agentic(
        {
            "source": "airflow",
            "action": "apply",
            "accept_gap": gap["gap_id"],
            "output_dir": str(output),
        }
    )
    assert applied["ok"] is True
    assert (applied["reviewed_count"], applied["unreviewed_count"]) == (1, 0)
    agentic_report = json.loads((output / ".work" / "translation_report.agentic.json").read_text(encoding="utf-8"))
    assert agentic_contract.airflow_gap_findings(agentic_report) == []

    resumed = server._cmd_migrate({"source": "airflow", "output_dir": str(output), "resume_agentic": True})
    assert (resumed["ok"], resumed["status"], resumed["reviewed_count"], resumed["unreviewed_count"]) == (
        True,
        "completed",
        1,
        0,
    )


def test_package_parses_airflow_review_counts_from_later_stdout_line(monkeypatch, tmp_path: Path) -> None:
    output = tmp_path / "out"
    report = output / ".work" / "translation_report.agentic.json"
    report.parent.mkdir(parents=True)
    report.write_text(json.dumps(_airflow_report(operator="KubernetesPodOperator")), encoding="utf-8")

    monkeypatch.setattr(
        runner,
        "run_adapter",
        lambda _args: _FakeResult(
            stdout=(
                "Diagnostic: preparing preflight\n"
                'Airflow gap review: {"accepted_count": 1, "reviewed_count": 1, "unreviewed_count": 2}\n'
                "Loading translation report\n"
            )
        ),
    )
    monkeypatch.setattr(runner, "list_tree", lambda _out: ["databricks.yml"])
    monkeypatch.setattr(runner, "read_tree", lambda _out: {"files": {}})

    result = server._cmd_package({"output_dir": str(output), "report_path": str(report)})
    assert (result["status"], result["accepted_count"], result["reviewed_count"], result["unreviewed_count"]) == (
        "completed_with_reviewed_gaps",
        1,
        1,
        2,
    )


def test_airflow_unreviewed_count_matches_before_review_after_reset_and_after_partial_accept(tmp_path: Path) -> None:
    source = tmp_path / "dags"
    source.mkdir()
    (source / "parent.py").write_text(
        "from airflow import DAG\n"
        "from airflow.operators.trigger_dagrun import TriggerDagRunOperator\n"
        "from airflow.providers.ssh.operators.ssh import SSHOperator\n"
        "with DAG(dag_id='parent') as dag:\n"
        "    run_ssh = SSHOperator(task_id='run_ssh', ssh_conn_id='prod', command='echo hi')\n"
        "    trig = TriggerDagRunOperator(task_id='trig', trigger_dag_id='excluded_child')\n"
        "    run_ssh >> trig\n",
        encoding="utf-8",
    )
    (source / "excluded_child.py").write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='excluded_child') as dag:\n"
        "    child = BashOperator(task_id='child', bash_command='echo child')\n",
        encoding="utf-8",
    )
    output = tmp_path / "out"

    paused = server._cmd_migrate(
        {
            "source": "airflow",
            "airflow_source_path": str(source),
            "output_dir": str(output),
            "exclude_dag": ["excluded_child"],
        }
    )
    assert paused["status"] == "needs_agentic_resolution"
    (gap,) = paused["eligible_gaps"]

    raw_packaged = server._cmd_package(
        {
            "output_dir": str(output),
            "report_path": paused["report_path"],
            "allow_unresolved_gaps": True,
            "keep_intermediates": True,
        }
    )
    assert (raw_packaged["status"], raw_packaged["reviewed_count"], raw_packaged["unreviewed_count"]) == (
        "packaged_with_unresolved_gaps",
        0,
        2,
    )

    reset = server._cmd_resolve_agentic(
        {"source": "airflow", "action": "apply", "reset": True, "output_dir": str(output)}
    )
    assert (reset["ok"], reset["reviewed_count"], reset["unreviewed_count"]) == (True, 0, 2)
    reset_packaged = server._cmd_migrate(
        {
            "source": "airflow",
            "output_dir": str(output),
            "resume_agentic": True,
            "allow_unresolved_gaps": True,
            "keep_intermediates": True,
        }
    )
    assert (reset_packaged["status"], reset_packaged["reviewed_count"], reset_packaged["unreviewed_count"]) == (
        "packaged_with_unresolved_gaps",
        0,
        2,
    )

    assert server._cmd_resolve_agentic(
        {
            "source": "airflow",
            "action": "stage",
            "candidates": [_build_candidate_for_gap(gap, status="resolved")],
            "output_dir": str(output),
        }
    )["ok"]
    applied = server._cmd_resolve_agentic(
        {
            "source": "airflow",
            "action": "apply",
            "accept_gap": gap["gap_id"],
            "output_dir": str(output),
        }
    )
    assert (applied["ok"], applied["reviewed_count"], applied["unreviewed_count"]) == (True, 1, 1)

    resumed = server._cmd_migrate({"source": "airflow", "output_dir": str(output), "resume_agentic": True})
    assert (resumed["status"], resumed["reviewed_count"], resumed["unreviewed_count"]) == (
        "completed_with_reviewed_gaps",
        1,
        1,
    )


def _failed_airflow_pipeline(name: str) -> dict:
    return {
        "name": name,
        "tags": {"source": "airflow"},
        "reconciliation_status": "failed",
        "tasks": [],
        "not_translatable": [
            {"fingerprint": f"{name}-gap", "code": "operator_placeholder", "severity": "gap", "message": "gap"},
            {"fingerprint": f"{name}-failed", "code": "unaudited_task", "severity": "failed", "message": "failed"},
        ],
        "audit": {"audited_activity_count": 1, "transformations": []},
    }


_SKIP_HEADER = (
    "from datetime import datetime\n"
    "from airflow import DAG\n"
    "from airflow.operators.bash import BashOperator\n"
    "from airflow.operators.trigger_dagrun import TriggerDagRunOperator\n"
)
_SKIP_SCHEDULE = "start_date=datetime(2024, 1, 1), schedule='@daily', catchup=False, max_active_runs=1"
_SKIP_DAGS = {
    "good.py": _SKIP_HEADER
    + f"with DAG('good', {_SKIP_SCHEDULE}) as dag:\n    BashOperator(task_id='hi', bash_command='echo hi')\n",
    "bad.py": _SKIP_HEADER
    + f"with DAG('bad', {_SKIP_SCHEDULE}) as dag:\n"
    + "    tasks = [BashOperator(task_id=f't{i}', bash_command='echo hi') for i in range(2)]\n",
}
_SKIP_CALLER = (
    _SKIP_HEADER + f"with DAG('caller', {_SKIP_SCHEDULE}) as dag:\n"
    "    BashOperator(task_id='hi', bash_command='echo hi') >> "
    "TriggerDagRunOperator(task_id='kick', trigger_dag_id='bad')\n"
)


def _skip_migrate(tmp_path: Path, files: dict[str, str]) -> dict:
    source = tmp_path / "dags"
    source.mkdir()
    for name, content in files.items():
        (source / name).write_text(content, encoding="utf-8")
    return server._cmd_migrate(
        {
            "source": "airflow",
            "airflow_source_path": str(source),
            "output_dir": str(tmp_path / "out"),
            "interactive": False,
            "skip_failed_dags": True,
        }
    )


def test_airflow_migrate_skipping_failed_dags_packages_the_others(tmp_path: Path):
    result = _skip_migrate(tmp_path, _SKIP_DAGS)

    assert result["ok"] is True
    assert result["status"] == "packaged_without_failed_dags"
    assert result["left_out_dags"] == ["bad"]
    assert (tmp_path / "out" / "resources" / "good.yml").is_file()
    assert not (tmp_path / "out" / "resources" / "bad.yml").exists()


def test_airflow_migrate_without_skipping_stops_at_a_failed_dag(tmp_path: Path):
    source = tmp_path / "dags"
    source.mkdir()
    for name, content in _SKIP_DAGS.items():
        (source / name).write_text(content, encoding="utf-8")

    result = server._cmd_migrate(
        {
            "source": "airflow",
            "airflow_source_path": str(source),
            "output_dir": str(tmp_path / "out"),
            "interactive": False,
        }
    )

    assert result["ok"] is False
    assert result["failed_phase"] == "discover"


def test_airflow_migrate_skipping_failed_dags_reports_triggers_of_them_as_excluded_references(tmp_path: Path):
    result = _skip_migrate(tmp_path, {**_SKIP_DAGS, "caller.py": _SKIP_CALLER})

    assert result["status"] == "needs_agentic_resolution"
    assert result["left_out_dags"] == ["bad"]
    assert [gap["code"] for gap in result["structural_gaps"]] == ["excluded_dag_reference"]


def test_airflow_migrate_skipping_failed_dags_prepares_gaps_in_the_others(tmp_path: Path):
    caller = _SKIP_CALLER.replace(
        "BashOperator(task_id='hi', bash_command='echo hi')", "SSHOperator(task_id='s', ssh_conn_id='x', command='ls')"
    ).replace(
        "from airflow.operators.bash",
        "from airflow.providers.ssh.operators.ssh import SSHOperator\nfrom airflow.operators.bash",
    )

    result = _skip_migrate(tmp_path, {**_SKIP_DAGS, "caller.py": caller})

    assert result["status"] == "needs_agentic_resolution"
    assert [gap["operator"] for gap in result["eligible_gaps"]] == ["SSHOperator"]
    assert result["left_out_dags"] == ["bad"]


def test_package_reports_unresolved_gaps_before_left_out_dags(monkeypatch, tmp_path: Path):
    output = tmp_path / "out"
    report = output / ".work" / "translation_report.json"
    report.parent.mkdir(parents=True)
    payload = _airflow_report(operator="KubernetesPodOperator")
    payload["pipelines"].append(_failed_airflow_pipeline("bad"))
    report.write_text(json.dumps(payload), encoding="utf-8")
    calls: list[list[str]] = []

    def package_and_prune(args):
        calls.append([str(value) for value in args])
        report.unlink()
        return _FakeResult()

    monkeypatch.setattr(runner, "run_adapter", package_and_prune)
    monkeypatch.setattr(runner, "list_tree", lambda _out: ["databricks.yml"])
    monkeypatch.setattr(runner, "read_tree", lambda _out: {"files": {}})

    result = server._cmd_package({"output_dir": str(output), "allow_unresolved_gaps": True, "skip_failed_dags": True})

    assert result["ok"] is True
    assert result["status"] == "packaged_with_unresolved_gaps"
    assert result["left_out_dags"] == ["bad"]
    assert {"--allow-unresolved-gaps", "--skip-failed-dags"} <= set(calls[0])


def test_package_without_left_out_dags_is_completed_when_only_failed_dags_had_gaps(monkeypatch, tmp_path: Path):
    output = tmp_path / "out"
    report = output / ".work" / "translation_report.json"
    report.parent.mkdir(parents=True)
    report.write_text(json.dumps({"pipelines": [_failed_airflow_pipeline("bad")]}), encoding="utf-8")
    monkeypatch.setattr(runner, "run_adapter", lambda _args: _FakeResult())
    monkeypatch.setattr(runner, "list_tree", lambda _out: ["databricks.yml"])
    monkeypatch.setattr(runner, "read_tree", lambda _out: {"files": {}})

    result = server._cmd_package({"output_dir": str(output), "allow_unresolved_gaps": True, "skip_failed_dags": True})

    assert result["status"] == "packaged_without_failed_dags"
