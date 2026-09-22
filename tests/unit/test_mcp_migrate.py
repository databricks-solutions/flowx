"""Tests for the MCP server's agent-driven interactive ``migrate`` flow.

The server returns the full option schema once (``needs_input``); the agent walks the chain locally
and re-calls ``migrate`` once with the complete answers, which applies and packages. These tests
guard that one-shot contract. Skipped where the optional ``mcp`` dependency is absent.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("mcp")

from flowx.mcp import runner, server  # noqa: E402


class _FakeResult:
    def __init__(self, *, ok: bool = True, stdout: str = "", stderr: str = "") -> None:
        self.ok = ok
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = 0 if ok else 1

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
    return {
        "contract_version": "1",
        "gap_id": "gap-1",
        "pipeline_name": "example",
        "operator": operator,
        "finding_fingerprints": ["gap-1"],
    }


def test_airflow_gap_state_never_advertises_contract_restricted_gap_as_leaf_eligible() -> None:
    gap = {**_gap("CustomOperator"), "allowed_replacement_kinds": []}

    state = server._airflow_gap_state(_airflow_report(operator="CustomOperator"), [gap])

    assert state["eligible_gaps"] == []
    assert state["structural_gaps"][0]["required_capability"] == "non_leaf_patch"


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
    assert [argv[0] for argv in calls] == ["discover", "convert", "resolve-agentic"]


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


def test_airflow_migrate_resumes_from_reviewed_agentic_report(monkeypatch, tmp_path: Path):
    output = tmp_path / "out"
    report = output / ".work" / "translation_report.agentic.json"
    report.parent.mkdir(parents=True)
    report.write_text(json.dumps(_airflow_report(operator="KubernetesPodOperator")), encoding="utf-8")
    calls: list[list[str]] = []

    def fake_run_adapter(args):
        calls.append([str(value) for value in args])
        return _FakeResult()

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
