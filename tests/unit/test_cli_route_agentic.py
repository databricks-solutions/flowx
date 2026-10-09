"""Tests for the ``route`` CLI and the ``fill-agentic`` surface.

``route`` writes its recommendation straight into ``metadata/conversion_plan.json`` (keeping the
decisions already recorded there, every other one pending) and ``metadata/routing_review.html``; the
decisions can also come from ``--plan-path``, ``--plan-path -`` (stdin) or an interactive TTY prompt.
Once nothing is pending it edits the report for the routed-agentic units. ``fill-agentic`` fills a
routed-agentic unit, validating structurally before writing.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pytest

from flowx.adapter.__main__ import main as adapter_cli_main
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.models.ir import ControlEdge, Lineage
from flowx.route_agentic import GAPS_FILENAME, REPORT_FILENAME, WORK_DIRNAME


def _node(task_key: str, native_type: str, *, strategy: str = "deterministic") -> SourceNode:
    return SourceNode(
        source_id=task_key,
        task_key=task_key,
        concept=CONCEPT_NOTEBOOK,
        source="unit",
        name=task_key,
        native_type=native_type,
        properties={STRATEGY_PROPERTY: strategy},
        raw={"name": task_key, "type": native_type},
    )


def _inventory() -> dict[str, Any]:
    """One component 'parent -> child' plus a standalone 'solo'."""
    parent = SourceGraph(
        name="parent",
        source="unit",
        tasks=[_node("call_child", "ExecutePipeline")],
        lineage=Lineage(
            control_edges=[ControlEdge(source_workflow="parent", target_workflow="child", via_task_key="call_child")]
        ),
    )
    child = SourceGraph(name="child", source="unit", tasks=[_node("copy_orders", "Copy")])
    solo = SourceGraph(name="solo", source="unit", tasks=[_node("load", "Notebook")])
    return build_source_inventory([parent, child, solo], source="adf", source_dir="/tmp/src")


def _report() -> dict[str, Any]:
    return {
        "pipelines": [
            {"name": "parent", "tasks": [{"name": "call_child", "task_key": "call_child", "type": "CopyActivity"}]},
            {"name": "child", "tasks": [{"name": "copy_orders", "task_key": "copy_orders", "type": "CopyActivity"}]},
            {
                "name": "solo",
                "tasks": [
                    {"name": "load", "task_key": "load", "type": "NotebookActivity", "notebook_path": "/x"},
                ],
            },
        ]
    }


def _setup(output_dir: Path) -> None:
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    (metadata / "inventory.json").write_text(json.dumps(_inventory(), indent=2), encoding="utf-8")
    work = output_dir / WORK_DIRNAME
    work.mkdir(parents=True, exist_ok=True)
    (work / REPORT_FILENAME).write_text(json.dumps(_report(), indent=2), encoding="utf-8")
    (work / GAPS_FILENAME).write_text("[]", encoding="utf-8")


def _agentic_plan() -> dict[str, Any]:
    """Route the parent<->child component agentic; leave solo deterministic."""
    return {
        "components": [
            {"component_id": "component-1", "members": ["child", "parent"], "decision": "agentic"},
            {"component_id": "component-2", "members": ["solo"], "decision": "deterministic"},
        ]
    }


def test_route_without_a_decision_writes_a_pending_plan_and_review_page_and_leaves_the_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _setup(tmp_path)
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    report_before = report_path.read_bytes()
    monkeypatch.setattr("sys.stdin", io.StringIO(""))  # not a TTY

    code = adapter_cli_main(["route", "--output-dir", str(tmp_path)])

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["applied"] is False
    assert payload["pending"] == ["component-1", "component-2"]
    plan = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))
    assert [component["decision"] for component in plan["components"]] == [None, None]
    assert [component["recommended"] for component in plan["components"]] == ["deterministic", "deterministic"]
    assert (tmp_path / "metadata" / "routing_review.html").is_file()
    assert report_path.read_bytes() == report_before
    assert not (tmp_path / WORK_DIRNAME / "route_baseline").exists()


def _edit_recorded_plan(tmp_path: Path, decisions: dict[str, str]) -> None:
    """Edit the recorded conversion_plan.json in place, the way an agent does."""
    plan_path = tmp_path / "metadata" / "conversion_plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    for component in plan["components"]:
        component["decision"] = decisions[component["component_id"]]
    plan_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")


def test_editing_the_recorded_plan_and_re_running_route_applies_it(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _setup(tmp_path)
    monkeypatch.setattr("sys.stdin", io.StringIO(""))
    assert adapter_cli_main(["route", "--output-dir", str(tmp_path)]) == 0
    _edit_recorded_plan(tmp_path, {"component-1": "agentic", "component-2": "deterministic"})
    capsys.readouterr()

    assert adapter_cli_main(["route", "--output-dir", str(tmp_path)]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["applied"] is True and payload["edit"]["agentic_pipelines"] == ["child", "parent"]
    plan = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))
    assert [component["decision"] for component in plan["components"]] == ["agentic", "deterministic"]
    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    by_name = {pipeline["name"]: pipeline for pipeline in report["pipelines"]}
    assert by_name["parent"]["tasks"][0]["type"] == "PlaceholderActivity"


def test_route_recomputes_library_fields_an_agent_edited(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _setup(tmp_path)
    monkeypatch.setattr("sys.stdin", io.StringIO(""))
    assert adapter_cli_main(["route", "--output-dir", str(tmp_path)]) == 0
    plan_path = tmp_path / "metadata" / "conversion_plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    recorded_hash = plan["inventory_sha256"]
    plan["inventory_sha256"] = "forged"
    plan["components"][0]["recommended"] = "agentic"
    plan["components"][0]["options"] = {"deterministic": {"capable": False}}
    plan_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")

    assert adapter_cli_main(["route", "--output-dir", str(tmp_path)]) == 0

    rewritten = json.loads(plan_path.read_text(encoding="utf-8"))
    assert rewritten["inventory_sha256"] == recorded_hash
    assert rewritten["components"][0]["recommended"] == "deterministic"
    assert rewritten["components"][0]["options"]["deterministic"]["capable"] is True


def test_route_refuses_an_invalid_edited_decision_and_keeps_the_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _setup(tmp_path)
    monkeypatch.setattr("sys.stdin", io.StringIO(""))
    assert adapter_cli_main(["route", "--output-dir", str(tmp_path)]) == 0
    _edit_recorded_plan(tmp_path, {"component-1": "Agentic", "component-2": "deterministic"})
    edited = (tmp_path / "metadata" / "conversion_plan.json").read_bytes()
    capsys.readouterr()

    assert adapter_cli_main(["route", "--output-dir", str(tmp_path)]) == 1

    payload = json.loads(capsys.readouterr().out)
    assert any("'decision' must be one of" in violation for violation in payload["violations"])
    assert (tmp_path / "metadata" / "conversion_plan.json").read_bytes() == edited


def test_route_with_plan_path_records_and_edits_the_report(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _setup(tmp_path)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(_agentic_plan()), encoding="utf-8")
    code = adapter_cli_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["edit"]["agentic_pipelines"] == ["child", "parent"]

    # The recorded plan is written (fingerprint binding preserved).
    assert (tmp_path / "metadata" / "conversion_plan.json").exists()
    # The routed-agentic pipelines were placeholdered; the deterministic 'solo' is untouched.
    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    by_name = {pipeline["name"]: pipeline for pipeline in report["pipelines"]}
    assert by_name["parent"]["tasks"][0]["type"] == "PlaceholderActivity"
    assert by_name["child"]["tasks"][0]["type"] == "PlaceholderActivity"
    assert by_name["solo"]["tasks"][0]["type"] == "NotebookActivity"
    gaps = json.loads((tmp_path / WORK_DIRNAME / GAPS_FILENAME).read_text(encoding="utf-8"))
    assert sorted(gap["pipeline"] for gap in gaps) == ["child", "parent"]


def test_route_reads_a_plan_from_stdin_when_plan_path_is_dash(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _setup(tmp_path)
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(_agentic_plan())))
    code = adapter_cli_main(["route", "--output-dir", str(tmp_path), "--plan-path", "-"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True and payload["edit"]["agentic_pipelines"] == ["child", "parent"]


def test_route_interactive_prompt_records_the_users_decision(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _setup(tmp_path)

    class _Tty(io.StringIO):
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr("sys.stdin", _Tty(""))
    answers = iter(["a", ""])  # component-1 -> agentic; component-2 -> accept recommendation (deterministic)
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(answers))

    code = adapter_cli_main(["route", "--output-dir", str(tmp_path)])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True and payload["edit"]["agentic_pipelines"] == ["child", "parent"]


def test_route_validation_failure_returns_1_and_leaves_report_untouched(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _setup(tmp_path)
    report_before = (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes()
    bad = _agentic_plan()
    bad["components"].pop()  # partial plan: component-2 undecided
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(bad), encoding="utf-8")
    code = adapter_cli_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)])
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False and payload["violations"]
    assert (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes() == report_before


def test_route_with_a_missing_plan_file_fails_cleanly(tmp_path: Path) -> None:
    _setup(tmp_path)
    code = adapter_cli_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(tmp_path / "nope.json")])
    assert code == 1


def test_route_errors_when_report_missing_and_no_source_to_trigger_convert(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir(parents=True)
    (metadata / "inventory.json").write_text(json.dumps(_inventory(), indent=2), encoding="utf-8")
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(_agentic_plan()), encoding="utf-8")
    code = adapter_cli_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)])
    assert code == 1


# --------------------------------------------------------------------------- #
# fill-agentic.
# --------------------------------------------------------------------------- #


def _lfc_pipeline() -> dict[str, Any]:
    definition = {"name": "orders_ingestion", "catalog": "${var.catalog}", "target": "${var.schema}"}
    return {
        "name": "orders_lfc",
        "tags": {"source": "adf"},
        "tasks": [
            {
                "name": "Ingest orders",
                "task_key": "ingest_orders",
                "type": "AgenticComponentActivity",
                "files": [],
                "resources": [{"resource_key": "orders_ingestion", "definition": definition}],
                "task": {"pipeline_task": {"pipeline_id": "${resources.pipelines.orders_ingestion.id}"}},
            }
        ],
    }


def _record_agentic_plan(tmp_path: Path) -> None:
    """Route the parent<->child component agentic and record the plan so fill-agentic can bind to it."""
    plan_path = tmp_path / "route_plan.json"
    plan_path.write_text(json.dumps(_agentic_plan()), encoding="utf-8")
    assert adapter_cli_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)]) == 0


def _run_fill(tmp_path: Path, members: str, authored: list[dict[str, Any]]) -> int:
    pipelines_path = tmp_path / "authored.json"
    pipelines_path.write_text(json.dumps(authored), encoding="utf-8")
    return adapter_cli_main(
        [
            "fill-agentic",
            "--output-dir",
            str(tmp_path),
            "--members",
            members,
            "--pipelines-path",
            str(pipelines_path),
        ]
    )


def test_fill_agentic_writes_merged_report(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _setup(tmp_path)
    _record_agentic_plan(tmp_path)
    capsys.readouterr()  # drop the route output so only the fill JSON remains
    code = _run_fill(tmp_path, "child,parent", [_lfc_pipeline()])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    names = sorted(pipeline["name"] for pipeline in report["pipelines"])
    assert names == ["orders_lfc", "solo"]


def test_fill_agentic_rejects_dangling_reference(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _setup(tmp_path)
    _record_agentic_plan(tmp_path)
    capsys.readouterr()  # drop the route output so only the fill JSON remains
    report_before = (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes()
    dangling = _lfc_pipeline()
    dangling["tasks"][0]["task"] = {"pipeline_task": {"pipeline_id": "${resources.pipelines.ghost.id}"}}
    code = _run_fill(tmp_path, "child,parent", [dangling])
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False and payload["violations"]
    assert (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes() == report_before


def test_fill_agentic_rejects_a_deterministic_member_set(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _setup(tmp_path)
    _record_agentic_plan(tmp_path)  # component-1 (child, parent) agentic; solo is deterministic
    capsys.readouterr()  # drop the route output so only the fill JSON remains
    code = _run_fill(tmp_path, "solo", [_lfc_pipeline()])
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False and payload["error"]


def test_fill_agentic_has_no_no_validate_flag(tmp_path: Path) -> None:
    # The validation bypass must not exist as a CLI surface; argparse rejects the unknown flag.
    with pytest.raises(SystemExit) as excinfo:
        adapter_cli_main(
            [
                "fill-agentic",
                "--output-dir",
                str(tmp_path),
                "--members",
                "child,parent",
                "--pipelines-path",
                str(tmp_path / "x.json"),
                "--no-validate",
            ]
        )
    assert excinfo.value.code == 2


def test_route_triggers_convert_when_report_absent(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    # Inventory present, report absent: with --source/--source-path, route triggers convert. Stub the
    # phase runner to write the report the trigger would have produced, then assert route records+edits.
    metadata = tmp_path / "metadata"
    metadata.mkdir(parents=True)
    (metadata / "inventory.json").write_text(json.dumps(_inventory(), indent=2), encoding="utf-8")

    triggered: list[list[str]] = []

    def fake_run_phase(phase: str, forward: list[str]) -> int:
        triggered.append([phase, *forward])
        work = tmp_path / WORK_DIRNAME
        work.mkdir(parents=True, exist_ok=True)
        (work / REPORT_FILENAME).write_text(json.dumps(_report(), indent=2), encoding="utf-8")
        return 0

    monkeypatch.setattr("flowx.adapter.__main__._run_phase", fake_run_phase)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(_agentic_plan()), encoding="utf-8")
    code = adapter_cli_main(
        [
            "route",
            "--output-dir",
            str(tmp_path),
            "--plan-path",
            str(plan_path),
            "--source",
            "adf",
            "--source-path",
            str(tmp_path / "adf_src"),
        ]
    )
    assert code == 0
    assert triggered and triggered[0][0] == "convert"
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True and payload["edit"]["agentic_pipelines"] == ["child", "parent"]


def test_the_old_combine_action_is_gone(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as excinfo:
        adapter_cli_main(
            ["fill-agentic", "combine", "--output-dir", str(tmp_path), "--members", "solo", "--pipelines-path", "x"]
        )
    assert excinfo.value.code == 2


def test_recorded_decisions_survive_a_re_enrich_and_route_re_applies_them(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from flowx.discovery_insights import enrich_inventory

    _setup(tmp_path)
    monkeypatch.setattr("sys.stdin", io.StringIO(""))
    assert adapter_cli_main(["route", "--output-dir", str(tmp_path)]) == 0
    _edit_recorded_plan(tmp_path, {"component-1": "agentic", "component-2": "deterministic"})
    assert adapter_cli_main(["route", "--output-dir", str(tmp_path)]) == 0
    assert enrich_inventory(tmp_path, insights={"overview": "Orders flow from parent to child."})["ok"] is True
    capsys.readouterr()

    assert adapter_cli_main(["route", "--output-dir", str(tmp_path)]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["applied"] is True and payload["pending"] == []
    plan = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))
    assert [component["decision"] for component in plan["components"]] == ["agentic", "deterministic"]
    assert plan["agentic_insights_sha256"] is not None


def test_route_records_the_routing_conversation_and_refuses_a_malformed_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _setup(tmp_path)
    plan_path = tmp_path / "plan.json"
    conversation = [{"question": "Which databases do the extractors read?", "answer": "SQL Server 2019"}]
    plan_path.write_text(json.dumps({**_agentic_plan(), "conversation": conversation}), encoding="utf-8")
    assert adapter_cli_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)]) == 0
    recorded = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))
    assert recorded["conversation"] == conversation
    capsys.readouterr()

    plan_path.write_text(json.dumps({**_agentic_plan(), "conversation": [{"question": " "}]}), encoding="utf-8")

    assert adapter_cli_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)]) == 1
    violations = json.loads(capsys.readouterr().out)["violations"]
    assert violations == [
        "conversation[0]: 'answer' must be a non-empty string",
        "conversation[0]: 'question' must be a non-empty string",
    ]
