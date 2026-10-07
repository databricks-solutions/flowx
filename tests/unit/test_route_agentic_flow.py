"""Tests for the Phase-1 in-engine agentic conversion flow (route -> alter -> fill).

The confirmed flow, built here:

* ``route`` groups pipelines by connected component (reusing :mod:`flowx.routing`), takes a
  per-component deterministic/agentic decision, records the fingerprint-bound conversion plan, and
  then **edits** the deterministic ``translation_report.json``: for every pipeline in an
  agentic-routed component it removes the deterministic tasks, replaces them with
  ``PlaceholderActivity`` entries, and appends one ``AgenticGap`` per task to ``gaps.json``. Pipelines
  in deterministic components are left byte-identical, and with no agentic decision the report and
  gaps are untouched (the non-breaking guarantee).
* the agent then authors the fill. A routed-agentic group is filled only by the **cross-pipeline
  COMBINE** (N pipelines -> M, e.g. one Lakeflow Connect pipeline), which swaps the group's pipelines
  for the agent-authored pipeline(s), typically carrying ``AgenticComponentActivity`` nodes. The
  name-matched :func:`flowx.ir_serde.merge_agentic_results` fills only convert's own gaps.
* the merged report is validated structurally via the existing
  :func:`flowx.validate.bundle_invariants.check_bundle_dir` (unique keys, no dangling deps, acyclic,
  no dangling pipeline/run_job references).
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any

import pytest

from flowx.bundler.dab_writer import main as package_main
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.ir_serde import merge_agentic_results
from flowx.models.conversion_plan import ConversionPlan
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.models.ir import ControlEdge, Lineage
from flowx.route_agentic import (
    GAPS_FILENAME,
    REPORT_FILENAME,
    ROUTED_AGENTIC_MERGE_REFUSED,
    ROUTING_RECORD_KEY,
    WORK_DIRNAME,
    agentic_pipeline_names,
    alter_report,
    apply_combine_fill,
    apply_plan,
    apply_plan_to_report,
    combine_group_fill,
    prompt_for_decisions,
    routing_record,
    validate_report_structurally,
)
from flowx.routing import build_recommendation, record_plan
from flowx.sources.adf.translate import main as translate_main

# --------------------------------------------------------------------------- #
# Fixtures.
# --------------------------------------------------------------------------- #


def _notebook_task(name: str, task_key: str, path: str = "/Workspace/Shared/x") -> dict[str, Any]:
    return {"name": name, "task_key": task_key, "type": "NotebookActivity", "notebook_path": path}


def _copy_task(name: str, task_key: str) -> dict[str, Any]:
    return {"name": name, "task_key": task_key, "type": "CopyActivity"}


def _report_two_pipelines() -> dict[str, Any]:
    """A 'parent' pipeline (one Copy) and a 'child' pipeline (one Notebook)."""
    return {
        "pipelines": [
            {"name": "parent", "tasks": [_copy_task("Extract", "extract")]},
            {"name": "child", "tasks": [_notebook_task("Load", "load")]},
        ]
    }


def _plan(*, parent: str = "agentic", child: str = "deterministic") -> dict[str, Any]:
    """An authored/recorded plan: 'parent' and 'child' each their own component."""
    return {
        "components": [
            {"component_id": "component-1", "members": ["parent"], "decision": parent},
            {"component_id": "component-2", "members": ["child"], "decision": child},
        ]
    }


def _recommendation() -> dict[str, Any]:
    return {
        "components": [
            {
                "component_id": "component-1",
                "members": ["parent"],
                "recommended": "deterministic",
                "options": {"deterministic": {"capable": True}, "agentic": {"has_simplification": True}},
            },
            {
                "component_id": "component-2",
                "members": ["child"],
                "recommended": "agentic",
                "options": {"deterministic": {"capable": False}, "agentic": {"has_simplification": False}},
            },
        ],
        "findings": [],
        "default_plan": {"components": []},
    }


def _lfc_pipeline() -> dict[str, Any]:
    """One agent-authored Lakeflow Connect pipeline via the AgenticComponentActivity escape hatch."""
    pipeline_definition = {
        "name": "orders_ingestion",
        "catalog": "${var.catalog}",
        "target": "${var.schema}",
        "ingestion_definition": {"connection_name": "c", "objects": []},
    }
    return {
        "name": "orders_lfc",
        "tags": {"source": "adf"},
        "tasks": [
            {
                "name": "Ingest orders",
                "task_key": "ingest_orders",
                "type": "AgenticComponentActivity",
                "files": [],
                "resources": [{"resource_key": "orders_ingestion", "definition": pipeline_definition}],
                "task": {"pipeline_task": {"pipeline_id": "${resources.pipelines.orders_ingestion.id}"}},
            }
        ],
    }


def _named_lfc_pipeline(name: str) -> dict[str, Any]:
    """A correctly-tagged authored combine pipeline with a caller-chosen ``name``."""
    pipeline = _lfc_pipeline()
    pipeline["name"] = name
    return pipeline


def _write_work(output_dir: Path, report: dict[str, Any], gaps: list[dict[str, Any]] | None = None) -> None:
    work = output_dir / WORK_DIRNAME
    work.mkdir(parents=True, exist_ok=True)
    (work / REPORT_FILENAME).write_text(json.dumps(report, indent=2), encoding="utf-8")
    if gaps is not None:
        (work / GAPS_FILENAME).write_text(json.dumps(gaps, indent=2), encoding="utf-8")


def _node(task_key: str, native_type: str) -> SourceNode:
    return SourceNode(
        source_id=task_key,
        task_key=task_key,
        concept=CONCEPT_NOTEBOOK,
        source="unit",
        name=task_key,
        native_type=native_type,
        properties={STRATEGY_PROPERTY: "deterministic"},
        raw={"name": task_key, "type": native_type},
    )


def _routed_inventory() -> dict[str, Any]:
    """Inventory whose single 'parent -> child' control edge forms one component {child, parent}."""
    parent = SourceGraph(
        name="parent",
        source="unit",
        tasks=[_node("call_child", "ExecutePipeline")],
        lineage=Lineage(
            control_edges=[ControlEdge(source_workflow="parent", target_workflow="child", via_task_key="call_child")]
        ),
    )
    child = SourceGraph(name="child", source="unit", tasks=[_node("copy_orders", "Copy")])
    return build_source_inventory([parent, child], source="adf", source_dir="/tmp/src")


def _setup_routed_agentic(output_dir: Path, *, decision: str = "agentic") -> None:
    """Write inventory + report, then record and apply a plan routing {child, parent} per ``decision``."""
    _write_work(output_dir, _report_two_pipelines())
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    (metadata / "inventory.json").write_text(json.dumps(_routed_inventory(), indent=2), encoding="utf-8")
    plan = {"components": [{"component_id": "component-1", "members": ["child", "parent"], "decision": decision}]}
    result = record_plan(output_dir, plan=plan)
    assert result["ok"], result
    recorded = ConversionPlan.load(output_dir)
    assert recorded is not None
    apply_plan(output_dir, recorded)


def _two_component_inventory() -> dict[str, Any]:
    """Inventory with no control edge, so 'parent' and 'child' are separate components."""
    parent = SourceGraph(name="parent", source="unit", tasks=[_node("extract", "Copy")])
    child = SourceGraph(name="child", source="unit", tasks=[_node("load", "Script")])
    return build_source_inventory([parent, child], source="adf", source_dir="/tmp/src")


def _route_parent_agentic(output_dir: Path, report: dict[str, Any]) -> None:
    """Write ``report``, record a plan routing 'parent' agentic and 'child' deterministic, and apply it."""
    _write_work(output_dir, report, gaps=[])
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    inventory = _two_component_inventory()
    (metadata / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    plan = build_recommendation(inventory)["default_plan"]
    for component in plan["components"]:
        component["decision"] = "agentic" if component["members"] == ["parent"] else "deterministic"
    assert record_plan(output_dir, plan=plan)["ok"] is True
    recorded = ConversionPlan.load(output_dir)
    assert recorded is not None
    apply_plan(output_dir, recorded)


def _report_with_child_gap() -> dict[str, Any]:
    """The two-pipeline report where convert left 'child''s top-level 'Load' as an agentic gap."""
    report = _report_two_pipelines()
    report["pipelines"][1]["tasks"] = [
        {"name": "Load", "task_key": "load", "type": "PlaceholderActivity", "original_type": "Script"}
    ]
    return report


def _record_outcomes(report_path: Path) -> dict[str, str]:
    record = routing_record(json.loads(report_path.read_text(encoding="utf-8")))
    assert record is not None
    return {component_id: entry["outcome"] for component_id, entry in record["components"].items()}


# --------------------------------------------------------------------------- #
# Decision selection.
# --------------------------------------------------------------------------- #


def test_agentic_pipeline_names_selects_only_agentic_component_members() -> None:
    plan = {
        "components": [
            {"component_id": "component-1", "members": ["parent", "shared"], "decision": "agentic"},
            {"component_id": "component-2", "members": ["child"], "decision": "deterministic"},
        ]
    }
    assert agentic_pipeline_names(plan) == {"parent", "shared"}


def test_prompt_for_decisions_defaults_to_recommendation_and_honours_overrides() -> None:
    answers = iter(["", "d"])  # component-1: accept (deterministic); component-2: override to deterministic
    lines: list[str] = []
    plan = prompt_for_decisions(_recommendation(), input_fn=lambda _prompt: next(answers), output_fn=lines.append)
    decisions = {c["component_id"]: c["decision"] for c in plan["components"]}
    assert decisions == {"component-1": "deterministic", "component-2": "deterministic"}
    # Each component carries its members so the plan validates against the inventory on record.
    assert plan["components"][0]["members"] == ["parent"]
    # The user saw the members and the prominent simplification option before answering.
    assert any("component-1" in line for line in lines)
    assert any("simplification" in line.lower() for line in lines)


def test_prompt_for_decisions_selects_agentic_on_a() -> None:
    plan = prompt_for_decisions(_recommendation(), input_fn=lambda _prompt: "a", output_fn=lambda _line: None)
    assert [c["decision"] for c in plan["components"]] == ["agentic", "agentic"]


# --------------------------------------------------------------------------- #
# Report alteration (the edit step).
# --------------------------------------------------------------------------- #


def test_alter_report_with_no_agentic_pipelines_is_identity() -> None:
    report = _report_two_pipelines()
    gaps: list[dict[str, Any]] = []
    original = copy.deepcopy(report)
    new_report, new_gaps = alter_report(report, gaps, set())
    assert new_report == original
    assert new_gaps == []


def test_alter_report_placeholders_agentic_pipeline_and_leaves_deterministic_untouched() -> None:
    report = _report_two_pipelines()
    child_before = copy.deepcopy(report["pipelines"][1])
    new_report, new_gaps = alter_report(report, [], {"parent"})

    parent = next(p for p in new_report["pipelines"] if p["name"] == "parent")
    child = next(p for p in new_report["pipelines"] if p["name"] == "child")
    # The agentic pipeline's deterministic Copy task became a placeholder marking the gap.
    assert [task["type"] for task in parent["tasks"]] == ["PlaceholderActivity"]
    placeholder = parent["tasks"][0]
    assert placeholder["name"] == "Extract" and placeholder["task_key"] == "extract"
    assert placeholder["original_type"] == "CopyActivity"
    # The deterministic pipeline is byte-identical.
    assert child == child_before


def test_alter_report_emits_one_gap_per_removed_task_tagged_with_pipeline() -> None:
    _report, gaps = alter_report(_report_two_pipelines(), [], {"parent"})
    assert len(gaps) == 1
    gap = gaps[0]
    assert gap["activity_name"] == "Extract"
    assert gap["activity_type"] == "CopyActivity"
    assert gap["pipeline"] == "parent"


def test_alter_report_emits_exactly_one_gap_per_task_replacing_prior_gaps() -> None:
    report = {
        "pipelines": [
            {
                "name": "parent",
                "tasks": [_copy_task("Extract", "extract"), _copy_task("Flow", "flow")],
            }
        ]
    }
    # A pre-existing (untagged) convert gap for a task that will be routed must not be duplicated.
    existing_gaps = [{"activity_name": "Flow", "activity_type": "ExecuteDataFlow", "raw_definition": None}]
    _report, gaps = alter_report(report, existing_gaps, {"parent"})
    # One gap per routed task, no duplicates, all tagged with the pipeline.
    assert len(gaps) == 2
    identities = sorted((gap["pipeline"], gap["activity_name"]) for gap in gaps)
    assert identities == [("parent", "Extract"), ("parent", "Flow")]
    assert len(identities) == len(set(identities))


def test_alter_report_is_idempotent_on_gaps() -> None:
    report = {"pipelines": [{"name": "parent", "tasks": [_copy_task("Extract", "extract")]}]}
    once_report, once_gaps = alter_report(report, [], {"parent"})
    twice_report, twice_gaps = alter_report(once_report, once_gaps, {"parent"})
    # Running the edit again does not append a second gap for the same routed task.
    assert twice_gaps == once_gaps
    assert len(twice_gaps) == 1


def test_alter_report_keeps_a_non_routed_gap_that_shares_a_task_name_with_a_routed_task() -> None:
    # Both pipelines have a task named "Sync"; only 'parent' is routed agentic. The untagged convert
    # gap belongs to the non-routed 'other' and must survive -- the drop must not match by global name.
    report = {
        "pipelines": [
            {"name": "parent", "tasks": [_copy_task("Sync", "sync_parent")]},
            {"name": "other", "tasks": [_copy_task("Sync", "sync_other")]},
        ]
    }
    existing = [{"activity_name": "Sync", "activity_type": "ExecuteDataFlow", "raw_definition": None}]
    _report, gaps = alter_report(report, existing, {"parent"})

    # The non-routed pipeline's untagged "Sync" gap survives.
    assert existing[0] in gaps
    # The routed pipeline contributes exactly one tagged gap for its own "Sync" task.
    tagged = [gap for gap in gaps if gap.get("pipeline") == "parent"]
    assert tagged == [
        {
            "activity_name": "Sync",
            "activity_type": "CopyActivity",
            "raw_definition": tagged[0]["raw_definition"],
            "pipeline": "parent",
        }
    ]
    # No duplicate tagged gap for the routed task.
    assert len(tagged) == 1


def test_alter_report_keeps_gaps_for_non_routed_pipelines() -> None:
    report = {
        "pipelines": [
            {"name": "parent", "tasks": [_copy_task("Extract", "extract")]},
            {"name": "other", "tasks": [_copy_task("Keep", "keep")]},
        ]
    }
    existing = [{"activity_name": "OtherGap", "activity_type": "Custom", "raw_definition": None, "pipeline": "other"}]
    _report, gaps = alter_report(report, existing, {"parent"})
    # The non-routed pipeline's gap survives; the routed pipeline contributes exactly one.
    assert {gap["activity_name"] for gap in gaps} == {"OtherGap", "Extract"}


def test_alter_report_preserves_depends_on_edges_on_placeholders() -> None:
    report = {
        "pipelines": [
            {
                "name": "parent",
                "tasks": [
                    _copy_task("A", "a"),
                    {**_copy_task("B", "b"), "depends_on": [{"task_key": "a", "outcome": "Succeeded"}]},
                ],
            }
        ]
    }
    new_report, _gaps = alter_report(report, [], {"parent"})
    task_b = new_report["pipelines"][0]["tasks"][1]
    assert task_b["type"] == "PlaceholderActivity"
    assert task_b["depends_on"] == [{"task_key": "a", "outcome": "Succeeded"}]


def test_alter_report_handles_the_single_pipeline_report_shape() -> None:
    report = {"name": "parent", "tasks": [_copy_task("Extract", "extract")]}
    new_report, gaps = alter_report(report, [], {"parent"})
    assert new_report["tasks"][0]["type"] == "PlaceholderActivity"
    assert len(gaps) == 1


# --------------------------------------------------------------------------- #
# apply_plan_to_report: I/O + non-breaking golden.
# --------------------------------------------------------------------------- #


def test_apply_plan_to_report_edits_report_and_gaps_files(tmp_path: Path) -> None:
    _write_work(tmp_path, _report_two_pipelines(), gaps=[])
    summary = apply_plan_to_report(tmp_path, _plan(parent="agentic", child="deterministic"))
    assert summary["agentic_pipelines"] == ["parent"]

    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    parent = next(p for p in report["pipelines"] if p["name"] == "parent")
    assert parent["tasks"][0]["type"] == "PlaceholderActivity"
    gaps = json.loads((tmp_path / WORK_DIRNAME / GAPS_FILENAME).read_text(encoding="utf-8"))
    assert [gap["pipeline"] for gap in gaps] == ["parent"]


def test_apply_plan_to_report_all_deterministic_leaves_files_byte_identical(tmp_path: Path) -> None:
    _write_work(tmp_path, _report_two_pipelines(), gaps=[])
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    gaps_path = tmp_path / WORK_DIRNAME / GAPS_FILENAME
    report_before = report_path.read_bytes()
    gaps_before = gaps_path.read_bytes()

    summary = apply_plan_to_report(tmp_path, _plan(parent="deterministic", child="deterministic"))
    assert summary["agentic_pipelines"] == []
    assert report_path.read_bytes() == report_before
    assert gaps_path.read_bytes() == gaps_before


# --------------------------------------------------------------------------- #
# Per-pipeline agentic fill: reuse the existing name-matched merge.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("pipeline", ["parent", None])
def test_merge_into_a_routed_agentic_pipeline_is_refused(tmp_path: Path, pipeline: str | None) -> None:
    _write_work(tmp_path, _report_two_pipelines(), gaps=[])
    apply_plan_to_report(tmp_path, _plan(parent="agentic", child="deterministic"))
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    routed_report = report_path.read_bytes()
    results_dir = tmp_path / "agentic_results"
    results_dir.mkdir()
    result: dict[str, Any] = {"activity_name": "Extract", "task": _notebook_task("Extract", "extract")}
    if pipeline is not None:
        result["pipeline"] = pipeline
    (results_dir / "extract.json").write_text(json.dumps(result), encoding="utf-8")

    with pytest.raises(ValueError, match=re.escape(ROUTED_AGENTIC_MERGE_REFUSED)):
        merge_agentic_results(report_path, results_dir)
    assert report_path.read_bytes() == routed_report


@pytest.mark.parametrize("pipeline", ["child", None])
def test_merge_still_fills_convert_gaps_in_a_deterministic_pipeline(tmp_path: Path, pipeline: str | None) -> None:
    _write_work(tmp_path, _report_with_child_gap(), gaps=[])
    apply_plan_to_report(tmp_path, _plan(parent="agentic", child="deterministic"))
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    results_dir = tmp_path / "agentic_results"
    results_dir.mkdir()
    result: dict[str, Any] = {"activity_name": "Load", "task": _notebook_task("Load", "load", "/Workspace/Shared/l")}
    if pipeline is not None:
        result["pipeline"] = pipeline
    (results_dir / "load.json").write_text(json.dumps(result), encoding="utf-8")

    assert merge_agentic_results(report_path, results_dir) == (1, 0)

    merged = json.loads(report_path.read_text(encoding="utf-8"))
    child = next(p for p in merged["pipelines"] if p["name"] == "child")
    assert child["tasks"][0]["type"] == "NotebookActivity"
    assert _record_outcomes(report_path) == {"component-1": "agentic-not-viable", "component-2": "deterministic"}


def test_a_merged_top_level_gap_survives_a_re_route_and_package_accepts_it(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report = _report_with_child_gap()
    for pipeline in report["pipelines"]:
        pipeline["tags"] = {"source": "adf"}
    _route_parent_agentic(tmp_path, report)
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    results_dir = tmp_path / "agentic_results"
    _write_merge_result(results_dir, "child", "Load", "load")
    assert merge_agentic_results(report_path, results_dir) == (1, 0)

    recorded = ConversionPlan.load(tmp_path)
    assert recorded is not None
    assert apply_plan(tmp_path, recorded)["altered"] is False
    for path in (report_path, tmp_path / WORK_DIRNAME / "route_baseline" / REPORT_FILENAME):
        child = next(p for p in json.loads(path.read_text(encoding="utf-8"))["pipelines"] if p["name"] == "child")
        assert child["tasks"][0]["type"] == "NotebookActivity"
    capsys.readouterr()
    package_main(["--output-dir", str(tmp_path), "--no-download-workspace-files", "--keep-intermediates"])
    assert "preflight failed" not in capsys.readouterr().err
    assert (tmp_path / "child" / "databricks.yml").exists()


def test_an_untargeted_merge_is_checked_where_it_lands_after_earlier_merges(tmp_path: Path) -> None:
    report = {
        "pipelines": [
            {"name": "child", "tasks": [{"name": "Script1", "task_key": "s", "type": "PlaceholderActivity"}]},
            {"name": "parent", "tasks": [_copy_task("Script1", "s")]},
        ]
    }
    _write_work(tmp_path, report, gaps=[])
    apply_plan_to_report(tmp_path, _plan(parent="agentic", child="deterministic"))
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    routed_report = report_path.read_bytes()
    results_dir = tmp_path / "agentic_results"
    results_dir.mkdir()
    for file_name, task_name in (("a.json", "Script1_done"), ("b.json", "Script1")):
        result = {"activity_name": "Script1", "task": _notebook_task(task_name, "s")}
        (results_dir / file_name).write_text(json.dumps(result), encoding="utf-8")

    with pytest.raises(ValueError, match=re.escape(ROUTED_AGENTIC_MERGE_REFUSED)):
        merge_agentic_results(report_path, results_dir)
    assert report_path.read_bytes() == routed_report


# --------------------------------------------------------------------------- #
# The routing record: re-routing under different decisions is refused.
# --------------------------------------------------------------------------- #


def test_route_writes_no_record_when_nothing_is_routed_agentic(tmp_path: Path) -> None:
    _write_work(tmp_path, _report_two_pipelines(), gaps=[])
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    before = report_path.read_bytes()

    apply_plan_to_report(tmp_path, _plan(parent="deterministic", child="deterministic"))

    assert report_path.read_bytes() == before


def test_reroute_to_different_decision_rebuilds_the_report(tmp_path: Path) -> None:
    """Re-routing with different decisions rebuilds deterministically; switching back restores convert's bytes."""
    _write_work(tmp_path, _report_two_pipelines(), gaps=[])
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    gaps_path = tmp_path / WORK_DIRNAME / GAPS_FILENAME
    converted_report = report_path.read_bytes()
    converted_gaps = gaps_path.read_bytes()
    apply_plan_to_report(tmp_path, _plan(parent="agentic", child="deterministic"))
    first = json.loads(report_path.read_text(encoding="utf-8"))
    first_outcomes = {cid: entry["outcome"] for cid, entry in first[ROUTING_RECORD_KEY]["components"].items()}
    assert first_outcomes["component-1"] == "agentic-not-viable"
    assert first_outcomes["component-2"] == "deterministic"

    apply_plan_to_report(tmp_path, _plan(parent="deterministic", child="deterministic"))

    assert report_path.read_bytes() == converted_report
    assert gaps_path.read_bytes() == converted_gaps


def test_route_takes_a_fresh_convert_as_the_new_baseline(tmp_path: Path) -> None:
    _write_work(tmp_path, _report_two_pipelines(), gaps=[])
    apply_plan_to_report(tmp_path, _plan(parent="agentic", child="deterministic"))
    reconverted = _report_two_pipelines()
    reconverted["pipelines"][1]["tasks"] = [_notebook_task("Load", "load", "/Workspace/Shared/reconverted")]
    _write_work(tmp_path, reconverted, gaps=[])

    apply_plan_to_report(tmp_path, _plan(parent="agentic", child="deterministic"))

    baseline = tmp_path / WORK_DIRNAME / "route_baseline" / REPORT_FILENAME
    assert json.loads(baseline.read_text(encoding="utf-8")) == reconverted
    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    child = next(p for p in report["pipelines"] if p["name"] == "child")
    assert child["tasks"][0]["notebook_path"] == "/Workspace/Shared/reconverted"


def test_reroute_under_the_same_decisions_keeps_fills_and_updates_the_plan_hash(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    assert apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    first_record = routing_record(json.loads(report_path.read_text(encoding="utf-8")))
    assert first_record is not None

    rationale_plan = {
        "components": [
            {
                "component_id": "component-1",
                "members": ["child", "parent"],
                "decision": "agentic",
                "rationale": "same decision, recorded again",
            }
        ]
    }
    assert record_plan(tmp_path, plan=rationale_plan)["ok"] is True
    recorded = ConversionPlan.load(tmp_path)
    assert recorded is not None
    assert apply_plan(tmp_path, recorded)["altered"] is False

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert [pipeline["name"] for pipeline in report["pipelines"]] == ["orders_lfc"]
    second_record = routing_record(report)
    assert second_record is not None
    assert second_record["conversion_plan_sha256"] != first_record["conversion_plan_sha256"]
    assert second_record["baseline_report_sha256"] == first_record["baseline_report_sha256"]
    assert second_record["components"] == first_record["components"]


# --------------------------------------------------------------------------- #
# Cross-pipeline COMBINE: the new pipeline-grain fill (N pipelines -> one LFC).
# --------------------------------------------------------------------------- #


def test_combine_group_fill_replaces_group_pipelines_with_authored_ones() -> None:
    report = _report_two_pipelines()
    merged = combine_group_fill(report, {"parent", "child"}, [_lfc_pipeline()])
    names = [pipeline["name"] for pipeline in merged["pipelines"]]
    assert names == ["orders_lfc"]


def test_combine_group_fill_keeps_pipelines_outside_the_group() -> None:
    report = {
        "pipelines": [
            {"name": "parent", "tasks": [_copy_task("Extract", "extract")]},
            {"name": "child", "tasks": [_notebook_task("Load", "load")]},
            {"name": "unrelated", "tasks": [_notebook_task("Keep", "keep")]},
        ]
    }
    merged = combine_group_fill(report, {"parent", "child"}, [_lfc_pipeline()])
    names = sorted(pipeline["name"] for pipeline in merged["pipelines"])
    assert names == ["orders_lfc", "unrelated"]


def test_combine_fill_of_two_pipelines_into_one_lfc_passes_structural_validation() -> None:
    report = _report_two_pipelines()
    merged = combine_group_fill(report, {"parent", "child"}, [_lfc_pipeline()])
    result = validate_report_structurally(merged)
    assert result.ok, [f"{finding.code}: {finding.message}" for finding in result.findings]


def test_combine_fill_with_a_dangling_pipeline_reference_is_caught() -> None:
    dangling = _lfc_pipeline()
    # Point the pipeline_task at a resource that is never declared in this activity's resources list.
    dangling["tasks"][0]["task"] = {"pipeline_task": {"pipeline_id": "${resources.pipelines.ghost.id}"}}
    merged = combine_group_fill(_report_two_pipelines(), {"parent", "child"}, [dangling])
    result = validate_report_structurally(merged)
    assert not result.ok
    assert any(finding.code == "dangling_pipeline_reference" for finding in result.violations)


def test_apply_combine_fill_writes_merged_report_when_valid(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    result = apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert result["ok"] is True
    assert result["component_id"] == "component-1"
    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    assert [pipeline["name"] for pipeline in report["pipelines"]] == ["orders_lfc"]


def test_apply_combine_fill_rejects_and_does_not_write_on_dangling_reference(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    report_before = (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes()
    dangling = _lfc_pipeline()
    dangling["tasks"][0]["task"] = {"pipeline_task": {"pipeline_id": "${resources.pipelines.ghost.id}"}}

    result = apply_combine_fill(tmp_path, ["parent", "child"], [dangling])
    assert result["ok"] is False
    assert any("dangling_pipeline_reference" in violation for violation in result["violations"])
    # The report on disk is untouched when validation fails.
    assert (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes() == report_before


def test_combine_rejects_an_authored_pipeline_missing_the_source_tag(tmp_path: Path) -> None:
    """FIX 2: an authored pipeline without tags.source == 'adf' fails closed at combine (nothing written)."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    report_before = (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes()
    untagged = _lfc_pipeline()
    del untagged["tags"]

    result = apply_combine_fill(tmp_path, ["parent", "child"], [untagged])

    assert result["ok"] is False
    assert any("tags.source" in violation and "adf" in violation for violation in result["violations"])
    # Nothing is written when the source tag is missing.
    assert (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes() == report_before


def test_combine_rejects_an_authored_pipeline_with_wrong_source_tag(tmp_path: Path) -> None:
    """A non-'adf' source tag is rejected the same way (routing/agentic is ADF-only)."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    mistagged = _lfc_pipeline()
    mistagged["tags"] = {"source": "airflow"}

    result = apply_combine_fill(tmp_path, ["parent", "child"], [mistagged])

    assert result["ok"] is False
    assert any("tags.source" in violation for violation in result["violations"])


def test_combine_is_idempotent_running_twice_yields_no_duplicate(tmp_path: Path) -> None:
    """FIX 3: re-running combine with the same members + authored pipeline does not duplicate it."""
    _setup_routed_agentic(tmp_path, decision="agentic")

    first = apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert first["ok"] is True
    assert first["already_combined"] is False

    # The recorded plan still lists {parent, child}, so the plan/membership check passes again; the
    # report, however, now contains only the authored pipeline. A naive re-run would re-append it.
    second = apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert second["ok"] is True
    assert second["already_combined"] is True

    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    names = [pipeline["name"] for pipeline in report["pipelines"]]
    assert names == ["orders_lfc"]  # exactly one authored pipeline, no duplicate


def test_a_different_combine_on_an_applied_component_replaces_and_rebuilds(tmp_path: Path) -> None:
    """A second combine with a different hash replaces the stored entry and rebuilds."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    first = apply_combine_fill(tmp_path, ["parent", "child"], [_named_lfc_pipeline("orders_lfc")])
    assert first["ok"] is True and first["already_combined"] is False
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    first_names = [p["name"] for p in json.loads(report_path.read_text(encoding="utf-8"))["pipelines"]]
    assert first_names == ["orders_lfc"]

    second = apply_combine_fill(tmp_path, ["parent", "child"], [_named_lfc_pipeline("orders_lfc_v2")])

    assert second["ok"] is True
    assert second["already_combined"] is False
    second_names = [p["name"] for p in json.loads(report_path.read_text(encoding="utf-8"))["pipelines"]]
    assert second_names == ["orders_lfc_v2"]


def test_combine_idempotent_when_authored_name_collides_with_a_former_member(tmp_path: Path) -> None:
    """FIX 3 (b): an authored name colliding with a former member is still detected as already-combined.

    After the first combine the report holds a pipeline named 'parent' (a former member). Name-based
    detection would see 'parent' present and conclude the combine had not happened; the routing record
    keeps the detection correct and independent of names.
    """
    _setup_routed_agentic(tmp_path, decision="agentic")

    first = apply_combine_fill(tmp_path, ["parent", "child"], [_named_lfc_pipeline("parent")])
    assert first["ok"] is True and first["already_combined"] is False

    second = apply_combine_fill(tmp_path, ["parent", "child"], [_named_lfc_pipeline("parent")])
    assert second["ok"] is True
    assert second["already_combined"] is True

    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    names = [pipeline["name"] for pipeline in report["pipelines"]]
    assert names == ["parent"]  # exactly one authored pipeline, no duplicate


# --------------------------------------------------------------------------- #
# Combine membership is bound to the recorded, fingerprint-bound plan.
# --------------------------------------------------------------------------- #


def test_combine_rejects_a_deterministic_component(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="deterministic")
    result = apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert result["ok"] is False
    assert "not agentic" in result["error"]
    # A deterministic component is never swapped out.
    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    assert sorted(pipeline["name"] for pipeline in report["pipelines"]) == ["child", "parent"]


def test_combine_rejects_a_partial_member_set(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    result = apply_combine_fill(tmp_path, ["parent"], [_lfc_pipeline()])
    assert result["ok"] is False
    assert "do not exactly match" in result["error"]


def test_combine_rejects_a_superset_member_set(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    result = apply_combine_fill(tmp_path, ["parent", "child", "extra"], [_lfc_pipeline()])
    assert result["ok"] is False
    assert "do not exactly match" in result["error"]


def test_combine_rejects_a_typoed_member(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    result = apply_combine_fill(tmp_path, ["parent", "chld"], [_lfc_pipeline()])
    assert result["ok"] is False
    assert "do not exactly match" in result["error"]


def test_combine_rejects_a_stale_fingerprint_plan(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    # Mutate the inventory after recording so the recorded fingerprint no longer matches.
    inventory_path = tmp_path / "metadata" / "inventory.json"
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    inventory["pipelines"].append({"name": "late_addition", "activities": [], "motifs": []})
    inventory_path.write_text(json.dumps(inventory, indent=2), encoding="utf-8")

    result = apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert result["ok"] is False
    assert "stale" in result["error"]


def test_combine_marks_the_component_applied(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    assert _record_outcomes(report_path) == {"component-1": "agentic-not-viable"}

    assert apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True

    assert _record_outcomes(report_path) == {"component-1": "agentic-applied"}


def _write_merge_result(results_dir: Path, pipeline: str, activity_name: str, task_key: str) -> None:
    results_dir.mkdir(exist_ok=True)
    (results_dir / f"{task_key}.json").write_text(
        json.dumps(
            {
                "pipeline": pipeline,
                "activity_name": activity_name,
                "task": _notebook_task(activity_name, task_key, f"/Workspace/Shared/agentic_{task_key}"),
            }
        ),
        encoding="utf-8",
    )


def test_merge_after_a_combine_is_refused(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    assert apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    combined_report = report_path.read_bytes()
    results_dir = tmp_path / "agentic_results"
    _write_merge_result(results_dir, "parent", "Extract", "extract")

    with pytest.raises(ValueError, match=re.escape(ROUTED_AGENTIC_MERGE_REFUSED)):
        merge_agentic_results(report_path, results_dir)
    exit_code = translate_main(["--merge-agentic", "--report", str(report_path), "--agentic-results", str(results_dir)])

    assert exit_code == 1
    assert ROUTED_AGENTIC_MERGE_REFUSED in capsys.readouterr().err
    assert report_path.read_bytes() == combined_report


def _colliding_authored_pipelines() -> list[dict[str, Any]]:
    """Authored combine pipelines that reuse both member names and the routed task names."""
    parent = _named_lfc_pipeline("parent")
    parent["tasks"][0]["name"] = "Extract"
    child = {"name": "child", "tags": {"source": "adf"}, "tasks": [_notebook_task("Load", "load")]}
    return [parent, child]


def test_combine_rerun_is_idempotent_when_authored_names_reuse_every_member(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")

    first = apply_combine_fill(tmp_path, ["parent", "child"], _colliding_authored_pipelines())
    assert first["ok"] is True and first["already_combined"] is False
    second = apply_combine_fill(tmp_path, ["parent", "child"], _colliding_authored_pipelines())

    assert second["ok"] is True
    assert second["already_combined"] is True
    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    assert [pipeline["name"] for pipeline in report["pipelines"]] == ["parent", "child"]


@pytest.mark.parametrize(
    ("pipeline", "activity_name"),
    [("parent", "Extract"), ("child", "Load"), (None, "Load")],
)
def test_merge_after_a_combine_reusing_every_member_name_is_refused(
    tmp_path: Path, pipeline: str | None, activity_name: str
) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    assert apply_combine_fill(tmp_path, ["parent", "child"], _colliding_authored_pipelines())["ok"] is True
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    combined_report = report_path.read_bytes()
    results_dir = tmp_path / "agentic_results"
    results_dir.mkdir()
    result: dict[str, Any] = {"activity_name": activity_name, "task": _notebook_task(activity_name, "replaced")}
    if pipeline is not None:
        result["pipeline"] = pipeline
    (results_dir / "result.json").write_text(json.dumps(result), encoding="utf-8")

    with pytest.raises(ValueError, match=re.escape(ROUTED_AGENTIC_MERGE_REFUSED)):
        merge_agentic_results(report_path, results_dir)
    assert report_path.read_bytes() == combined_report


def test_merge_into_an_authored_combine_pipeline_is_refused(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    assert apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True
    results_dir = tmp_path / "agentic_results"
    _write_merge_result(results_dir, "orders_lfc", "Ingest orders", "ingest_orders")

    with pytest.raises(ValueError, match=re.escape(ROUTED_AGENTIC_MERGE_REFUSED)):
        merge_agentic_results(tmp_path / WORK_DIRNAME / REPORT_FILENAME, results_dir)


def test_combine_requires_route_to_have_applied_the_plan(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    _write_work(tmp_path, _report_two_pipelines())  # convert ran again, so the report has no record

    result = apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])

    assert result["ok"] is False
    assert "routing record" in result["error"]


def test_combine_requires_a_recorded_plan(tmp_path: Path) -> None:
    _write_work(tmp_path, _report_two_pipelines())  # report only; no plan recorded
    result = apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert result["ok"] is False
    assert "conversion_plan.json" in result["error"]


def test_an_invalid_replacement_combine_is_refused_and_keeps_the_applied_one(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    assert apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    combines_path = tmp_path / "metadata" / "agentic_combines.json"
    report_before = report_path.read_bytes()
    combines_before = combines_path.read_bytes()
    dangling = _named_lfc_pipeline("orders_lfc_v2")
    dangling["tasks"][0]["task"] = {"pipeline_task": {"pipeline_id": "${resources.pipelines.ghost.id}"}}

    result = apply_combine_fill(tmp_path, ["parent", "child"], [dangling])

    assert result["ok"] is False
    assert any("dangling_pipeline_reference" in violation for violation in result["violations"])
    assert report_path.read_bytes() == report_before
    assert combines_path.read_bytes() == combines_before


def test_combine_stores_sorted_members_and_hashes_every_pipeline_field(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    assert apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True
    described = _lfc_pipeline()
    described["description"] = "Ingest orders through Lakeflow Connect"

    result = apply_combine_fill(tmp_path, ["parent", "child"], [described])

    assert result["ok"] is True and result["already_combined"] is False
    combines = json.loads((tmp_path / "metadata" / "agentic_combines.json").read_text(encoding="utf-8"))
    assert combines["component-1"]["members"] == ["child", "parent"]
    assert combines["component-1"]["pipelines"] == [described]
    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    assert report["pipelines"] == [described]


def test_combine_replaces_a_hand_edited_store_entry(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    assert apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True
    combines_path = tmp_path / "metadata" / "agentic_combines.json"
    combined = combines_path.read_bytes()
    edited = json.loads(combined)
    edited["component-1"]["pipelines"][0]["tasks"] = []
    combines_path.write_text(json.dumps(edited, indent=2), encoding="utf-8")

    result = apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])

    assert result["ok"] is True and result["already_combined"] is False
    assert combines_path.read_bytes() == combined


@pytest.mark.parametrize(
    "authored_names",
    [["child"], ["orders_lfc", "orders_lfc"]],
    ids=["outside-the-component", "within-the-authored-list"],
)
def test_combine_refuses_authored_names_that_clash(tmp_path: Path, authored_names: list[str]) -> None:
    _route_parent_agentic(tmp_path, _report_two_pipelines())
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    report_before = report_path.read_bytes()
    authored = [_named_lfc_pipeline(name) for name in authored_names]
    for index, pipeline in enumerate(authored):
        pipeline["tasks"][0]["resources"][0]["resource_key"] = f"orders_ingestion_{index}"
        pipeline["tasks"][0]["task"] = {
            "pipeline_task": {"pipeline_id": f"${{resources.pipelines.orders_ingestion_{index}.id}}"}
        }

    result = apply_combine_fill(tmp_path, ["parent"], authored)

    assert result["ok"] is False
    assert "clash" in result["error"]
    assert report_path.read_bytes() == report_before
    assert not (tmp_path / "metadata" / "agentic_combines.json").exists()


def test_combine_same_hash_returns_already_combined_true_with_unchanged_message(tmp_path: Path) -> None:
    """Same authored pipelines hash returns already_combined: True with message 'already applied, unchanged'."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    combines_path = tmp_path / "metadata" / "agentic_combines.json"

    first = apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert first["ok"] is True and first["already_combined"] is False
    report_before = report_path.read_bytes()
    combines_before = combines_path.read_bytes()

    second = apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert second["ok"] is True
    assert second["already_combined"] is True
    assert second.get("message") == "already applied, unchanged"

    assert report_path.read_bytes() == report_before
    assert combines_path.read_bytes() == combines_before


def test_combine_empty_pipelines_list_is_refused(tmp_path: Path) -> None:
    """Empty pipelines list is refused and writes nothing."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    combines_path = tmp_path / "metadata" / "agentic_combines.json"
    report_before = report_path.read_bytes()

    result = apply_combine_fill(tmp_path, ["parent", "child"], [])

    assert result["ok"] is False
    assert "cannot be empty" in result["error"]
    assert report_path.read_bytes() == report_before
    assert not combines_path.exists()


def test_combine_validation_failure_writes_nothing(tmp_path: Path) -> None:
    """When structural validation fails, neither the store nor the report is written."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    combines_path = tmp_path / "metadata" / "agentic_combines.json"
    report_before = report_path.read_bytes()

    bad_pipeline = copy.deepcopy(_lfc_pipeline())
    bad_pipeline["name"] = "orders_lfc_bad"
    bad_task = bad_pipeline["tasks"][0]
    bad_task["depends_on"] = [{"task_key": "nonexistent"}]

    result = apply_combine_fill(tmp_path, ["parent", "child"], [bad_pipeline])

    assert result["ok"] is False
    assert "violations" in result
    assert report_path.read_bytes() == report_before
    assert not combines_path.exists()


def test_combine_re_apply_after_reroute_rebuilds_unchanged(tmp_path: Path) -> None:
    """After a re-route, re-applying the same combine rebuilds from baseline."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME

    first = apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert first["ok"] is True
    report_after_combine = report_path.read_bytes()

    plan = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))
    apply_plan_to_report(tmp_path, plan)
    report_after_reroute = report_path.read_bytes()

    assert report_after_combine == report_after_reroute


def test_merge_agentic_refuses_routed_agentic_pipeline(tmp_path: Path) -> None:
    """merge_agentic refuses a result landing in a routed-agentic pipeline."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])

    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    results_dir = tmp_path / "agentic_results"
    _write_merge_result(results_dir, "parent", "Extract", "extract")

    with pytest.raises(ValueError) as exc:
        merge_agentic_results(report_path, results_dir)
    assert "routed agentic" in str(exc.value)
