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

from flowx.adapter.__main__ import main as adapter_main
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
    apply_agentic_output,
    apply_plan,
    apply_plan_to_report,
    replace_unit_pipelines,
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


def _route_two_components(output_dir: Path, *, parent: str, child: str) -> None:
    """Record a plan deciding 'parent' and 'child' (each its own component) as given, and apply it."""
    inventory = json.loads((output_dir / "metadata" / "inventory.json").read_text(encoding="utf-8"))
    components = [
        {
            "component_id": component["component_id"],
            "members": component["members"],
            "decision": parent if component["members"] == ["parent"] else child,
        }
        for component in build_recommendation(inventory)["components"]
    ]
    assert record_plan(output_dir, plan={"components": components})["ok"] is True
    recorded = ConversionPlan.load(output_dir)
    assert recorded is not None
    apply_plan(output_dir, recorded)


def _route_parent_agentic(output_dir: Path, report: dict[str, Any], *, child: str = "deterministic") -> None:
    """Write ``report`` and the two-component inventory, then route 'parent' agentic and 'child' as given."""
    _write_work(output_dir, report, gaps=[])
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    (metadata / "inventory.json").write_text(json.dumps(_two_component_inventory(), indent=2), encoding="utf-8")
    _route_two_components(output_dir, parent="agentic", child=child)


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


def _if_with_nested_gap(name: str, task_key: str) -> dict[str, Any]:
    """An IfCondition whose true branch holds convert's own placeholder for an unsupported activity."""
    return {
        "name": name,
        "task_key": task_key,
        "type": "IfConditionActivity",
        "if_true_activities": [{"name": "Nested Gap", "task_key": "nested_gap", "type": "PlaceholderActivity"}],
    }


def _until_with_nested_gap(name: str, task_key: str) -> dict[str, Any]:
    """Convert's placeholder for an Until, whose loop body holds an unsupported activity only in its ADF JSON."""
    nested = {"name": "Nested Gap", "type": "Script", "typeProperties": {}}
    raw_definition = {
        "name": name,
        "type": "Until",
        "typeProperties": {
            "expression": {"value": "@equals(1, 1)", "type": "Expression"},
            "activities": [{"name": "Gate", "type": "IfCondition", "typeProperties": {"ifTrueActivities": [nested]}}],
        },
    }
    return {
        "name": name,
        "task_key": task_key,
        "type": "PlaceholderActivity",
        "original_type": "Until",
        "raw_definition": raw_definition,
    }


@pytest.mark.parametrize("container", [_if_with_nested_gap, _until_with_nested_gap])
def test_alter_report_supersedes_an_untagged_gap_nested_inside_a_routed_container(container: Any) -> None:
    report = {"pipelines": [{"name": "parent", "tasks": [container("Check", "check")]}]}
    nested = {"activity_name": "Nested Gap", "activity_type": "Script", "raw_definition": None}

    _report, gaps = alter_report(report, [nested], {"parent"})

    assert nested not in gaps
    assert [(gap["pipeline"], gap["activity_name"]) for gap in gaps] == [("parent", "Check")]


@pytest.mark.parametrize("other_container", [_if_with_nested_gap, _until_with_nested_gap])
def test_alter_report_keeps_a_nested_gap_whose_name_a_non_routed_container_also_holds(other_container: Any) -> None:
    report = {
        "pipelines": [
            {"name": "parent", "tasks": [_if_with_nested_gap("Check", "check")]},
            {"name": "other", "tasks": [other_container("Wait", "wait")]},
        ]
    }
    nested = {"activity_name": "Nested Gap", "activity_type": "Script", "raw_definition": None}

    _report, gaps = alter_report(report, [nested], {"parent"})

    assert nested in gaps


@pytest.mark.parametrize("container", [_if_with_nested_gap, _until_with_nested_gap])
def test_a_filled_unit_with_a_nested_convert_gap_leaves_no_gaps(tmp_path: Path, container: Any) -> None:
    report = _report_two_pipelines()
    report["pipelines"][0]["tasks"] = [container("Check", "check")]
    _write_work(tmp_path, report, gaps=[{"activity_name": "Nested Gap", "activity_type": "Script"}])
    metadata = tmp_path / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    (metadata / "inventory.json").write_text(json.dumps(_two_component_inventory(), indent=2), encoding="utf-8")
    _route_two_components(tmp_path, parent="agentic", child="deterministic")

    assert apply_agentic_output(tmp_path, ["parent"], [_named_lfc_pipeline("parent_lfc")])["ok"] is True

    assert json.loads((tmp_path / WORK_DIRNAME / GAPS_FILENAME).read_text(encoding="utf-8")) == []


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


def test_a_merged_top_level_gap_is_stored_apart_and_survives_a_re_route_with_the_baseline_unchanged(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report = _report_with_child_gap()
    for pipeline in report["pipelines"]:
        pipeline["tags"] = {"source": "adf"}
    _route_parent_agentic(tmp_path, report)
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    baseline_path = tmp_path / WORK_DIRNAME / "route_baseline" / REPORT_FILENAME
    baseline_before = baseline_path.read_bytes()
    record_before = routing_record(json.loads(report_path.read_text(encoding="utf-8")))
    results_dir = tmp_path / "agentic_results"
    _write_merge_result(results_dir, "child", "Load", "load")
    assert merge_agentic_results(report_path, results_dir) == (1, 0)

    assert baseline_path.read_bytes() == baseline_before
    record_after = routing_record(json.loads(report_path.read_text(encoding="utf-8")))
    assert record_before is not None and record_after is not None
    assert record_after["gap_fills_sha256"] != record_before["gap_fills_sha256"]
    assert record_after == {**record_before, "gap_fills_sha256": record_after["gap_fills_sha256"]}
    stored = json.loads((tmp_path / "metadata" / "agentic_conversion.json").read_text(encoding="utf-8"))
    assert [(fill["pipeline"], fill["activity_name"]) for fill in stored["gap_fills"]] == [("child", "Load")]

    recorded = ConversionPlan.load(tmp_path)
    assert recorded is not None
    assert apply_plan(tmp_path, recorded)["altered"] is False
    child = next(p for p in json.loads(report_path.read_text(encoding="utf-8"))["pipelines"] if p["name"] == "child")
    assert child["tasks"][0]["type"] == "NotebookActivity"
    assert baseline_path.read_bytes() == baseline_before
    _route_two_components(tmp_path, parent="deterministic", child="deterministic")
    capsys.readouterr()
    package_main(["--output-dir", str(tmp_path), "--no-download-workspace-files", "--keep-intermediates"])
    assert "preflight failed" not in capsys.readouterr().err
    assert (tmp_path / "child" / "databricks.yml").exists()


def test_a_gap_fill_survives_switching_every_component_back_to_deterministic(tmp_path: Path) -> None:
    _route_parent_agentic(tmp_path, _report_with_child_gap())
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    results_dir = tmp_path / "agentic_results"
    _write_merge_result(results_dir, "child", "Load", "load")
    assert merge_agentic_results(report_path, results_dir) == (1, 0)

    _route_two_components(tmp_path, parent="deterministic", child="deterministic")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    child = next(p for p in report["pipelines"] if p["name"] == "child")
    assert child["tasks"][0]["type"] == "NotebookActivity"
    assert _record_outcomes(report_path) == {"component-1": "deterministic", "component-2": "deterministic"}


def test_a_re_convert_drops_gap_fills_merged_against_the_old_baseline(tmp_path: Path) -> None:
    _route_parent_agentic(tmp_path, _report_with_child_gap())
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    results_dir = tmp_path / "agentic_results"
    _write_merge_result(results_dir, "child", "Load", "load")
    assert merge_agentic_results(report_path, results_dir) == (1, 0)
    reconverted = _report_with_child_gap()
    reconverted["pipelines"][0]["tasks"][0]["task_key"] = "extract_again"
    _write_work(tmp_path, reconverted, gaps=[])

    recorded = ConversionPlan.load(tmp_path)
    assert recorded is not None
    assert apply_plan(tmp_path, recorded)["dropped_gap_fills"] == 1

    stored = json.loads((tmp_path / "metadata" / "agentic_conversion.json").read_text(encoding="utf-8"))
    assert stored["gap_fills"] == []
    child = next(p for p in json.loads(report_path.read_text(encoding="utf-8"))["pipelines"] if p["name"] == "child")
    assert child["tasks"][0]["type"] == "PlaceholderActivity"


def test_a_merge_written_to_a_separate_copy_leaves_the_baseline_and_live_report_alone(tmp_path: Path) -> None:
    _route_parent_agentic(tmp_path, _report_with_child_gap())
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    baseline_path = tmp_path / WORK_DIRNAME / "route_baseline" / REPORT_FILENAME
    live_before = report_path.read_bytes()
    baseline_before = baseline_path.read_bytes()
    results_dir = tmp_path / "agentic_results"
    _write_merge_result(results_dir, "child", "Load", "load")
    copy_path = tmp_path / WORK_DIRNAME / "merged.json"

    assert merge_agentic_results(report_path, results_dir, copy_path) == (1, 0)

    assert report_path.read_bytes() == live_before
    assert baseline_path.read_bytes() == baseline_before
    merged_copy = json.loads(copy_path.read_text(encoding="utf-8"))
    child = next(p for p in merged_copy["pipelines"] if p["name"] == "child")
    assert child["tasks"][0]["type"] == "NotebookActivity"
    assert routing_record(merged_copy) == routing_record(json.loads(live_before))


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


def test_a_re_convert_without_gaps_clears_the_gaps_an_agentic_route_wrote(tmp_path: Path) -> None:
    pipelines = tmp_path / "export" / "pipelines"
    pipelines.mkdir(parents=True)
    wait = {"name": "pause", "type": "Wait", "dependsOn": [], "typeProperties": {"waitTimeInSeconds": 1}}
    document = {"name": "orders", "properties": {"activities": [wait]}}
    (pipelines / "orders.json").write_text(json.dumps(document), encoding="utf-8")
    output_dir = tmp_path / "out"
    source = ["--source", "adf", "--source-path", str(tmp_path / "export"), "--output-dir", str(output_dir)]
    report_path = output_dir / WORK_DIRNAME / REPORT_FILENAME
    gaps_path = output_dir / WORK_DIRNAME / GAPS_FILENAME
    plan_path = tmp_path / "plan.json"

    def route(decision: str) -> None:
        plan = {"components": [{"component_id": "component-1", "members": ["orders"], "decision": decision}]}
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        assert adapter_main(["route", "--output-dir", str(output_dir), "--plan-path", str(plan_path)]) == 0

    assert adapter_main(["discover", *source]) == 0
    assert adapter_main(["convert", *source]) == 0
    route("agentic")
    assert [gap["pipeline"] for gap in json.loads(gaps_path.read_text(encoding="utf-8"))] == ["orders"]
    assert adapter_main(["convert", *source]) == 0
    reconverted_report = report_path.read_bytes()

    route("deterministic")

    assert not gaps_path.exists()
    assert report_path.read_bytes() == reconverted_report


def test_reroute_under_the_same_decisions_keeps_fills_and_updates_the_plan_hash(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    assert apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True
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


def test_replace_unit_pipelines_replaces_group_pipelines_with_authored_ones() -> None:
    report = _report_two_pipelines()
    merged = replace_unit_pipelines(report, {"parent", "child"}, [_lfc_pipeline()])
    names = [pipeline["name"] for pipeline in merged["pipelines"]]
    assert names == ["orders_lfc"]


def test_replace_unit_pipelines_keeps_pipelines_outside_the_group() -> None:
    report = {
        "pipelines": [
            {"name": "parent", "tasks": [_copy_task("Extract", "extract")]},
            {"name": "child", "tasks": [_notebook_task("Load", "load")]},
            {"name": "unrelated", "tasks": [_notebook_task("Keep", "keep")]},
        ]
    }
    merged = replace_unit_pipelines(report, {"parent", "child"}, [_lfc_pipeline()])
    names = sorted(pipeline["name"] for pipeline in merged["pipelines"])
    assert names == ["orders_lfc", "unrelated"]


def test_combine_fill_of_two_pipelines_into_one_lfc_passes_structural_validation() -> None:
    report = _report_two_pipelines()
    merged = replace_unit_pipelines(report, {"parent", "child"}, [_lfc_pipeline()])
    result = validate_report_structurally(merged)
    assert result.ok, [f"{finding.code}: {finding.message}" for finding in result.findings]


def test_combine_fill_with_a_dangling_pipeline_reference_is_caught() -> None:
    dangling = _lfc_pipeline()
    # Point the pipeline_task at a resource that is never declared in this activity's resources list.
    dangling["tasks"][0]["task"] = {"pipeline_task": {"pipeline_id": "${resources.pipelines.ghost.id}"}}
    merged = replace_unit_pipelines(_report_two_pipelines(), {"parent", "child"}, [dangling])
    result = validate_report_structurally(merged)
    assert not result.ok
    assert any(finding.code == "dangling_pipeline_reference" for finding in result.violations)


def test_apply_agentic_output_writes_merged_report_when_valid(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    result = apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert result["ok"] is True
    assert result["component_id"] == "component-1"
    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    assert [pipeline["name"] for pipeline in report["pipelines"]] == ["orders_lfc"]


def test_apply_agentic_output_rejects_and_does_not_write_on_dangling_reference(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    report_before = (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes()
    dangling = _lfc_pipeline()
    dangling["tasks"][0]["task"] = {"pipeline_task": {"pipeline_id": "${resources.pipelines.ghost.id}"}}

    result = apply_agentic_output(tmp_path, ["parent", "child"], [dangling])
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

    result = apply_agentic_output(tmp_path, ["parent", "child"], [untagged])

    assert result["ok"] is False
    assert any("tags.source" in violation and "adf" in violation for violation in result["violations"])
    # Nothing is written when the source tag is missing.
    assert (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes() == report_before


def test_combine_rejects_an_authored_pipeline_with_wrong_source_tag(tmp_path: Path) -> None:
    """A non-'adf' source tag is rejected the same way (routing/agentic is ADF-only)."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    mistagged = _lfc_pipeline()
    mistagged["tags"] = {"source": "airflow"}

    result = apply_agentic_output(tmp_path, ["parent", "child"], [mistagged])

    assert result["ok"] is False
    assert any("tags.source" in violation for violation in result["violations"])


def test_combine_is_idempotent_running_twice_yields_no_duplicate(tmp_path: Path) -> None:
    """FIX 3: re-running combine with the same members + authored pipeline does not duplicate it."""
    _setup_routed_agentic(tmp_path, decision="agentic")

    first = apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert first["ok"] is True
    assert first["already_applied"] is False

    # The recorded plan still lists {parent, child}, so the plan/membership check passes again; the
    # report, however, now contains only the authored pipeline. A naive re-run would re-append it.
    second = apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert second["ok"] is True
    assert second["already_applied"] is True

    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    names = [pipeline["name"] for pipeline in report["pipelines"]]
    assert names == ["orders_lfc"]  # exactly one authored pipeline, no duplicate


def test_a_different_combine_on_an_applied_component_replaces_and_rebuilds(tmp_path: Path) -> None:
    """A second combine with a different hash replaces the stored entry and rebuilds."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    first = apply_agentic_output(tmp_path, ["parent", "child"], [_named_lfc_pipeline("orders_lfc")])
    assert first["ok"] is True and first["already_applied"] is False
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    first_names = [p["name"] for p in json.loads(report_path.read_text(encoding="utf-8"))["pipelines"]]
    assert first_names == ["orders_lfc"]

    second = apply_agentic_output(tmp_path, ["parent", "child"], [_named_lfc_pipeline("orders_lfc_v2")])

    assert second["ok"] is True
    assert second["already_applied"] is False
    second_names = [p["name"] for p in json.loads(report_path.read_text(encoding="utf-8"))["pipelines"]]
    assert second_names == ["orders_lfc_v2"]


def test_combine_idempotent_when_authored_name_collides_with_a_former_member(tmp_path: Path) -> None:
    """FIX 3 (b): an authored name colliding with a former member is still detected as already-combined.

    After the first combine the report holds a pipeline named 'parent' (a former member). Name-based
    detection would see 'parent' present and conclude the combine had not happened; the routing record
    keeps the detection correct and independent of names.
    """
    _setup_routed_agentic(tmp_path, decision="agentic")

    first = apply_agentic_output(tmp_path, ["parent", "child"], [_named_lfc_pipeline("parent")])
    assert first["ok"] is True and first["already_applied"] is False

    second = apply_agentic_output(tmp_path, ["parent", "child"], [_named_lfc_pipeline("parent")])
    assert second["ok"] is True
    assert second["already_applied"] is True

    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    names = [pipeline["name"] for pipeline in report["pipelines"]]
    assert names == ["parent"]  # exactly one authored pipeline, no duplicate


# --------------------------------------------------------------------------- #
# Combine membership is bound to the recorded, fingerprint-bound plan.
# --------------------------------------------------------------------------- #


def test_combine_rejects_a_deterministic_component(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="deterministic")
    result = apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert result["ok"] is False
    assert "not agentic" in result["error"]
    # A deterministic component is never swapped out.
    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    assert sorted(pipeline["name"] for pipeline in report["pipelines"]) == ["child", "parent"]


def test_combine_rejects_a_partial_member_set(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    result = apply_agentic_output(tmp_path, ["parent"], [_lfc_pipeline()])
    assert result["ok"] is False
    assert "do not exactly match" in result["error"]


def test_combine_rejects_a_superset_member_set(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    result = apply_agentic_output(tmp_path, ["parent", "child", "extra"], [_lfc_pipeline()])
    assert result["ok"] is False
    assert "do not exactly match" in result["error"]


def test_combine_rejects_a_typoed_member(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    result = apply_agentic_output(tmp_path, ["parent", "chld"], [_lfc_pipeline()])
    assert result["ok"] is False
    assert "do not exactly match" in result["error"]


def test_combine_rejects_a_stale_fingerprint_plan(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    # Mutate the inventory after recording so the recorded fingerprint no longer matches.
    inventory_path = tmp_path / "metadata" / "inventory.json"
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    inventory["pipelines"].append({"name": "late_addition", "activities": [], "motifs": []})
    inventory_path.write_text(json.dumps(inventory, indent=2), encoding="utf-8")

    result = apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert result["ok"] is False
    assert "stale" in result["error"]


def test_combine_marks_the_component_applied(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    assert _record_outcomes(report_path) == {"component-1": "agentic-not-viable"}

    assert apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True

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
    assert apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True
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

    first = apply_agentic_output(tmp_path, ["parent", "child"], _colliding_authored_pipelines())
    assert first["ok"] is True and first["already_applied"] is False
    second = apply_agentic_output(tmp_path, ["parent", "child"], _colliding_authored_pipelines())

    assert second["ok"] is True
    assert second["already_applied"] is True
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
    assert apply_agentic_output(tmp_path, ["parent", "child"], _colliding_authored_pipelines())["ok"] is True
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
    assert apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True
    results_dir = tmp_path / "agentic_results"
    _write_merge_result(results_dir, "orders_lfc", "Ingest orders", "ingest_orders")

    with pytest.raises(ValueError, match=re.escape(ROUTED_AGENTIC_MERGE_REFUSED)):
        merge_agentic_results(tmp_path / WORK_DIRNAME / REPORT_FILENAME, results_dir)


def test_combine_requires_route_to_have_applied_the_plan(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    _write_work(tmp_path, _report_two_pipelines())  # convert ran again, so the report has no record

    result = apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])

    assert result["ok"] is False
    assert "routing record" in result["error"]


def test_combine_requires_a_recorded_plan(tmp_path: Path) -> None:
    _write_work(tmp_path, _report_two_pipelines())  # report only; no plan recorded
    result = apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert result["ok"] is False
    assert "conversion_plan.json" in result["error"]


def test_an_invalid_replacement_combine_is_refused_and_keeps_the_applied_one(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    assert apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    combines_path = tmp_path / "metadata" / "agentic_conversion.json"
    report_before = report_path.read_bytes()
    combines_before = combines_path.read_bytes()
    dangling = _named_lfc_pipeline("orders_lfc_v2")
    dangling["tasks"][0]["task"] = {"pipeline_task": {"pipeline_id": "${resources.pipelines.ghost.id}"}}

    result = apply_agentic_output(tmp_path, ["parent", "child"], [dangling])

    assert result["ok"] is False
    assert any("dangling_pipeline_reference" in violation for violation in result["violations"])
    assert report_path.read_bytes() == report_before
    assert combines_path.read_bytes() == combines_before


def test_combine_stores_sorted_members_and_hashes_every_pipeline_field(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    assert apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True
    described = _lfc_pipeline()
    described["description"] = "Ingest orders through Lakeflow Connect"

    result = apply_agentic_output(tmp_path, ["parent", "child"], [described])

    assert result["ok"] is True and result["already_applied"] is False
    combines = json.loads((tmp_path / "metadata" / "agentic_conversion.json").read_text(encoding="utf-8"))
    assert combines["components"]["component-1"]["members"] == ["child", "parent"]
    assert combines["components"]["component-1"]["pipelines"] == [described]
    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    assert report["pipelines"] == [described]


def test_combine_replaces_a_hand_edited_store_entry(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    assert apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True
    combines_path = tmp_path / "metadata" / "agentic_conversion.json"
    combined = combines_path.read_bytes()
    edited = json.loads(combined)
    edited["components"]["component-1"]["pipelines"][0]["tasks"] = []
    combines_path.write_text(json.dumps(edited, indent=2), encoding="utf-8")

    result = apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])

    assert result["ok"] is True and result["already_applied"] is False
    assert combines_path.read_bytes() == combined


def test_a_combine_routed_back_to_deterministic_records_no_combine_hash(tmp_path: Path) -> None:
    _route_parent_agentic(tmp_path, _report_two_pipelines(), child="agentic")
    assert apply_agentic_output(tmp_path, ["parent"], [_named_lfc_pipeline("parent_lfc")])["ok"] is True

    _route_two_components(tmp_path, parent="deterministic", child="agentic")

    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    record = routing_record(report)
    assert record is not None
    (parent_entry,) = [entry for entry in record["components"].values() if entry["members"] == ["parent"]]
    assert parent_entry["outcome"] == "deterministic"
    assert parent_entry["output_sha256"] is None
    assert [pipeline["name"] for pipeline in report["pipelines"]] == ["parent", "child"]


def test_combine_refuses_a_name_that_another_components_combine_would_drop(tmp_path: Path) -> None:
    _route_parent_agentic(tmp_path, _report_two_pipelines(), child="agentic")
    child_lfc = _named_lfc_pipeline("child_lfc")
    child_lfc["tasks"][0]["resources"][0]["resource_key"] = "child_ingestion"
    child_lfc["tasks"][0]["task"] = {"pipeline_task": {"pipeline_id": "${resources.pipelines.child_ingestion.id}"}}
    assert apply_agentic_output(tmp_path, ["child"], [child_lfc])["ok"] is True
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    combines_path = tmp_path / "metadata" / "agentic_conversion.json"
    report_before = report_path.read_bytes()
    combines_before = combines_path.read_bytes()

    result = apply_agentic_output(tmp_path, ["parent"], [_named_lfc_pipeline("child")])

    assert result["ok"] is False
    assert "clash" in result["error"]
    assert report_path.read_bytes() == report_before
    assert combines_path.read_bytes() == combines_before


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

    result = apply_agentic_output(tmp_path, ["parent"], authored)

    assert result["ok"] is False
    assert "clash" in result["error"]
    assert report_path.read_bytes() == report_before
    assert not (tmp_path / "metadata" / "agentic_conversion.json").exists()


def test_combine_same_hash_returns_already_applied_true_with_unchanged_message(tmp_path: Path) -> None:
    """Same authored pipelines hash returns already_applied: True with message 'already applied, unchanged'."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    combines_path = tmp_path / "metadata" / "agentic_conversion.json"

    first = apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert first["ok"] is True and first["already_applied"] is False
    report_before = report_path.read_bytes()
    combines_before = combines_path.read_bytes()

    second = apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert second["ok"] is True
    assert second["already_applied"] is True
    assert second.get("message") == "already applied, unchanged"

    assert report_path.read_bytes() == report_before
    assert combines_path.read_bytes() == combines_before


def test_combine_empty_pipelines_list_is_refused(tmp_path: Path) -> None:
    """Empty pipelines list is refused and writes nothing."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    combines_path = tmp_path / "metadata" / "agentic_conversion.json"
    report_before = report_path.read_bytes()

    result = apply_agentic_output(tmp_path, ["parent", "child"], [])

    assert result["ok"] is False
    assert "cannot be empty" in result["error"]
    assert report_path.read_bytes() == report_before
    assert not combines_path.exists()


def test_combine_validation_failure_writes_nothing(tmp_path: Path) -> None:
    """When structural validation fails, neither the store nor the report is written."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    combines_path = tmp_path / "metadata" / "agentic_conversion.json"
    report_before = report_path.read_bytes()

    bad_pipeline = copy.deepcopy(_lfc_pipeline())
    bad_pipeline["name"] = "orders_lfc_bad"
    bad_task = bad_pipeline["tasks"][0]
    bad_task["depends_on"] = [{"task_key": "nonexistent"}]

    result = apply_agentic_output(tmp_path, ["parent", "child"], [bad_pipeline])

    assert result["ok"] is False
    assert "violations" in result
    assert report_path.read_bytes() == report_before
    assert not combines_path.exists()


def test_combine_re_apply_after_reroute_rebuilds_unchanged(tmp_path: Path) -> None:
    """After a re-route, re-applying the same combine rebuilds from baseline."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME

    first = apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert first["ok"] is True
    report_after_combine = report_path.read_bytes()

    plan = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))
    apply_plan_to_report(tmp_path, plan)
    report_after_reroute = report_path.read_bytes()

    assert report_after_combine == report_after_reroute


def test_merge_agentic_refuses_routed_agentic_pipeline(tmp_path: Path) -> None:
    """merge_agentic refuses a result landing in a routed-agentic pipeline."""
    _setup_routed_agentic(tmp_path, decision="agentic")
    apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])

    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    results_dir = tmp_path / "agentic_results"
    _write_merge_result(results_dir, "parent", "Extract", "extract")

    with pytest.raises(ValueError) as exc:
        merge_agentic_results(report_path, results_dir)
    assert "routed agentic" in str(exc.value)
    assert "fill-agentic" in str(exc.value)
    assert "re-run convert" not in str(exc.value) and "combine" not in str(exc.value)


# --------------------------------------------------------------------------- #
# The agent's output history, gaps of applied units, and fill-time checks.
# --------------------------------------------------------------------------- #


def _stored_output(output_dir: Path, unit_id: str) -> dict[str, Any]:
    stored = json.loads((output_dir / "metadata" / "agentic_conversion.json").read_text(encoding="utf-8"))
    return stored["components"][unit_id]


def test_the_first_fill_is_not_a_replacement_and_a_different_output_is(tmp_path: Path) -> None:
    _setup_routed_agentic(tmp_path, decision="agentic")
    assert apply_agentic_output(tmp_path, ["parent", "child"], [_lfc_pipeline()])["ok"] is True
    first = _stored_output(tmp_path, "component-1")
    assert first["replaced"] == []
    described = _lfc_pipeline()
    described["description"] = "a different pattern"

    assert apply_agentic_output(tmp_path, ["parent", "child"], [described])["ok"] is True

    second = _stored_output(tmp_path, "component-1")
    assert second["replaced"] == [{"from": first["output_sha256"], "to": second["output_sha256"]}]
    record = routing_record(json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8")))
    assert record is not None and "replacements" not in record["components"]["component-1"]


def test_the_output_history_survives_switching_to_deterministic_and_back(tmp_path: Path) -> None:
    _route_parent_agentic(tmp_path, _report_two_pipelines())
    assert apply_agentic_output(tmp_path, ["parent"], [_named_lfc_pipeline("parent_lfc")])["ok"] is True
    changed = _named_lfc_pipeline("parent_lfc")
    changed["description"] = "changed"
    assert apply_agentic_output(tmp_path, ["parent"], [changed])["ok"] is True

    _route_two_components(tmp_path, parent="deterministic", child="deterministic")
    assert routing_record(json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))) is None
    _route_two_components(tmp_path, parent="agentic", child="deterministic")

    assert len(_stored_output(tmp_path, "component-2")["replaced"]) == 1
    assert _record_outcomes(tmp_path / WORK_DIRNAME / REPORT_FILENAME)["component-2"] == "agentic-applied"


def test_an_applied_unit_leaves_no_routed_gaps(tmp_path: Path) -> None:
    _route_parent_agentic(tmp_path, _report_two_pipelines())
    gaps_path = tmp_path / WORK_DIRNAME / GAPS_FILENAME
    assert [gap["pipeline"] for gap in json.loads(gaps_path.read_text(encoding="utf-8"))] == ["parent"]

    assert apply_agentic_output(tmp_path, ["parent"], [_named_lfc_pipeline("parent_lfc")])["ok"] is True

    assert json.loads(gaps_path.read_text(encoding="utf-8")) == []


@pytest.mark.parametrize(
    "authored_names",
    [["Child"], ["Sales Load", "Sales-Load"]],
    ids=["with-a-deterministic-pipeline", "within-the-authored-list"],
)
def test_fill_refuses_names_that_share_a_bundle_folder(tmp_path: Path, authored_names: list[str]) -> None:
    _route_parent_agentic(tmp_path, _report_two_pipelines())
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    report_before = report_path.read_bytes()
    authored = [_named_lfc_pipeline(name) for name in authored_names]
    for index, pipeline in enumerate(authored):
        pipeline["tasks"][0]["resources"][0]["resource_key"] = f"ingestion_{index}"
        pipeline["tasks"][0]["task"] = {
            "pipeline_task": {"pipeline_id": f"${{resources.pipelines.ingestion_{index}.id}}"}
        }

    result = apply_agentic_output(tmp_path, ["parent"], authored)

    assert result["ok"] is False
    assert any("shares the bundle folder" in violation for violation in result["violations"])
    assert report_path.read_bytes() == report_before
    assert not (tmp_path / "metadata" / "agentic_conversion.json").exists()


def test_fill_refuses_an_authored_pipeline_that_still_holds_a_placeholder(tmp_path: Path) -> None:
    _route_parent_agentic(tmp_path, _report_two_pipelines())
    authored = _named_lfc_pipeline("parent_lfc")
    authored["tasks"].append(
        {
            "name": "Loop",
            "task_key": "loop",
            "type": "ForEachActivity",
            "inner_activities": [{"name": "Todo", "task_key": "todo", "type": "PlaceholderActivity"}],
        }
    )

    result = apply_agentic_output(tmp_path, ["parent"], [authored])

    assert result["ok"] is False
    assert result["violations"] == [
        "parent_lfc: still has placeholder tasks ['Todo']; convert every task before filling the unit"
    ]


def test_route_refuses_a_report_converted_from_a_different_discover(tmp_path: Path) -> None:
    stale = _report_two_pipelines()
    stale["pipelines"][1]["name"] = "removed_since"
    _write_work(tmp_path, stale, gaps=[])
    metadata = tmp_path / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    (metadata / "inventory.json").write_text(json.dumps(_two_component_inventory(), indent=2), encoding="utf-8")
    report_before = (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes()

    with pytest.raises(ValueError, match=r"different discover \(missing \['child'\], extra \['removed_since'\]\)"):
        _route_two_components(tmp_path, parent="agentic", child="deterministic")
    assert (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes() == report_before


def test_a_pipeline_without_activities_is_not_taken_for_a_different_discover(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """ADF discover leaves a pipeline with no activities out of the inventory; convert still writes it."""
    pipelines = tmp_path / "export" / "pipelines"
    pipelines.mkdir(parents=True)
    wait = {"name": "pause", "type": "Wait", "dependsOn": [], "typeProperties": {"waitTimeInSeconds": 1}}
    for name, activities in (("empty", []), ("orders", [wait])):
        document = {"name": name, "properties": {"activities": activities}}
        (pipelines / f"{name}.json").write_text(json.dumps(document), encoding="utf-8")
    output_dir = tmp_path / "out"
    source = ["--source", "adf", "--source-path", str(tmp_path / "export"), "--output-dir", str(output_dir)]
    assert adapter_main(["discover", *source]) == 0
    assert adapter_main(["convert", *source]) == 0
    plan_path = tmp_path / "plan.json"
    plan = {"components": [{"component_id": "component-1", "members": ["orders"], "decision": "agentic"}]}
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    capsys.readouterr()

    assert adapter_main(["route", "--output-dir", str(output_dir), "--plan-path", str(plan_path)]) == 0

    assert _record_outcomes(output_dir / WORK_DIRNAME / REPORT_FILENAME) == {"component-1": "agentic-not-viable"}
    capsys.readouterr()
    assert package_main(["--output-dir", str(output_dir), "--no-download-workspace-files"]) == 1
    error = capsys.readouterr().err
    assert "different discover" not in error
    assert "component 'component-1' (orders) is routed agentic but has no agent output yet" in error


def test_package_asks_to_re_run_modify_after_a_merge_made_since_modify(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report = _report_with_child_gap()
    for pipeline in report["pipelines"]:
        pipeline["tags"] = {"source": "adf"}
    _route_parent_agentic(tmp_path, report)
    assert apply_agentic_output(tmp_path, ["parent"], [_named_lfc_pipeline("parent_lfc")])["ok"] is True
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME
    assert adapter_main(["modify", str(report_path), "--output-dir", str(tmp_path)]) == 0
    results_dir = tmp_path / "agentic_results"
    _write_merge_result(results_dir, "child", "Load", "load")
    assert merge_agentic_results(report_path, results_dir) == (1, 0)
    capsys.readouterr()

    assert package_main(["--output-dir", str(tmp_path), "--no-download-workspace-files", "--keep-intermediates"]) == 1

    assert "the configured report is out of date; re-run modify" in capsys.readouterr().err
    assert not (tmp_path / "child" / "databricks.yml").exists()
    assert adapter_main(["modify", str(report_path), "--output-dir", str(tmp_path)]) == 0
    assert package_main(["--output-dir", str(tmp_path), "--no-download-workspace-files", "--keep-intermediates"]) == 0
    audit = json.loads((tmp_path / "metadata" / "route_audit.json").read_text(encoding="utf-8"))
    assert [(fill["pipeline"], fill["activity_name"]) for fill in audit["gap_fills"]] == [("child", "Load")]


def test_package_refuses_an_edited_gap_fill(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    report = _report_with_child_gap()
    for pipeline in report["pipelines"]:
        pipeline["tags"] = {"source": "adf"}
    _route_parent_agentic(tmp_path, report)
    results_dir = tmp_path / "agentic_results"
    _write_merge_result(results_dir, "child", "Load", "load")
    assert merge_agentic_results(tmp_path / WORK_DIRNAME / REPORT_FILENAME, results_dir) == (1, 0)
    store_path = tmp_path / "metadata" / "agentic_conversion.json"
    store = json.loads(store_path.read_text(encoding="utf-8"))
    store["gap_fills"][0]["task"]["notebook_path"] = "/Workspace/Shared/tampered"
    store_path.write_text(json.dumps(store), encoding="utf-8")
    capsys.readouterr()

    assert package_main(["--output-dir", str(tmp_path), "--no-download-workspace-files"]) == 1

    assert "agentic_conversion.json has been edited (gap fill 'Load')" in capsys.readouterr().err
