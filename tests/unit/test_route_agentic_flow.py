"""Tests for the Phase-1 in-engine agentic conversion flow (route -> alter -> fill).

The confirmed flow, built here:

* ``route`` groups pipelines by connected component (reusing :mod:`flowx.routing`), takes a
  per-component deterministic/agentic decision, records the fingerprint-bound conversion plan, and
  then **edits** the deterministic ``translation_report.json``: for every pipeline in an
  agentic-routed component it removes the deterministic tasks, replaces them with
  ``PlaceholderActivity`` entries, and appends one ``AgenticGap`` per task to ``gaps.json``. Pipelines
  in deterministic components are left byte-identical, and with no agentic decision the report and
  gaps are untouched (the non-breaking guarantee).
* the agent then authors the fill. **Per-pipeline** agentic reuses the existing name-matched
  :func:`flowx.ir_serde.merge_agentic_results`. **Cross-pipeline COMBINE** (N pipelines -> M, e.g. one
  Lakeflow Connect pipeline) uses a new pipeline-grain fill that swaps the routed group's pipelines
  for the agent-authored pipeline(s), which carry ``AgenticComponentActivity`` nodes.
* the merged report is validated structurally via the existing
  :func:`flowx.validate.bundle_invariants.check_bundle_dir` (unique keys, no dangling deps, acyclic,
  no dangling pipeline/run_job references).
"""

from __future__ import annotations

import copy
import inspect
import json
from pathlib import Path
from typing import Any

from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.ir_serde import merge_agentic_results
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.models.ir import ControlEdge, Lineage
from flowx.route_agentic import (
    GAPS_FILENAME,
    REPORT_FILENAME,
    WORK_DIRNAME,
    agentic_pipeline_names,
    alter_report,
    apply_combine_fill,
    apply_plan_to_report,
    combine_group_fill,
    prompt_for_decisions,
    validate_report_structurally,
)
from flowx.routing import record_plan

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
    return build_source_inventory([parent, child], source="unit", source_dir="/tmp/src")


def _setup_routed_agentic(output_dir: Path, *, decision: str = "agentic") -> None:
    """Write inventory + report + a recorded conversion_plan.json routing {child, parent} per ``decision``."""
    _write_work(output_dir, _report_two_pipelines())
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    (metadata / "inventory.json").write_text(json.dumps(_routed_inventory(), indent=2), encoding="utf-8")
    plan = {"components": [{"component_id": "component-1", "members": ["child", "parent"], "decision": decision}]}
    result = record_plan(output_dir, plan=plan)
    assert result["ok"], result


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


def test_per_pipeline_fill_reuses_merge_agentic_results(tmp_path: Path) -> None:
    _write_work(tmp_path, _report_two_pipelines(), gaps=[])
    apply_plan_to_report(tmp_path, _plan(parent="agentic", child="deterministic"))
    report_path = tmp_path / WORK_DIRNAME / REPORT_FILENAME

    results_dir = tmp_path / "agentic_results"
    results_dir.mkdir()
    (results_dir / "extract.json").write_text(
        json.dumps(
            {
                "pipeline": "parent",
                "activity_name": "Extract",
                "task": {
                    "type": "NotebookActivity",
                    "name": "Extract",
                    "task_key": "extract",
                    "notebook_path": "/Workspace/Shared/agentic_extract",
                },
            }
        ),
        encoding="utf-8",
    )
    merged, unmatched = merge_agentic_results(report_path, results_dir)
    assert (merged, unmatched) == (1, 0)

    report = json.loads(report_path.read_text(encoding="utf-8"))
    parent = next(p for p in report["pipelines"] if p["name"] == "parent")
    assert parent["tasks"][0]["type"] == "NotebookActivity"
    assert validate_report_structurally(report).ok


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


def test_combine_idempotent_with_a_differently_named_authored_pipeline(tmp_path: Path) -> None:
    """FIX 3 (a): a second combine whose authored replacement is renamed must NOT duplicate.

    Name-set matching would fail here (the new name is absent from the report, the old members are
    already gone), fall through, and append a second authored pipeline. Provenance keyed on
    (component_id, fingerprint) catches the re-run regardless of the authored name.
    """
    _setup_routed_agentic(tmp_path, decision="agentic")

    first = apply_combine_fill(tmp_path, ["parent", "child"], [_named_lfc_pipeline("orders_lfc")])
    assert first["ok"] is True and first["already_combined"] is False

    # Same members, but the authored replacement is named differently this time.
    second = apply_combine_fill(tmp_path, ["parent", "child"], [_named_lfc_pipeline("orders_lfc_v2")])
    assert second["ok"] is True
    assert second["already_combined"] is True

    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    names = [pipeline["name"] for pipeline in report["pipelines"]]
    assert names == ["orders_lfc"]  # first authored pipeline kept; the renamed re-run added nothing


def test_combine_idempotent_when_authored_name_collides_with_a_former_member(tmp_path: Path) -> None:
    """FIX 3 (b): an authored name colliding with a former member is still detected as already-combined.

    After the first combine the report holds a pipeline named 'parent' (a former member). Name-based
    detection would see 'parent' present and conclude the combine had not happened; provenance keeps
    the detection correct and independent of names.
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


def test_combine_requires_a_recorded_plan(tmp_path: Path) -> None:
    _write_work(tmp_path, _report_two_pipelines())  # report only; no plan recorded
    result = apply_combine_fill(tmp_path, ["parent", "child"], [_lfc_pipeline()])
    assert result["ok"] is False
    assert "conversion_plan.json" in result["error"]


def test_apply_combine_fill_has_no_validation_bypass() -> None:
    # There must be no surface that writes the combined report without structural validation.
    assert "validate" not in inspect.signature(apply_combine_fill).parameters
