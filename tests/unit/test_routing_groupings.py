"""Suggested groupings: whole components the insights link, converted together agentically as one unit.

Route suggests a grouping from two kinds of link in the enriched insights: an ``inferred`` pipeline
relationship between pipelines of different components, and one simplification pattern recommended
for pipelines in more than one component. Linked components form one suggestion (so suggestions
never overlap); the user accepts one by setting ``accepted`` and deciding its components agentic, and
it then routes, fills, packages and audits as one unit.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from flowx import routing
from flowx.adapter.__main__ import main as adapter_main
from flowx.bundler.dab_writer import main as package_main
from flowx.discovery_insights import enrich_inventory
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.ir_serde import pipeline_to_dict
from flowx.models.conversion_plan import ConversionPlan
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.models.ir import ControlEdge, Lineage, NotebookActivity, Pipeline
from flowx.route_agentic import REPORT_FILENAME, WORK_DIRNAME, apply_agentic_output, apply_plan, routing_record

_PIPELINES = ("child", "extract_a", "extract_b", "extract_c", "mart", "orchestrator")
_LAKEFLOW_CONNECT = "Lakeflow Connect SQL Server connector"
_EXTRACTS = routing.grouping_id_for(["extract_a", "extract_b"])
_EXTRACT_C_AND_MART = routing.grouping_id_for(["extract_c", "mart"])


def _node(task_key: str, native_type: str = "DatabricksNotebook") -> SourceNode:
    return SourceNode(
        source_id=task_key,
        task_key=task_key,
        concept=CONCEPT_NOTEBOOK,
        source="adf",
        name=task_key,
        native_type=native_type,
        properties={STRATEGY_PROPERTY: "deterministic"},
    )


def _inventory(source: str = "adf") -> dict[str, Any]:
    """'orchestrator' calls 'child'; the four other pipelines are independent components."""
    graphs = [
        SourceGraph(
            name="orchestrator",
            source=source,
            tasks=[_node("call_child", "ExecutePipeline")],
            lineage=Lineage(
                control_edges=[
                    ControlEdge(source_workflow="orchestrator", target_workflow="child", via_task_key="call_child")
                ]
            ),
        ),
        *(SourceGraph(name=name, source=source, tasks=[_node(f"{name}_load")]) for name in _PIPELINES[:-1]),
    ]
    return build_source_inventory(graphs, source=source, source_dir="/src")


def _pattern(name: str, *, simplification: bool = True) -> dict[str, Any]:
    pattern: dict[str, Any] = {"pattern": name, "fit": "One managed pipeline", "simplification_pattern": simplification}
    if simplification:
        pattern["release_state"] = "ga"
    return pattern


def _insights(*, extra_patterns: dict[str, str] | None = None) -> dict[str, Any]:
    """extract_a + extract_b share a simplification pattern; extract_c feeds mart through a shared table."""
    pattern_by_pipeline = {"extract_a": _LAKEFLOW_CONNECT, "extract_b": _LAKEFLOW_CONNECT, **(extra_patterns or {})}
    return {
        "overview": "Extractors land tables the mart reads.",
        "pipeline_insights": [
            {"pipeline": pipeline, "intent": f"Load {pipeline}", "recommended_patterns": [_pattern(pattern)]}
            for pipeline, pattern in sorted(pattern_by_pipeline.items())
        ]
        + [{"pipeline": "mart", "recommended_patterns": [_pattern("Databricks SQL", simplification=False)]}],
        "pipeline_relationships": [
            {
                "from_pipeline": "extract_c",
                "to_pipeline": "mart",
                "lineage_edge": {
                    "edge_type": "inferred",
                    "edge_identity": "sales.orders",
                    "evidence": "extract_c writes sales.orders, which mart reads",
                    "confidence": "high",
                },
                "relationship_summary": "mart reads what extract_c lands",
            },
            {
                "from_pipeline": "orchestrator",
                "to_pipeline": "child",
                "lineage_edge": {"edge_type": "control", "edge_identity": "call_child"},
            },
            {
                "from_pipeline": "child",
                "to_pipeline": "orchestrator",
                "lineage_edge": {
                    "edge_type": "inferred",
                    "edge_identity": "audit.log",
                    "evidence": "both write audit.log",
                    "confidence": "low",
                },
            },
        ],
    }


def _enriched(output_dir: Path, **insight_options: Any) -> dict[str, Any]:
    """Write the inventory, enrich it for real, and return the enriched inventory."""
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    (metadata / "inventory.json").write_text(json.dumps(_inventory(), indent=2), encoding="utf-8")
    result = enrich_inventory(output_dir, insights=_insights(**insight_options))
    assert result["ok"] is True, result
    return json.loads((metadata / "inventory.json").read_text(encoding="utf-8"))


def _write_report(output_dir: Path) -> None:
    work = output_dir / WORK_DIRNAME
    work.mkdir(parents=True, exist_ok=True)
    pipelines = [
        pipeline_to_dict(
            Pipeline(
                name=name,
                tasks=[NotebookActivity(name="load", task_key="load", notebook_path=f"/Workspace/Shared/{name}")],
                tags={"source": "adf"},
            )
        )
        for name in _PIPELINES
    ]
    (work / REPORT_FILENAME).write_text(json.dumps({"pipelines": pipelines}, indent=2), encoding="utf-8")
    (work / "gaps.json").write_text("[]", encoding="utf-8")


def _plan(inventory: dict[str, Any], agentic: set[str], accepted: set[str]) -> dict[str, Any]:
    """A plan routing ``agentic`` components agentic, the rest deterministic, with ``accepted`` groupings accepted."""
    recommendation = routing.build_recommendation(inventory)
    components = [
        {
            "component_id": component["component_id"],
            "members": component["members"],
            "decision": "agentic" if component["component_id"] in agentic else "deterministic",
        }
        for component in recommendation["components"]
    ]
    groupings = [
        {"grouping_id": grouping["grouping_id"], "accepted": grouping["grouping_id"] in accepted}
        for grouping in recommendation["suggested_groupings"]
    ]
    return {"components": components, "suggested_groupings": groupings}


def _route(output_dir: Path, plan: dict[str, Any]) -> dict[str, Any]:
    assert routing.record_plan(output_dir, plan=plan)["ok"] is True
    recorded = ConversionPlan.load(output_dir)
    assert recorded is not None
    return apply_plan(output_dir, recorded)


def _authored(name: str) -> dict[str, Any]:
    return pipeline_to_dict(
        Pipeline(
            name=name,
            tasks=[NotebookActivity(name="ingest", task_key="ingest", notebook_path="/Workspace/Shared/lakeflow")],
            tags={"source": "adf"},
        )
    )


def test_groupings_come_from_a_shared_simplification_pattern_and_an_inferred_relationship(tmp_path: Path) -> None:
    inventory = _enriched(tmp_path)

    groupings = routing.suggest_groupings(inventory)

    assert [(grouping["grouping_id"], grouping["components"]) for grouping in groupings] == [
        (_EXTRACTS, ["component-2", "component-3"]),
        (_EXTRACT_C_AND_MART, ["component-4", "component-5"]),
    ]
    assert groupings[0]["members"] == ["extract_a", "extract_b"]
    assert groupings[0]["basis"] == [
        {"kind": "shared_pattern", "pattern": _LAKEFLOW_CONNECT, "pipelines": ["extract_a", "extract_b"]}
    ]
    (inferred,) = groupings[1]["basis"]
    assert inferred["kind"] == "inferred_relationship"
    assert (inferred["from_pipeline"], inferred["to_pipeline"]) == ("extract_c", "mart")
    assert inferred["evidence"] == "extract_c writes sales.orders, which mart reads"
    assert inferred["confidence"] == "high"
    assert all(grouping["accepted"] is False for grouping in groupings)


def test_links_of_both_kinds_join_into_one_suggestion_so_suggestions_never_overlap(tmp_path: Path) -> None:
    inventory = _enriched(tmp_path, extra_patterns={"extract_c": _LAKEFLOW_CONNECT})

    (grouping,) = routing.suggest_groupings(inventory)

    assert grouping["components"] == ["component-2", "component-3", "component-4", "component-5"]
    assert grouping["members"] == ["extract_a", "extract_b", "extract_c", "mart"]
    assert {basis["kind"] for basis in grouping["basis"]} == {"shared_pattern", "inferred_relationship"}


def test_no_grouping_without_insights_or_for_an_airflow_inventory() -> None:
    assert routing.suggest_groupings(_inventory()) == []
    airflow = _inventory("airflow")
    airflow["insights"] = _insights()
    assert routing.suggest_groupings(airflow) == []


def test_an_accepted_grouping_routes_fills_and_packages_as_one_unit(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    inventory = _enriched(tmp_path)
    _write_report(tmp_path)

    outcome = _route(tmp_path, _plan(inventory, {"component-2", "component-3"}, {_EXTRACTS}))

    assert outcome["outcomes"][_EXTRACTS] == "agentic-not-viable"
    assert "component-2" not in outcome["outcomes"] and "component-3" not in outcome["outcomes"]
    result = apply_agentic_output(tmp_path, ["extract_a", "extract_b"], [_authored("sql_server_ingest")])
    assert result["ok"] is True and result["component_id"] == _EXTRACTS
    report = json.loads((tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_text(encoding="utf-8"))
    names = sorted(pipeline["name"] for pipeline in report["pipelines"])
    assert names == ["child", "extract_c", "mart", "orchestrator", "sql_server_ingest"]
    record = routing_record(report)
    assert record is not None and record["components"][_EXTRACTS]["outcome"] == "agentic-applied"

    assert package_main(["--output-dir", str(tmp_path), "--no-download-workspace-files", "--keep-intermediates"]) == 0
    audit = json.loads((tmp_path / "metadata" / "route_audit.json").read_text(encoding="utf-8"))
    grouped = next(unit for unit in audit["components"] if unit["component_id"] == _EXTRACTS)
    assert grouped["grouped_components"] == ["component-2", "component-3"]
    assert grouped["outcome"] == "agentic-applied"


def test_un_accepting_a_grouping_routes_its_components_separately_again(tmp_path: Path) -> None:
    inventory = _enriched(tmp_path)
    _write_report(tmp_path)
    _route(tmp_path, _plan(inventory, {"component-2", "component-3"}, {_EXTRACTS}))
    assert apply_agentic_output(tmp_path, ["extract_a", "extract_b"], [_authored("sql_server_ingest")])["ok"] is True

    outcome = _route(tmp_path, _plan(inventory, {"component-2", "component-3"}, set()))

    assert outcome["outcomes"]["component-2"] == outcome["outcomes"]["component-3"] == "agentic-not-viable"
    assert _EXTRACTS not in outcome["outcomes"]
    assert apply_agentic_output(tmp_path, ["extract_a", "extract_b"], [_authored("x")])["ok"] is False


def test_a_grouping_with_a_component_decided_deterministic_is_refused(tmp_path: Path) -> None:
    inventory = _enriched(tmp_path)

    violations = routing.validate_plan(_plan(inventory, {"component-2"}, {_EXTRACTS}), inventory)

    assert any(f"grouping {_EXTRACTS!r}" in violation and "'component-3'" in violation for violation in violations)


def test_a_grouping_and_its_own_components_may_reuse_each_others_pipeline_names(tmp_path: Path) -> None:
    inventory = _enriched(tmp_path)
    _write_report(tmp_path)
    _route(tmp_path, _plan(inventory, {"component-2", "component-3"}, set()))
    assert apply_agentic_output(tmp_path, ["extract_a"], [_authored("extract_a")])["ok"] is True
    assert apply_agentic_output(tmp_path, ["extract_b"], [_authored("extract_b")])["ok"] is True
    _route(tmp_path, _plan(inventory, {"component-2", "component-3"}, {_EXTRACTS}))

    grouped = apply_agentic_output(tmp_path, ["extract_a", "extract_b"], [_authored("extract_a")])

    assert grouped["ok"] is True, grouped
    _route(tmp_path, _plan(inventory, {"component-2", "component-3"}, set()))
    changed = _authored("extract_a")
    changed["description"] = "kept 1:1"
    one_to_one = apply_agentic_output(tmp_path, ["extract_a"], [changed])
    assert one_to_one["ok"] is True, one_to_one


def test_a_passed_in_grouping_whose_suggestion_changed_since_is_refused(tmp_path: Path) -> None:
    before = _enriched(tmp_path / "before")
    stale_copy = _plan(before, {"component-2", "component-3", "component-4", "component-5"}, set())
    suggestion = routing.suggest_groupings(before)[0]
    stale_copy["suggested_groupings"] = [{**suggestion, "accepted": True}]
    assert routing.validate_plan(stale_copy, before) == []
    after = _enriched(tmp_path / "after", extra_patterns={"extract_c": _LAKEFLOW_CONNECT})
    (current,) = routing.suggest_groupings(after)
    relabelled = {
        **stale_copy,
        "suggested_groupings": [{**suggestion, "grouping_id": current["grouping_id"], "accepted": True}],
    }

    stale = routing.validate_plan(stale_copy, after)
    mislabelled = routing.validate_plan(relabelled, after)

    assert stale == [
        f"suggested_groupings[0]: {_EXTRACTS!r} is not a grouping route suggests for this inventory; only a "
        "suggested grouping can be accepted (to group other components, record an inferred relationship through "
        "enrich)"
    ]
    assert mislabelled == [
        f"suggested_groupings[0]: grouping {current['grouping_id']!r} now joins ['component-2', 'component-3', "
        "'component-4', 'component-5'] (['extract_a', 'extract_b', 'extract_c', 'mart']), not the components and "
        "members this plan lists; the suggestions changed since it was written, so review the grouping again"
    ]


def test_a_re_enrich_that_drops_another_suggestion_keeps_a_grouping_accepted_and_filled(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    inventory = _enriched(tmp_path)
    _write_report(tmp_path)
    agentic = {"component-2", "component-3", "component-4", "component-5"}
    _route(
        tmp_path,
        _plan(inventory, agentic, {grouping["grouping_id"] for grouping in routing.suggest_groupings(inventory)}),
    )
    assert apply_agentic_output(tmp_path, ["extract_a", "extract_b"], [_authored("lfc_a")])["ok"] is True
    assert apply_agentic_output(tmp_path, ["extract_c", "mart"], [_authored("lfc_b")])["ok"] is True
    assert enrich_inventory(tmp_path, insights=_insights(extra_patterns={"extract_b": "Auto Loader"}))["ok"] is True
    re_enriched = json.loads((tmp_path / "metadata" / "inventory.json").read_text(encoding="utf-8"))
    (remaining,) = routing.suggest_groupings(re_enriched)
    assert remaining["members"] == ["extract_c", "mart"]
    capsys.readouterr()

    assert adapter_main(["route", "--output-dir", str(tmp_path)]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["accepted_groupings"] == [remaining["grouping_id"]]
    assert payload["edit"]["outcomes"][remaining["grouping_id"]] == "agentic-applied"
    refill = apply_agentic_output(tmp_path, ["extract_c", "mart"], [_authored("lfc_b")])
    assert refill["ok"] is True and refill["already_applied"] is True
    store = json.loads((tmp_path / "metadata" / "agentic_conversion.json").read_text(encoding="utf-8"))
    assert sorted(entry["members"] for entry in store["components"].values()) == [
        ["extract_a", "extract_b"],
        ["extract_c", "mart"],
    ]
    assert all(entry["replaced"] == [] for entry in store["components"].values())


def test_only_a_suggested_grouping_can_be_accepted(tmp_path: Path) -> None:
    inventory = _enriched(tmp_path)
    plan = _plan(inventory, set(), set())
    plan["suggested_groupings"].append({"grouping_id": "grouping-9", "accepted": True})

    violations = routing.validate_plan(plan, inventory)

    assert any("'grouping-9' is not a grouping route suggests" in violation for violation in violations)


def test_an_accepted_grouping_is_kept_when_route_runs_again_without_a_plan(tmp_path: Path) -> None:
    inventory = _enriched(tmp_path)
    assert routing.record_plan(tmp_path, plan=_plan(inventory, {"component-2", "component-3"}, {_EXTRACTS}))["ok"]
    recorded = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))

    carried = routing.carried_forward_plan(recorded, inventory)

    assert {entry["grouping_id"]: entry["accepted"] for entry in carried["suggested_groupings"]} == {
        _EXTRACTS: True,
        _EXTRACT_C_AND_MART: False,
    }
    decisions = {entry["component_id"]: entry["decision"] for entry in carried["components"]}
    assert decisions["component-2"] == decisions["component-3"] == "agentic"
