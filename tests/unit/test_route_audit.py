"""Tests for the routing audit artifact the package phase persists.

Package prunes the transient ``.work/`` folder by default, which erases the routing trail
(translation report + ``gaps.json``). ``_write_route_audit`` summarises the recorded plan's routing
units, their decisions and outcomes, the agent's output history, the gap fills and the routing
conversation into ``metadata/route_audit.json`` so the trail survives the prune. It must no-op (write
nothing) when no plan was recorded, keeping the no-route path byte-identical.
"""

from __future__ import annotations

import json
from pathlib import Path

from flowx.bundler.dab_writer import _write_route_audit
from flowx.discovery_serde import canonical_sha256
from flowx.models.conversion_plan import ComponentPlan, ConversationEntry, ConversionPlan, SuggestedGrouping


def _plan(*, accepted: bool = False) -> ConversionPlan:
    return ConversionPlan(
        inventory_sha256="abc123",
        source_graphs_sha256="graphs123",
        agentic_insights_sha256="insights123",
        components=[
            ComponentPlan(component_id="component-1", members=["child", "parent"], decision="agentic"),
            ComponentPlan(
                component_id="component-2", members=["solo"], decision="agentic" if accepted else "deterministic"
            ),
        ],
        suggested_groupings=[
            SuggestedGrouping(
                grouping_id="grouping-1",
                components=["component-1", "component-2"],
                members=["child", "parent", "solo"],
                accepted=accepted,
            )
        ],
        conversation=[ConversationEntry(question="What should the conversion achieve?", answer="Fewer pipelines")],
    )


def test_route_audit_summarises_units_decisions_and_the_conversation(tmp_path: Path) -> None:
    plan = _plan()
    plan.write(tmp_path)

    audit_path = _write_route_audit(tmp_path)

    assert audit_path == tmp_path / "metadata" / "route_audit.json"
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    assert audit["recorded_against_inventory_sha256"] == "abc123"
    assert audit["source_graphs_sha256"] == "graphs123"
    assert audit["agentic_insights_sha256"] == "insights123"
    assert audit["conversion_plan_sha256"] == canonical_sha256(plan.to_dict())
    decisions = {component["component_id"]: component["decision"] for component in audit["components"]}
    assert decisions == {"component-1": "agentic", "component-2": "deterministic"}
    assert audit["agentic_pipelines"] == ["child", "parent"]
    assert audit["conversation"] == [{"question": "What should the conversion achieve?", "answer": "Fewer pipelines"}]
    assert "gaps_introduced" not in audit and "gaps_count" not in audit


def test_route_audit_lists_an_accepted_grouping_as_one_unit(tmp_path: Path) -> None:
    _plan(accepted=True).write(tmp_path)

    audit = json.loads(_write_route_audit(tmp_path).read_text(encoding="utf-8"))

    (unit,) = audit["components"]
    assert unit["component_id"] == "grouping-1"
    assert unit["members"] == ["child", "parent", "solo"]
    assert unit["grouped_components"] == ["component-1", "component-2"]
    assert unit["decision"] == "agentic"


def test_route_audit_copies_the_output_replacement_history(tmp_path: Path) -> None:
    _plan().write(tmp_path)
    pipelines = [{"name": "orders_lfc", "tasks": []}]
    store = {
        "components": {
            "component-1": {
                "members": ["child", "parent"],
                "pipelines": pipelines,
                "output_sha256": canonical_sha256(pipelines),
                "replaced": [{"from": "old", "to": canonical_sha256(pipelines)}],
            }
        },
        "gap_fills": [],
    }
    (tmp_path / "metadata" / "agentic_conversion.json").write_text(json.dumps(store), encoding="utf-8")

    audit = json.loads(_write_route_audit(tmp_path).read_text(encoding="utf-8"))

    component = next(entry for entry in audit["components"] if entry["component_id"] == "component-1")
    assert component["replaced"] == [{"from": "old", "to": canonical_sha256(pipelines)}]


def test_route_audit_noops_without_a_recorded_plan(tmp_path: Path) -> None:
    # No conversion_plan.json -> no routing happened -> nothing is written (no-route path stays clean).
    (tmp_path / "metadata").mkdir(parents=True, exist_ok=True)

    audit_path = _write_route_audit(tmp_path)

    assert audit_path is None
    assert not (tmp_path / "metadata" / "route_audit.json").exists()
