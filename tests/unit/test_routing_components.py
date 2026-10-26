"""Tests for connected-component computation over control lineage (routing, #77).

The inventory fixtures are built by hand through the source-agnostic emitter
(:func:`flowx.discovery_inventory.build_source_inventory`) -- no ADF, no Airflow -- so the
routing engine is proven against the standardised inventory shape and its per-pipeline
control-edge lineage, exactly as it will see it in production. Components are weak/undirected:
two pipelines land in the same component when a resolved control edge joins them in either
direction.
"""

from __future__ import annotations

from typing import Any

import pytest

from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.models.ir import ControlEdge, Lineage
from flowx.routing import build_components, build_plan, build_recommendation, carried_forward_plan, validate_plan


def _node(task_key: str, native_type: str = "Notebook", *, strategy: str = "deterministic") -> SourceNode:
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


def _graph(name: str, *, edges: list[ControlEdge] | None = None, nodes: list[SourceNode] | None = None) -> SourceGraph:
    """A one-node graph named *name*, optionally carrying control edges to other graphs."""
    return SourceGraph(
        name=name,
        source="unit",
        tasks=nodes if nodes is not None else [_node(f"{name}_task")],
        lineage=Lineage(control_edges=edges or []),
    )


def _edge(source: str, target: str, via: str, *, resolved: bool = True) -> ControlEdge:
    return ControlEdge(source_workflow=source, target_workflow=target, via_task_key=via, resolved=resolved)


def _inventory(graphs: list[SourceGraph]) -> dict[str, Any]:
    return build_source_inventory(graphs, source="adf", source_dir="/tmp/src")


def test_isolated_pipelines_each_form_their_own_component() -> None:
    inventory = _inventory([_graph("a"), _graph("b"), _graph("c")])
    components, findings = build_components(inventory)
    assert components == [["a"], ["b"], ["c"]]
    assert findings == []


def test_a_chain_of_calls_forms_one_component() -> None:
    graphs = [
        _graph("a", edges=[_edge("a", "b", "call_b")], nodes=[_node("call_b", "ExecutePipeline")]),
        _graph("b", edges=[_edge("b", "c", "call_c")], nodes=[_node("call_c", "ExecutePipeline")]),
        _graph("c"),
    ]
    components, findings = build_components(_inventory(graphs))
    assert components == [["a", "b", "c"]]
    assert findings == []


def test_a_branch_fans_into_one_component() -> None:
    graphs = [
        _graph(
            "a",
            edges=[_edge("a", "b", "call_b"), _edge("a", "c", "call_c")],
            nodes=[_node("call_b", "ExecutePipeline"), _node("call_c", "ExecutePipeline")],
        ),
        _graph("b"),
        _graph("c"),
    ]
    components, _ = build_components(_inventory(graphs))
    assert components == [["a", "b", "c"]]


def test_a_cycle_forms_one_component() -> None:
    graphs = [
        _graph("a", edges=[_edge("a", "b", "call_b")], nodes=[_node("call_b", "ExecutePipeline")]),
        _graph("b", edges=[_edge("b", "a", "call_a")], nodes=[_node("call_a", "ExecutePipeline")]),
    ]
    components, findings = build_components(_inventory(graphs))
    assert components == [["a", "b"]]
    assert findings == []


def test_multiple_edges_between_the_same_pair_still_one_component() -> None:
    graphs = [
        _graph(
            "a",
            edges=[_edge("a", "b", "call_b1"), _edge("a", "b", "call_b2")],
            nodes=[_node("call_b1", "ExecutePipeline"), _node("call_b2", "ExecutePipeline")],
        ),
        _graph("b"),
    ]
    components, _ = build_components(_inventory(graphs))
    assert components == [["a", "b"]]


def test_two_separate_clusters_are_distinct_components() -> None:
    graphs = [
        _graph("a", edges=[_edge("a", "b", "call_b")], nodes=[_node("call_b", "ExecutePipeline")]),
        _graph("b"),
        _graph("x", edges=[_edge("x", "y", "call_y")], nodes=[_node("call_y", "ExecutePipeline")]),
        _graph("y"),
    ]
    components, _ = build_components(_inventory(graphs))
    assert components == [["a", "b"], ["x", "y"]]


def test_unresolved_callee_is_a_finding_not_a_severed_edge() -> None:
    graphs = [
        _graph(
            "a", edges=[_edge("a", "", "call_ghost", resolved=False)], nodes=[_node("call_ghost", "ExecutePipeline")]
        )
    ]
    components, findings = build_components(_inventory(graphs))
    # 'a' still forms its own component; the unresolved edge is recorded, not dropped.
    assert components == [["a"]]
    assert len(findings) == 1
    assert "call_ghost" in findings[0] and "unresolved" in findings[0].lower()


def test_edge_to_pipeline_absent_from_inventory_is_a_finding() -> None:
    graphs = [
        _graph("a", edges=[_edge("a", "ghost", "call_ghost")], nodes=[_node("call_ghost", "ExecutePipeline")]),
        _graph("b"),
    ]
    components, findings = build_components(_inventory(graphs))
    assert components == [["a"], ["b"]]
    assert len(findings) == 1
    assert "ghost" in findings[0]


def test_component_membership_and_ordering_are_deterministic() -> None:
    # Declared out of alphabetical order; members and components must still sort deterministically.
    graphs = [
        _graph("z"),
        _graph("m", edges=[_edge("m", "a", "call_a")], nodes=[_node("call_a", "ExecutePipeline")]),
        _graph("a"),
    ]
    first, _ = build_components(_inventory(graphs))
    second, _ = build_components(_inventory(graphs))
    assert first == second
    assert first == [["a", "m"], ["z"]]


def _hand_edited(inventory: dict[str, Any], pipeline: str, **fields: Any) -> dict[str, Any]:
    """The inventory with one pipeline's entry overwritten, as a hand edit would leave it."""
    for entry in inventory["pipelines"]:
        if entry["name"] == pipeline:
            entry.update(fields)
    return inventory


@pytest.mark.parametrize(
    "edge",
    [
        {"source_workflow": "a", "target_workflow": "b", "via_task_key": "call_b"},
        {"source_workflow": "a", "target_workflow": "b", "via_task_key": "call_b", "resolved": "false"},
        {"source_workflow": "a", "target_workflow": "b", "via_task_key": "call_b", "resolved": None},
    ],
    ids=["missing", "the-string-false", "null"],
)
def test_an_edge_not_recorded_as_resolved_never_joins_two_pipelines(edge: dict[str, Any]) -> None:
    inventory = _hand_edited(_inventory([_graph("a"), _graph("b")]), "a", lineage={"control_edges": [edge]})

    components, findings = build_components(inventory)

    assert components == [["a"], ["b"]]
    assert len(findings) == 1 and "call_b" in findings[0]


@pytest.mark.parametrize(
    "lineage", [[1], {"control_edges": {"a": "b"}}, {"control_edges": "b"}, "a->b"], ids=["list", "dict", "str", "text"]
)
def test_a_malformed_lineage_block_is_read_as_no_edges(lineage: Any) -> None:
    inventory = _hand_edited(_inventory([_graph("a"), _graph("b")]), "a", lineage=lineage)

    assert build_components(inventory) == ([["a"], ["b"]], [])


@pytest.mark.parametrize("pipelines", [1, {"a": {"name": "a"}}, "ab", None], ids=["int", "dict", "str", "null"])
def test_an_inventory_whose_pipelines_is_not_a_list_lists_no_pipelines(pipelines: Any) -> None:
    """The same reading enrich gives it: a malformed ``pipelines`` value lists none, rather than crashing route."""
    inventory = {**_inventory([_graph("a")]), "pipelines": pipelines}

    assert build_components(inventory) == ([], [])
    assert build_recommendation(inventory)["components"] == []


def test_a_pipeline_name_that_is_not_a_string_is_not_a_pipeline_identity() -> None:
    """A number would become the string "7" and join an edge to a real pipeline named "7"."""
    edge = {"source_workflow": "a", "target_workflow": "7", "via_task_key": "call_7", "resolved": True}
    inventory = _inventory([_graph("a"), _graph("7"), _graph("b")])
    _hand_edited(inventory, "7", name=7)
    _hand_edited(inventory, "a", lineage={"control_edges": [edge]})

    components, findings = build_components(inventory)

    assert components == [["a"], ["b"]]
    assert len(findings) == 1 and "call_7" in findings[0]


def test_an_edge_endpoint_that_is_not_a_string_is_unresolved() -> None:
    edge = {"source_workflow": "a", "target_workflow": ["b"], "via_task_key": "call_b", "resolved": True}
    inventory = _hand_edited(_inventory([_graph("a"), _graph("b")]), "a", lineage={"control_edges": [edge]})

    components, findings = build_components(inventory)

    assert components == [["a"], ["b"]]
    assert findings and "['b']" not in findings[0]


@pytest.mark.parametrize("source", ["", "ghost", 5], ids=["empty", "not-in-the-inventory", "not-a-string"])
def test_a_resolved_edge_whose_source_is_not_a_pipeline_is_a_finding(source: Any) -> None:
    """Both ends are checked, so an edge from an unknown caller is reported, never dropped quietly."""
    edge = {"source_workflow": source, "target_workflow": "b", "via_task_key": "call_b", "resolved": True}
    inventory = _hand_edited(_inventory([_graph("a"), _graph("b")]), "a", lineage={"control_edges": [edge]})

    components, findings = build_components(inventory)

    assert components == [["a"], ["b"]]
    assert len(findings) == 1 and "call_b" in findings[0] and "source" in findings[0]


def test_carrying_a_plan_forward_moves_each_decision_to_its_components_current_id() -> None:
    """A new pipeline shifts the ids; each carried row takes the id its members have now, so no two rows clash."""
    before = _inventory([_graph("beta"), _graph("gamma")])
    after = _inventory([_graph("alpha"), _graph("beta"), _graph("gamma")])
    decided = [
        {"component_id": "component-1", "members": ["beta"], "decision": "deterministic"},
        {"component_id": "component-2", "members": ["gamma"], "decision": "agentic"},
    ]
    recorded = build_plan(before, {"components": decided}).to_dict()

    carried, _dropped = carried_forward_plan(recorded, after)

    rows = sorted((row["component_id"], row["members"], row["decision"]) for row in carried["components"])
    assert rows == [
        ("component-1", ["alpha"], None),
        ("component-2", ["beta"], "deterministic"),
        ("component-3", ["gamma"], "agentic"),
    ]
    assert validate_plan(carried, after) == []
