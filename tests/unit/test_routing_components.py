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

from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.models.ir import ControlEdge, Lineage
from flowx.routing import build_components


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
    return build_source_inventory(graphs, source="unit", source_dir="/tmp/src")


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
