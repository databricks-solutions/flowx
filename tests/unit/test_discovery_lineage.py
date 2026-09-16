"""Tests for lineage over the shared discovery AST (:mod:`flowx.discovery_lineage`).

The source-neutral derivation itself -- the identity vs signature join tiers, no
self-edges, no duplicates, fan-out, and Switch / ForEach / If recursion -- driven
straight off :class:`SourceGraph` / :class:`SourceNode` values. Per-source
population of a node's reads / writes / invocation markers (the ADF and Airflow
mappers) is exercised in the per-source test suites (#62 / #63); this file stays
free of any ``sources/*`` coupling.
"""

from __future__ import annotations

from flowx.discovery_lineage import (
    INVOKES_WAIT_PROPERTY,
    INVOKES_WORKFLOW_PROPERTY,
    build_graph_lineage,
    walk_nodes,
    with_graph_lineage,
)
from flowx.models.discovery import (
    SOURCE_ADF,
    ContainerNode,
    SourceGraph,
    SourceNode,
)
from flowx.models.ir import DataAsset

# --------------------------------------------------------------------------- #
# Helpers for the source-neutral layer
# --------------------------------------------------------------------------- #


def _node(task_key: str, *, reads=None, writes=None, invokes=None, wait=None) -> SourceNode:
    properties: dict = {}
    if invokes is not None:
        properties[INVOKES_WORKFLOW_PROPERTY] = invokes
        properties[INVOKES_WAIT_PROPERTY] = wait
    return SourceNode(
        source_id=task_key,
        task_key=task_key,
        concept="x",
        source=SOURCE_ADF,
        data_reads=list(reads or []),
        data_writes=list(writes or []),
        properties=properties,
    )


def _graph(*tasks: SourceNode, name: str = "pl") -> SourceGraph:
    return SourceGraph(name=name, source=SOURCE_ADF, tasks=list(tasks))


# --------------------------------------------------------------------------- #
# Data-edge tiers (source-neutral)
# --------------------------------------------------------------------------- #


def test_identity_tier_joins_across_different_signatures() -> None:
    graph = _graph(
        _node("writer", writes=[DataAsset(signature="ds_out", identity="curated.orders")]),
        _node("reader", reads=[DataAsset(signature="ds_in_other", identity="curated.orders")]),
    )
    edges = build_graph_lineage(graph).data_edges
    assert len(edges) == 1
    assert (edges[0].source_task_key, edges[0].target_task_key) == ("writer", "reader")
    assert edges[0].match_kind == "identity"
    assert edges[0].identity == "curated.orders"


def test_signature_tier_when_identity_unresolved() -> None:
    graph = _graph(
        _node("writer", writes=[DataAsset(signature="FP[wm|slots=1]/FN[v.txt|slots=0]")]),
        _node("reader", reads=[DataAsset(signature="FP[wm|slots=1]/FN[v.txt|slots=0]")]),
    )
    edges = build_graph_lineage(graph).data_edges
    assert len(edges) == 1
    assert edges[0].match_kind == "signature"
    assert edges[0].identity is None


def test_distinct_identities_do_not_fall_back_to_signature() -> None:
    """Two resolved-but-different identities never manufacture a signature edge (#36)."""
    graph = _graph(
        _node("writer", writes=[DataAsset(signature="shared", identity="a.first")]),
        _node("reader", reads=[DataAsset(signature="shared", identity="b.second")]),
    )
    assert build_graph_lineage(graph).data_edges == []


def test_fan_out_one_writer_many_readers() -> None:
    graph = _graph(
        _node("writer", writes=[DataAsset(signature="ds", identity="x.y")]),
        _node("reader_one", reads=[DataAsset(signature="ds", identity="x.y")]),
        _node("reader_two", reads=[DataAsset(signature="ds", identity="x.y")]),
    )
    edges = build_graph_lineage(graph).data_edges
    assert {edge.target_task_key for edge in edges} == {"reader_one", "reader_two"}


def test_no_self_edge_and_no_duplicates() -> None:
    graph = _graph(
        _node(
            "both",
            writes=[DataAsset(signature="ds", identity="x.y"), DataAsset(signature="ds", identity="x.y")],
            reads=[DataAsset(signature="ds", identity="x.y")],
        ),
        _node("reader", reads=[DataAsset(signature="ds", identity="x.y")]),
    )
    edges = build_graph_lineage(graph).data_edges
    assert [(edge.source_task_key, edge.target_task_key) for edge in edges] == [("both", "reader")]


def test_data_edges_recurse_into_switch_and_foreach_branches() -> None:
    """A writer buried in a Switch case hands off to a reader in a ForEach body."""
    writer = _node("writer", writes=[DataAsset(signature="ds", identity="x.y")])
    reader = _node("reader", reads=[DataAsset(signature="ds", identity="x.y")])
    switch = ContainerNode(
        source_id="sw",
        task_key="sw",
        concept="switch",
        source=SOURCE_ADF,
        branches={"caseA": [writer], "default": []},
    )
    loop = ContainerNode(
        source_id="fe",
        task_key="fe",
        concept="loop",
        source=SOURCE_ADF,
        branches={"body": [reader]},
    )
    edges = build_graph_lineage(_graph(switch, loop)).data_edges
    assert [(edge.source_task_key, edge.target_task_key) for edge in edges] == [("writer", "reader")]


def test_walk_nodes_visits_every_branch_including_default() -> None:
    switch = ContainerNode(
        source_id="sw",
        task_key="sw",
        concept="switch",
        source=SOURCE_ADF,
        branches={"caseA": [_node("in_case")], "default": [_node("in_default")]},
    )
    keys = [node.task_key for node in walk_nodes([switch])]
    assert keys == ["sw", "in_case", "in_default"]


# --------------------------------------------------------------------------- #
# Control edges (source-neutral)
# --------------------------------------------------------------------------- #


def test_control_edge_from_invocation_marker() -> None:
    graph = _graph(_node("call", invokes="child", wait=True), name="parent")
    edges = build_graph_lineage(graph).control_edges
    assert len(edges) == 1
    assert (edges[0].source_workflow, edges[0].target_workflow) == ("parent", "child")
    assert edges[0].wait_for_completion is True
    assert edges[0].resolved is True


def test_control_edge_unresolved_callee_recorded_not_dropped() -> None:
    graph = _graph(_node("call", invokes="", wait=True), name="parent")
    edges = build_graph_lineage(graph).control_edges
    assert len(edges) == 1
    assert edges[0].target_workflow == ""
    assert edges[0].resolved is False


def test_control_edge_self_call_is_dropped() -> None:
    graph = _graph(_node("call", invokes="parent", wait=True), name="parent")
    assert build_graph_lineage(graph).control_edges == []


def test_with_graph_lineage_returns_new_graph_and_leaves_input_untouched() -> None:
    graph = _graph(_node("call", invokes="child", wait=False), name="parent")
    result = with_graph_lineage(graph)
    assert graph.lineage is None
    assert result is not graph
    assert len(result.lineage.control_edges) == 1
