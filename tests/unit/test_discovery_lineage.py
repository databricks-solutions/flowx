"""Tests for lineage over the discovery graph contract (:mod:`flowx.discovery_lineage`).

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
from flowx.lineage import control_edges_from_calls
from flowx.models.discovery import (
    SOURCE_ADF,
    ContainerNode,
    SourceDependency,
    SourceGraph,
    SourceNode,
)
from flowx.models.ir import DataAsset, Lineage, MotifAnnotation

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


def test_signature_tier_with_one_resolved_side_claims_no_identity() -> None:
    """A signature match where only one endpoint resolved stays a weak match with no identity."""
    graph = _graph(
        _node("writer", writes=[DataAsset(signature="shared", identity="curated.orders")]),
        _node("reader", reads=[DataAsset(signature="shared")]),
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


def test_empty_identities_do_not_join_on_identity() -> None:
    """An empty identity is not a resolved one, so two different paths never join on it."""
    graph = _graph(
        _node("writer", writes=[DataAsset(signature="bronze/orders", identity="")]),
        _node("reader", reads=[DataAsset(signature="silver/orders", identity="")]),
    )
    assert build_graph_lineage(graph).data_edges == []


def test_signature_match_needs_the_same_asset_type_when_both_are_known() -> None:
    """A file and an in-memory value that share a weak name are different things and never link."""
    mismatched = _graph(
        _node("writer", writes=[DataAsset(signature="orders", asset_type="file")]),
        _node("reader", reads=[DataAsset(signature="orders", asset_type="value")]),
    )
    one_side_unknown = _graph(
        _node("writer", writes=[DataAsset(signature="orders", asset_type="file")]),
        _node("reader", reads=[DataAsset(signature="orders")]),
    )
    assert build_graph_lineage(mismatched).data_edges == []
    assert [edge.asset_type for edge in build_graph_lineage(one_side_unknown).data_edges] == ["file"]


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
    edges = build_graph_lineage(graph, known_workflows={"parent", "child"}).control_edges
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


def test_control_edge_to_a_callee_missing_from_known_workflows_is_unresolved() -> None:
    """A named callee that is not in the export stays unresolved instead of looking resolved."""
    graph = _graph(_node("call_child", invokes="child"), _node("call_missing", invokes="missing"), name="parent")
    edges = build_graph_lineage(graph, known_workflows={"parent", "child"}).control_edges
    assert [(edge.target_workflow, edge.resolved) for edge in edges] == [("child", True), ("missing", False)]


def test_with_graph_lineage_marks_a_callee_missing_from_known_workflows_unresolved() -> None:
    graph = _graph(_node("call_child", invokes="child"), _node("call_missing", invokes="missing"), name="parent")
    edges = with_graph_lineage(graph, known_workflows={"parent", "child"}).lineage.control_edges
    assert [(edge.target_workflow, edge.resolved) for edge in edges] == [("child", True), ("missing", False)]


def test_control_edges_from_calls_checks_known_workflows() -> None:
    edges = control_edges_from_calls(
        "parent", [("child", None, "a"), ("missing", None, "b")], known_workflows={"child"}
    )
    assert [edge.resolved for edge in edges] == [True, False]


# --------------------------------------------------------------------------- #
# Data edges ignore run order
# --------------------------------------------------------------------------- #


def test_every_writer_of_an_asset_hands_off_to_its_reader() -> None:
    """Two parallel writers of one asset each get an edge to the reader; the match does not pick one."""
    graph = _graph(
        _node("writer_one", writes=[DataAsset(signature="ds", identity="x.y")]),
        _node("writer_two", writes=[DataAsset(signature="ds", identity="x.y")]),
        _node("reader", reads=[DataAsset(signature="ds", identity="x.y")]),
    )
    edges = build_graph_lineage(graph).data_edges
    assert [(edge.source_task_key, edge.target_task_key) for edge in edges] == [
        ("writer_one", "reader"),
        ("writer_two", "reader"),
    ]


def test_writer_that_runs_after_the_reader_still_gets_an_edge() -> None:
    """The match ignores run order, so a writer downstream of the reader still pairs with it."""
    reader = _node("reader", reads=[DataAsset(signature="ds", identity="x.y")])
    writer = _node("writer", writes=[DataAsset(signature="ds", identity="x.y")])
    writer.dependencies = [SourceDependency(upstream="reader")]
    edges = build_graph_lineage(_graph(reader, writer)).data_edges
    assert [(edge.source_task_key, edge.target_task_key) for edge in edges] == [("writer", "reader")]


def test_named_callee_is_unresolved_when_known_workflows_is_not_given() -> None:
    """Without the export's workflow names nothing can be confirmed, so a named callee stays unresolved."""
    graph = _graph(_node("call", invokes="child"), name="parent")
    assert [edge.resolved for edge in build_graph_lineage(graph).control_edges] == [False]
    assert [edge.resolved for edge in control_edges_from_calls("parent", [("child", None, "call")])] == [False]


def test_with_graph_lineage_keeps_motifs_already_on_the_graph() -> None:
    motif = MotifAnnotation(motif_id="activity_and_notify", member_task_keys=["call", "notify"])
    graph = _graph(_node("call", invokes="child"), _node("notify"), name="parent")
    graph.lineage = Lineage(motifs=[motif])

    result = with_graph_lineage(graph, known_workflows={"parent", "child"})

    assert result.lineage.motifs == [motif]
    assert [edge.target_workflow for edge in result.lineage.control_edges] == ["child"]


def test_invocation_target_that_is_not_a_string_stays_unresolved() -> None:
    """A list, number or mapping is never turned into a callee name, so it cannot match a real workflow."""
    graph = _graph(
        _node("call_list", invokes=["child"], wait="yes"),
        _node("call_number", invokes=7),
        _node("call_mapping", invokes={"referenceName": "child"}),
        name="parent",
    )
    edges = build_graph_lineage(graph, known_workflows={"['child']", "7", "child"}).control_edges
    assert [(edge.target_workflow, edge.resolved) for edge in edges] == [("", False), ("", False), ("", False)]
    assert edges[0].wait_for_completion is None
