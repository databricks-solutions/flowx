"""Source-neutral lineage derivation over the shared discovery AST.

The parallel of :mod:`flowx.lineage`, which derives a
:class:`~flowx.models.ir.Lineage` block over the Databricks IR. This module
derives the same block over the shared discovery AST
(:mod:`flowx.models.discovery`) instead, so the discover phase can attach lineage
to a :class:`~flowx.models.discovery.SourceGraph` before any IR translation
exists.

It reuses :mod:`flowx.lineage`'s primitive cores -- :func:`control_edges_from_calls`
and :func:`data_edges_from_endpoints` -- so the two-tier match, the self-edge drop,
and the dedup live in exactly one place and both phases behave identically. It
imports nothing from ``sources/*``: the walk is over the neutral
:class:`~flowx.models.discovery.ContainerNode` branch shape, so an ADF or an
Airflow graph derives lineage through this one code path once its nodes carry
``data_reads`` / ``data_writes`` and (for control edges) the
:data:`INVOKES_WORKFLOW_PROPERTY` marker.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator

from flowx.lineage import control_edges_from_calls, data_edges_from_endpoints
from flowx.models.discovery import ContainerNode, SourceGraph, SourceNode
from flowx.models.ir import ControlEdge, DataEdge, Lineage

# Neutral node-property key under which a mapper records the workflow a node
# invokes (an ADF ``ExecutePipeline`` callee, an Airflow triggered job). Kept in
# the free-form ``properties`` seam because the invocation target is a per-source
# detail with no shared typed field; :func:`build_graph_control_edges` reads it
# here so the control-edge derivation stays source-agnostic.
INVOKES_WORKFLOW_PROPERTY = "invokes_workflow"
# Companion key: whether the caller waits for the invoked workflow to complete
# (``True`` / ``False``), or absent when the source has no such notion.
INVOKES_WAIT_PROPERTY = "invokes_wait"


def walk_nodes(nodes: list[SourceNode]) -> Iterator[SourceNode]:
    """Yield every node depth-first, descending into every container branch.

    Recurses through :class:`ContainerNode` branches in their insertion order, so
    a Switch's cases *and* its ``default`` branch, a ForEach / Until ``body``, and
    both sides of an IfCondition are all reached -- a data asset or an invocation
    buried inside a Switch case is still found.

    Args:
        nodes: Top-level (or already-nested) node list to walk.

    Yields:
        Each node, container nodes included, in depth-first order.
    """
    for node in nodes:
        yield node
        if isinstance(node, ContainerNode):
            for children in node.branches.values():
                yield from walk_nodes(children)


def build_graph_control_edges(graph: SourceGraph) -> list[ControlEdge]:
    """Derive cross-workflow invocation edges for a discovery graph.

    One edge per node that carries the :data:`INVOKES_WORKFLOW_PROPERTY` marker,
    found anywhere in the graph (fan-out inside ForEach / If / Switch preserved).
    Delegates the self-edge drop, dedup, and unresolved-callee recording to the
    shared :func:`~flowx.lineage.control_edges_from_calls`.

    Args:
        graph: The source graph to derive control edges for.

    Returns:
        Deduplicated control edges, in first-seen order.
    """

    def _calls() -> Iterator[tuple[str, bool | None, str]]:
        for node in walk_nodes(graph.tasks):
            if INVOKES_WORKFLOW_PROPERTY not in node.properties:
                continue
            target = node.properties.get(INVOKES_WORKFLOW_PROPERTY) or ""
            wait = node.properties.get(INVOKES_WAIT_PROPERTY)
            yield str(target), wait, node.task_key

    return control_edges_from_calls(graph.name, _calls())


def build_graph_data_edges(graph: SourceGraph) -> list[DataEdge]:
    """Derive proven producer -> consumer data hand-offs for a discovery graph.

    A producer is any node with a ``data_writes`` asset; a consumer any node with a
    ``data_reads`` asset, gathered across the whole graph (every container branch
    included). Delegates the two-tier match, self-edge drop, and dedup to the shared
    :func:`~flowx.lineage.data_edges_from_endpoints`.

    Args:
        graph: The source graph to derive data edges for.

    Returns:
        Deduplicated data edges, in first-seen order.
    """
    nodes = list(walk_nodes(graph.tasks))
    producers = [(node.task_key, asset) for node in nodes for asset in node.data_writes]
    consumers = [(node.task_key, asset) for node in nodes for asset in node.data_reads]
    return data_edges_from_endpoints(producers, consumers)


def build_graph_lineage(graph: SourceGraph) -> Lineage:
    """Compose the source-neutral lineage block for a discovery graph.

    Motif annotations are a convert-time IR concern (motifs are detected during
    translation, not discovery), so the discovery lineage block leaves them empty.

    Args:
        graph: The source graph to derive lineage for.

    Returns:
        A :class:`Lineage` with control edges and data edges (motifs empty).
    """
    return Lineage(
        control_edges=build_graph_control_edges(graph),
        data_edges=build_graph_data_edges(graph),
    )


def with_graph_lineage(graph: SourceGraph) -> SourceGraph:
    """Return a *new* graph carrying its derived lineage, leaving the input untouched.

    Mirrors :func:`flowx.lineage.with_lineage` for the discovery AST.

    Args:
        graph: The source graph to copy.

    Returns:
        A shallow copy of *graph* with :attr:`SourceGraph.lineage` populated.
    """
    return dataclasses.replace(graph, lineage=build_graph_lineage(graph))
