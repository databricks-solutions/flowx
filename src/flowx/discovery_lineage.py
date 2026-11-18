"""Source-neutral lineage derivation over the discovery graph contract.

Derives a :class:`~flowx.models.lineage.Lineage` block over the source-neutral
discovery graph (:mod:`flowx.models.discovery`), so the discover phase can attach
lineage to a :class:`~flowx.models.discovery.SourceGraph` before any IR
translation exists.

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
from collections.abc import Callable, Collection, Iterable, Iterator
from typing import TypeVar

from flowx.lineage import control_edges_from_calls, data_edges_from_endpoints
from flowx.models.discovery import ContainerNode, SourceGraph, SourceNode
from flowx.models.lineage import ControlEdge, DataEdge, Lineage

# Neutral node-property key under which a mapper records the workflow a node
# invokes (an ADF ``ExecutePipeline`` callee, an Airflow triggered job). Kept in
# the free-form ``properties`` seam because the invocation target is a per-source
# detail with no shared typed field; :func:`build_graph_control_edges` reads it
# here so the control-edge derivation stays source-agnostic.
INVOKES_WORKFLOW_PROPERTY = "invokes_workflow"
# Companion key: whether the caller waits for the invoked workflow to complete
# (``True`` / ``False``), or absent when the source has no such notion.
INVOKES_WAIT_PROPERTY = "invokes_wait"

_Match = TypeVar("_Match")


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


def build_graph_control_edges(graph: SourceGraph, *, known_workflows: Collection[str]) -> list[ControlEdge]:
    """Derive cross-workflow invocation edges for a discovery graph.

    One edge per node that carries the :data:`INVOKES_WORKFLOW_PROPERTY` marker,
    found anywhere in the graph (fan-out inside ForEach / If / Switch preserved).
    A marker that is not a string gives an empty, unresolved target rather than a
    made-up name, and a wait flag that is not a boolean is recorded as unknown.
    Delegates the self-edge drop, dedup, and unresolved-callee recording to the
    shared :func:`~flowx.lineage.control_edges_from_calls`.

    Args:
        graph: The source graph to derive control edges for.
        known_workflows: Names of the workflows present in this export; see
            :func:`~flowx.lineage.control_edges_from_calls`.

    Returns:
        Deduplicated control edges, in first-seen order.
    """

    def _calls() -> Iterator[tuple[str, bool | None, str]]:
        for node in walk_nodes(graph.tasks):
            if INVOKES_WORKFLOW_PROPERTY not in node.properties:
                continue
            target = node.properties.get(INVOKES_WORKFLOW_PROPERTY)
            wait = node.properties.get(INVOKES_WAIT_PROPERTY)
            yield (
                target if isinstance(target, str) else "",
                wait if isinstance(wait, bool) else None,
                node.task_key,
            )

    return control_edges_from_calls(graph.name, _calls(), known_workflows=known_workflows)


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


def build_graph_lineage(graph: SourceGraph, *, known_workflows: Collection[str]) -> Lineage:
    """Compose the source-neutral lineage block for a discovery graph.

    Edges are derived here from the graph alone. Motifs come from a source's own
    detector, so this leaves ``motifs`` empty; a source that detects motifs at
    discover attaches them to ``lineage.motifs`` afterwards, so they are saved
    (and hashed) with the graph.

    Args:
        graph: The source graph to derive lineage for.
        known_workflows: Names of the workflows present in this export; an
            invocation edge is resolved only when its target is one of them. See
            :func:`~flowx.lineage.control_edges_from_calls`.

    Returns:
        A :class:`Lineage` with control edges and data edges (motifs empty).
    """
    return Lineage(
        control_edges=build_graph_control_edges(graph, known_workflows=known_workflows),
        data_edges=build_graph_data_edges(graph),
    )


def with_graph_lineage(graph: SourceGraph, *, known_workflows: Collection[str]) -> SourceGraph:
    """Return a *new* graph carrying its derived lineage, leaving the input untouched.

    Pure: builds the lineage block and attaches it via :func:`dataclasses.replace`
    rather than mutating the input graph. Motifs already recorded on the graph are
    kept, since :func:`build_graph_lineage` derives edges only.

    Args:
        graph: The source graph to copy.
        known_workflows: Names of the workflows present in this export, passed
            straight to :func:`build_graph_lineage`.

    Returns:
        A shallow copy of *graph* with :attr:`SourceGraph.lineage` populated.
    """
    motifs = list(graph.lineage.motifs) if graph.lineage else []
    lineage = dataclasses.replace(build_graph_lineage(graph, known_workflows=known_workflows), motifs=motifs)
    return dataclasses.replace(graph, lineage=lineage)


def distinct_motif_matches(
    matches: Iterable[_Match], *, motif_key: Callable[[_Match], tuple[str, Iterable[str]]]
) -> list[_Match]:
    """Drop repeated motif matches, keeping the first one seen and the original order.

    *motif_key* gives a match's motif id and its members' task keys. Two matches are
    the same motif when they have the same motif id and the same members, in any order;
    a match that only overlaps another (same motif id, other members) is kept. A
    detector can report one match twice (two upstreams that each pair with the same
    notification activity, say), and member names that differ only in punctuation share
    a task key, so the ADF loader, which counts motifs for the profile report, and the
    inventory both use this one rule to agree on the count.
    """
    seen: set[tuple[str, frozenset[str]]] = set()
    distinct: list[_Match] = []
    for match in matches:
        motif_id, member_task_keys = motif_key(match)
        identity = (motif_id, frozenset(member_task_keys))
        if identity not in seen:
            seen.add(identity)
            distinct.append(match)
    return distinct
