"""Project discovery graphs into the ``inventory.json`` document.

Every source that maps onto :class:`~flowx.models.discovery.SourceGraph` gets the
same ``{source, source_dir, pipelines, summary}`` document from this one code
path, which the reporting layer and the MCP surface read back. It keeps the
historical ADF activity keys and adds standardised ones; see
:func:`build_source_inventory`.
"""

from __future__ import annotations

from typing import Any

from flowx.discovery_lineage import walk_nodes
from flowx.ir_serde import lineage_to_dict
from flowx.models.discovery import SourceGraph, SourceNode
from flowx.models.lineage import Lineage, MotifAnnotation

# Well-known property key under which a mapper records a node's Databricks-target
# translation strategy ("deterministic" / "agentic" / "unsupported"). Kept in the
# free-form properties seam because strategy is a target concern, not a shared
# source concept, so it earns no typed field on the discovery graph.
STRATEGY_PROPERTY = "strategy"
INVENTORY_VISIBLE_PROPERTY = "inventory_visible"

_DETERMINISTIC = "deterministic"
_AGENTIC = "agentic"
_UNSUPPORTED = "unsupported"


def build_source_inventory(
    graphs: list[SourceGraph],
    *,
    source: str,
    source_dir: str,
    include_empty_pipelines: bool = True,
    source_graphs_sha256: str | None = None,
) -> dict[str, Any]:
    """Project a list of source graphs into the ``inventory.json`` document.

    Args:
        graphs: The source workflows to inventory, already mapped onto the
            discovery graph contract.
        source: Source discriminator for the top-level ``source`` field
            (``SOURCE_ADF`` / ``SOURCE_AIRFLOW``).
        source_dir: Original source directory, echoed back for provenance.
        include_empty_pipelines: When ``False``, a graph that contributes no
            activities is left out of the ``pipelines`` list but still counted in
            ``summary.pipeline_count`` -- this reproduces ADF's long-standing
            behaviour of omitting zero-activity pipelines from the per-pipeline
            listing while still reporting them in the totals. Sources that list
            every workflow (Airflow) leave this ``True``.
        source_graphs_sha256: The ``document_sha256`` of the persisted
            ``source_graphs.json`` these graphs were written to, recorded as a
            top-level key so later phases can tell which saved graph the
            inventory describes. Omitted when ``None``.

    Each activity entry is built by :func:`_activity_entry`, with the strategy read
    from ``node.properties["strategy"]`` (:data:`STRATEGY_PROPERTY`). Container
    children are listed depth-first after their container. A node whose
    ``inventory_visible`` property is exactly ``False`` (for example an Airflow
    TaskGroup kept only for structure) is left out of the list and the counts,
    while its children are still listed.

    A graph with derived lineage gives its pipeline entry a ``lineage`` block with
    its control and data edges, written by :func:`flowx.ir_serde.lineage_to_dict`.
    That block's ``motifs`` is always empty. Detected motifs go in a separate
    ``motifs`` list instead, and their member activities stay listed: collapsing a
    motif is a convert decision, never a discover one. Exact-duplicate detections
    (same ``motif_id`` and same member set) collapse to one (see
    :func:`_dedupe_motif_entries`); overlapping-but-distinct matches are all kept.

    Returns:
        A JSON-friendly dict with ``source``, ``source_dir``, ``pipelines`` and
        ``summary`` keys, plus ``source_graphs_sha256`` when given.
    """
    pipeline_entries: list[dict[str, Any]] = []
    deterministic = 0
    agentic = 0
    unsupported = 0

    for graph in graphs:
        flattened = [
            node for node in walk_nodes(graph.tasks) if node.properties.get(INVENTORY_VISIBLE_PROPERTY) is not False
        ]
        for node in flattened:
            strategy = _strategy(node)
            if strategy == _DETERMINISTIC:
                deterministic += 1
            elif strategy == _AGENTIC:
                agentic += 1
            else:
                unsupported += 1
        if flattened or include_empty_pipelines:
            entry: dict[str, Any] = {
                "name": graph.name,
                "activities": [_activity_entry(node) for node in flattened],
            }
            if graph.lineage is not None:
                edges_only = Lineage(control_edges=graph.lineage.control_edges, data_edges=graph.lineage.data_edges)
                entry["lineage"] = lineage_to_dict(edges_only)
                if graph.lineage.motifs:
                    entry["motifs"] = _dedupe_motif_entries([_motif_entry(motif) for motif in graph.lineage.motifs])
            pipeline_entries.append(entry)

    total = deterministic + agentic + unsupported
    coverage_pct = round((deterministic + agentic) / total * 100, 1) if total else 0.0

    inventory: dict[str, Any] = {
        "source": source,
        "source_dir": source_dir,
        "pipelines": pipeline_entries,
        "summary": {
            "pipeline_count": len(graphs),
            "activity_count": total,
            "deterministic_count": deterministic,
            "agentic_count": agentic,
            "unsupported_count": unsupported,
            "coverage_pct": coverage_pct,
        },
    }
    if source_graphs_sha256 is not None:
        inventory["source_graphs_sha256"] = source_graphs_sha256
    return inventory


def _strategy(node: SourceNode) -> str:
    """Return the node's strategy, treating a missing or unrecognised one as unsupported.

    The summary counts and the activity entry both use this, so they always agree.
    """
    strategy = node.properties.get(STRATEGY_PROPERTY)
    return strategy if strategy in (_DETERMINISTIC, _AGENTIC) else _UNSUPPORTED


def _activity_entry(node: SourceNode) -> dict[str, Any]:
    """Build one per-activity inventory entry from a node.

    The first three keys (plus ``depends_on`` when the node has dependencies)
    reproduce the historical ADF activity shape byte-for-byte; the rest are the
    additive standardised fields.
    """
    entry: dict[str, Any] = {
        "name": node.name if node.name is not None else node.task_key,
        "type": node.native_type,
        "strategy": _strategy(node),
    }
    upstream_names = [dependency.upstream for dependency in node.dependencies]
    if upstream_names:
        entry["depends_on"] = upstream_names

    entry["task_key"] = node.task_key
    entry["original_type"] = node.native_type
    entry["dependencies"] = [
        {
            "upstream": dependency.upstream,
            "conditions": list(dependency.conditions),
            "resolved": dependency.resolved,
        }
        for dependency in node.dependencies
    ]
    if node.raw is not None:
        entry["raw"] = node.raw
    return entry


def _motif_entry(motif: MotifAnnotation) -> dict[str, Any]:
    """Project one motif annotation into its additive inventory entry.

    Surfacing only: ``member_task_keys`` names the participating activities as the
    detector claimed them (for ADF these are the activity names, which are the
    same values used as each activity entry's ``task_key``, so a consumer can join
    a motif back to its members). The detector reports its confidence as
    human-readable rationale rather than a numeric score, so the annotation's
    ``notes`` are surfaced verbatim as ``confidence_notes``, the inventory's
    long-standing key.
    """
    return {
        "motif_id": motif.motif_id,
        "display_name": motif.display_name,
        "databricks_replacement": motif.databricks_replacement,
        "member_task_keys": list(motif.member_task_keys),
        "source_type_hint": motif.source_type_hint,
        "confidence_notes": list(motif.notes),
    }


def _dedupe_motif_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse exact-duplicate motif entries, preserving first-seen order.

    A detector can report the same match more than once (e.g. two upstreams that
    each pair with the same notification activity), which would otherwise emit
    identical inventory entries. Two entries are the *same* motif only when they
    share both ``motif_id`` **and** the exact same set of ``member_task_keys``;
    such duplicates collapse to the first occurrence. Entries that merely overlap
    -- same ``motif_id`` but a different member set -- are genuinely distinct
    matches and are all kept. The member comparison is order-insensitive (a set),
    so the same activities in a different order still count as one motif.
    """
    seen: set[tuple[str, frozenset[str]]] = set()
    deduped: list[dict[str, Any]] = []
    for entry in entries:
        identity = (entry["motif_id"], frozenset(entry["member_task_keys"]))
        if identity in seen:
            continue
        seen.add(identity)
        deduped.append(entry)
    return deduped
