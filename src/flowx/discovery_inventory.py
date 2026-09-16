"""Source-agnostic projection of the shared discovery AST to ``inventory.json``.

The discover phase writes ``metadata/inventory.json`` and the reporting layer
(:mod:`flowx.reporting.coverage`) and MCP surface (:mod:`flowx.mcp.runner`) read
it back. Historically each source built that JSON straight from its own AST, so
the shape drifted per source. This module is the single place that turns the
shared discovery AST (:mod:`flowx.models.discovery`) into the inventory shape, so
every source that maps onto :class:`~flowx.models.discovery.SourceGraph` emits the
*same* top-level document -- ``{source, source_dir, pipelines, summary}`` -- from
one code path.

The projection is deliberately small and additive over the historical ADF shape:

* top level gains a ``source`` discriminator (``"adf"`` / ``"airflow"``);
* each activity keeps its byte-compatible ``name`` / ``type`` / ``strategy`` (and
  ``depends_on`` names when present) and gains the standardised
  ``original_type``, ``dependencies`` (upstream **with conditions**), and the
  verbatim per-node ``raw``;
* each pipeline entry gains an additive ``lineage`` block (control + data edges)
  when its :class:`~flowx.models.discovery.SourceGraph` carries derived lineage;
* each pipeline entry gains an additive ``motifs`` list -- the multi-activity ADF
  patterns the profiler *detects* at discover time (see
  :mod:`flowx.motifs.detector`) surfaced verbatim, **without** collapsing the
  member activities. Motifs are their own inventory concept and are deliberately
  **decoupled from the lineage block** (they do not nest under ``lineage.motifs``,
  which stays a convert-time IR concern); the key is omitted when a pipeline has
  no detected motif. Collapse remains a *convert* decision
  (:mod:`flowx.motifs.collapser`), never a discover one, so every member activity
  still appears as its own entry in ``activities``;
* the ``summary`` keeps the historical count block.

Lineage is placed per pipeline -- one block beside that pipeline's ``activities``
-- to mirror the shared discovery serde, where lineage is a per-graph field
(:func:`flowx.discovery_serde.source_graph_to_dict`). The block is produced by the
one shared serialiser (:func:`flowx.ir_serde.lineage_to_dict`, the same one the
serde consumes), so an emitted block is byte-identical to the serde's and
round-trips through :func:`flowx.discovery_serde.source_graph_from_dict`. It is a
new key only: a graph with no derived lineage (``graph.lineage is None``) omits it
entirely, so the historical consumer keys (``source`` / ``pipelines`` /
``activities`` / ``summary``) are untouched.

A node's translation ``strategy`` is a Databricks-*target* classification rather
than a source concept, so it is not a typed field on the discovery AST. By
convention a mapper stashes it under ``node.properties["strategy"]`` (see
:data:`STRATEGY_PROPERTY`); this module reads it there. Detected motifs are
handled the same way -- they carry a Databricks-*target* replacement and are not
a shared source concept, so they are not a typed field on the AST either; a
source supplies them per pipeline via ``motifs_by_pipeline`` and this module
projects them. Anything else a source wants to layer on -- Airflow's
audited-count block, findings, reconciliation status -- rides additively on top
of this base and is out of scope here.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from flowx.ir_serde import lineage_to_dict
from flowx.models.discovery import ContainerNode, SourceGraph, SourceNode
from flowx.models.motifs import DetectedMotif

# Well-known property key under which a mapper records a node's Databricks-target
# translation strategy ("deterministic" / "agentic" / "unsupported"). Kept in the
# free-form properties seam because strategy is a target concern, not a shared
# source concept, so it earns no typed field on the discovery AST.
STRATEGY_PROPERTY = "strategy"

_DETERMINISTIC = "deterministic"
_AGENTIC = "agentic"


def build_source_inventory(
    graphs: list[SourceGraph],
    *,
    source: str,
    source_dir: str,
    include_empty_pipelines: bool = True,
    motifs_by_pipeline: Mapping[str, list[DetectedMotif]] | None = None,
) -> dict[str, Any]:
    """Project a list of source graphs into the ``inventory.json`` document.

    Args:
        graphs: The source workflows to inventory, already mapped onto the shared
            discovery AST.
        source: Source discriminator for the top-level ``source`` field
            (``SOURCE_ADF`` / ``SOURCE_AIRFLOW``).
        source_dir: Original source directory, echoed back for provenance.
        include_empty_pipelines: When ``False``, a graph that contributes no
            activities is left out of the ``pipelines`` list but still counted in
            ``summary.pipeline_count`` -- this reproduces ADF's long-standing
            behaviour of omitting zero-activity pipelines from the per-pipeline
            listing while still reporting them in the totals. Sources that list
            every workflow (Airflow) leave this ``True``.
        motifs_by_pipeline: Optional map from pipeline name to the motifs a source
            detected in it (see :mod:`flowx.motifs.detector`). A pipeline with a
            non-empty entry gains an additive ``motifs`` list; the key is omitted
            otherwise. This is surfacing only -- the member activities are never
            collapsed here (collapse is a convert decision). Exact-duplicate
            detections (same ``motif_id`` and same member set) are collapsed to one
            (see :func:`_dedupe_motif_entries`); overlapping-but-distinct matches
            are all kept. Keyed by
            :attr:`~flowx.models.discovery.SourceGraph.name`, so a source with no
            motif detector simply passes ``None``.

    Returns:
        A JSON-friendly dict with ``source``, ``source_dir``, ``pipelines`` and
        ``summary`` keys.
    """
    motifs_by_pipeline = motifs_by_pipeline or {}
    pipeline_entries: list[dict[str, Any]] = []
    deterministic = 0
    agentic = 0
    unsupported = 0

    for graph in graphs:
        flattened = _flatten_nodes(graph.tasks)
        for node in flattened:
            strategy = node.properties.get(STRATEGY_PROPERTY)
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
                entry["lineage"] = lineage_to_dict(graph.lineage)
            detected = motifs_by_pipeline.get(graph.name)
            if detected:
                entry["motifs"] = _dedupe_motif_entries([_motif_entry(motif) for motif in detected])
            pipeline_entries.append(entry)

    total = deterministic + agentic + unsupported
    coverage_pct = round((deterministic + agentic) / total * 100, 1) if total else 0.0

    return {
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


def _flatten_nodes(nodes: list[SourceNode]) -> list[SourceNode]:
    """Flatten container branches into one depth-first activity list.

    Order is parent, then each branch's children in the branch's own insertion
    order, recursively -- so an ``IfCondition``'s ``true`` branch precedes its
    ``false`` branch and a ``Switch``'s cases precede its ``default``, matching the
    order the source declared them.
    """
    flattened: list[SourceNode] = []
    for node in nodes:
        flattened.append(node)
        if isinstance(node, ContainerNode):
            for children in node.branches.values():
                flattened.extend(_flatten_nodes(children))
    return flattened


def _activity_entry(node: SourceNode) -> dict[str, Any]:
    """Build one per-activity inventory entry from a node.

    The first three keys (plus ``depends_on`` when the node has dependencies)
    reproduce the historical ADF activity shape byte-for-byte; the rest are the
    additive standardised fields.
    """
    entry: dict[str, Any] = {
        "name": node.name if node.name is not None else node.task_key,
        "type": node.native_type,
        "strategy": node.properties.get(STRATEGY_PROPERTY),
    }
    upstream_names = [dependency.upstream for dependency in node.dependencies]
    if upstream_names:
        entry["depends_on"] = upstream_names

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


def _motif_entry(motif: DetectedMotif) -> dict[str, Any]:
    """Project one detected motif into its additive inventory entry.

    Surfacing only: ``member_task_keys`` names the participating activities as the
    detector claimed them (for ADF these are the activity names, which are the
    same values used as each activity entry's ``name`` / task key, so a consumer
    can join a motif back to its members). The field names mirror the existing
    :class:`~flowx.models.ir.MotifAnnotation` vocabulary so the two motif views
    read the same, while staying a separate key from the ``lineage`` block. The
    detector reports its confidence as human-readable rationale rather than a
    numeric score, so ``confidence_notes`` carries that verbatim.
    """
    return {
        "motif_id": motif.definition.motif_id,
        "display_name": motif.definition.display_name,
        "databricks_replacement": motif.definition.databricks_replacement,
        "member_task_keys": list(motif.matched_activities),
        "source_type_hint": motif.source_type_hint,
        "confidence_notes": list(motif.confidence_notes),
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
