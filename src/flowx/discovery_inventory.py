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
* the ``summary`` keeps the historical count block.

A node's translation ``strategy`` is a Databricks-*target* classification rather
than a source concept, so it is not a typed field on the discovery AST. By
convention a mapper stashes it under ``node.properties["strategy"]`` (see
:data:`STRATEGY_PROPERTY`); this module reads it there. Anything else a source
wants to layer on -- Airflow's audited-count block, findings, reconciliation
status -- rides additively on top of this base and is out of scope here.
"""

from __future__ import annotations

from typing import Any

from flowx.models.discovery import ContainerNode, SourceGraph, SourceNode

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

    Returns:
        A JSON-friendly dict with ``source``, ``source_dir``, ``pipelines`` and
        ``summary`` keys.
    """
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
            pipeline_entries.append(
                {
                    "name": graph.name,
                    "activities": [_activity_entry(node) for node in flattened],
                }
            )

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
