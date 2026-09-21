"""Source-neutral projection of shared discovery graphs to ``inventory.json``."""

from __future__ import annotations

from typing import Any

from flowx.ir_serde import lineage_to_dict
from flowx.models.discovery import ContainerNode, SourceGraph, SourceNode

STRATEGY_PROPERTY = "strategy"
INVENTORY_VISIBLE_PROPERTY = "inventory_visible"

_DETERMINISTIC = "deterministic"
_AGENTIC = "agentic"


def build_source_inventory(
    graphs: list[SourceGraph],
    *,
    source: str,
    source_dir: str,
    include_empty_pipelines: bool = True,
) -> dict[str, Any]:
    """Projects source graphs into the common discovery inventory shape."""
    pipeline_entries: list[dict[str, Any]] = []
    deterministic = 0
    agentic = 0
    unsupported = 0

    for graph in graphs:
        flattened = [
            node for node in _flatten_nodes(graph.tasks) if node.properties.get(INVENTORY_VISIBLE_PROPERTY, True)
        ]
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
    """Flattens container branches in source order."""
    flattened: list[SourceNode] = []
    for node in nodes:
        flattened.append(node)
        if isinstance(node, ContainerNode):
            for children in node.branches.values():
                flattened.extend(_flatten_nodes(children))
    return flattened


def _activity_entry(node: SourceNode) -> dict[str, Any]:
    """Builds one additive per-activity inventory entry."""
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
