"""JSON serialisation for the shared discovery AST (:mod:`flowx.models.discovery`).

Kept separate from :mod:`flowx.ir_serde` on purpose: ``ir_serde`` owns the
``translation_report.json`` convert->package contract for the Databricks IR, and
the discovery AST is a different model with a different lifecycle. This module is
its own round-trip pair so evolving one shape never disturbs the other.

The DataAsset (de)serialisers are reused from ``ir_serde`` (``data_asset_to_dict``
/ ``data_asset_from_dict``) so the data-asset shape has a single definition
shared by the lineage substrate and the discovery AST. That shape is a general,
best-effort description of what a task reads/writes -- not physical-only: a
resolvable physical ``identity`` when there is one (else ``None``), an
always-present ``signature``, and an open ``asset_type`` that also covers
non-physical / logical / value hand-offs (e.g. an Airflow XCom). A graph's derived
:class:`~flowx.models.ir.Lineage` block is serialised through ``ir_serde``'s
``lineage_to_dict`` for the same reason; its inverse (:func:`_lineage_from_dict`)
lives here because ``ir_serde`` ships only the forward direction.

Every node dict carries a ``node_type`` discriminator (the dataclass name) so a
:class:`~flowx.models.discovery.ContainerNode` or
:class:`~flowx.models.discovery.GapNode` rehydrates to the right class. Lists are
always emitted as lists (never ``None``) for stable golden diffs.
"""

from __future__ import annotations

from typing import Any

from flowx.ir_serde import data_asset_from_dict, data_asset_to_dict, lineage_to_dict
from flowx.models.discovery import (
    ContainerNode,
    GapNode,
    ParameterSpec,
    PolicySpec,
    ScheduleSpec,
    SourceDependency,
    SourceGraph,
    SourceNode,
)
from flowx.models.ir import ControlEdge, DataEdge, Lineage, MotifAnnotation


def source_graph_to_dict(graph: SourceGraph) -> dict[str, Any]:
    """Serialise a :class:`SourceGraph` to a JSON-friendly dictionary."""
    result: dict[str, Any] = {
        "name": graph.name,
        "source": graph.source,
        "parameters": {name: _parameter_to_dict(spec) for name, spec in graph.parameters.items()},
        "variables": {name: _parameter_to_dict(spec) for name, spec in graph.variables.items()},
        "tags": list(graph.tags),
        "tasks": [_node_to_dict(node) for node in graph.tasks],
    }
    if graph.description is not None:
        result["description"] = graph.description
    if graph.schedule is not None:
        result["schedule"] = _schedule_to_dict(graph.schedule)
    if graph.default_policy is not None:
        result["default_policy"] = _policy_to_dict(graph.default_policy)
    if graph.run_timeout_seconds is not None:
        result["run_timeout_seconds"] = graph.run_timeout_seconds
    if graph.lineage is not None:
        result["lineage"] = lineage_to_dict(graph.lineage)
    if graph.properties:
        result["properties"] = dict(graph.properties)
    if graph.extensions:
        result["extensions"] = dict(graph.extensions)
    if graph.raw is not None:
        result["raw"] = graph.raw
    return result


def source_graph_from_dict(raw: dict[str, Any]) -> SourceGraph:
    """Rehydrate a :class:`SourceGraph` from the dict :func:`source_graph_to_dict` emits."""
    schedule = raw.get("schedule")
    default_policy = raw.get("default_policy")
    lineage = raw.get("lineage")
    return SourceGraph(
        name=raw.get("name", ""),
        source=raw.get("source", ""),
        description=raw.get("description"),
        parameters={name: _parameter_from_dict(spec) for name, spec in (raw.get("parameters") or {}).items()},
        variables={name: _parameter_from_dict(spec) for name, spec in (raw.get("variables") or {}).items()},
        schedule=_schedule_from_dict(schedule) if schedule else None,
        default_policy=_policy_from_dict(default_policy) if default_policy else None,
        run_timeout_seconds=raw.get("run_timeout_seconds"),
        tags=list(raw.get("tags") or []),
        tasks=[_node_from_dict(node) for node in raw.get("tasks") or []],
        lineage=_lineage_from_dict(lineage) if lineage else None,
        properties=dict(raw.get("properties") or {}),
        extensions=dict(raw.get("extensions") or {}),
        raw=raw.get("raw"),
    )


def _lineage_from_dict(raw: dict[str, Any]) -> Lineage:
    """Rehydrate a :class:`Lineage` block from the dict ``ir_serde.lineage_to_dict`` emits.

    The inverse of that forward serialiser (which ``ir_serde`` does not itself
    ship), so a discovery graph's lineage round-trips through this module.
    """
    return Lineage(
        control_edges=[
            ControlEdge(
                source_workflow=edge.get("source_workflow", ""),
                target_workflow=edge.get("target_workflow", ""),
                via_task_key=edge.get("via_task_key", ""),
                wait_for_completion=edge.get("wait_for_completion"),
                resolved=bool(edge.get("resolved", True)),
            )
            for edge in raw.get("control_edges") or []
        ],
        data_edges=[
            DataEdge(
                source_task_key=edge.get("source_task_key", ""),
                target_task_key=edge.get("target_task_key", ""),
                match_kind=edge.get("match_kind", ""),
                match_key=edge.get("match_key", ""),
                identity=edge.get("identity"),
                asset_type=edge.get("asset_type"),
            )
            for edge in raw.get("data_edges") or []
        ],
        motifs=[
            MotifAnnotation(
                motif_id=motif.get("motif_id", ""),
                member_task_keys=list(motif.get("member_task_keys") or []),
                display_name=motif.get("display_name"),
                databricks_replacement=motif.get("databricks_replacement"),
                notes=list(motif.get("notes") or []),
            )
            for motif in raw.get("motifs") or []
        ],
    )


def _parameter_to_dict(spec: ParameterSpec) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if spec.type is not None:
        result["type"] = spec.type
    if spec.default is not None:
        result["default"] = spec.default
    return result


def _parameter_from_dict(raw: dict[str, Any]) -> ParameterSpec:
    return ParameterSpec(type=raw.get("type"), default=raw.get("default"))


def _schedule_to_dict(schedule: ScheduleSpec) -> dict[str, Any]:
    result: dict[str, Any] = {"kind": schedule.kind}
    if schedule.expression is not None:
        result["expression"] = schedule.expression
    if schedule.timezone is not None:
        result["timezone"] = schedule.timezone
    if schedule.extensions:
        result["extensions"] = dict(schedule.extensions)
    return result


def _schedule_from_dict(raw: dict[str, Any]) -> ScheduleSpec:
    return ScheduleSpec(
        kind=raw.get("kind", ""),
        expression=raw.get("expression"),
        timezone=raw.get("timezone"),
        extensions=dict(raw.get("extensions") or {}),
    )


def _policy_to_dict(policy: PolicySpec) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if policy.timeout_seconds is not None:
        result["timeout_seconds"] = policy.timeout_seconds
    if policy.max_retries is not None:
        result["max_retries"] = policy.max_retries
    if policy.retry_interval_seconds is not None:
        result["retry_interval_seconds"] = policy.retry_interval_seconds
    if policy.extensions:
        result["extensions"] = dict(policy.extensions)
    return result


def _policy_from_dict(raw: dict[str, Any]) -> PolicySpec:
    return PolicySpec(
        timeout_seconds=raw.get("timeout_seconds"),
        max_retries=raw.get("max_retries"),
        retry_interval_seconds=raw.get("retry_interval_seconds"),
        extensions=dict(raw.get("extensions") or {}),
    )


def _dependency_to_dict(dependency: SourceDependency) -> dict[str, Any]:
    return {
        "upstream": dependency.upstream,
        "conditions": list(dependency.conditions),
        "resolved": dependency.resolved,
    }


def _dependency_from_dict(raw: dict[str, Any]) -> SourceDependency:
    return SourceDependency(
        upstream=raw.get("upstream", ""),
        conditions=list(raw.get("conditions") or []),
        resolved=bool(raw.get("resolved", True)),
    )


def _node_to_dict(node: SourceNode) -> dict[str, Any]:
    """Serialise any SourceNode (including Container/Gap subclasses) to a dict.

    The ``node_type`` discriminator is the dataclass name so the matching
    subclass is rebuilt on the way back.
    """
    result: dict[str, Any] = {
        "node_type": type(node).__name__,
        "source_id": node.source_id,
        "task_key": node.task_key,
        "concept": node.concept,
        "source": node.source,
        "dependencies": [_dependency_to_dict(dependency) for dependency in node.dependencies],
        "data_reads": [data_asset_to_dict(asset) for asset in node.data_reads],
        "data_writes": [data_asset_to_dict(asset) for asset in node.data_writes],
    }
    if node.name is not None:
        result["name"] = node.name
    if node.native_type is not None:
        result["native_type"] = node.native_type
    if node.run_condition is not None:
        result["run_condition"] = node.run_condition
    if node.policy is not None:
        result["policy"] = _policy_to_dict(node.policy)
    if node.properties:
        result["properties"] = dict(node.properties)
    if node.raw is not None:
        result["raw"] = node.raw
    if isinstance(node, ContainerNode):
        result["branches"] = {
            label: [_node_to_dict(child) for child in children] for label, children in node.branches.items()
        }
    if isinstance(node, GapNode) and node.reason is not None:
        result["reason"] = node.reason
    return result


def _node_from_dict(raw: dict[str, Any]) -> SourceNode:
    """Rehydrate any SourceNode from its dict, dispatching on ``node_type``.

    Optional fields absent from the dict fall back to each class's model
    default rather than being forced to ``""`` / empty. In particular
    ``concept`` is passed through only when the dict carries a non-empty value,
    so a partial :class:`GapNode` dict (no ``concept`` key) rehydrates with the
    model default ``CONCEPT_GAP`` instead of being overridden with ``""``.
    """
    common: dict[str, Any] = {
        "source_id": raw.get("source_id", ""),
        "task_key": raw.get("task_key", ""),
        "source": raw.get("source", ""),
        "name": raw.get("name"),
        "native_type": raw.get("native_type"),
        "run_condition": raw.get("run_condition"),
        "dependencies": [_dependency_from_dict(dependency) for dependency in raw.get("dependencies") or []],
        "policy": _policy_from_dict(raw["policy"]) if raw.get("policy") else None,
        "data_reads": [data_asset_from_dict(asset) for asset in raw.get("data_reads") or []],
        "data_writes": [data_asset_from_dict(asset) for asset in raw.get("data_writes") or []],
        "properties": dict(raw.get("properties") or {}),
        "raw": raw.get("raw"),
    }
    concept = raw.get("concept")
    if concept:
        common["concept"] = concept

    node_type = raw.get("node_type", "SourceNode")
    if node_type == "GapNode":
        # Leave `concept` unset when absent so GapNode's CONCEPT_GAP default wins.
        return GapNode(**common, reason=raw.get("reason"))
    # SourceNode / ContainerNode require `concept`; only a malformed dict omits it.
    common.setdefault("concept", "")
    if node_type == "ContainerNode":
        return ContainerNode(
            **common,
            branches={
                label: [_node_from_dict(child) for child in children]
                for label, children in (raw.get("branches") or {}).items()
            },
        )
    return SourceNode(**common)
