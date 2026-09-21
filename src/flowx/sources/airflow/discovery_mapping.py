"""Map Airflow source captures onto the shared discovery graph."""

from __future__ import annotations

import ast
import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flowx.discovery_lineage import INVOKES_WAIT_PROPERTY, INVOKES_WORKFLOW_PROPERTY, walk_nodes, with_graph_lineage
from flowx.models.discovery import (
    CONCEPT_BRANCH,
    CONCEPT_GAP,
    CONCEPT_GROUP,
    CONCEPT_LOOP,
    CONCEPT_NOTEBOOK,
    CONCEPT_QUERY,
    CONCEPT_RUN_WORKFLOW,
    CONCEPT_SCRIPT,
    CONCEPT_WAIT,
    SOURCE_AIRFLOW,
    ContainerNode,
    GapNode,
    ParameterSpec,
    PolicySpec,
    ScheduleSpec,
    SourceDependency,
    SourceGraph,
    SourceNode,
)
from flowx.models.ir import Activity, DataAsset, ForEachActivity, Pipeline, PlaceholderActivity
from flowx.sources.airflow import operators as ops
from flowx.sources.airflow import templating
from flowx.sources.airflow.audit import SourceAudit
from flowx.sources.airflow.loader.captures import DagDeclaration, SourceSpan
from flowx.sources.airflow.loader.graph import _allocate_task_keys, _expand_group_edges
from flowx.sources.airflow.loader.policy import _job_timeout_seconds
from flowx.sources.airflow.loader.schedule import _asset_expression
from flowx.sources.airflow.loader.visitor import _DagVisitor

_QUERY_OPERATORS = frozenset(
    {
        "DatabricksSqlOperator",
        "DatabricksSQLStatementsOperator",
        "SQLExecuteQueryOperator",
        "PostgresOperator",
        "MySqlOperator",
        "HiveOperator",
        "DatabricksCopyIntoOperator",
    }
)
_SCRIPT_OPERATORS = frozenset(
    {
        "BashOperator",
        "SSHOperator",
        "PythonOperator",
        "PythonVirtualenvOperator",
        "ExternalPythonOperator",
        "SparkSubmitOperator",
    }
)
_BRANCH_OPERATORS = frozenset({"BranchPythonOperator", "ShortCircuitOperator"})


@dataclass(slots=True, kw_only=True)
class AirflowDiscoveryResult:
    """One Airflow DAG represented as both current IR and source-faithful discovery graph."""

    pipeline: Pipeline
    graph: SourceGraph


def sync_graph_translation_metadata(graph: SourceGraph, pipeline: Pipeline) -> None:
    """Refreshes target classifications after exclusions or cross-DAG rewrites."""
    activities = _activity_index(pipeline.tasks)
    for node in walk_nodes(graph.tasks):
        if node.properties.get("structural_only"):
            continue
        if node.task_key in activities:
            node.properties["strategy"] = _strategy_for(node.task_key, activities, pipeline.reconciliation_status)
    graph.properties.update(
        {
            "reconciliation_status": pipeline.reconciliation_status,
            "migration_status": pipeline.migration_status,
            "findings": list(pipeline.not_translatable),
            "transformations": list(pipeline.audit.get("transformations", [])),
        }
    )


def build_airflow_source_graph(
    *,
    dag_path: Path,
    source_file: str,
    source: str,
    declaration: DagDeclaration,
    visitor: _DagVisitor,
    audit: SourceAudit,
    pipeline: Pipeline,
) -> SourceGraph:
    """Projects one captured DAG onto ``SourceGraph`` without reparsing it."""
    task_keys = _allocate_task_keys(
        visitor.operators,
        {variable: task.task_id for variable, task in visitor.taskflow_tasks.items()},
        {variable: task_id for variable, (task_id, _, _) in visitor.taskgroup_calls.items()},
        visitor.groups,
        visitor.capture_source_nodes,
    )
    expanded_edges = _expand_group_edges(visitor.edges, visitor.groups, visitor.group_vars)
    upstreams: dict[str, list[str]] = {capture_id: [] for capture_id in task_keys}
    for upstream, downstream in expanded_edges:
        if upstream in task_keys and downstream in upstreams:
            upstreams[downstream].append(upstream)

    activities = _activity_index(pipeline.tasks)
    nodes_by_capture: dict[str, SourceNode] = {}
    for capture_id, task_key in task_keys.items():
        nodes_by_capture[capture_id] = _captured_node(
            capture_id=capture_id,
            task_key=task_key,
            upstreams=upstreams.get(capture_id, []),
            task_keys=task_keys,
            visitor=visitor,
            source=source,
            activities=activities,
        )

    tasks = _nest_task_groups(nodes_by_capture, visitor, source)
    tasks.extend(_gap_nodes(visitor, source))
    declaration_raw = {
        "capture_id": declaration.capture_id,
        "kind": declaration.kind,
        "source_file": source_file,
        "source_span": _span_dict(declaration.span),
        "source": ast.get_source_segment(source, declaration.node) or ast.unparse(declaration.node),
        "dag_arguments": {name: _expression_payload(value, source) for name, value in visitor.dag_kwargs.items()},
    }
    if declaration.factory is not None:
        declaration_raw["factory_definition"] = _definition_payload(declaration.factory, source)
    graph = SourceGraph(
        name=pipeline.name,
        source=SOURCE_AIRFLOW,
        description=visitor.dag_description,
        parameters={name: ParameterSpec(default=value) for name, value in visitor.dag_params.items()},
        schedule=_source_schedule(visitor, source),
        default_policy=_policy(visitor.default_args),
        run_timeout_seconds=_job_timeout_seconds(visitor),
        tags=list(visitor.dag_user_tags),
        tasks=tasks,
        properties={
            "reconciliation_status": pipeline.reconciliation_status,
            "migration_status": pipeline.migration_status,
            "findings": list(pipeline.not_translatable),
            "transformations": list(pipeline.audit.get("transformations", [])),
        },
        extensions={
            "source_file": source_file,
            "source_path": dag_path.as_posix(),
            "airflow_generation": visitor.airflow_generation,
            "declaration_capture_id": declaration.capture_id,
            "edge_captures": [
                {
                    "upstream_capture_id": edge.upstream_id,
                    "downstream_capture_id": edge.downstream_id,
                    "source_span": _span_dict(edge.span),
                }
                for edge in visitor.edge_captures
            ],
            "audit": {
                "tasks": [_audit_candidate_payload(candidate) for candidate in audit.tasks],
                "edges": [_audit_candidate_payload(candidate) for candidate in audit.edges],
                "settings": [_audit_candidate_payload(candidate) for candidate in audit.settings],
                "unresolved": [_audit_candidate_payload(candidate) for candidate in audit.unresolved],
            },
        },
        raw=declaration_raw,
    )
    return with_graph_lineage(graph)


def failed_declaration_source_graph(
    *,
    dag_path: Path,
    source_file: str,
    source: str,
    declaration: DagDeclaration,
    pipeline: Pipeline,
) -> SourceGraph:
    """Builds a reportable graph for a DAG declaration that static capture rejected."""
    raw_source = ast.get_source_segment(source, declaration.node) or ast.unparse(declaration.node)
    gap = GapNode(
        source_id=declaration.capture_id,
        task_key=f"__flowx_gap_{declaration.span.line}_{declaration.span.column}",
        source=SOURCE_AIRFLOW,
        name=declaration.variable or dag_path.stem,
        native_type="DagDeclaration",
        reason=declaration.unsupported_reason,
        properties={"strategy": "unsupported"},
        raw={"source": raw_source, "source_span": _span_dict(declaration.span)},
    )
    return SourceGraph(
        name=pipeline.name,
        source=SOURCE_AIRFLOW,
        tasks=[gap],
        properties={
            "reconciliation_status": pipeline.reconciliation_status,
            "findings": list(pipeline.not_translatable),
        },
        extensions={"source_file": source_file, "source_path": dag_path.as_posix()},
        raw={
            "capture_id": declaration.capture_id,
            "kind": declaration.kind,
            "source_file": source_file,
            "source_span": _span_dict(declaration.span),
            "source": raw_source,
        },
    )


def _captured_node(
    *,
    capture_id: str,
    task_key: str,
    upstreams: list[str],
    task_keys: dict[str, str],
    visitor: _DagVisitor,
    source: str,
    activities: dict[str, Activity],
) -> SourceNode:
    dependencies = [
        SourceDependency(upstream=task_keys[upstream], resolved=True)
        for upstream in dict.fromkeys(upstreams)
        if upstream in task_keys
    ]
    source_node = visitor.capture_source_nodes[capture_id]
    span = SourceSpan(
        line=getattr(source_node, "lineno", 0),
        column=getattr(source_node, "col_offset", 0),
        end_line=getattr(source_node, "end_lineno", getattr(source_node, "lineno", 0)),
        end_column=getattr(source_node, "end_col_offset", getattr(source_node, "col_offset", 0)),
    )
    raw: dict[str, Any] = {
        "source": ast.get_source_segment(source, source_node) or ast.unparse(source_node),
        "source_span": _span_dict(span),
    }
    properties: dict[str, Any] = {"strategy": _strategy_for(task_key, activities, pipeline_status=None)}
    run_condition: str | None = None

    if capture_id in visitor.operators:
        task_id, operator, kwargs = visitor.operators[capture_id]
        raw["arguments"] = {name: _expression_payload(value, source) for name, value in kwargs.items()}
        raw["operator_fqn"] = visitor.task_captures[capture_id].operator_fqn
        raw["argument_disposition"] = ops.argument_classification(operator, kwargs)
        callable_definition = visitor.resolved_callable_for(capture_id)
        if callable_definition is not None:
            raw["callable_definition"] = _definition_payload(callable_definition, source)
        run_condition = ops.literal_str(kwargs.get("trigger_rule"))
        policy = _policy(kwargs)
        concept = _operator_concept(operator, capture_id in visitor.mapped)
        reads, writes = _operator_assets(operator, kwargs)
        if operator == "TriggerDagRunOperator":
            target = ops.literal_str(kwargs.get("trigger_dag_id"))
            if target is not None:
                properties[INVOKES_WORKFLOW_PROPERTY] = target
                wait_node = kwargs.get("wait_for_completion")
                wait_value = ops.literal_value(wait_node)
                properties[INVOKES_WAIT_PROPERTY] = (
                    False if wait_node is None else wait_value if isinstance(wait_value, bool) else None
                )
        elif operator in {"DatabricksRunNowOperator", "DatabricksRunNowDeferrableOperator"}:
            job_id = ops.literal_value(kwargs.get("job_id"))
            if job_id is not None:
                properties[INVOKES_WORKFLOW_PROPERTY] = f"databricks-job:{job_id}"
        elif operator in {"ExternalTaskSensor", "ExternalTaskSensorAsync"}:
            properties["external_workflow_wait"] = {
                "dag_id": ops.literal_str(kwargs.get("external_dag_id")),
                "task_id": ops.literal_str(kwargs.get("external_task_id")),
            }
        if capture_id in visitor.mapped:
            properties["mapping"] = {
                "expand_arguments": list(visitor.expand_kwargs.get(capture_id, [])),
                "has_partial": capture_id in visitor.partial_mapped,
            }
            return ContainerNode(
                source_id=capture_id,
                task_key=task_key,
                concept=CONCEPT_LOOP,
                source=SOURCE_AIRFLOW,
                name=task_id,
                native_type=operator,
                dependencies=dependencies,
                run_condition=run_condition,
                policy=policy,
                data_reads=reads,
                data_writes=writes,
                properties=properties,
                raw=raw,
                branches={"body": []},
            )
        activity = activities.get(task_key)
        if isinstance(activity, PlaceholderActivity) or concept == CONCEPT_GAP:
            reason = activity.comment if isinstance(activity, PlaceholderActivity) else None
            return GapNode(
                source_id=capture_id,
                task_key=task_key,
                source=SOURCE_AIRFLOW,
                name=task_id,
                native_type=operator,
                dependencies=dependencies,
                run_condition=run_condition,
                policy=policy,
                data_reads=reads,
                data_writes=writes,
                properties=properties,
                raw=raw,
                reason=reason or f"Airflow operator {operator!r} has no deterministic mapping.",
            )
        return SourceNode(
            source_id=capture_id,
            task_key=task_key,
            concept=concept,
            source=SOURCE_AIRFLOW,
            name=task_id,
            native_type=operator,
            dependencies=dependencies,
            run_condition=run_condition,
            policy=policy,
            data_reads=reads,
            data_writes=writes,
            properties=properties,
            raw=raw,
        )

    if capture_id in visitor.taskflow_tasks:
        task = visitor.taskflow_tasks[capture_id]
        raw.update(
            {
                "decorator": task.decorator,
                "callable": task.def_name,
                "source_reference": task.source_reference,
                "positional_arguments": dict(task.positional_values),
                "keyword_arguments": dict(task.keyword_values),
                "unresolved_arguments": list(task.unresolved_arguments),
            }
        )
        taskflow_definition = visitor.taskflow_defs[task.def_name][0]
        raw["callable_definition"] = _definition_payload(taskflow_definition, source)
        data_upstreams = [
            *[task.positional_deps[position] for position in sorted(task.positional_deps)],
            *task.keyword_deps.values(),
        ]
        reads = [
            DataAsset(signature=f"xcom:{task_keys[upstream]}", asset_type="value")
            for upstream in dict.fromkeys(data_upstreams)
            if upstream in task_keys
        ]
        writes = [DataAsset(signature=f"xcom:{task_key}", asset_type="value")]
        if capture_id in visitor.mapped:
            properties["mapping"] = {
                "expand_argument": task.expand_kwarg,
                "expand_items_json": task.expand_items_json,
            }
            return ContainerNode(
                source_id=capture_id,
                task_key=task_key,
                concept=CONCEPT_LOOP,
                source=SOURCE_AIRFLOW,
                name=task.task_id,
                native_type=task.decorator,
                dependencies=dependencies,
                data_reads=reads,
                data_writes=writes,
                properties=properties,
                raw=raw,
                branches={"body": []},
            )
        activity = activities.get(task_key)
        if isinstance(activity, PlaceholderActivity):
            return GapNode(
                source_id=capture_id,
                task_key=task_key,
                source=SOURCE_AIRFLOW,
                name=task.task_id,
                native_type=task.decorator,
                dependencies=dependencies,
                data_reads=reads,
                data_writes=writes,
                properties=properties,
                raw=raw,
                reason=activity.comment or "TaskFlow invocation requires manual migration.",
            )
        return SourceNode(
            source_id=capture_id,
            task_key=task_key,
            concept=CONCEPT_SCRIPT,
            source=SOURCE_AIRFLOW,
            name=task.task_id,
            native_type=task.decorator,
            dependencies=dependencies,
            data_reads=reads,
            data_writes=writes,
            properties=properties,
            raw=raw,
        )

    task_id, definition, mapped = visitor.taskgroup_calls[capture_id]
    raw.update({"task_group_callable": definition, "mapped": mapped})
    taskgroup_definition = visitor.taskgroup_defs.get(definition)
    if taskgroup_definition is not None:
        raw["callable_definition"] = _definition_payload(taskgroup_definition, source)
    return ContainerNode(
        source_id=capture_id,
        task_key=task_key,
        concept=CONCEPT_GROUP,
        source=SOURCE_AIRFLOW,
        name=task_id,
        native_type="task_group",
        dependencies=dependencies,
        properties=properties,
        raw=raw,
        branches={"group": []},
    )


def _nest_task_groups(
    nodes_by_capture: dict[str, SourceNode],
    visitor: _DagVisitor,
    source: str,
) -> list[SourceNode]:
    roots: list[SourceNode] = []
    containers: dict[str, ContainerNode] = {}

    def container(path: str) -> ContainerNode:
        existing = containers.get(path)
        if existing is not None:
            return existing
        parent_path, _, leaf = path.rpartition("__")
        group_node = visitor.group_source_nodes.get(path)
        raw = None
        if group_node is not None:
            raw = {
                "source": ast.get_source_segment(source, group_node) or ast.unparse(group_node),
                "source_span": {
                    "line": getattr(group_node, "lineno", 0),
                    "column": getattr(group_node, "col_offset", 0),
                    "end_line": getattr(group_node, "end_lineno", 0),
                    "end_column": getattr(group_node, "end_col_offset", 0),
                },
            }
        created = ContainerNode(
            source_id=f"task-group:{path}",
            task_key=path,
            concept=CONCEPT_GROUP,
            source=SOURCE_AIRFLOW,
            name=leaf,
            native_type="TaskGroup",
            properties={"inventory_visible": False, "structural_only": True},
            raw=raw,
            branches={"group": []},
        )
        containers[path] = created
        if parent_path:
            container(parent_path).branches["group"].append(created)
        else:
            roots.append(created)
        return created

    for capture_id, node in nodes_by_capture.items():
        group_path = visitor.groups.get(capture_id)
        if group_path:
            container(group_path).branches["group"].append(node)
        else:
            roots.append(node)
    return roots


def _gap_nodes(visitor: _DagVisitor, source: str) -> list[GapNode]:
    entries: list[tuple[str, ast.AST, str]] = []
    entries.extend(
        ("unclaimed_task_call", node, "Airflow task call was not captured by the static subset.")
        for node in visitor.unclaimed_task_calls
    )
    entries.extend(
        ("unclaimed_statement", node, "DAG-body statement was not claimed by the static subset.")
        for node in visitor.unclaimed_statements
    )
    entries.extend(("unresolved_construct", node, reason) for reason, node in visitor.unresolved_constructs)
    gaps: list[GapNode] = []
    seen: set[tuple[str, int, int, int, int]] = set()
    for code, node, reason in entries:
        key = (
            code,
            getattr(node, "lineno", 0),
            getattr(node, "col_offset", 0),
            getattr(node, "end_lineno", 0),
            getattr(node, "end_col_offset", 0),
        )
        if key in seen:
            continue
        seen.add(key)
        source_id = f"{code}:{key[1]}:{key[2]}:{len(gaps) + 1}"
        gaps.append(
            GapNode(
                source_id=source_id,
                task_key=f"__flowx_gap_{key[1]}_{key[2]}_{len(gaps) + 1}",
                source=SOURCE_AIRFLOW,
                name=code,
                native_type=type(node).__name__,
                reason=reason,
                properties={"strategy": "unsupported"},
                raw={
                    "source": ast.get_source_segment(source, node) or ast.unparse(node),
                    "source_span": {
                        "line": key[1],
                        "column": key[2],
                        "end_line": key[3],
                        "end_column": key[4],
                    },
                },
            )
        )
    return gaps


def _operator_concept(operator: str, mapped: bool) -> str:
    if mapped:
        return CONCEPT_LOOP
    if operator in _QUERY_OPERATORS:
        return CONCEPT_QUERY
    if operator in _SCRIPT_OPERATORS:
        return CONCEPT_SCRIPT
    if operator in _BRANCH_OPERATORS:
        return CONCEPT_BRANCH
    if operator == "TriggerDagRunOperator" or operator.startswith("DatabricksRunNow"):
        return CONCEPT_RUN_WORKFLOW
    if operator.endswith("Sensor") or operator.endswith("SensorAsync"):
        return CONCEPT_WAIT
    if operator in ops.OPERATOR_REGISTRY:
        return CONCEPT_NOTEBOOK
    return CONCEPT_GAP


def _operator_assets(operator: str, kwargs: dict[str, ast.expr]) -> tuple[list[DataAsset], list[DataAsset]]:
    reads = _declared_assets(kwargs.get("inlets"), direction="read")
    writes = _declared_assets(kwargs.get("outlets"), direction="write")
    if operator in ops.FILE_SENSORS:
        path = ops.file_sensor_path(kwargs)
        if path:
            reads.append(DataAsset(signature=path, identity=path, asset_type="file"))
    if operator in ops.TABLE_SENSORS:
        table = ops.literal_str(kwargs.get("table_name"))
        if table:
            reads.append(DataAsset(signature=table, identity=table, asset_type="table"))
    if operator == "DatabricksCopyIntoOperator":
        location = ops.literal_str(kwargs.get("file_location"))
        table = ops.literal_str(kwargs.get("table_name"))
        if location:
            reads.append(DataAsset(signature=location, identity=location, asset_type="file"))
        if table:
            writes.append(DataAsset(signature=table, identity=table, asset_type="table"))
    sql = ops.literal_str(kwargs.get("sql")) or ops.literal_str(kwargs.get("hql"))
    if sql:
        reads.append(DataAsset(signature=sql.strip(), asset_type="query", properties={"role": "source_sql"}))
    return _deduplicate_assets(reads), _deduplicate_assets(writes)


def _declared_assets(node: ast.expr | None, *, direction: str) -> list[DataAsset]:
    if node is None:
        return []
    candidates = list(node.elts) if isinstance(node, (ast.List, ast.Tuple, ast.Set)) else [node]
    assets: list[DataAsset] = []
    for candidate in candidates:
        value: str | None = None
        if isinstance(candidate, ast.Call) and candidate.args:
            value = ops.literal_str(candidate.args[0])
        else:
            value = ops.literal_str(candidate)
        if value is not None:
            assets.append(
                DataAsset(
                    signature=value,
                    identity=value if "://" in value else None,
                    asset_type="logical",
                    properties={"airflow_direction": direction},
                )
            )
    return assets


def _deduplicate_assets(assets: Iterable[DataAsset]) -> list[DataAsset]:
    result: list[DataAsset] = []
    seen: set[tuple[str, str | None, str | None]] = set()
    for asset in assets:
        key = (asset.signature, asset.identity, asset.asset_type)
        if key not in seen:
            seen.add(key)
            result.append(asset)
    return result


def _activity_index(tasks: list[Activity]) -> dict[str, Activity]:
    result: dict[str, Activity] = {}
    stack = list(tasks)
    while stack:
        activity = stack.pop(0)
        result.setdefault(activity.task_key, activity)
        if isinstance(activity, ForEachActivity):
            stack[0:0] = activity.inner_activities
    return result


def _strategy_for(task_key: str, activities: dict[str, Activity], pipeline_status: str | None) -> str:
    activity = activities.get(task_key)
    if isinstance(activity, PlaceholderActivity):
        return "agentic"
    if isinstance(activity, ForEachActivity):
        nested = _activity_index(activity.inner_activities)
        if any(isinstance(item, PlaceholderActivity) for item in nested.values()):
            return "agentic"
    if activity is not None:
        return "deterministic"
    return "unsupported" if pipeline_status == "failed" else "deterministic"


def _policy(arguments: dict[str, ast.expr]) -> PolicySpec | None:
    retries = ops.literal_value(arguments.get("retries"))
    max_retries = retries if isinstance(retries, int) and not isinstance(retries, bool) else None
    retry_interval = templating.timedelta_seconds(arguments.get("retry_delay"))
    timeout = templating.timedelta_seconds(arguments.get("execution_timeout"))
    extensions: dict[str, Any] = {}
    for name in ("depends_on_past", "email", "email_on_failure", "email_on_retry"):
        if name in arguments:
            extensions[name] = _json_safe(ops.literal_value(arguments[name]), fallback=ast.unparse(arguments[name]))
    if max_retries is None and retry_interval is None and timeout is None and not extensions:
        return None
    return PolicySpec(
        timeout_seconds=timeout,
        max_retries=max_retries,
        retry_interval_seconds=retry_interval,
        extensions=extensions,
    )


def _source_schedule(visitor: _DagVisitor, source: str) -> ScheduleSpec | None:
    node = visitor.schedule_node
    if node is None or (isinstance(node, ast.Constant) and node.value is None):
        return None
    literal = ops.literal_value(node)
    source_expression = ast.get_source_segment(source, node) or ast.unparse(node)
    expression = source_expression if literal is None else _json_safe(literal, fallback=source_expression)
    kind = "schedule" if isinstance(literal, str) else "interval"
    is_asset_expression = _asset_expression(node, visitor._aliases, visitor.asset_definitions) is not None
    if is_asset_expression:
        kind = "asset"
    return ScheduleSpec(
        kind=kind,
        expression=expression,
        timezone=visitor.timezone,
        extensions={"source_expression": source_expression},
    )


def _expression_payload(node: ast.expr, source: str) -> dict[str, Any]:
    value = ops.literal_value(node)
    return {
        "source": ast.get_source_segment(source, node) or ast.unparse(node),
        "value": _json_safe(value, fallback=None),
    }


def _definition_payload(node: ast.AST, source: str) -> dict[str, Any]:
    return {
        "source": ast.get_source_segment(source, node) or ast.unparse(node),
        "source_span": {
            "line": getattr(node, "lineno", 0),
            "column": getattr(node, "col_offset", 0),
            "end_line": getattr(node, "end_lineno", getattr(node, "lineno", 0)),
            "end_column": getattr(node, "end_col_offset", getattr(node, "col_offset", 0)),
        },
    }


def _json_safe(value: Any, *, fallback: Any) -> Any:
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return fallback
    return value


def _span_dict(span: SourceSpan) -> dict[str, int]:
    return {
        "line": span.line,
        "column": span.column,
        "end_line": span.end_line,
        "end_column": span.end_column,
    }


def _audit_candidate_payload(candidate: Any) -> dict[str, Any]:
    return {
        "kind": candidate.kind,
        "code": candidate.code,
        "line": candidate.line,
        "column": candidate.column,
        "end_line": candidate.end_line,
        "end_column": candidate.end_column,
        "occurrence": candidate.occurrence,
        "details": dict(candidate.details),
    }
