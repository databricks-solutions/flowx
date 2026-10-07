"""Source reconciliation: compare captured constructs against emitted IR."""

from __future__ import annotations

import ast
import json
from typing import Any

from flowx.models.ir import (
    Activity,
    Dependency,
    ForEachActivity,
    NotebookActivity,
    Pipeline,
    PlaceholderActivity,
)
from flowx.sources.airflow import audit as source_audit
from flowx.sources.airflow.loader.ast_utils import _sanitize_task_key, _span
from flowx.sources.airflow.loader.policy import (
    _NON_EXECUTION_DAG_SETTINGS,
    _RECOGNIZED_DAG_SETTINGS,
    _dag_setting_disposition,
)
from flowx.sources.airflow.loader.visitor import _DagVisitor


def _semantic_finding(
    source_file: str,
    node: ast.AST | None,
    *,
    code: str,
    message: str,
    task_key: str,
    capture_id: str,
    **details: Any,
) -> dict[str, Any]:
    """Builds a stable gap finding for a captured task-level semantic limitation."""
    candidate = source_audit.AuditCandidate(
        kind="task_semantics",
        code=code,
        line=getattr(node, "lineno", 0),
        column=getattr(node, "col_offset", 0),
        occurrence=1,
        end_line=getattr(node, "end_lineno", 0),
        end_column=getattr(node, "end_col_offset", 0),
        details={"task_key": task_key, "capture_id": capture_id, **details},
    )
    return source_audit.finding(
        source_file=source_file,
        code=code,
        severity="gap",
        message=message,
        candidate=candidate,
    )


def _iter_placeholders_with_paths(
    tasks: list[Activity],
    path: tuple[str | int, ...] = ("tasks",),
) -> list[tuple[tuple[str | int, ...], PlaceholderActivity]]:
    """Returns placeholders with their stable serialized Pipeline IR paths."""
    placeholders: list[tuple[tuple[str | int, ...], PlaceholderActivity]] = []
    for index, task in enumerate(tasks):
        task_path = (*path, index)
        if isinstance(task, PlaceholderActivity):
            placeholders.append((task_path, task))
        if isinstance(task, ForEachActivity):
            placeholders.extend(_iter_placeholders_with_paths(task.inner_activities, (*task_path, "inner_activities")))
    return placeholders


def _iter_placeholders(tasks: list[Activity]) -> list[PlaceholderActivity]:
    """Returns placeholders in top-level and Airflow-generated for_each tasks."""
    return [placeholder for _, placeholder in _iter_placeholders_with_paths(tasks)]


def _reconcile_pipeline(
    pipeline: Pipeline,
    *,
    audit: source_audit.SourceAudit,
    visitor: _DagVisitor,
    source_file: str,
    var_to_task_key: dict[str, str],
    dropped: set[str],
    dbt_vars: list[str],
    semantic_findings: list[dict[str, Any]],
    sensor_lift_proof: dict[str, Any] | None,
    schedule_proof: dict[str, Any] | None,
    argument_proofs: list[dict[str, Any]],
    expected_ir_edges: set[tuple[str, str]],
    placeholder_capture_ids: dict[int, str],
) -> Pipeline:
    """Reconciles an independent source audit with captured graph and emitted IR."""
    findings: list[dict[str, Any]] = list(semantic_findings)
    transformations: list[dict[str, Any]] = list(argument_proofs)
    transformations.extend(visitor.helper_expansions)
    transformations.extend(
        {
            "code": "edge_captured",
            "upstream_capture_id": edge.upstream_id,
            "downstream_capture_id": edge.downstream_id,
            "upstream_task_key": var_to_task_key.get(edge.upstream_id),
            "downstream_task_key": var_to_task_key.get(edge.downstream_id),
            "source_span": {
                "line": edge.span.line,
                "column": edge.span.column,
                "end_line": edge.span.end_line,
                "end_column": edge.span.end_column,
            },
        }
        for edge in visitor.edge_captures
    )
    if sensor_lift_proof is not None:
        transformations.append(sensor_lift_proof)
    if schedule_proof is not None:
        transformations.append(schedule_proof)
    captured_task_count = len(visitor.operators) + len(visitor.taskflow_tasks) + len(visitor.taskgroup_calls)

    unresolved = list(audit.unresolved)
    for code, node in visitor.unresolved_constructs:
        if not any(
            candidate.line == getattr(node, "lineno", 0) and candidate.column == getattr(node, "col_offset", 0)
            for candidate in unresolved
        ):
            unresolved.append(
                source_audit.AuditCandidate(
                    kind="unresolved",
                    code=code,
                    line=getattr(node, "lineno", 0),
                    column=getattr(node, "col_offset", 0),
                    occurrence=1,
                    end_line=getattr(node, "end_lineno", 0),
                    end_column=getattr(node, "end_col_offset", 0),
                    details={"expression": ast.unparse(node)},
                )
            )

    helper_capture_ids = {str(item["capture_id"]) for item in visitor.helper_expansions}
    capture_claims: dict[tuple[str, int, int, int, int, str], list[str]] = {}

    def add_capture_claim(code: str, node: ast.AST, discriminator: str, capture_id: str) -> None:
        span = _span(node)
        key = (code, span.line, span.column, span.end_line, span.end_column, discriminator)
        capture_claims.setdefault(key, []).append(capture_id)

    for item in visitor.helper_expansions:
        capture_id = str(item["capture_id"])
        add_capture_claim(
            "helper_factory_task",
            visitor.capture_source_nodes[capture_id],
            str(item["helper"]),
            capture_id,
        )
    for capture in visitor.task_captures.values():
        if capture.capture_id not in helper_capture_ids:
            add_capture_claim(
                "operator_task",
                visitor.capture_source_nodes[capture.capture_id],
                capture.operator,
                capture.capture_id,
            )
    for var, taskflow_task in visitor.taskflow_tasks.items():
        add_capture_claim("taskflow_task", visitor.capture_source_nodes[var], taskflow_task.def_name, var)

    unmatched_audit_tasks: list[source_audit.AuditCandidate] = []
    audit_candidate_by_capture: dict[str, source_audit.AuditCandidate] = {}
    for candidate in audit.tasks:
        discriminator = str(
            candidate.details.get("operator")
            or candidate.details.get("helper")
            or candidate.details.get("callable")
            or ""
        )
        key = (
            candidate.code,
            candidate.line,
            candidate.column,
            candidate.end_line,
            candidate.end_column,
            discriminator,
        )
        capture_ids = capture_claims.get(key)
        if not capture_ids:
            unmatched_audit_tasks.append(candidate)
            continue
        audit_candidate_by_capture[capture_ids.pop(0)] = candidate

    for candidate in unmatched_audit_tasks:
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="task_capture_mismatch",
                severity="failed",
                message="An independently audited Airflow task candidate was not claimed by the capture pass.",
                candidate=candidate,
            )
        )
    for call in visitor.unclaimed_task_calls:
        candidate = source_audit.AuditCandidate(
            kind="task",
            code="unclaimed_dag_task",
            line=getattr(call, "lineno", 0),
            column=getattr(call, "col_offset", 0),
            occurrence=1,
            end_line=getattr(call, "end_lineno", 0),
            end_column=getattr(call, "end_col_offset", 0),
            details={"expression": ast.unparse(call)},
        )
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="unclaimed_dag_task",
                severity="failed",
                message="A task-producing call in the DAG body was not claimed by the capture pass.",
                candidate=candidate,
            )
        )
    for statement in visitor.unclaimed_statements:
        candidate = source_audit.AuditCandidate(
            kind="statement",
            code="unclaimed_dag_statement",
            line=getattr(statement, "lineno", 0),
            column=getattr(statement, "col_offset", 0),
            occurrence=1,
            end_line=getattr(statement, "end_lineno", 0),
            end_column=getattr(statement, "end_col_offset", 0),
            details={"expression": ast.unparse(statement)},
        )
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="unclaimed_dag_statement",
                severity="failed",
                message="A DAG-body statement was not classified by the static capture pass.",
                candidate=candidate,
            )
        )

    if len(audit.edges) != len(visitor.edge_captures):
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="edge_capture_mismatch",
                severity="failed",
                message=(
                    f"Source audit found {len(audit.edges)} dependency edge(s), but capture produced "
                    f"{len(visitor.edge_captures)}."
                ),
                details={"audited": len(audit.edges), "captured": len(visitor.edge_captures)},
            )
        )
    comparable_audit_edges = [
        candidate
        for candidate in audit.edges
        if candidate.details.get("syntax") != "taskflow_data"
        and candidate.details.get("upstream")
        and candidate.details.get("downstream")
    ]
    comparable_spans = {
        (candidate.line, candidate.column, candidate.end_line, candidate.end_column)
        for candidate in comparable_audit_edges
    }

    def source_reference(capture_id: str) -> str:
        capture = visitor.task_captures.get(capture_id)
        if capture is not None:
            return capture.variable
        taskflow = visitor.taskflow_tasks.get(capture_id)
        if taskflow is not None:
            return taskflow.source_reference
        return capture_id.split("__L", 1)[0]

    audited_edge_identities = sorted(
        (str(candidate.details["upstream"]), str(candidate.details["downstream"]))
        for candidate in comparable_audit_edges
    )
    captured_edge_identities = sorted(
        (source_reference(edge.upstream_id), source_reference(edge.downstream_id))
        for edge in visitor.edge_captures
        if (edge.span.line, edge.span.column, edge.span.end_line, edge.span.end_column) in comparable_spans
    )
    if audited_edge_identities != captured_edge_identities:
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="edge_identity_mismatch",
                severity="failed",
                message="Captured Airflow dependency endpoints do not match the audited source endpoints.",
                details={
                    "audited_edges": [list(edge) for edge in audited_edge_identities],
                    "captured_edges": [list(edge) for edge in captured_edge_identities],
                },
            )
        )

    emitted_ir_edges = {
        (dependency.task_key, task.task_key) for task in pipeline.tasks for dependency in task.depends_on or []
    }
    missing_ir_edges = sorted(expected_ir_edges - emitted_ir_edges)
    unexpected_ir_edges = sorted(emitted_ir_edges - expected_ir_edges)
    if missing_ir_edges:
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="captured_edge_not_emitted",
                severity="failed",
                message="Captured dependency edge(s) were not emitted to Pipeline IR.",
                details={"missing_edges": [list(edge) for edge in missing_ir_edges]},
            )
        )
    if unexpected_ir_edges:
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="unexplained_emitted_edge",
                severity="failed",
                message="Pipeline IR contains dependency edge(s) absent from the transformation ledger.",
                details={"unexpected_edges": [list(edge) for edge in unexpected_ir_edges]},
            )
        )

    argument_failure_keys: set[str] = set()
    for capture in visitor.task_captures.values():
        argument_candidate = audit_candidate_by_capture.get(capture.capture_id)
        if argument_candidate is None or argument_candidate.code != "operator_task":
            continue
        expected = set(argument_candidate.details.get("kwargs", []))
        actual = set(visitor.operators[capture.capture_id][2])
        if expected == actual:
            continue
        task_key = var_to_task_key.get(capture.capture_id, capture.capture_id)
        argument_failure_keys.add(task_key)
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="operator_argument_capture_mismatch",
                severity="failed",
                message=(
                    f"Airflow task {capture.task_id!r} audited argument(s) {sorted(expected)}, "
                    f"but capture retained {sorted(actual)}."
                ),
                candidate=argument_candidate,
                details={
                    "task_key": task_key,
                    "missing": sorted(expected - actual),
                    "unexpected": sorted(actual - expected),
                },
            )
        )

    dbt_factory_var = dbt_vars[0] if dbt_vars else None
    expected_key_by_capture: dict[str, str] = {}
    for var, task_key in var_to_task_key.items():
        if var in dropped:
            continue
        expected_key_by_capture[var] = (
            var_to_task_key[dbt_factory_var] if var in dbt_vars and dbt_factory_var is not None else task_key
        )
    expected_task_keys = set(expected_key_by_capture.values())
    emitted_task_keys = {task.task_key for task in pipeline.tasks}
    missing_task_keys = sorted(expected_task_keys - emitted_task_keys)
    unexpected_task_keys = sorted(emitted_task_keys - expected_task_keys)
    if missing_task_keys:
        missing_capture = next(
            (var for var, task_key in expected_key_by_capture.items() if task_key in missing_task_keys),
            None,
        )
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="captured_task_not_emitted",
                severity="failed",
                message=f"Captured Airflow task key(s) were not emitted to Pipeline IR: {missing_task_keys}.",
                candidate=audit_candidate_by_capture.get(missing_capture or ""),
                details={"task_keys": missing_task_keys},
            )
        )
    if unexpected_task_keys:
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="unexplained_emitted_task",
                severity="failed",
                message=f"Pipeline IR contains task key(s) with no captured Airflow task: {unexpected_task_keys}.",
                details={"task_keys": unexpected_task_keys},
            )
        )

    setting_dispositions = [
        (candidate, _dag_setting_disposition(str(candidate.details.get("name")), visitor))
        for candidate in audit.settings
    ]
    unsupported_settings = [
        candidate
        for candidate, disposition in setting_dispositions
        if disposition is not None and disposition["status"] == "gap"
    ]
    missing_supported_settings = [
        candidate
        for candidate in audit.settings
        if candidate.details.get("name") in _RECOGNIZED_DAG_SETTINGS
        and candidate.details.get("name") not in visitor.captured_dag_settings
    ]
    for candidate in missing_supported_settings:
        name = str(candidate.details.get("name"))
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="dag_setting_capture_mismatch",
                severity="failed",
                message=f"Audited DAG setting {name!r} was not captured by the Airflow loader.",
                candidate=candidate,
            )
        )
    for candidate, disposition in setting_dispositions:
        if disposition is None:
            continue
        if disposition["status"] == "gap" and not disposition.get("target"):
            continue
        transformations.append(
            {
                "code": (
                    "dag_setting_mapped"
                    if disposition["status"] == "mapped"
                    else "dag_setting_partially_mapped"
                    if disposition["status"] == "gap"
                    else "dag_setting_ignored"
                ),
                "setting": str(candidate.details.get("name")),
                **({"target": disposition["target"]} if disposition.get("target") else {}),
                "rationale": disposition["rationale"],
            }
        )
    disposition_by_candidate_id = {
        id(candidate): disposition for candidate, disposition in setting_dispositions if disposition is not None
    }
    for candidate in unsupported_settings:
        name = str(candidate.details.get("name"))
        disposition = disposition_by_candidate_id[id(candidate)]
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="unsupported_dag_setting",
                severity="gap",
                message=disposition["message"],
                candidate=candidate,
                details={"name": name, "rationale": disposition["rationale"]},
            )
        )

    for candidate in audit.settings:
        name = str(candidate.details.get("name"))
        if name not in _NON_EXECUTION_DAG_SETTINGS:
            continue
        emitted_user_tag_count = sum(key.startswith("airflow_tag_") for key in pipeline.tags)
        partially_mapped = name == "tags" and emitted_user_tag_count < len(visitor.dag_user_tags)
        mapped = (
            (name == "tags" and bool(visitor.dag_user_tags))
            or (name == "description" and visitor.dag_description is not None)
            or (name == "default_args.owner" and visitor.dag_owner is not None)
        )
        transformations.append(
            {
                "code": (
                    "dag_setting_partially_mapped"
                    if partially_mapped
                    else "dag_setting_mapped"
                    if mapped
                    else "dag_setting_ignored"
                ),
                "setting": name,
                "target": {
                    "tags": "job.tags",
                    "description": "job.description",
                    "default_args.owner": "job.tags.airflow_owner",
                }.get(name),
                "rationale": (
                    "databricks_jobs_support_at_most_25_tags"
                    if partially_mapped
                    else "preserved_as_databricks_job_metadata"
                    if mapped
                    else "non_execution_metadata_has_no_required_runtime_effect"
                ),
                **(
                    {"source_count": len(visitor.dag_user_tags), "emitted_count": emitted_user_tag_count}
                    if name == "tags"
                    else {}
                ),
            }
        )

    unresolved_messages = {
        "unresolved_asset_schedule": (
            "An Airflow Asset/Dataset schedule lacks an explicit Databricks table mapping. Add "
            "extra={'databricks_table': '<catalog>.<schema>.<table>'} or use an "
            "x-databricks-table: URI."
        ),
        "unsupported_asset_or_time_schedule": (
            "Airflow AssetOrTimeSchedule combines time and asset triggers, but a Databricks Job can "
            "use only one job-level trigger."
        ),
        "unsupported_asset_schedule_expression": (
            "The Airflow Asset/Dataset boolean expression cannot be represented by one Databricks "
            "ANY_UPDATED or ALL_UPDATED table trigger."
        ),
        "unsupported_dag_schedule": (
            "The Airflow DAG schedule or timetable has no proven static Databricks Jobs mapping."
        ),
        "ambiguous_airflow_1_10_default_schedule": (
            "This DAG uses strong Airflow 1.10 syntax and omits schedule_interval. Historical default "
            "schedule and catchup behavior cannot be inferred safely without the deployed Airflow version."
        ),
        "reserved_airflow_parameter_name": (
            "Airflow DAG parameter names beginning with '__flowx_' are reserved for flowx runtime bindings. "
            "Rename the DAG parameter before migration."
        ),
    }
    for candidate in unresolved:
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code=candidate.code,
                severity="gap",
                message=unresolved_messages.get(
                    candidate.code,
                    "Dynamic Airflow control flow could not be expanded safely by the static parser.",
                ),
                candidate=candidate,
            )
        )

    def source_task_id(capture_id: str) -> str:
        return (
            visitor.operators[capture_id][0]
            if capture_id in visitor.operators
            else visitor.taskflow_tasks[capture_id].task_id
            if capture_id in visitor.taskflow_tasks
            else visitor.taskgroup_calls[capture_id][1]
        )

    for var, task_key in var_to_task_key.items():
        task_id = source_task_id(var)
        base = _sanitize_task_key(task_id)
        if var in visitor.groups:
            base = f"{visitor.groups[var]}__{base}"
        transformations.append(
            {
                "code": "task_key_allocated",
                "capture_id": var,
                "source_task_id": task_id,
                "task_key": task_key,
                "emitted_task_key": expected_key_by_capture.get(var),
            }
        )
        if task_key != base:
            transformations.append(
                {
                    "code": "task_key_collision_resolved",
                    "capture_id": var,
                    "source_task_id": task_id,
                    "task_key": task_key,
                }
            )
    for var in sorted(dropped):
        transformations.append(
            {
                "code": "structural_task_rewired",
                "capture_id": var,
                "task_key": var_to_task_key.get(var, var),
            }
        )
    if len(dbt_vars) > 1:
        transformations.append(
            {
                "code": "dbt_chain_collapsed",
                "capture_ids": list(dbt_vars),
                "task_key": var_to_task_key.get(dbt_vars[0], ""),
            }
        )

    if not pipeline.tasks and not any(item["severity"] == "failed" for item in findings):
        pipeline.tasks.append(
            NotebookActivity(
                name="Airflow DAG completion",
                task_key="__flowx_empty_dag",
                notebook_path="notebooks/__flowx_empty_dag.py",
                generated_source=(
                    "# Databricks notebook source\n"
                    "# This DAG contained no executable tasks after structural operators were rewired.\n"
                    "print('Airflow DAG completed without executable tasks.')\n"
                ),
            )
        )
        transformations.append(
            {
                "code": "empty_dag_sentinel_emitted",
                "task_key": "__flowx_empty_dag",
                "rationale": "preserve_a_runnable_job_for_a_structural_or_empty_airflow_dag",
            }
        )

    blocking_gaps = [*unsupported_settings, *unresolved]
    placeholder_entries = [
        (
            ("tasks", int(task_path[1]) + 1, *task_path[2:]) if blocking_gaps else task_path,
            placeholder,
        )
        for task_path, placeholder in _iter_placeholders_with_paths(pipeline.tasks)
        if not placeholder.task_key.startswith("__flowx_")
    ]
    placeholders = [placeholder for _, placeholder in placeholder_entries]
    for task_path, placeholder in placeholder_entries:
        placeholder_capture_id = placeholder_capture_ids.get(id(placeholder))
        if placeholder_capture_id is None:
            findings.append(
                source_audit.finding(
                    source_file=source_file,
                    code="operator_placeholder_capture_mismatch",
                    severity="failed",
                    message=(f"Placeholder task {placeholder.task_key!r} has no captured Airflow task identity."),
                    details={
                        "task_key": placeholder.task_key,
                        "operator": placeholder.original_type,
                        "task_path": list(task_path),
                    },
                )
            )
            continue
        placeholder_candidate = audit_candidate_by_capture.get(placeholder_capture_id)
        if placeholder_candidate is None:
            node = visitor.capture_source_nodes[placeholder_capture_id]
            span = _span(node)
            placeholder_candidate = source_audit.AuditCandidate(
                kind="task",
                code="captured_task",
                line=span.line,
                column=span.column,
                occurrence=1,
                end_line=span.end_line,
                end_column=span.end_column,
                details={
                    "task_id": source_task_id(placeholder_capture_id),
                    "operator": placeholder.original_type,
                },
            )
        findings.append(
            source_audit.finding(
                source_file=source_file,
                code="operator_placeholder",
                severity="gap",
                message=(
                    f"Airflow task {placeholder.name!r} ({placeholder.original_type}) requires explicit migration."
                ),
                candidate=placeholder_candidate,
                details={
                    "task_key": placeholder.task_key,
                    "operator": placeholder.original_type,
                    "capture_id": placeholder_capture_id,
                    "source_task_id": source_task_id(placeholder_capture_id),
                    "task_path": list(task_path),
                },
                identity_discriminator=json.dumps(
                    [pipeline.name, source_task_id(placeholder_capture_id)],
                    separators=(",", ":"),
                ),
            )
        )

    if blocking_gaps:
        placeholder_key = "__flowx_source_gaps"
        gap_task = PlaceholderActivity(
            name="Airflow source semantics requiring migration",
            task_key=placeholder_key,
            original_type="AirflowSourceSemantics",
            comment="Resolve the source-audit findings before enabling this DAG.",
            raw_definition={"findings": [item for item in findings if item["severity"] == "gap"]},
        )
        for task in pipeline.tasks:
            if not task.depends_on:
                task.depends_on = [Dependency(task_key=placeholder_key)]
        pipeline.tasks.insert(0, gap_task)

    failed_findings = [item for item in findings if item["severity"] == "failed"]
    gap_findings = [item for item in findings if item["severity"] == "gap"]
    status = "failed" if failed_findings else "verified_with_gaps" if gap_findings else "verified"
    failed_capture_keys = argument_failure_keys | set(missing_task_keys)
    agentic_captured_count = len(placeholders)
    deterministic_count = captured_task_count - len(failed_capture_keys) - agentic_captured_count
    agentic_count = agentic_captured_count + len(unresolved)
    failed_count = (
        len(failed_capture_keys)
        + len(unmatched_audit_tasks)
        + len(visitor.unclaimed_task_calls)
        + len(visitor.unclaimed_statements)
    )
    audited_task_count = (
        captured_task_count
        + len(unresolved)
        + len(unmatched_audit_tasks)
        + len(visitor.unclaimed_task_calls)
        + len(visitor.unclaimed_statements)
    )

    pipeline.not_translatable = findings
    pipeline.reconciliation_status = status
    pipeline.audit = {
        "source_file": source_file,
        "audited_activity_count": audited_task_count,
        "captured_task_count": captured_task_count,
        "audited_edge_count": len(audit.edges),
        "captured_edge_count": len(visitor.edge_captures),
        "deterministic_count": deterministic_count,
        "agentic_count": agentic_count,
        "failed_count": failed_count,
        "excluded_count": 0,
        "transformations": transformations,
    }
    return pipeline
