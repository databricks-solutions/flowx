"""Source-neutral serialization for the flowx Pipeline IR.

Every source's convert phase serialises its :class:`~flowx.models.ir.Pipeline`
to the ``translation_report.json`` shape these functions produce, and the
package phase rehydrates from it, so the report format is the one contract both
halves share.  This lives at the top level (not inside a source) because it
belongs to the IR, not to ADF: the Airflow convert phase and the bundler import
it just as the ADF engine does.

Also hosts the legacy ``merge_agentic_results`` implementation used by the ADF
source. Airflow does not expose this name-based merge because it cannot preserve
the source-audit and graph-identity guarantees.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from flowx.models.ir import (
    Activity,
    AgenticComponentActivity,
    AppendVariableActivity,
    ControlEdge,
    CopyActivity,
    DataAsset,
    DataEdge,
    DbtFactoryActivity,
    DeleteActivity,
    ExecutePipelineActivity,
    FilterActivity,
    ForEachActivity,
    IfConditionActivity,
    Lineage,
    LookupActivity,
    MotifActivity,
    MotifAnnotation,
    NotebookActivity,
    Pipeline,
    PlaceholderActivity,
    RunJobActivity,
    SetVariableActivity,
    SparkJarActivity,
    SparkPythonActivity,
    SqlActivity,
    SwitchActivity,
    UnsupportedActivity,
    WaitActivity,
    WebActivity,
)

logger = logging.getLogger(__name__)


def pipeline_to_dict(pipeline: Pipeline) -> dict[str, Any]:
    """Serialise a Pipeline IR to a JSON-friendly dictionary.

    Args:
        pipeline: The translated pipeline IR.

    Returns:
        Dictionary suitable for ``json.dumps``.
    """
    result: dict[str, Any] = {
        "name": pipeline.name,
        "parameters": pipeline.parameters,
        "schedule": pipeline.schedule,
        "tags": pipeline.tags,
        "tasks": [activity_to_dict(task) for task in pipeline.tasks],
        "not_translatable": list(pipeline.not_translatable),
        "reconciliation_status": pipeline.reconciliation_status,
        "migration_status": pipeline.migration_status,
        "audit": dict(pipeline.audit),
    }
    if pipeline.bundle_variables:
        result["bundle_variables"] = pipeline.bundle_variables
    if pipeline.description is not None:
        result["description"] = pipeline.description
    if pipeline.timeout_seconds is not None:
        result["timeout_seconds"] = pipeline.timeout_seconds
    if pipeline.email_notifications:
        result["email_notifications"] = {
            event: list(recipients) for event, recipients in pipeline.email_notifications.items()
        }
    if pipeline.translation_configuration is not None:
        result["translation_configuration"] = configuration_to_dict(pipeline.translation_configuration)
    if pipeline.lineage is not None:
        result["lineage"] = lineage_to_dict(pipeline.lineage)
    return result


def data_asset_to_dict(asset: DataAsset) -> dict[str, Any]:
    """Serialise a :class:`DataAsset` to a JSON-friendly dictionary.

    ``signature`` always appears; the optional ``identity`` / ``asset_type`` /
    ``properties`` are emitted only when set so reports stay compact.
    """
    result: dict[str, Any] = {"signature": asset.signature}
    if asset.identity is not None:
        result["identity"] = asset.identity
    if asset.asset_type is not None:
        result["asset_type"] = asset.asset_type
    if asset.properties:
        result["properties"] = dict(asset.properties)
    return result


def data_asset_from_dict(raw: dict[str, Any]) -> DataAsset:
    """Rehydrate a :class:`DataAsset` from the dict :func:`data_asset_to_dict` emits.

    The canonical inverse of :func:`data_asset_to_dict`, so every consumer of the
    serialised DataAsset shape (the lineage substrate and the discovery graph)
    reads it the same way.
    """
    return DataAsset(
        signature=raw.get("signature", ""),
        identity=raw.get("identity"),
        asset_type=raw.get("asset_type"),
        properties=dict(raw.get("properties") or {}),
    )


def lineage_to_dict(lineage: Lineage) -> dict[str, Any]:
    """Serialise a :class:`Lineage` block to a JSON-friendly dictionary.

    Edge lists are always emitted (empty, never ``None``) for stable diffs.
    """
    return {
        "control_edges": [
            {
                "source_workflow": edge.source_workflow,
                "target_workflow": edge.target_workflow,
                "via_task_key": edge.via_task_key,
                "wait_for_completion": edge.wait_for_completion,
                "resolved": edge.resolved,
            }
            for edge in lineage.control_edges
        ],
        "data_edges": [
            {
                "source_task_key": edge.source_task_key,
                "target_task_key": edge.target_task_key,
                "match_kind": edge.match_kind,
                "match_key": edge.match_key,
                "identity": edge.identity,
                "asset_type": edge.asset_type,
            }
            for edge in lineage.data_edges
        ],
        "motifs": [_motif_annotation_to_dict(motif) for motif in lineage.motifs],
    }


def lineage_from_dict(raw: dict[str, Any]) -> Lineage:
    """Rehydrate a :class:`Lineage` block from the dict :func:`lineage_to_dict` emits.

    The one inverse shared by the translation report and the discovery graph, so a
    lineage block reads back the same way wherever it was written.
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
                source_type_hint=motif.get("source_type_hint"),
            )
            for motif in raw.get("motifs") or []
        ],
    )


def _motif_annotation_to_dict(motif: MotifAnnotation) -> dict[str, Any]:
    """Serialise one motif annotation; ``source_type_hint`` is written only when set.

    Leaving the key out when it is ``None`` keeps every existing lineage block
    byte-identical, since only discovery-time detections carry a hint.
    """
    result: dict[str, Any] = {
        "motif_id": motif.motif_id,
        "member_task_keys": list(motif.member_task_keys),
        "display_name": motif.display_name,
        "databricks_replacement": motif.databricks_replacement,
        "notes": list(motif.notes),
    }
    if motif.source_type_hint is not None:
        result["source_type_hint"] = motif.source_type_hint
    return result


def configuration_to_dict(configuration: Any) -> dict[str, Any]:
    """Serialise a TranslationConfiguration instance to a JSON-friendly dictionary.

    Args:
        configuration: The :class:`TranslationConfiguration` snapshot to serialise.

    Returns:
        Dictionary with each StrEnum field rendered as its string value
        and per-task overrides preserved verbatim.
    """
    return {
        "copy_activity_paradigm": str(configuration.copy_activity_paradigm),
        "non_databricks_task_compute": str(configuration.non_databricks_task_compute),
        "use_lakeflow_connectors": str(configuration.use_lakeflow_connectors),
        "lakeflow_connector_type": str(configuration.lakeflow_connector_type),
        "motif_consolidations": {
            motif_id: str(choice) for motif_id, choice in configuration.motif_consolidations.items()
        },
        "per_task": dict(configuration.per_task),
    }


def activity_to_dict(task: Activity) -> dict[str, Any]:
    """Serialise a single Activity IR node to a JSON-friendly dictionary.

    Args:
        task: Any Activity IR node.

    Returns:
        Dictionary suitable for ``json.dumps``.
    """
    task_dict: dict[str, Any] = {
        "name": task.name,
        "task_key": task.task_key,
        "type": type(task).__name__,
    }
    if task.description:
        task_dict["description"] = task.description
    if task.timeout_seconds:
        task_dict["timeout_seconds"] = task.timeout_seconds
    if task.max_retries:
        task_dict["max_retries"] = task.max_retries
    if task.min_retry_interval_millis:
        task_dict["min_retry_interval_millis"] = task.min_retry_interval_millis
    if task.depends_on:
        task_dict["depends_on"] = [
            {"task_key": dependency.task_key, "outcome": dependency.outcome} for dependency in task.depends_on
        ]
    if task.cluster:
        task_dict["cluster"] = task.cluster
    if task.existing_cluster_id:
        task_dict["existing_cluster_id"] = task.existing_cluster_id
    if task.compute_mode:
        task_dict["compute_mode"] = task.compute_mode
    if task.notifications:
        task_dict["notifications"] = task.notifications
    if task.libraries:
        task_dict["libraries"] = task.libraries
    if task.parameter_approximations:
        task_dict["parameter_approximations"] = task.parameter_approximations
    if task.motif_id:
        task_dict["motif_id"] = task.motif_id
    if task.data_reads:
        task_dict["data_reads"] = [data_asset_to_dict(asset) for asset in task.data_reads]
    if task.data_writes:
        task_dict["data_writes"] = [data_asset_to_dict(asset) for asset in task.data_writes]

    extra = activity_extra_fields(task)
    task_dict.update(extra)
    return task_dict


def activity_extra_fields(activity: Activity) -> dict[str, Any]:
    """Extracts type-specific fields from an Activity subclass.

    Args:
        activity: Any Activity IR node.

    Returns:
        Dictionary of extra fields beyond the base Activity.
    """
    extra: dict[str, Any] = {}

    match activity:
        case AgenticComponentActivity():
            extra["files"] = activity.files
            extra["resources"] = activity.resources
            extra["task"] = activity.task
            if activity.raw_definition is not None:
                extra["raw_definition"] = activity.raw_definition
        case NotebookActivity():
            extra["notebook_path"] = activity.notebook_path
            if activity.base_parameters:
                extra["base_parameters"] = activity.base_parameters
            if activity.notebook_path_unresolved:
                extra["notebook_path_unresolved"] = True
                if activity.notebook_path_expression is not None:
                    extra["notebook_path_expression"] = activity.notebook_path_expression
            if activity.unresolved_libraries:
                extra["unresolved_libraries"] = list(activity.unresolved_libraries)
            if activity.generated_source is not None:
                extra["generated_source"] = activity.generated_source
        case DbtFactoryActivity():
            extra["project_dir"] = activity.project_dir
            extra["profiles_dir"] = activity.profiles_dir
            extra["target"] = activity.target
            if activity.manifest_path is not None:
                extra["manifest_path"] = activity.manifest_path
            extra["render_mode"] = activity.render_mode
            if activity.selectors:
                extra["selectors"] = list(activity.selectors)
            if activity.exclude_selectors:
                extra["exclude_selectors"] = list(activity.exclude_selectors)
            if activity.variables is not None:
                extra["variables"] = activity.variables
            if activity.full_refresh:
                extra["full_refresh"] = True
            if activity.resource_types:
                extra["resource_types"] = list(activity.resource_types)
            if activity.nodes:
                extra["nodes"] = list(activity.nodes)
        case CopyActivity():
            extra["source_type"] = activity.source_type
            extra["sink_type"] = activity.sink_type
            if activity.source_properties:
                extra["source_properties"] = activity.source_properties
            if activity.sink_properties:
                extra["sink_properties"] = activity.sink_properties
            if activity.sink_dataset_type:
                extra["sink_dataset_type"] = activity.sink_dataset_type
            if activity.sink_format:
                extra["sink_format"] = activity.sink_format
            if activity.sink_resolved_path:
                extra["sink_resolved_path"] = activity.sink_resolved_path
            if activity.column_mapping:
                extra["column_mapping"] = activity.column_mapping
            if activity.target_format:
                extra["target_format"] = activity.target_format
            if activity.use_lakeflow_connector:
                extra["use_lakeflow_connector"] = activity.use_lakeflow_connector
            if activity.lakeflow_connector_type:
                extra["lakeflow_connector_type"] = activity.lakeflow_connector_type
        case ForEachActivity():
            extra["items_expression"] = activity.items_expression
            extra["concurrency"] = activity.concurrency
            extra["inner_activities"] = [activity_to_dict(inner) for inner in activity.inner_activities]
            if activity.inputs_bridge_notebook_code:
                extra["inputs_bridge_notebook_code"] = activity.inputs_bridge_notebook_code
            if activity.inputs_bridge_notebook_imports:
                extra["inputs_bridge_notebook_imports"] = list(activity.inputs_bridge_notebook_imports)
            if activity.inputs_bridge_required_parameters:
                extra["inputs_bridge_required_parameters"] = dict(activity.inputs_bridge_required_parameters)
        case IfConditionActivity():
            extra["op"] = activity.op
            extra["left"] = activity.left
            extra["right"] = activity.right
            extra["if_true_activities"] = [activity_to_dict(inner) for inner in activity.if_true_activities]
            extra["if_false_activities"] = [activity_to_dict(inner) for inner in activity.if_false_activities]
            if activity.bridge_notebook_code:
                extra["bridge_notebook_code"] = activity.bridge_notebook_code
            if activity.bridge_notebook_imports:
                extra["bridge_notebook_imports"] = list(activity.bridge_notebook_imports)
            if activity.bridge_required_parameters:
                extra["bridge_required_parameters"] = dict(activity.bridge_required_parameters)
        case LookupActivity():
            extra["source_type"] = activity.source_type
            if activity.source_properties:
                extra["source_properties"] = activity.source_properties
            extra["first_row_only"] = activity.first_row_only
            if activity.source_query:
                extra["source_query"] = activity.source_query
        case SetVariableActivity():
            extra["variable_name"] = activity.variable_name
            extra["variable_value"] = activity.variable_value
            extra["value_kind"] = activity.value_kind
            if activity.notebook_code:
                extra["notebook_code"] = activity.notebook_code
            if activity.notebook_imports:
                extra["notebook_imports"] = activity.notebook_imports
            if activity.required_parameters:
                extra["required_parameters"] = dict(activity.required_parameters)
            if activity.raw_expression:
                extra["raw_expression"] = activity.raw_expression
        case FilterActivity():
            extra["items_expression"] = activity.items_expression
            extra["condition_expression"] = activity.condition_expression
            if activity.condition_code is not None:
                extra["condition_code"] = activity.condition_code
            if activity.condition_imports:
                extra["condition_imports"] = list(activity.condition_imports)
        case AppendVariableActivity():
            extra["variable_name"] = activity.variable_name
            extra["append_value"] = activity.append_value
            extra["value_kind"] = activity.value_kind
            if activity.notebook_code:
                extra["notebook_code"] = activity.notebook_code
            if activity.notebook_imports:
                extra["notebook_imports"] = activity.notebook_imports
            if activity.required_parameters:
                extra["required_parameters"] = dict(activity.required_parameters)
        case SwitchActivity():
            extra["on_expression"] = activity.on_expression
            extra["cases"] = [
                {"value": case_item.value, "activities": [activity_to_dict(inner) for inner in case_item.activities]}
                for case_item in activity.cases
            ]
            extra["default_activities"] = [activity_to_dict(inner) for inner in activity.default_activities]
            if activity.bridge_notebook_code:
                extra["bridge_notebook_code"] = activity.bridge_notebook_code
            if activity.bridge_notebook_imports:
                extra["bridge_notebook_imports"] = list(activity.bridge_notebook_imports)
            if activity.bridge_required_parameters:
                extra["bridge_required_parameters"] = dict(activity.bridge_required_parameters)
        case WaitActivity():
            extra["wait_time_seconds"] = activity.wait_time_seconds
        case SqlActivity():
            extra["sql"] = activity.sql
            if activity.parameters:
                extra["parameters"] = dict(activity.parameters)
            extra["warehouse_ref"] = activity.warehouse_ref
        case SparkJarActivity():
            extra["main_class_name"] = activity.main_class_name
            if activity.parameters:
                extra["parameters"] = activity.parameters
        case SparkPythonActivity():
            extra["python_file"] = activity.python_file
            if activity.parameters:
                extra["parameters"] = activity.parameters
            if activity.generated_source is not None:
                extra["generated_source"] = activity.generated_source
        case WebActivity():
            extra["url"] = activity.url
            extra["method"] = activity.method
            if activity.body is not None:
                extra["body"] = activity.body
            if activity.headers:
                extra["headers"] = activity.headers
            if activity.authentication:
                extra["authentication"] = activity.authentication
            if activity.body_code is not None:
                extra["body_code"] = activity.body_code
            if activity.body_imports:
                extra["body_imports"] = activity.body_imports
            if activity.body_required_parameters:
                extra["body_required_parameters"] = activity.body_required_parameters
            if activity.disable_cert_validation:
                extra["disable_cert_validation"] = activity.disable_cert_validation
            if activity.http_request_timeout_seconds:
                extra["http_request_timeout_seconds"] = activity.http_request_timeout_seconds
        case DeleteActivity():
            extra["dataset_name"] = activity.dataset_name
            if activity.folder_path:
                extra["folder_path"] = activity.folder_path
            extra["recursive"] = activity.recursive
        case ExecutePipelineActivity():
            extra["pipeline_name"] = activity.pipeline_name
            extra["wait_on_completion"] = activity.wait_on_completion
            if activity.parameters:
                extra["parameters"] = activity.parameters
        case RunJobActivity():
            extra["job_name"] = activity.job_name
            if activity.existing_job_id:
                extra["existing_job_id"] = activity.existing_job_id
            if activity.job_parameters:
                extra["job_parameters"] = activity.job_parameters
        case MotifActivity():
            # motif_id now lives on the Activity base and is serialised by activity_to_dict.
            extra["display_name"] = activity.display_name
            extra["databricks_replacement"] = activity.databricks_replacement
            extra["matched_activity_names"] = activity.matched_activity_names
            if activity.source_type_hint:
                extra["source_type_hint"] = activity.source_type_hint
            if activity.confidence_notes:
                extra["confidence_notes"] = activity.confidence_notes
            if activity.notebook_template:
                extra["notebook_template"] = activity.notebook_template
            if activity.motif_config:
                extra["motif_config"] = activity.motif_config
            if activity.consolidate_metadata_driven:
                extra["consolidate_metadata_driven"] = activity.consolidate_metadata_driven
            if activity.lookup_values:
                extra["lookup_values"] = activity.lookup_values
        case PlaceholderActivity():
            extra["original_type"] = activity.original_type
            extra["comment"] = activity.comment
            if activity.raw_definition is not None:
                extra["raw_definition"] = activity.raw_definition
        case UnsupportedActivity():
            extra["original_type"] = activity.original_type
            extra["reason"] = activity.reason

    return extra


def activity_to_debug_dict(activity: Activity) -> dict[str, Any]:
    """Serialise an Activity to a full debug dict showing all dataclass fields.

    Args:
        activity: Any Activity IR node.

    Returns:
        Dict with ``__class__`` plus every dataclass field.
    """
    result: dict[str, Any] = {"__class__": type(activity).__name__}

    for field in activity.__dataclass_fields__:
        value = getattr(activity, field)

        if isinstance(value, Activity):
            result[field] = activity_to_debug_dict(value)
        elif isinstance(value, list) and value and isinstance(value[0], Activity):
            result[field] = [activity_to_debug_dict(inner) for inner in value]
        elif isinstance(value, list) and value and hasattr(value[0], "__dataclass_fields__"):
            result[field] = [dataclass_to_debug_dict(item) for item in value]
        else:
            result[field] = value

    return result


def dataclass_to_debug_dict(obj: Any) -> dict[str, Any]:
    """Serialise a generic dataclass (SwitchCase, Dependency, etc.) to a debug dict.

    Args:
        obj: A dataclass instance.

    Returns:
        Dict with ``__class__`` plus every dataclass field.
    """
    result: dict[str, Any] = {"__class__": type(obj).__name__}

    for field in obj.__dataclass_fields__:
        value = getattr(obj, field)

        if isinstance(value, Activity):
            result[field] = activity_to_debug_dict(value)
        elif isinstance(value, list) and value and isinstance(value[0], Activity):
            result[field] = [activity_to_debug_dict(inner) for inner in value]
        else:
            result[field] = value

    return result


def pipeline_to_debug_dict(pipeline: Pipeline) -> dict[str, Any]:
    """Serialise a Pipeline IR to a full debug dict.

    Args:
        pipeline: The translated pipeline IR.

    Returns:
        Dict with every field fully expanded.
    """
    return {
        "__class__": "Pipeline",
        "name": pipeline.name,
        "description": pipeline.description,
        "parameters": pipeline.parameters,
        "schedule": pipeline.schedule,
        "timeout_seconds": pipeline.timeout_seconds,
        "email_notifications": pipeline.email_notifications,
        "tags": pipeline.tags,
        "tasks": [activity_to_debug_dict(task) for task in pipeline.tasks],
        "not_translatable": list(pipeline.not_translatable),
        "reconciliation_status": pipeline.reconciliation_status,
        "migration_status": pipeline.migration_status,
        "audit": dict(pipeline.audit),
    }


def _locate_task(tasks: list[dict[str, Any]], activity_name: str) -> tuple[list[dict[str, Any]], int] | None:
    """Find the first task named *activity_name*, recursing into containers.

    Searches top-level tasks and the nested activity lists of IfCondition / ForEach / Switch
    containers. Returns the list holding the task and its index, or ``None`` when there is none.
    """
    nested_keys = ("inner_activities", "if_true_activities", "if_false_activities", "default_activities")
    for index, task in enumerate(tasks):
        if task.get("name") == activity_name:
            return tasks, index
        for key in nested_keys:
            child = task.get(key)
            if isinstance(child, list):
                found = _locate_task(child, activity_name)
                if found is not None:
                    return found
        for case in task.get("cases") or []:
            if isinstance(case, dict) and isinstance(case.get("activities"), list):
                found = _locate_task(case["activities"], activity_name)
                if found is not None:
                    return found
    return None


def _find_and_replace_task(tasks: list[dict[str, Any]], activity_name: str, replacement: dict[str, Any]) -> bool:
    """Replace the task named *activity_name* with *replacement*, recursing into containers.

    Preserves the placeholder's ``task_key`` and ``depends_on`` when the replacement omits them so
    downstream dependency edges stay intact.  Returns True when a match was replaced.
    """
    found = _locate_task(tasks, activity_name)
    if found is None:
        return False
    container, index = found
    task = container[index]
    replacement.setdefault("task_key", task.get("task_key"))
    replacement.setdefault("name", activity_name)
    if "depends_on" not in replacement and task.get("depends_on"):
        replacement["depends_on"] = task["depends_on"]
    container[index] = replacement
    return True


def _merge_target(pipelines: list[dict[str, Any]], wanted: Any, activity_name: Any) -> Any:
    """The pipeline a merge result lands in: the one it names, else the first holding its activity."""
    if wanted:
        return wanted
    for pipeline in pipelines:
        if _locate_task(pipeline.get("tasks", []), activity_name) is not None:
            return pipeline.get("name")
    return None


def merge_agentic_results(report_path: Path, results_dir: Path, output_path: Path | None = None) -> tuple[int, int]:
    """Merge agent-produced per-activity translations into a translation report.

    Each ``*.json`` file in *results_dir* describes one resolved agentic gap::

        {
          "activity_name": "<placeholder activity name>",   # required
          "pipeline": "<pipeline name>",                    # optional; for multi-pipeline reports
          "task": { ...IR task dict... }                    # required; replacement task
        }

    The matching placeholder task (located by ``name``, recursing into
    IfCondition / ForEach / Switch containers) is replaced by ``task``.  Use a
    ``NotebookActivity`` whose ``notebook_path`` points at a notebook the agent
    wrote to the workspace; the prepare phase then references it directly.

    This fills convert's own agentic gaps. A pipeline the routing record routes agentic is filled
    only by ``fill-agentic combine``, so a result that names one, or an untargeted result whose
    activity would first land in one, is refused before anything is written.

    When the report has a routing record, merges into non-routed pipelines are applied to both the
    live report and the stored baseline report (so the next rebuild keeps them), and the record's
    baseline_report_sha256 is refreshed. All merges are validated first (all-or-nothing).

    Args:
        report_path: ``translation_report.json`` produced by the translate phase.
        results_dir: Directory of per-activity result JSON files.
        output_path: Where to write the merged report; defaults to overwriting
            *report_path*.

    Returns:
        ``(merged, unmatched)`` counts.

    Raises:
        ValueError: A result would land in a pipeline the routing record routes agentic.
    """
    import hashlib

    from flowx.route_agentic import (
        BASELINE_DIRNAME,
        BASELINE_REPORT_FILENAME,
        ROUTED_AGENTIC_MERGE_REFUSED,
        ROUTING_RECORD_KEY,
        STAMPED_REPORT_FILENAME,
        WORK_DIRNAME,
        routed_agentic_pipelines,
        routing_record,
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    pipelines = report["pipelines"] if isinstance(report, dict) and "pipelines" in report else [report]
    results = [
        (result_file, json.loads(result_file.read_text(encoding="utf-8")))
        for result_file in sorted(results_dir.glob("*.json"))
    ]
    routed = routed_agentic_pipelines(report)
    for _, data in results:
        activity_name = data.get("activity_name") or data.get("activity")
        if routed and _merge_target(pipelines, data.get("pipeline"), activity_name) in routed:
            raise ValueError(ROUTED_AGENTIC_MERGE_REFUSED)

    merged = 0
    unmatched = 0
    for result_file, data in results:
        activity_name = data.get("activity_name") or data.get("activity")
        task = data.get("task") or data.get("ir")
        if not activity_name or not isinstance(task, dict):
            logger.warning("Skipping %s: missing 'activity_name' or 'task'.", result_file.name)
            unmatched += 1
            continue
        wanted = data.get("pipeline")
        candidates = [pipeline for pipeline in pipelines if not wanted or pipeline.get("name") == wanted]
        if any(_find_and_replace_task(pipeline.get("tasks", []), activity_name, dict(task)) for pipeline in candidates):
            merged += 1
            logger.info("Merged agentic result for '%s' from %s", activity_name, result_file.name)
        else:
            logger.warning("No placeholder named '%s' found for %s", activity_name, result_file.name)
            unmatched += 1

    destination = output_path or report_path
    destination.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    logger.info("Wrote merged report to %s (%d merged, %d unmatched)", destination, merged, unmatched)

    record = routing_record(report)
    if record is not None and merged > 0:
        output_dir = report_path.parent.parent
        baseline_dir = output_dir / WORK_DIRNAME / BASELINE_DIRNAME
        baseline_report_path = baseline_dir / BASELINE_REPORT_FILENAME

        if baseline_report_path.exists():
            try:
                baseline_report = json.loads(baseline_report_path.read_text(encoding="utf-8"))
                baseline_pipelines = (
                    baseline_report["pipelines"]
                    if isinstance(baseline_report, dict) and "pipelines" in baseline_report
                    else [baseline_report]
                )

                for result_file in results_dir.glob("*.json"):
                    data = json.loads(result_file.read_text(encoding="utf-8"))
                    activity_name = data.get("activity_name") or data.get("activity")
                    task = data.get("task") or data.get("ir")
                    if activity_name and isinstance(task, dict):
                        wanted = data.get("pipeline")
                        candidates = [
                            pipeline for pipeline in baseline_pipelines if not wanted or pipeline.get("name") == wanted
                        ]
                        _find_and_replace_task(
                            [task_item for pipeline in candidates for task_item in pipeline.get("tasks", [])],
                            activity_name,
                            dict(task),
                        )

                baseline_report_bytes = json.dumps(baseline_report, indent=2, default=str).encode("utf-8")
                baseline_report_path.write_bytes(baseline_report_bytes)

                record["baseline_report_sha256"] = hashlib.sha256(baseline_report_bytes).hexdigest()
                report[ROUTING_RECORD_KEY] = record
                destination.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

                stamped_path = report_path.parent / STAMPED_REPORT_FILENAME
                if stamped_path.exists():
                    stamped_report = json.loads(stamped_path.read_text(encoding="utf-8"))
                    stamped_report[ROUTING_RECORD_KEY] = record
                    stamped_path.write_text(json.dumps(stamped_report, indent=2, default=str), encoding="utf-8")
            except (OSError, json.JSONDecodeError) as error:
                logger.warning("Could not update baseline with merges: %s", error)

    return merged, unmatched
