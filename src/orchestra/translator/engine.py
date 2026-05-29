"""Core translation engine for ADF-to-Databricks pipeline conversion."""

from __future__ import annotations

import argparse
import json
import logging
import re
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable

from orchestra.models.adf_ast import (
    AdfActivity,
    AdfDefinitions,
    AdfPipeline,
    TranslationStrategy,
)
from orchestra.models.ir import (
    Activity,
    AgenticGap,
    AppendVariableActivity,
    CopyActivity,
    DeleteActivity,
    Dependency,
    ExecutePipelineActivity,
    FilterActivity,
    ForEachActivity,
    IfConditionActivity,
    LookupActivity,
    MotifActivity,
    NotebookActivity,
    Pipeline,
    PlaceholderActivity,
    RunJobActivity,
    SetVariableActivity,
    SparkJarActivity,
    SparkPythonActivity,
    SwitchActivity,
    TranslationContext,
    TranslationReport,
    UnsupportedActivity,
    WaitActivity,
    WebActivity,
)
from orchestra.motifs.collapser import collapse_motifs
from orchestra.motifs.detector import detect_motifs
from orchestra.parser.adf_loader import classify_activity, load_adf_definitions
from orchestra.translator.activity_translators import (
    append_variable,
    copy,
    databricks_job,
    delete,
    execute_pipeline,
    filter,
    for_each,
    if_condition,
    lookup,
    notebook,
    set_variable,
    spark_jar,
    spark_python,
    switch,
    wait,
    web_activity,
)

logger = logging.getLogger(__name__)

TRANSLATOR_REGISTRY: dict[str, Callable[..., Activity]] = {
    "Copy": copy.translate,
    "DatabricksNotebook": notebook.translate,
    "DatabricksSparkJar": spark_jar.translate,
    "DatabricksSparkPython": spark_python.translate,
    "Lookup": lookup.translate,
    "WebActivity": web_activity.translate,
    "Delete": delete.translate,
    "ExecutePipeline": execute_pipeline.translate,
    "DatabricksJob": databricks_job.translate,
    "Wait": wait.translate,
    "Filter": filter.translate,
}


def translate_pipeline(pipeline: AdfPipeline, definitions: AdfDefinitions) -> TranslationReport:
    """Translates an ADF pipeline into a Databricks pipeline IR.

    Args:
        pipeline: Parsed ADF pipeline AST.
        definitions: Full ADF definitions for cross-referencing datasets,
            linked services, etc.

    Returns:
        :class:`TranslationReport` containing the translated :class:`Pipeline`
        and any gaps encountered.
    """
    context = TranslationContext(
        activity_cache=MappingProxyType({}),
        registry=MappingProxyType(TRANSLATOR_REGISTRY),
        variable_cache=MappingProxyType({}),
    )

    gaps: list[AgenticGap] = []
    warnings: list[str] = []

    sorted_activities = _topological_visit(pipeline.activities)
    translated_activities: list[Activity] = []
    deterministic_count = 0
    agentic_count = 0
    unsupported_count = 0

    for adf_activity in sorted_activities:
        activity_ir, context = _dispatch_activity(adf_activity, context, definitions)
        translated_activities.append(activity_ir)

        strategy, skill = classify_activity(adf_activity.type)
        if strategy is TranslationStrategy.DETERMINISTIC:
            deterministic_count += 1
        elif strategy is TranslationStrategy.AGENTIC:
            agentic_count += 1
            gaps.append(
                AgenticGap(
                    activity_name=adf_activity.name,
                    activity_type=adf_activity.type,
                    recommended_skill=skill,
                    raw_definition=adf_activity.type_properties,
                )
            )
        else:
            unsupported_count += 1
            gaps.append(
                AgenticGap(
                    activity_name=adf_activity.name,
                    activity_type=adf_activity.type,
                    recommended_skill=None,
                    raw_definition=adf_activity.type_properties,
                )
            )
            warnings.append(f"Activity '{adf_activity.name}' (type={adf_activity.type}) has no translation path.")

    parameters: dict[str, Any] = {}
    if pipeline.parameters:
        for param_name, param_def in pipeline.parameters.items():
            parameters[param_name] = param_def.default_value

    pipeline_ir = Pipeline(
        name=pipeline.name,
        parameters=[{"name": param_name, "default": param_value} for param_name, param_value in parameters.items()]
        if parameters
        else None,
        tasks=translated_activities,
        tags={"source": "adf", "pipeline": pipeline.name},
    )

    # Motif detection and collapsing: scan for known multi-activity patterns
    # and replace matched groups with single MotifActivity nodes.
    detected_motifs = detect_motifs(pipeline, definitions)
    if detected_motifs:
        pipeline_ir = collapse_motifs(pipeline_ir, detected_motifs)
        for motif in detected_motifs:
            logger.info(
                "Collapsed motif '%s': %d activities -> %s",
                motif.definition.display_name,
                len(motif.matched_activities),
                motif.definition.databricks_replacement,
            )

    return TranslationReport(
        pipeline=pipeline_ir,
        deterministic_count=deterministic_count,
        agentic_count=agentic_count,
        unsupported_count=unsupported_count,
        gaps=gaps,
        warnings=warnings,
    )


def _dispatch_activity(
    activity: AdfActivity,
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> tuple[Activity, TranslationContext]:
    """Dispatch a single activity to its translator.

    Args:
        activity: ADF activity AST node.
        context: Current translation context.
        definitions: Full ADF definitions.

    Returns:
        Tuple of ``(translated_activity, updated_context)``.
    """
    base_kwargs = _build_base_kwargs(activity, definitions)

    match activity.type:
        case "ForEach":
            result, context = for_each.translate(
                activity,
                base_kwargs,
                context,
                definitions,
                translate_activities_fn=_translate_activity_list,
            )
            context = context.with_activity(activity.name, result)
            return result, context

        case "IfCondition":
            result, context = if_condition.translate(
                activity,
                base_kwargs,
                context,
                definitions,
                translate_activities_fn=_translate_activity_list,
            )
            context = context.with_activity(activity.name, result)
            return result, context

        case "SetVariable":
            result, context = set_variable.translate(
                activity,
                base_kwargs,
                context,
                definitions,
            )
            context = context.with_activity(activity.name, result)
            return result, context

        case "AppendVariable":
            result, context = append_variable.translate(
                activity,
                base_kwargs,
                context,
                definitions,
            )
            context = context.with_activity(activity.name, result)
            return result, context

        case "Switch":
            result, context = switch.translate(
                activity,
                base_kwargs,
                context,
                definitions,
                translate_activities_fn=_translate_activity_list,
            )
            context = context.with_activity(activity.name, result)
            return result, context

        case activity_type if activity_type in TRANSLATOR_REGISTRY:
            translator_fn = TRANSLATOR_REGISTRY[activity_type]
            result = translator_fn(activity, base_kwargs, context, definitions)
            context = context.with_activity(activity.name, result)
            return result, context

        case _:
            strategy, skill = classify_activity(activity.type)
            reason = f"Agentic skill: {skill}" if skill else f"No translator for type '{activity.type}'"
            placeholder = PlaceholderActivity(
                **base_kwargs,
                original_type=activity.type,
                comment=reason,
            )
            context = context.with_activity(activity.name, placeholder)
            return placeholder, context


def _translate_activity_list(
    activities: list[AdfActivity],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> tuple[list[Activity], TranslationContext]:
    """Translates a list of ADF activities, threading context through each.

    Args:
        activities: List of ADF activity AST nodes.
        context: Current translation context.
        definitions: Full ADF definitions.

    Returns:
        Tuple of ``(translated_activities, final_context)``.
    """
    sorted_activities = _topological_visit(activities)
    results: list[Activity] = []

    for adf_activity in sorted_activities:
        activity_ir, context = _dispatch_activity(adf_activity, context, definitions)
        results.append(activity_ir)

    return results, context


def _topological_visit(activities: list[AdfActivity]) -> list[AdfActivity]:
    """Return activities in dependency-first (topological) order.

    Args:
        activities: Flat list of ADF activities at one nesting level.

    Returns:
        Activities reordered so that dependencies are visited first.
    """
    if not activities:
        return []

    name_to_activity: dict[str, AdfActivity] = {act.name: act for act in activities}
    in_degree: dict[str, int] = {act.name: 0 for act in activities}
    dependents: dict[str, list[str]] = defaultdict(list)

    for activity in activities:
        if activity.depends_on:
            for dependency in activity.depends_on:
                dependency_name = dependency.activity
                if dependency_name in name_to_activity:
                    in_degree[activity.name] += 1
                    dependents[dependency_name].append(activity.name)

    queue: list[str] = [name for name, degree in in_degree.items() if degree == 0]
    result: list[AdfActivity] = []

    while queue:
        queue.sort()
        current = queue.pop(0)
        result.append(name_to_activity[current])
        for dependent in dependents.get(current, []):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(result) < len(activities):
        visited = {act.name for act in result}
        for activity in activities:
            if activity.name not in visited:
                logger.warning("Cycle detected: activity '%s' has unresolved dependencies.", activity.name)
                result.append(activity)

    return result


_TIMEOUT_RE = re.compile(r"(?:(\d+)\.)?(\d{2}):(\d{2}):(\d{2})")


def _build_base_kwargs(activity: AdfActivity, definitions: AdfDefinitions) -> dict[str, Any]:
    """Extracts common fields shared by all Activity IR subclasses.

    Args:
        activity: ADF activity AST node.
        definitions: Full ADF definitions for linked-service cluster config.

    Returns:
        Dictionary with keys: ``name``, ``task_key``, ``timeout_seconds``,
        ``max_retries``, ``depends_on``, ``cluster``.
    """
    task_key = _sanitize_task_key(activity.name)

    timeout_seconds: int | None = None
    if activity.policy and activity.policy.timeout:
        timeout_seconds = _parse_adf_timeout(activity.policy.timeout)

    max_retries: int | None = None
    if activity.policy and activity.policy.retry is not None:
        max_retries = activity.policy.retry

    min_retry_interval_millis: int | None = None
    if activity.policy and activity.policy.retry_interval_in_seconds is not None:
        min_retry_interval_millis = activity.policy.retry_interval_in_seconds * 1000

    depends_on: list[Dependency] | None = None
    if activity.depends_on:
        depends_on = []
        for dependency in activity.depends_on:
            outcome = dependency.dependency_conditions[0] if dependency.dependency_conditions else None
            depends_on.append(
                Dependency(
                    task_key=_sanitize_task_key(dependency.activity),
                    outcome=outcome,
                )
            )

    cluster: dict[str, Any] | None = None
    if activity.linked_service_name:
        linked_service_name = activity.linked_service_name.reference_name
        linked_service_def = definitions.linked_services.get(linked_service_name)
        if linked_service_def:
            cluster = _extract_cluster_config(linked_service_def.properties)

    return {
        "name": activity.name,
        "task_key": task_key,
        "description": None,
        "timeout_seconds": timeout_seconds,
        "max_retries": max_retries,
        "min_retry_interval_millis": min_retry_interval_millis,
        "depends_on": depends_on,
        "cluster": cluster,
    }


def _sanitize_task_key(name: str) -> str:
    """Converts an ADF activity name to a valid Databricks task key.

    Args:
        name: ADF activity name.

    Returns:
        Sanitised task key string.
    """
    key = re.sub(r"[^a-zA-Z0-9_-]", "_", name)
    key = re.sub(r"_+", "_", key)
    key = key.strip("_")
    return key or "unnamed"


def _parse_adf_timeout(timeout_str: str) -> int | None:
    """Parses an ADF timeout string to total seconds.

    Args:
        timeout_str: Timeout in ``"d.hh:mm:ss"`` or ``"hh:mm:ss"`` format.

    Returns:
        Total seconds, or ``None`` if the format is unrecognised.
    """
    match = _TIMEOUT_RE.match(timeout_str)
    if not match:
        return None
    days = int(match.group(1) or 0)
    hours = int(match.group(2))
    minutes = int(match.group(3))
    seconds = int(match.group(4))
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def _extract_cluster_config(ls_properties: dict[str, Any]) -> dict[str, Any] | None:
    """Extracts Databricks cluster configuration from a linked-service properties dict.

    Args:
        ls_properties: Full properties bag from the linked service JSON.

    Returns:
        Cluster configuration dict, or ``None`` if no Databricks cluster
        details are present.
    """
    nested = ls_properties.get("typeProperties") or {}
    # Merge: nested values win over flat ones when both exist (matches ARM
    # template precedence).
    fields: dict[str, Any] = {**ls_properties, **nested}

    config: dict[str, Any] = {}

    existing_cluster_id = fields.get("existingClusterId")
    if existing_cluster_id:
        config["existing_cluster_id"] = existing_cluster_id

    new_cluster = fields.get("newClusterVersion") or fields.get("newClusterSparkVersion")
    if new_cluster:
        config["spark_version"] = new_cluster
        num_workers_raw = fields.get("newClusterNumOfWorker", 1)
        try:
            config["num_workers"] = int(num_workers_raw)
        except (TypeError, ValueError):
            config["num_workers"] = num_workers_raw
        config["node_type_id"] = fields.get("newClusterNodeType", "Standard_DS3_v2")
        spark_conf = fields.get("newClusterSparkConf")
        if spark_conf:
            config["spark_conf"] = spark_conf

    return config if config else None


def _pipeline_to_dict(pipeline: Pipeline) -> dict[str, Any]:
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
        "tasks": [_activity_to_dict(task) for task in pipeline.tasks],
    }
    if pipeline.translation_preferences is not None:
        result["translation_preferences"] = _preferences_to_dict(pipeline.translation_preferences)
    return result


def _preferences_to_dict(preferences: Any) -> dict[str, Any]:
    """Serialise a TranslationPreferences instance to a JSON-friendly dictionary.

    Args:
        preferences: The :class:`TranslationPreferences` snapshot to serialise.

    Returns:
        Dictionary with the four StrEnum fields rendered as their string
        values and per-task overrides preserved verbatim.
    """
    return {
        "copy_activity_paradigm": str(preferences.copy_activity_paradigm),
        "non_databricks_task_compute": str(preferences.non_databricks_task_compute),
        "use_lakeflow_connectors": str(preferences.use_lakeflow_connectors),
        "databricks_task_compute": str(preferences.databricks_task_compute),
        "lakeflow_connector_type": str(preferences.lakeflow_connector_type),
        "per_task": dict(preferences.per_task),
    }


def _activity_to_dict(task: Activity) -> dict[str, Any]:
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
    if task.compute_mode:
        task_dict["compute_mode"] = task.compute_mode

    extra = _activity_extra_fields(task)
    task_dict.update(extra)
    return task_dict


def _activity_extra_fields(activity: Activity) -> dict[str, Any]:
    """Extracts type-specific fields from an Activity subclass.

    Args:
        activity: Any Activity IR node.

    Returns:
        Dictionary of extra fields beyond the base Activity.
    """
    extra: dict[str, Any] = {}

    match activity:
        case NotebookActivity():
            extra["notebook_path"] = activity.notebook_path
            if activity.base_parameters:
                extra["base_parameters"] = activity.base_parameters
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
            extra["inner_activities"] = [_activity_to_dict(inner) for inner in activity.inner_activities]
        case IfConditionActivity():
            extra["op"] = activity.op
            extra["left"] = activity.left
            extra["right"] = activity.right
            extra["if_true_activities"] = [_activity_to_dict(inner) for inner in activity.if_true_activities]
            extra["if_false_activities"] = [_activity_to_dict(inner) for inner in activity.if_false_activities]
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
                {"value": case_item.value, "activities": [_activity_to_dict(inner) for inner in case_item.activities]}
                for case_item in activity.cases
            ]
            extra["default_activities"] = [_activity_to_dict(inner) for inner in activity.default_activities]
        case WaitActivity():
            extra["wait_time_seconds"] = activity.wait_time_seconds
        case SparkJarActivity():
            extra["main_class_name"] = activity.main_class_name
            if activity.parameters:
                extra["parameters"] = activity.parameters
            if activity.libraries:
                extra["libraries"] = activity.libraries
        case SparkPythonActivity():
            extra["python_file"] = activity.python_file
            if activity.parameters:
                extra["parameters"] = activity.parameters
        case WebActivity():
            extra["url"] = activity.url
            extra["method"] = activity.method
            if activity.body is not None:
                extra["body"] = activity.body
            if activity.headers:
                extra["headers"] = activity.headers
            if activity.authentication:
                extra["authentication"] = activity.authentication
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
            extra["motif_id"] = activity.motif_id
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
        case UnsupportedActivity():
            extra["original_type"] = activity.original_type
            extra["reason"] = activity.reason

    return extra


def _activity_to_debug_dict(activity: Activity) -> dict[str, Any]:
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
            result[field] = _activity_to_debug_dict(value)
        elif isinstance(value, list) and value and isinstance(value[0], Activity):
            result[field] = [_activity_to_debug_dict(inner) for inner in value]
        elif isinstance(value, list) and value and hasattr(value[0], "__dataclass_fields__"):
            result[field] = [_dataclass_to_debug_dict(item) for item in value]
        else:
            result[field] = value

    return result


def _dataclass_to_debug_dict(obj: Any) -> dict[str, Any]:
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
            result[field] = _activity_to_debug_dict(value)
        elif isinstance(value, list) and value and isinstance(value[0], Activity):
            result[field] = [_activity_to_debug_dict(inner) for inner in value]
        else:
            result[field] = value

    return result


def _pipeline_to_debug_dict(pipeline: Pipeline) -> dict[str, Any]:
    """Serialise a Pipeline IR to a full debug dict.

    Args:
        pipeline: The translated pipeline IR.

    Returns:
        Dict with every field fully expanded.
    """
    return {
        "__class__": "Pipeline",
        "name": pipeline.name,
        "parameters": pipeline.parameters,
        "schedule": pipeline.schedule,
        "tags": pipeline.tags,
        "tasks": [_activity_to_debug_dict(task) for task in pipeline.tasks],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Translate ADF pipelines to Databricks IR.")
    parser.add_argument("--source-dir", required=True, type=Path, help="Root directory containing ADF JSON exports.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./orchestra_output/translate"),
        help="Directory to write translation results into.",
    )
    parser.add_argument(
        "--pipeline",
        type=str,
        default=None,
        help="Translate only the named pipeline (default: all).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Write a full debug IR dump alongside the normal output.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    definitions = load_adf_definitions(args.source_dir)
    logger.info("Loaded %d pipeline(s) from %s", len(definitions.pipelines), args.source_dir)

    output_dir: Path = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    total_deterministic = 0
    total_agentic = 0
    total_unsupported = 0
    all_gaps: list[dict[str, Any]] = []

    for pipeline in definitions.pipelines:
        if args.pipeline and pipeline.name != args.pipeline:
            continue

        report = translate_pipeline(pipeline, definitions)
        total_deterministic += report.deterministic_count
        total_agentic += report.agentic_count
        total_unsupported += report.unsupported_count

        pipeline_file = output_dir / f"{_sanitize_task_key(pipeline.name)}.json"
        pipeline_dict = _pipeline_to_dict(report.pipeline)
        pipeline_file.write_text(json.dumps(pipeline_dict, indent=2, default=str), encoding="utf-8")
        logger.info("Wrote pipeline IR to %s", pipeline_file)

        # Write debug IR if requested
        if args.debug:
            debug_file = output_dir / f"{_sanitize_task_key(pipeline.name)}.debug.json"
            debug_dict = _pipeline_to_debug_dict(report.pipeline)
            debug_file.write_text(json.dumps(debug_dict, indent=2, default=str), encoding="utf-8")
            logger.info("Wrote debug IR to %s", debug_file)

        for gap in report.gaps:
            all_gaps.append(asdict(gap))

        if report.warnings:
            for warning in report.warnings:
                logger.warning(warning)

    if all_gaps:
        gaps_file = output_dir / "gaps.json"
        gaps_file.write_text(json.dumps(all_gaps, indent=2, default=str), encoding="utf-8")
        logger.info("Wrote %d gap(s) to %s", len(all_gaps), gaps_file)

    total = total_deterministic + total_agentic + total_unsupported
    print("\nTranslation Summary")
    print("===================")
    print(f"Deterministic:    {total_deterministic}")
    print(f"Agentic:          {total_agentic}")
    print(f"Unsupported:      {total_unsupported}")
    print(f"Total:            {total}")
