"""Core translation engine for ADF-to-Databricks pipeline conversion."""

from __future__ import annotations

import argparse
import json
import logging
import re
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
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
from orchestra.parser.ir_rewriter import rewrite_pipeline_expressions
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


def translate_pipeline(
    pipeline: AdfPipeline,
    definitions: AdfDefinitions,
    *,
    motif_consolidations: dict[str, str] | None = None,
) -> TranslationReport:
    """Translates an ADF pipeline into a Databricks pipeline IR.

    Args:
        pipeline: Parsed ADF pipeline AST.
        definitions: Full ADF definitions for cross-referencing datasets,
            linked services, etc.
        motif_consolidations: Optional mapping of ``motif_id`` ->
            ``"keep"`` / ``"consolidate"`` answers gathered from the
            adapter.  When ``None`` (back-compat default) the translator
            consolidates every detected motif.  When provided, only
            motifs whose id maps to ``"consolidate"`` are collapsed; the
            rest remain as the original activity-by-activity translation.

    Returns:
        :class:`TranslationReport` containing the translated :class:`Pipeline`,
        the list of detected motifs, and any gaps encountered.

    Notes:
        After dispatching individual activities the translator runs
        :func:`~orchestra.parser.ir_rewriter.rewrite_pipeline_expressions`
        over the whole IR so that ``@{...}`` ADF expressions embedded in
        SQL bodies, REST payloads, dataset paths, and other string-typed
        fields are rewritten through the same parser the per-activity
        translators use.  Tokens that cannot be resolved are recorded
        as translation warnings instead of shipping into the bundle
        verbatim.
    """
    context = TranslationContext(
        activity_cache=MappingProxyType({}),
        registry=MappingProxyType(TRANSLATOR_REGISTRY),
        variable_cache=MappingProxyType({}),
        global_parameters=MappingProxyType(dict(definitions.global_parameters)),
    )

    # C-41 (CF5-001): seed declared variable types so the IfCondition
    # fallback can recognise Boolean variables that are backed only by a
    # literal default init task (and thus never populate
    # variable_value_cache).  Without this a `continue`-style Boolean
    # condition emits NOT_EQUAL(left, '0'), always true for a
    # 'true'/'false' string, making the false branch dead code.
    if pipeline.variables:
        default_literals: dict[str, str] = {}
        for name, var in pipeline.variables.items():
            default = var.default_value
            if isinstance(default, bool):
                default_literals[name] = "true" if default else "false"
            elif isinstance(default, str) and default.lower() in ("true", "false"):
                default_literals[name] = default.lower()
        context = context.with_variable_types(
            {name: var.type for name, var in pipeline.variables.items()},
            default_literals=default_literals,
        )

    gaps: list[AgenticGap] = []
    warnings: list[str] = []

    # C-05 (VAREX-002): synthesise init SetVariable tasks for pipeline
    # variables carrying a defaultValue.  This seeds variable_cache so
    # downstream @variables('X') references resolve to the init task's
    # value reference instead of falling back to a self-referential
    # {{tasks.X.values.X}} dangler.
    init_variable_activities, context = _build_variable_init_activities(pipeline, context)
    translated_activities: list[Activity] = list(init_variable_activities)

    sorted_activities = _topological_visit(pipeline.activities)
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

    parameter_entries: list[dict[str, Any]] = []
    if pipeline.parameters:
        for param_name, param_def in pipeline.parameters.items():
            entry: dict[str, Any] = {"name": param_name, "type": param_def.type}
            if param_def.default_value is not None:
                entry["default"] = _coerce_parameter_default(param_def.default_value, param_def.type)
            parameter_entries.append(entry)

    schedule = _compile_pipeline_schedule(pipeline, definitions)

    pipeline_ir = Pipeline(
        name=pipeline.name,
        parameters=parameter_entries or None,
        tasks=translated_activities,
        tags={"source": "adf", "pipeline": pipeline.name},
        schedule=schedule,
    )

    # Whole-IR expression rewrite: catches @{...} tokens the per-activity
    # translators didn't address (raw SQL WHERE clauses inside source_properties,
    # REST request bodies, dataset folder paths, ...).  Unresolved tokens are
    # surfaced as translation warnings.
    pipeline_ir = rewrite_pipeline_expressions(pipeline_ir, warnings=warnings)

    # Motif detection: scan for known multi-activity patterns.  Collapsing is
    # gated on the per-motif preference -- when *motif_consolidations* is
    # ``None`` we preserve back-compat behaviour and collapse every detected
    # motif; otherwise only motifs whose motif_id maps to ``"consolidate"`` are
    # collapsed.
    detected_motifs = detect_motifs(pipeline, definitions)
    motifs_to_collapse = _filter_motifs_for_collapse(detected_motifs, motif_consolidations)
    if motifs_to_collapse:
        pipeline_ir = collapse_motifs(pipeline_ir, motifs_to_collapse)
        for motif in motifs_to_collapse:
            logger.info(
                "Collapsed motif '%s': %d activities -> %s",
                motif.definition.display_name,
                len(motif.matched_activities),
                motif.definition.databricks_replacement,
            )
    for motif in detected_motifs:
        if motif not in motifs_to_collapse:
            logger.info(
                "Detected motif '%s' left expanded: matched %d activities (user opted to keep)",
                motif.definition.display_name,
                len(motif.matched_activities),
            )

    return TranslationReport(
        pipeline=pipeline_ir,
        deterministic_count=deterministic_count,
        agentic_count=agentic_count,
        unsupported_count=unsupported_count,
        gaps=gaps,
        warnings=warnings,
        detected_motifs=list(detected_motifs),
    )


def _filter_motifs_for_collapse(
    detected_motifs: list,
    motif_consolidations: dict[str, str] | None,
) -> list:
    """Returns the subset of detected motifs the caller asked to collapse.

    Args:
        detected_motifs: Output of
            :func:`orchestra.motifs.detector.detect_motifs`.
        motif_consolidations: Caller-supplied answers.  ``None`` means
            "collapse all" (back-compat).  Otherwise only motifs whose
            ``motif_id`` maps to ``"consolidate"`` are collapsed.

    Returns:
        The subset of motifs to pass to
        :func:`orchestra.motifs.collapser.collapse_motifs`.
    """
    if motif_consolidations is None:
        return list(detected_motifs)
    return [m for m in detected_motifs if motif_consolidations.get(m.definition.motif_id) == "consolidate"]


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
    base_kwargs = _build_base_kwargs(activity, definitions, context=context)

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


# C-10 (SCHED-001): map Windows timezone names ADF emits onto IANA names
# the Databricks DAB ``schedule.timezone_id`` field expects.  Only the
# ones observed in the corpus are mapped explicitly; anything else passes
# through unchanged (Databricks accepts any IANA zone).
_ADF_TIMEZONE_TO_IANA: dict[str, str] = {
    "UTC": "UTC",
    "Coordinated Universal Time": "UTC",
    "Romance Standard Time": "Europe/Madrid",
    "Central Europe Standard Time": "Europe/Budapest",
    "Central European Standard Time": "Europe/Warsaw",
    "W. Europe Standard Time": "Europe/Berlin",
    "GMT Standard Time": "Europe/London",
    "Eastern Standard Time": "America/New_York",
    "Central Standard Time": "America/Chicago",
    "Pacific Standard Time": "America/Los_Angeles",
    "Mountain Standard Time": "America/Denver",
    "Tokyo Standard Time": "Asia/Tokyo",
    "China Standard Time": "Asia/Shanghai",
    "India Standard Time": "Asia/Kolkata",
    "AUS Eastern Standard Time": "Australia/Sydney",
}

_DAYS_OF_WEEK_MAP: dict[str, str] = {
    "Sunday": "SUN",
    "Monday": "MON",
    "Tuesday": "TUE",
    "Wednesday": "WED",
    "Thursday": "THU",
    "Friday": "FRI",
    "Saturday": "SAT",
}


def _compile_pipeline_schedule(
    pipeline: AdfPipeline,
    definitions: AdfDefinitions,
) -> dict[str, Any] | None:
    """Compiles the first matching ADF trigger into a Pipeline.schedule dict.

    C-10 (SCHED-001): translates ScheduleTrigger recurrence into a
    quartz_cron_expression + timezone_id pair the DAB writer can emit
    as the job's ``schedule:`` block.  BlobEventsTrigger maps to a
    ``trigger.file_arrival`` spec.  TumblingWindowTrigger and
    CustomEventsTrigger are best-effort: they emit a SETUP-style hint
    so the user can finish wiring them manually.
    """
    triggers = getattr(definitions, "triggers", None) or []
    pipeline_name = pipeline.name
    matching_triggers = [t for t in triggers if _trigger_references(t, pipeline_name)]
    if not matching_triggers:
        return None

    # First matching trigger wins -- ADF allows multiple triggers per
    # pipeline but DAB schedules are 1:1.  Subsequent triggers can be
    # surfaced via SETUP.md by downstream tooling.
    trigger = matching_triggers[0]
    spec = _adf_trigger_to_schedule(trigger)
    if spec is not None:
        # SCHED3-003: pull per-pipeline parameter overrides off the
        # matching pipelineReference so trigger-injected params (e.g.
        # ``{applicationName: 'cli0010', negocio: 'GLP'}``) propagate to
        # the job's default parameter values.
        overrides = _extract_trigger_parameter_overrides(trigger, pipeline_name)
        if overrides:
            spec["parameter_overrides"] = overrides
    return spec


def _trigger_references(trigger: Any, pipeline_name: str) -> bool:
    """Returns True if *trigger* references the named pipeline."""
    refs = trigger.pipelines or []
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        pipeline_ref = ref.get("pipelineReference") or {}
        if isinstance(pipeline_ref, dict) and pipeline_ref.get("referenceName") == pipeline_name:
            return True
    return False


def _extract_trigger_parameter_overrides(trigger: Any, pipeline_name: str) -> dict[str, Any]:
    """Returns the parameters block on the trigger's pipelineReference entry.

    SCHED3-003: ADF triggers attach per-pipeline parameter overrides at the
    ``triggers[].pipelines[].parameters`` level so scheduled runs receive
    deterministic values for pipeline parameters.  Without surfacing them,
    scheduled invocations would receive the pipeline parameter defaults
    only.
    """
    refs = trigger.pipelines or []
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        pipeline_ref = ref.get("pipelineReference") or {}
        if not isinstance(pipeline_ref, dict):
            continue
        if pipeline_ref.get("referenceName") != pipeline_name:
            continue
        params = ref.get("parameters") or {}
        if isinstance(params, dict) and params:
            return dict(params)
    return {}


def _adf_trigger_to_schedule(trigger: Any) -> dict[str, Any] | None:
    """Compiles an :class:`AdfTrigger` into a Pipeline.schedule spec dict."""
    props = trigger.properties or {}
    type_properties = props.get("typeProperties") or {}
    runtime_state = props.get("runtimeState", "Started")
    pause_status = "PAUSED" if runtime_state == "Stopped" else "UNPAUSED"

    trigger_type = trigger.type
    if trigger_type == "ScheduleTrigger":
        recurrence = type_properties.get("recurrence") or {}
        # SCHED3-002: Day/Week/Month with interval > 1 cannot be represented
        # in quartz cron without enumerating every Nth occurrence; use the
        # trigger.periodic primitive so it ships correctly.
        periodic = _recurrence_to_periodic(recurrence)
        if periodic is not None:
            spec: dict[str, Any] = {
                "kind": "periodic",
                "interval": periodic["interval"],
                "unit": periodic["unit"],
                "pause_status": pause_status,
            }
            # C-36 (SCHED4-001): forward the captured time-of-day so the
            # bundler can flag it in SETUP.md.
            if "time_of_day_note" in periodic:
                spec["time_of_day_note"] = periodic["time_of_day_note"]
            return spec
        # C-45 (SCHED5-002): an interval > 1 Month recurrence has no
        # monthly-cron-expressible form (cron fires every month, ignoring the
        # interval) and the DAB periodic enum has no MONTHS unit, so surface a
        # manual setup note instead of silently emitting a monthly cron.
        if _is_multi_month_recurrence(recurrence):
            return {
                "kind": "manual_setup",
                "trigger_type": "ScheduleTrigger",
                "pause_status": pause_status,
                "note": (
                    "Month-frequency trigger with interval > 1 has no DAB "
                    "equivalent (PeriodicTriggerConfigurationTimeUnit lacks "
                    "MONTHS and quartz cron cannot encode every-Nth-month). "
                    "Configure the schedule manually."
                ),
            }
        cron = _recurrence_to_quartz_cron(recurrence)
        if cron is None:
            return None
        timezone_id = _normalize_timezone(recurrence.get("timeZone"))
        spec = {
            "kind": "schedule",
            "quartz_cron_expression": cron,
            "timezone_id": timezone_id,
            "pause_status": pause_status,
        }
        return spec
    if trigger_type == "TumblingWindowTrigger":
        # Approximate as a periodic schedule -- the user should review.
        frequency = type_properties.get("frequency", "Hour")
        interval = type_properties.get("interval", 1)
        spec = {
            "kind": "schedule",
            "tumbling": True,
            "frequency": frequency,
            "interval": interval,
            "pause_status": pause_status,
            "note": "Approximated from TumblingWindowTrigger; review window boundaries.",
        }
        return spec
    if trigger_type == "BlobEventsTrigger":
        scope = type_properties.get("scope", "")
        events = type_properties.get("events") or []
        spec = {
            "kind": "file_arrival",
            "url": scope,
            "events": list(events),
            "pause_status": pause_status,
        }
        return spec
    if trigger_type == "CustomEventsTrigger":
        spec = {
            "kind": "manual_setup",
            "trigger_type": "CustomEventsTrigger",
            "pause_status": pause_status,
            "note": "CustomEventsTrigger has no direct DAB equivalent; configure in SETUP.md.",
        }
        return spec
    return None


def _recurrence_to_periodic(recurrence: dict[str, Any]) -> dict[str, Any] | None:
    """Returns a {interval, unit} dict when the recurrence requires periodic.

    SCHED3-002: Day/Week/Month with ``interval > 1`` cannot be modelled in
    quartz cron without enumerating every Nth occurrence.  The DAB
    ``trigger.periodic`` primitive accepts ``{interval, unit}`` directly,
    so we emit a periodic spec instead.  Minute/Hour with interval > 1 are
    expressible in cron (``0/N``) so we still leave them to the cron path.

    C-36 (SCHED4-001): when the recurrence carries a non-empty
    ``schedule`` block (hours / minutes / weekDays / monthDays), the
    ``trigger.periodic`` primitive can't encode the time-of-day so the
    schedule silently fires at midnight instead.  Capture the original
    schedule on a ``time_of_day_note`` field so the bundler can emit a
    ``manual_schedule_time_of_day`` SetupTask (SETUP.md).
    """
    frequency = recurrence.get("frequency")
    interval = recurrence.get("interval", 1)
    if isinstance(interval, str) and interval.isdigit():
        interval = int(interval)
    if not isinstance(interval, int) or interval <= 1:
        return None
    # C-45 (SCHED5-002): the DAB PeriodicTriggerConfigurationTimeUnit enum
    # only defines DAYS / HOURS / WEEKS — emitting MONTHS makes bundle
    # validate/deploy reject the trigger.  Month frequencies are routed to
    # the quartz cron path (monthDays) instead; an interval > 1 Month, which
    # is not monthly-cron-expressible, is surfaced as a setup note by the
    # caller.
    unit_map = {"Day": "DAYS", "Week": "WEEKS"}
    unit = unit_map.get(frequency or "")
    if unit is None:
        return None
    spec: dict[str, Any] = {"interval": interval, "unit": unit}
    schedule = recurrence.get("schedule") or {}
    if isinstance(schedule, dict):
        time_of_day = {
            key: schedule.get(key) for key in ("hours", "minutes", "weekDays", "monthDays") if schedule.get(key)
        }
        if time_of_day:
            spec["time_of_day_note"] = time_of_day
    return spec


def _is_multi_month_recurrence(recurrence: dict[str, Any]) -> bool:
    """Returns True for a Month-frequency recurrence with ``interval > 1``.

    C-45 (SCHED5-002): these triggers cannot ship as either a periodic spec
    (no MONTHS unit in the DAB enum) or a quartz cron (cron has no
    every-Nth-month form), so the caller emits a manual setup note.
    """
    if recurrence.get("frequency") != "Month":
        return False
    interval = recurrence.get("interval", 1)
    if isinstance(interval, str) and interval.isdigit():
        interval = int(interval)
    return isinstance(interval, int) and interval > 1


def _recurrence_to_quartz_cron(recurrence: dict[str, Any]) -> str | None:
    """Compiles a ScheduleTrigger recurrence block into a quartz cron expression.

    ADF recurrence has ``frequency`` + ``interval`` + ``schedule``.  The
    quartz format expected by DAB is
    ``second minute hour day-of-month month day-of-week``.
    """
    frequency = recurrence.get("frequency")
    interval = recurrence.get("interval", 1)
    schedule = recurrence.get("schedule") or {}
    minutes = schedule.get("minutes")
    hours = schedule.get("hours")
    week_days = schedule.get("weekDays") or []
    month_days = schedule.get("monthDays") or []

    # C-44 (SCHED5-001): when the schedule block carries no explicit
    # time-of-day, ADF defaults it to the first-execution time derived from
    # ``startTime``.  Reading only ``schedule.minutes/hours`` (falling back
    # to '0'/'0') silently shifts a ``startTime`` of 21:00 to midnight.
    # Derive the hour/minute from ``startTime`` so the cron fires at the
    # ADF-intended time.
    start_hour, start_minute = _start_time_hour_minute(recurrence.get("startTime"))
    minute_default = str(start_minute) if start_minute is not None else "0"
    hour_default = str(start_hour) if start_hour is not None else "0"

    minute_field = _list_or_default(minutes, minute_default)
    hour_field = _list_or_default(hours, hour_default)
    if isinstance(interval, str) and interval.isdigit():
        interval = int(interval)

    if frequency == "Minute":
        if not isinstance(interval, int) or interval <= 0:
            interval = 1
        return f"0 0/{interval} * * * ?"
    if frequency == "Hour":
        if not isinstance(interval, int) or interval <= 0:
            interval = 1
        return f"0 {minute_field} 0/{interval} * * ?"
    if frequency == "Day":
        return f"0 {minute_field} {hour_field} * * ?"
    if frequency == "Week":
        days = ",".join(_DAYS_OF_WEEK_MAP.get(d, d) for d in week_days) or "MON"
        return f"0 {minute_field} {hour_field} ? * {days}"
    if frequency == "Month":
        dom_field = _list_or_default(month_days, "1")
        return f"0 {minute_field} {hour_field} {dom_field} * ?"
    return None


def _list_or_default(value: Any, default: str) -> str:
    """Renders a recurrence list/scalar as a cron-field string."""
    if value is None:
        return default
    if isinstance(value, list):
        if not value:
            return default
        return ",".join(str(v) for v in value)
    return str(value)


def _start_time_hour_minute(start_time: Any) -> tuple[int | None, int | None]:
    """Parses an ISO 8601 ``startTime`` into ``(hour, minute)``.

    C-44 (SCHED5-001): ADF uses the trigger's first-execution time (from
    ``startTime``) as the default time-of-day when the recurrence carries no
    explicit ``schedule.hours/minutes``.  Returns ``(None, None)`` when the
    value is missing or unparseable so the caller keeps the midnight
    fallback.
    """
    if not isinstance(start_time, str) or not start_time.strip():
        return None, None
    value = start_time.strip()
    # ``datetime.fromisoformat`` rejects a trailing 'Z' before 3.11; map it
    # to the explicit UTC offset so older interpreters parse it too.
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None, None
    return parsed.hour, parsed.minute


def _normalize_timezone(tz: Any) -> str:
    """Maps an ADF timezone string to an IANA zone the DAB writer accepts."""
    if not tz or not isinstance(tz, str):
        return "UTC"
    return _ADF_TIMEZONE_TO_IANA.get(tz, tz)


def _build_variable_init_activities(
    pipeline: AdfPipeline,
    context: TranslationContext,
) -> tuple[list[Activity], TranslationContext]:
    """Synthesise init SetVariable IR tasks for variables carrying defaultValue.

    C-05 (VAREX-002): without an explicit ADF SetVariable activity, a
    variable's defaultValue is never materialised, so downstream
    ``@variables('X')`` references fall back to a dangling
    ``{{tasks.X.values.X}}`` reference.  This helper emits an init task
    per default-valued variable so the variable_cache carries a real
    setter task_key.
    """
    from orchestra.parser.expression_parser import resolve_expression

    if not pipeline.variables:
        return [], context

    init_tasks: list[Activity] = []
    for var_name, var_def in pipeline.variables.items():
        default = var_def.default_value
        if default is None:
            return_default = False
        else:
            return_default = True
        if not return_default:
            continue
        task_key = f"_init_{_sanitize_task_key(var_name)}"
        expr_result = resolve_expression(default, context)
        if expr_result is not None:
            variable_value = expr_result.value
            value_kind = expr_result.kind
            notebook_code = expr_result.value if expr_result.kind == "notebook_code" else None
            notebook_imports = list(expr_result.imports) if expr_result.kind == "notebook_code" else []
            required_parameters = dict(expr_result.required_parameters)
        else:
            # VAREX3-002: Boolean defaults must render lowercase ('true'/'false')
            # so downstream ``@equals(variables('continue'), true)`` evaluates
            # consistently with ADF semantics.  Python ``str(True)`` would
            # produce title-case 'True' and silently invert the comparison.
            if isinstance(default, bool):
                variable_value = "true" if default else "false"
            else:
                variable_value = str(default) if not isinstance(default, str) else default
            value_kind = "literal"
            notebook_code = None
            notebook_imports = []
            required_parameters = {}

        init_activity = SetVariableActivity(
            name=f"_init_{var_name}",
            task_key=task_key,
            description=None,
            timeout_seconds=None,
            max_retries=None,
            min_retry_interval_millis=None,
            depends_on=None,
            cluster=None,
            variable_name=var_name,
            variable_value=variable_value,
            value_kind=value_kind,
            notebook_code=notebook_code,
            notebook_imports=notebook_imports,
            required_parameters=required_parameters,
        )
        init_tasks.append(init_activity)
        # Register the synthesised setter so @variables('X') resolves to
        # {{tasks._init_X.values.X}}.  When the value is itself a DAB ref
        # (e.g. from @utcNow()), inline it directly per existing semantics.
        dab_ref_value = variable_value if value_kind == "dab_ref" else None
        context = context.with_variable(var_name, task_key, dab_ref_value=dab_ref_value)
        context = context.with_activity(init_activity.name, init_activity)
    return init_tasks, context


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


def _build_base_kwargs(
    activity: AdfActivity,
    definitions: AdfDefinitions,
    *,
    context: TranslationContext | None = None,
) -> dict[str, Any]:
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
            outcome = _map_dependency_conditions(dependency.dependency_conditions)
            depends_on.append(
                Dependency(
                    task_key=_sanitize_task_key(dependency.activity),
                    outcome=outcome,
                )
            )

    cluster: dict[str, Any] | None = None
    existing_cluster_id: str | None = None
    if activity.linked_service_name:
        linked_service_name = activity.linked_service_name.reference_name
        linked_service_def = definitions.linked_services.get(linked_service_name)
        if linked_service_def:
            ls_param_overrides = _resolve_ls_parameters(
                linked_service_def.properties,
                activity.linked_service_name.parameters,
                context=context,
            )
            cluster = _extract_cluster_config(
                linked_service_def.properties,
                ls_param_overrides,
            )
            if cluster:
                existing_cluster_id = cluster.get("existing_cluster_id")

    return {
        "name": activity.name,
        "task_key": task_key,
        "description": None,
        "timeout_seconds": timeout_seconds,
        "max_retries": max_retries,
        "min_retry_interval_millis": min_retry_interval_millis,
        "depends_on": depends_on,
        "cluster": cluster,
        "existing_cluster_id": existing_cluster_id,
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


def _map_dependency_conditions(conditions: list[str] | None) -> str | None:
    """Map an ADF dependsOn[].dependencyConditions list to a single outcome.

    ADF accepts multiple conditions on a single edge — e.g.
    ``['Succeeded', 'Failed']`` means "run regardless of upstream
    success/failure".  Databricks Workflows encodes the same semantics
    by combining ``run_if`` and per-edge outcomes; the encoding that
    propagates correctly through this codebase is to pick a single
    representative outcome that the downstream
    ``run_if_from_adf_outcomes`` reducer can interpret.

    Mapping rules (single condition):
        Succeeded  -> "Succeeded"
        Failed     -> "Failed"
        Completed  -> "Completed"   (run regardless of upstream result)
        Skipped    -> "Skipped"

    Mapping rules (multi):
        Any list that includes ``Failed`` AND ``Succeeded``  -> "Completed"
        Any list that includes ``Skipped``                   -> "Skipped"
        Multi-element list including ``Failed`` only         -> "Failed"
        Anything else                                        -> first item
    """
    if not conditions:
        return None
    normalized = [c for c in conditions if c]
    if not normalized:
        return None
    if len(normalized) == 1:
        return normalized[0]
    cset = set(normalized)
    if "Failed" in cset and "Succeeded" in cset:
        return "Completed"
    if "Skipped" in cset:
        return "Skipped"
    if "Failed" in cset:
        return "Failed"
    return normalized[0]


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


def _resolve_ls_parameters(
    ls_properties: dict[str, Any],
    activity_supplied: dict[str, Any] | None,
    context: TranslationContext | None = None,
) -> dict[str, Any]:
    """Builds the effective LS parameter map for cluster-config resolution.

    Args:
        ls_properties: Full properties bag from the linked service JSON.
        activity_supplied: Per-activity parameter overrides from the
            ``linkedServiceName.parameters`` block (may be ``None``).
        context: Translation context used to resolve ``@``-prefixed
            activity-supplied values against factory global parameters.
            When omitted, ADF expressions are left as raw strings.

    Returns:
        Mapping of parameter name -> resolved value.  Activity-supplied
        overrides win over LS defaultValue.  Wrapped ``{"value": ..., "type":
        "Expression"}`` dicts are unwrapped and (when ``context`` is set)
        passed through :func:`resolve_expression` so ``@pipeline().
        globalParameters.X`` collapses to the factory value.
    """
    from orchestra.parser.expression_parser import resolve_expression

    resolved: dict[str, Any] = {}
    declared = ls_properties.get("parameters") or {}
    if isinstance(declared, dict):
        for pname, pdef in declared.items():
            if isinstance(pdef, dict) and "defaultValue" in pdef:
                resolved[pname] = _unwrap_expression_value(pdef["defaultValue"])
    if isinstance(activity_supplied, dict):
        for pname, pval in activity_supplied.items():
            raw = _unwrap_expression_value(pval)
            # C-03: route @-prefixed activity-supplied values through the
            # expression parser so @pipeline().globalParameters.X collapses
            # to the factory value when one is set.
            if context is not None and isinstance(raw, str) and raw.startswith("@"):
                result = resolve_expression(raw, context)
                # C-13 (NB-ITER3-002 / LSC3-003 / VAREX3-006): accept both
                # literal and dab_ref so @pipeline().parameters.X collapses
                # to {{job.parameters.X}} (valid in custom_tags map values).
                if result is not None and result.kind in ("literal", "dab_ref"):
                    raw = result.value
            resolved[pname] = raw
    return resolved


def _unwrap_expression_value(value: Any) -> Any:
    """Unwrap a ``{"value": ..., "type": "Expression"}`` ADF dict-wrapper.

    C-02 (NB-ITER2-2 / LSC2-003): activity-supplied LS parameter values and
    LS-derived cluster fields (``custom_tags``, ``spark_env_vars`` entries,
    ...) sometimes ship as the ADF expression-dict shape.  Databricks
    cluster YAML rejects nested dicts in ``custom_tags``; recursive unwrap
    flattens them to scalars while leaving regular dicts untouched.
    """
    if isinstance(value, dict):
        # Bare {"value": X, "type": "Expression"} -- collapse to inner X.
        if "value" in value and value.get("type") == "Expression":
            return _unwrap_expression_value(value["value"])
        # Some payloads omit the explicit type marker but follow the same
        # single-key shape.  Conservatively unwrap only when the dict has
        # the exact two keys {"value", "type"} so we don't corrupt regular
        # nested config blocks like {"workspace": {"destination": ...}}.
        if set(value.keys()) == {"value", "type"}:
            return _unwrap_expression_value(value["value"])
        return {k: _unwrap_expression_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_unwrap_expression_value(v) for v in value]
    return value


_LS_PARAM_REF_RE = re.compile(r"@linkedService\(\s*\)\.(\w+)", re.IGNORECASE)


def _substitute_ls_params(value: Any, params: dict[str, Any]) -> Any:
    """Replaces ``@linkedService().X`` tokens in *value* with bound params.

    Handles scalar strings and nested dicts/lists.  Returns the value
    unchanged when no substitution is possible.
    """
    if isinstance(value, str):
        if "@linkedService()" not in value:
            return value
        # Full-string single-reference: drop in the resolved value with its
        # original type so e.g. integer params don't get stringified.
        full = _LS_PARAM_REF_RE.fullmatch(value)
        if full is not None:
            name = full.group(1)
            if name in params:
                return params[name]
            return value

        def _sub(match: re.Match[str]) -> str:
            name = match.group(1)
            if name in params:
                return str(params[name])
            return match.group(0)

        return _LS_PARAM_REF_RE.sub(_sub, value)
    if isinstance(value, dict):
        return {k: _substitute_ls_params(v, params) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute_ls_params(v, params) for v in value]
    return value


def _coerce_int(value: Any) -> Any:
    """Coerce numeric strings (``"1"``) to ``int``; leave other values alone."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def _coerce_parameter_default(value: Any, declared_type: str) -> Any:
    """Coerce an ADF parameter default into a Python type matching its declared type.

    The declared type (``"Bool"`` / ``"Int"`` / ``"Float"`` / ``"String"`` /
    ``"Array"`` / ``"Object"``) is what ADF stores in the pipeline JSON.
    Defaults round-trip as strings through JSON, so a Bool parameter with
    default ``false`` arrives as the literal ``"False"``.  The fix re-types
    each default per the declared type so the emitted YAML carries a real
    bool / int / float, not a quoted string.
    """
    t = (declared_type or "String").lower()
    if t in ("bool", "boolean"):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in ("true", "false"):
                return lowered == "true"
        return value
    if t in ("int", "integer"):
        return _coerce_int(value)
    if t == "float":
        try:
            return float(value)
        except (TypeError, ValueError):
            return value
    return value


def _extract_cluster_config(
    ls_properties: dict[str, Any],
    ls_param_overrides: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Extracts Databricks cluster configuration from a linked-service properties dict.

    Args:
        ls_properties: Full properties bag from the linked service JSON.
        ls_param_overrides: Optional map of ``@linkedService().X`` -> value
            overrides to substitute before extraction.  When supplied,
            every string value in the LS payload is rewritten through
            ``_substitute_ls_params`` so cluster fields like
            ``newClusterVersion: '@linkedService().clusterVersion'``
            resolve to a real Spark version string.

    Returns:
        Cluster configuration dict, or ``None`` if no Databricks cluster
        details are present.
    """
    overrides = ls_param_overrides or {}
    if overrides:
        ls_properties = _substitute_ls_params(ls_properties, overrides)

    # C-02 (NB-ITER2-2 / LSC2-003): unwrap any {value, type:'Expression'}
    # dicts that survived the substitution pass.  Map fields like
    # custom_tags and spark_env_vars must be plain Map[String, String] for
    # Databricks to accept the cluster YAML.
    ls_properties = _unwrap_expression_value(ls_properties)

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
        config["num_workers"] = _coerce_int(fields.get("newClusterNumOfWorker", 1))
        config["node_type_id"] = fields.get("newClusterNodeType", "Standard_DS3_v2")
        spark_conf = fields.get("newClusterSparkConf")
        if spark_conf:
            config["spark_conf"] = spark_conf

        # Extended cluster fields (LSC-003, NB-3).
        driver_node = fields.get("newClusterDriverNodeType")
        if driver_node:
            config["driver_node_type_id"] = driver_node
        spark_env_vars = fields.get("newClusterSparkEnvVars")
        if spark_env_vars:
            config["spark_env_vars"] = spark_env_vars
        custom_tags = fields.get("newClusterCustomTags")
        if custom_tags:
            config["custom_tags"] = custom_tags
        init_scripts = fields.get("newClusterInitScripts")
        if init_scripts:
            config["init_scripts"] = init_scripts
        data_security_mode = fields.get("dataSecurityMode") or fields.get("newClusterDataSecurityMode")
        if data_security_mode:
            config["data_security_mode"] = data_security_mode
        cluster_log_conf = fields.get("clusterLogConf") or fields.get("newClusterLogDestination")
        if cluster_log_conf:
            config["cluster_log_conf"] = cluster_log_conf

    # C-39 (LSC4-004): capture the ADF authentication shape (e.g. "MSI" or
    # any CredentialReference) so the bundler can emit a manual_credential
    # SetupTask warning that ``single_user_name`` was rewritten to the
    # deploying user.
    authentication = fields.get("authentication")
    if authentication:
        config["_adf_authentication"] = authentication
    credential = fields.get("credential")
    if isinstance(credential, dict) and credential.get("type") == "CredentialReference":
        config["_adf_credential_reference"] = credential.get("referenceName") or "<unspecified>"

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
        Dictionary with each StrEnum field rendered as its string value
        and per-task overrides preserved verbatim.
    """
    return {
        "copy_activity_paradigm": str(preferences.copy_activity_paradigm),
        "non_databricks_task_compute": str(preferences.non_databricks_task_compute),
        "use_lakeflow_connectors": str(preferences.use_lakeflow_connectors),
        "lakeflow_connector_type": str(preferences.lakeflow_connector_type),
        "motif_consolidations": {
            motif_id: str(choice) for motif_id, choice in preferences.motif_consolidations.items()
        },
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
    if task.existing_cluster_id:
        task_dict["existing_cluster_id"] = task.existing_cluster_id
    if task.compute_mode:
        task_dict["compute_mode"] = task.compute_mode
    if task.libraries:
        task_dict["libraries"] = task.libraries
    if task.parameter_approximations:
        task_dict["parameter_approximations"] = task.parameter_approximations

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
            if activity.notebook_path_unresolved:
                extra["notebook_path_unresolved"] = True
                if activity.notebook_path_expression is not None:
                    extra["notebook_path_expression"] = activity.notebook_path_expression
            if activity.unresolved_libraries:
                extra["unresolved_libraries"] = list(activity.unresolved_libraries)
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
            extra["if_true_activities"] = [_activity_to_dict(inner) for inner in activity.if_true_activities]
            extra["if_false_activities"] = [_activity_to_dict(inner) for inner in activity.if_false_activities]
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
                {"value": case_item.value, "activities": [_activity_to_dict(inner) for inner in case_item.activities]}
                for case_item in activity.cases
            ]
            extra["default_activities"] = [_activity_to_dict(inner) for inner in activity.default_activities]
            if activity.bridge_notebook_code:
                extra["bridge_notebook_code"] = activity.bridge_notebook_code
            if activity.bridge_notebook_imports:
                extra["bridge_notebook_imports"] = list(activity.bridge_notebook_imports)
            if activity.bridge_required_parameters:
                extra["bridge_required_parameters"] = dict(activity.bridge_required_parameters)
        case WaitActivity():
            extra["wait_time_seconds"] = activity.wait_time_seconds
        case SparkJarActivity():
            extra["main_class_name"] = activity.main_class_name
            if activity.parameters:
                extra["parameters"] = activity.parameters
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
