"""Translates ADF Switch activities to Databricks SwitchActivity IR."""

from __future__ import annotations

from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDefinitions
from flowx.models.ir import Activity, SwitchActivity, SwitchCase, TranslationContext
from flowx.parser.adf_loader import parse_activity
from flowx.parser.expression_parser import resolve_expression, resolve_interpolated_string
from flowx.translator.activity_translators.resolve import resolve_field


def _resolve_on_expression(on_expression: str, context: TranslationContext) -> str:
    """Resolves the ``on`` expression to a DAB dynamic value ref.

    Args:
        on_expression: Raw ADF on-expression string.
        context: Translation context for resolving variables.

    Returns:
        Resolved DAB ref string, or the original if unresolvable.
    """
    if "@{" in on_expression:
        return resolve_interpolated_string(on_expression, context)

    if on_expression.startswith("@"):
        result = resolve_expression(on_expression, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value

    return on_expression


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
    *,
    translate_activities_fn: Any = None,
) -> tuple[Activity, TranslationContext]:
    """Translates a Switch activity with recursive branch translation.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.
        translate_activities_fn: Callback to translate branch activities.
            Signature: ``(activities, context, definitions) -> (list[Activity], TranslationContext)``.

    Returns:
        Tuple of ``(SwitchActivity, updated_context)``.
    """
    type_properties = activity.type_properties or {}

    on_raw = type_properties.get("on", {})
    if isinstance(on_raw, dict):
        on_expression_raw = on_raw.get("value", "")
    elif isinstance(on_raw, str):
        on_expression_raw = on_raw
    else:
        on_expression_raw = str(on_raw) if on_raw else ""

    on_expression = _resolve_on_expression(on_expression_raw, context)

    cases: list[SwitchCase] = []
    raw_cases = type_properties.get("cases", [])
    for raw_case in raw_cases:
        case_value = resolve_field(raw_case.get("value", ""), context)
        case_activities_raw = raw_case.get("activities", [])

        case_adf_activities = _ensure_adf_activities(case_activities_raw)

        case_translated: list[Activity] = []
        if translate_activities_fn and case_adf_activities:
            case_translated, _ = translate_activities_fn(
                case_adf_activities,
                context,
                definitions,
            )

        cases.append(SwitchCase(value=case_value, activities=case_translated))

    default_activities: list[Activity] = []
    default_raw = type_properties.get("defaultActivities", [])
    default_adf_activities = _ensure_adf_activities(default_raw)
    if translate_activities_fn and default_adf_activities:
        default_activities, _ = translate_activities_fn(
            default_adf_activities,
            context,
            definitions,
        )

    switch_activity = SwitchActivity(
        **base_kwargs,
        on_expression=on_expression,
        cases=cases,
        default_activities=default_activities,
    )

    return switch_activity, context


def _ensure_adf_activities(raw_activities: list[Any]) -> list[AdfActivity]:
    """Ensure a list of activities are AdfActivity instances.

    Args:
        raw_activities: List that may contain AdfActivity instances or raw dicts.

    Returns:
        List of AdfActivity instances.
    """
    result: list[AdfActivity] = []
    for item in raw_activities:
        if isinstance(item, AdfActivity):
            result.append(item)
        elif isinstance(item, dict):
            result.append(parse_activity(item))
    return result
