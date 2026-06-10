"""Translates ADF Switch activities to Databricks SwitchActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, SwitchActivity, SwitchCase, TranslationContext
from orchestra.parser.adf_loader import parse_activity
from orchestra.parser.expression_parser import resolve_interpolated_string
from orchestra.translator.activity_translators.resolve import (
    BridgeRequest,
    lower_to_bridge,
    resolve_field,
)

_BRIDGE_PLACEHOLDER = "__BRIDGE__::result"


def _resolve_on_expression(on_expression: str, context: TranslationContext) -> tuple[str, BridgeRequest | None]:
    """Resolves the ``on`` expression to a DAB dynamic value ref or a bridge request.

    C-07 (CF-iter2-001 / CF-iter2-003): when the expression involves an
    ADF function call (e.g. ``@toUpper(coalesce(...))``), lower it to a
    bridge SetVariable task instead of shipping the raw ADF string into
    the condition_task operand.

    Args:
        on_expression: Raw ADF on-expression string.
        context: Translation context for resolving variables.

    Returns:
        Tuple of ``(resolved_value_or_placeholder, bridge_request_or_None)``.
    """
    if "@{" in on_expression:
        return resolve_interpolated_string(on_expression, context), None

    if on_expression.startswith("@"):
        operand, bridge = lower_to_bridge(on_expression, context)
        if operand is not None:
            return operand, None
        if bridge is not None:
            return _BRIDGE_PLACEHOLDER, bridge
        # Resolution failed entirely -- preserve the raw string so the
        # preparer can flag it via SETUP.md.
        return on_expression, None

    return on_expression, None


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

    on_expression, bridge = _resolve_on_expression(on_expression_raw, context)

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

    bridge_kwargs: dict[str, Any] = {}
    if bridge is not None:
        bridge_kwargs = {
            "bridge_notebook_code": bridge.notebook_code,
            "bridge_notebook_imports": list(bridge.notebook_imports),
            "bridge_required_parameters": dict(bridge.required_parameters),
        }

    switch_activity = SwitchActivity(
        **base_kwargs,
        on_expression=on_expression,
        cases=cases,
        default_activities=default_activities,
        **bridge_kwargs,
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
