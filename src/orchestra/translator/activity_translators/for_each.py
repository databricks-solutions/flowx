"""Translates ADF ForEach activities to Databricks ForEachActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, ForEachActivity, TranslationContext
from orchestra.parser.expression_parser import resolve_expression
from orchestra.translator.activity_translators.resolve import resolve_field_int


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
    *,
    translate_activities_fn: Any = None,
) -> tuple[Activity, TranslationContext]:
    """Translates a ForEach activity with recursive inner translation.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.
        translate_activities_fn: Callback to translate inner activities.
            Signature: ``(activities, context, definitions) -> (list[Activity], TranslationContext)``.

    Returns:
        Tuple of ``(ForEachActivity, updated_context)``.
    """
    type_properties = activity.type_properties or {}

    items_raw = type_properties.get("items")
    expr_result = resolve_expression(items_raw, context) if items_raw is not None else None
    inputs_bridge_notebook_code: str | None = None
    inputs_bridge_notebook_imports: list[str] = []
    inputs_bridge_required_parameters: dict[str, str] = {}
    if expr_result is not None and expr_result.kind in ("dab_ref", "literal"):
        items_expression = expr_result.value
    elif expr_result is not None and expr_result.kind == "notebook_code":
        # C-31 (CF4-001): the preparer used to construct a bare
        # TranslationContext() and re-resolve the items expression on the
        # JSON-reload path, but ``variable_cache`` is empty there so the
        # bridge never fired and DAB rejected the raw @split(...) call.
        # Capture the resolved notebook_code here while the full context
        # is available; the preparer reads it from these IR fields.
        if isinstance(items_raw, dict) and items_raw.get("type") == "Expression":
            items_expression = items_raw.get("value", "")
        elif isinstance(items_raw, str):
            items_expression = items_raw
        else:
            items_expression = ""
        inputs_bridge_notebook_code = expr_result.value
        inputs_bridge_notebook_imports = list(expr_result.imports)
        inputs_bridge_required_parameters = dict(expr_result.required_parameters)
    else:
        # Fallback: extract raw string
        if isinstance(items_raw, dict) and items_raw.get("type") == "Expression":
            items_expression = items_raw.get("value", "")
        elif isinstance(items_raw, str):
            items_expression = items_raw
        else:
            items_expression = ""

    is_sequential = type_properties.get("isSequential", False)
    default_batch = 1 if is_sequential else 20
    batch_count_raw = type_properties.get("batchCount")
    batch_count = (
        resolve_field_int(batch_count_raw, context, default=default_batch)
        if batch_count_raw is not None
        else default_batch
    )

    inner_activities: list[Activity] = []
    child_adf_activities = activity.activities or []

    if translate_activities_fn and child_adf_activities:
        child_context = TranslationContext(
            activity_cache=context.activity_cache,
            registry=context.registry,
            variable_cache=context.variable_cache,
            variable_value_cache=context.variable_value_cache,
            global_parameters=context.global_parameters,
            linked_service_parameters=context.linked_service_parameters,
        )
        inner_activities, _ = translate_activities_fn(child_adf_activities, child_context, definitions)

    foreach_activity = ForEachActivity(
        **base_kwargs,
        items_expression=items_expression,
        inner_activities=inner_activities,
        concurrency=batch_count,
        inputs_bridge_notebook_code=inputs_bridge_notebook_code,
        inputs_bridge_notebook_imports=inputs_bridge_notebook_imports,
        inputs_bridge_required_parameters=inputs_bridge_required_parameters,
    )

    return foreach_activity, context
