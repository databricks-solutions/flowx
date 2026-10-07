"""Translates ADF AppendVariable activities to Databricks AppendVariableActivity IR."""

from __future__ import annotations

from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDefinitions
from flowx.models.ir import Activity, AppendVariableActivity, TranslationContext
from flowx.parser.expression_parser import resolve_expression


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> tuple[Activity, TranslationContext]:
    """Translates an AppendVariable activity and register the variable in context.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        Tuple of ``(AppendVariableActivity, updated_context)`` where the context
        now maps the variable name to this activity's task key.
    """
    type_properties = activity.type_properties or {}

    variable_name = type_properties.get("variableName", "")
    value_raw = type_properties.get("value", "")

    expr_result = resolve_expression(value_raw, context)

    required_parameters: dict[str, str] = {}
    if expr_result is not None:
        append_value = expr_result.value
        value_kind = expr_result.kind
        notebook_code = expr_result.value if expr_result.kind == "notebook_code" else None
        notebook_imports = list(expr_result.imports) if expr_result.kind == "notebook_code" else []
        required_parameters = dict(expr_result.required_parameters)
    else:
        # Fall back to raw string representation
        append_value = value_raw if isinstance(value_raw, str) else str(value_raw)
        value_kind = "literal"
        notebook_code = None
        notebook_imports = []

    append_var_activity = AppendVariableActivity(
        **base_kwargs,
        variable_name=variable_name,
        append_value=append_value,
        value_kind=value_kind,
        notebook_code=notebook_code,
        notebook_imports=notebook_imports,
        required_parameters=required_parameters,
    )

    new_context = context.with_variable(variable_name, base_kwargs["task_key"])

    return append_var_activity, new_context
