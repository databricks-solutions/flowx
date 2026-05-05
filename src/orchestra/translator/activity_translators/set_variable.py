"""Translates ADF SetVariable activities to Databricks SetVariableActivity IR."""

from __future__ import annotations

from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDefinitions
from flowx.models.ir import Activity, SetVariableActivity, TranslationContext
from flowx.parser.expression_parser import resolve_expression


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> tuple[Activity, TranslationContext]:
    """Translates a SetVariable activity and register the variable in context.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        Tuple of ``(SetVariableActivity, updated_context)`` where the context
        now maps the variable name to this activity's task key.
    """
    type_properties = activity.type_properties or {}

    variable_name = type_properties.get("variableName", "")
    value_raw = type_properties.get("value", "")

    expr_result = resolve_expression(value_raw, context)

    required_parameters: dict[str, str] = {}
    if expr_result is not None:
        variable_value = expr_result.value
        value_kind = expr_result.kind
        notebook_code = expr_result.value if expr_result.kind == "notebook_code" else None
        notebook_imports = list(expr_result.imports) if expr_result.kind == "notebook_code" else []
        required_parameters = dict(expr_result.required_parameters)
    else:
        # Fallback: unwrap expression-type dicts to at least preserve the string
        if isinstance(value_raw, dict) and value_raw.get("type") == "Expression":
            variable_value = value_raw.get("value", "")
        elif isinstance(value_raw, str):
            variable_value = value_raw
        else:
            variable_value = str(value_raw)
        value_kind = "literal"
        notebook_code = None
        notebook_imports = []

    set_var_activity = SetVariableActivity(
        **base_kwargs,
        variable_name=variable_name,
        variable_value=variable_value,
        value_kind=value_kind,
        notebook_code=notebook_code,
        notebook_imports=notebook_imports,
        required_parameters=required_parameters,
    )

    # Register variable -> task_key mapping in context.
    # When the value is a DAB ref (e.g. {{job.start_time.iso_datetime}} from
    # @utcNow()), store it so downstream @variables() calls can inline it
    # instead of routing through the task value.
    dab_ref_value = variable_value if value_kind == "dab_ref" else None
    new_context = context.with_variable(
        variable_name,
        base_kwargs["task_key"],
        dab_ref_value=dab_ref_value,
    )

    return set_var_activity, new_context
