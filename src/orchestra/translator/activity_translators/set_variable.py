"""Translates ADF SetVariable activities to Databricks SetVariableActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, SetVariableActivity, TranslationContext
from orchestra.parser.expression_parser import resolve_expression


def _unwrap_return_value_pairs(value: Any) -> Any:
    """Unwrap a Set Pipeline Return Value list-of-pairs to a resolvable value.

    A ``pipelineReturnValue`` value is shaped as a list of
    ``{'key': ..., 'value': <literal | {'type': 'Expression', 'content': ...}>}``
    entries.  ADF's expression dicts here use the ``content`` key (not
    ``value``).  We normalise a single pair's inner value into the
    ``{'type': 'Expression', 'value': ...}`` shape (or a bare literal) that
    :func:`resolve_expression` understands, so the inner ``@variables('X')``
    reference is preserved instead of stringifying the whole list.

    When the list is empty or carries more than one pair (no single
    canonical result), the original value is returned unchanged so the
    legacy unresolved/blanking path still applies.
    """
    if not isinstance(value, list) or len(value) != 1:
        return value
    entry = value[0]
    if not isinstance(entry, dict) or "value" not in entry:
        return value
    inner = entry["value"]
    if isinstance(inner, dict) and inner.get("type") == "Expression" and "content" in inner:
        return {"type": "Expression", "value": inner["content"]}
    if isinstance(inner, dict) and inner.get("type") == "Expression" and "value" in inner:
        return inner
    return inner


def _raw_expression_text(value: Any) -> str:
    """Returns the original ADF expression text for *value*."""
    if isinstance(value, dict) and value.get("type") == "Expression" and "value" in value:
        return str(value["value"])
    return "" if value is None else str(value)


def _is_adf_expression(value: Any) -> bool:
    """Returns True when *value* is an ADF expression we can't pass through verbatim."""
    if isinstance(value, dict) and value.get("type") == "Expression" and "value" in value:
        inner = value["value"]
        return isinstance(inner, str) and inner.startswith("@")
    if isinstance(value, str):
        return value.startswith("@")
    return False


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

    # C-42 (VAREX5-001): a Set Pipeline Return Value activity carries a
    # list of {key, value} pairs (e.g.
    # [{'key': 'result', 'value': {'type': 'Expression',
    #   'content': "@variables('executionOutputs')"}}]).  The legacy path
    # fails _is_adf_expression and stringifies the whole list, which the
    # bundler then blanks.  The inner expression is resolvable, so unwrap a
    # single pair's value and route it through the normal resolution
    # pipeline instead of losing the reference.
    value_raw = _unwrap_return_value_pairs(value_raw)

    expr_result = resolve_expression(value_raw, context)

    required_parameters: dict[str, str] = {}
    raw_expression_text = _raw_expression_text(value_raw)
    if expr_result is not None:
        variable_value = expr_result.value
        value_kind = expr_result.kind
        notebook_code = expr_result.value if expr_result.kind == "notebook_code" else None
        notebook_imports = list(expr_result.imports) if expr_result.kind == "notebook_code" else []
        required_parameters = dict(expr_result.required_parameters)
    elif _is_adf_expression(value_raw):
        # C-33 (VAREX4-001 / CF4-003): when the value is an ADF expression
        # the resolver couldn't handle (e.g. a nested function call we
        # don't model), do NOT stamp value_kind='literal' with the raw
        # @concat text — that ships uninterpretable Python source through
        # SETUP.md.  Blank the value and mark it unresolved so the bundler
        # emits a manual_variable_init SetupTask the user can act on.
        variable_value = ""
        value_kind = "unresolved"
        notebook_code = None
        notebook_imports = []
    else:
        # Fallback: unwrap expression-type dicts to at least preserve the string
        if isinstance(value_raw, dict) and value_raw.get("type") == "Expression":
            variable_value = value_raw.get("value", "")
        elif isinstance(value_raw, str):
            variable_value = value_raw
        elif isinstance(value_raw, bool):
            # VAREX3-002: render Python bool as lowercase 'true'/'false' so
            # downstream ADF comparisons like @equals(variables('X'), true)
            # match consistently.  ``str(True)`` would emit 'True' and silently
            # invert the comparison.
            variable_value = "true" if value_raw else "false"
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
        raw_expression=raw_expression_text if value_kind == "unresolved" else None,
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
