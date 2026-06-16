"""Translates ADF Filter activities to Databricks FilterActivity IR."""

from __future__ import annotations

import re
from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDefinitions
from flowx.models.ir import Activity, FilterActivity, TranslationContext
from flowx.parser.expression_parser import resolve_expression

# The resolver maps item().X to dbutils.widgets.get('X') (the DAB ForEach item ref). Inside a Filter
# notebook the array is iterated locally, so each widget read is rewritten to a dict lookup.
_WIDGET_ITEM_ACCESS_RE = re.compile(r"""dbutils\.widgets\.get\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)""")


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translates a Filter activity, pre-resolving the condition where safe."""
    type_properties = activity.type_properties or {}
    items_raw = type_properties.get("items", {})
    condition_raw = type_properties.get("condition", {})

    items_result = resolve_expression(items_raw, context)
    if items_result is not None:
        items_expression = items_result.value
    else:
        items_expression = items_raw.get("value", "") if isinstance(items_raw, dict) else str(items_raw)

    # Preserve the original ADF expression text in condition_expression for a doc comment; the
    # resolved form lives in condition_code.
    condition_expression = condition_raw.get("value", "") if isinstance(condition_raw, dict) else str(condition_raw)

    condition_result = resolve_expression(condition_raw, context)
    condition_code, condition_imports = _resolve_condition_code(condition_result)

    return FilterActivity(
        **base_kwargs,
        items_expression=items_expression,
        condition_expression=condition_expression,
        condition_code=condition_code,
        condition_imports=condition_imports,
    )


def _resolve_condition_code(condition_result: Any) -> tuple[str | None, list[str]]:
    """Returns (python_expression, imports) for an ADF Filter condition.

    Returns ``(None, [])`` when the condition cannot be safely lowered to
    Python -- the generator falls back to a TODO placeholder notebook.
    A condition is unsafe to lower when the resolver returned ``None``,
    when the result kind is not ``notebook_code``, or when the resolved
    text still carries unresolved DAB-syntax markers (``{{...}}``) the
    generator can't legally evaluate at notebook runtime.
    """
    if condition_result is None or condition_result.kind != "notebook_code":
        return None, []
    rewritten = _WIDGET_ITEM_ACCESS_RE.sub(r"item.get('\1')", condition_result.value)
    if "{{" in rewritten:
        return None, []
    return rewritten, list(condition_result.imports or [])
