"""Translates ADF DatabricksSparkPython activities to Databricks SparkPythonActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, SparkPythonActivity, TranslationContext
from orchestra.parser.expression_parser import resolve_expression, resolve_interpolated_string
from orchestra.translator.activity_translators.resolve import resolve_field


def _resolve_parameter(param: str, context: TranslationContext) -> str:
    """Resolves a single ADF parameter string to a DAB value.

    Args:
        param: A parameter string that may contain ADF expressions.
        context: Translation context for variable resolution.

    Returns:
        Resolved parameter string.
    """
    if not isinstance(param, str):
        return param

    if "@{" in param:
        return resolve_interpolated_string(param, context)

    if param.startswith("@"):
        result = resolve_expression(param, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value

    return param


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translates a DatabricksSparkPython activity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        A :class:`SparkPythonActivity` IR node.
    """
    type_properties = activity.type_properties or {}

    python_file = resolve_field(type_properties.get("pythonFile", ""), context)
    raw_parameters = type_properties.get("parameters") or []
    libraries = type_properties.get("libraries")

    parameters = [_resolve_parameter(p, context) for p in raw_parameters]

    return SparkPythonActivity(
        **base_kwargs,
        python_file=python_file,
        parameters=parameters,
        libraries=libraries,
    )
