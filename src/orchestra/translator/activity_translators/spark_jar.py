"""Translates ADF DatabricksSparkJar activities to Databricks SparkJarActivity IR."""

from __future__ import annotations

from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDefinitions
from flowx.models.ir import Activity, SparkJarActivity, TranslationContext
from flowx.parser.expression_parser import resolve_expression, resolve_interpolated_string
from flowx.translator.activity_translators.resolve import resolve_field


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translates a DatabricksSparkJar activity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        A :class:`SparkJarActivity` IR node.
    """
    type_properties = activity.type_properties or {}

    main_class_name = resolve_field(type_properties.get("mainClassName", ""), context)
    raw_parameters = type_properties.get("parameters") or []
    libraries = type_properties.get("libraries") or []

    parameters = [_resolve_parameter(param, context) for param in raw_parameters]

    return SparkJarActivity(
        **base_kwargs,
        main_class_name=main_class_name,
        parameters=parameters,
        libraries=libraries,
    )


def _resolve_parameter(param: Any, context: TranslationContext) -> str:
    """Resolves a single ADF parameter to a DAB value string.

    Args:
        param: A parameter string or ``{"type": "Expression", "value": "..."}`` dict.
        context: Translation context for variable resolution.

    Returns:
        Resolved parameter string.
    """
    if isinstance(param, dict) and param.get("type") == "Expression":
        return _resolve_parameter(param["value"], context)

    if not isinstance(param, str):
        return str(param)

    if "@{" in param:
        return resolve_interpolated_string(param, context)

    if param.startswith("@"):
        result = resolve_expression(param, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value

    return param
