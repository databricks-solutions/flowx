"""Translates ADF DatabricksNotebook activities to Databricks NotebookActivity IR."""

from __future__ import annotations

from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDefinitions
from flowx.models.ir import Activity, NotebookActivity, TranslationContext
from flowx.parser.expression_parser import resolve_expression
from flowx.translator.activity_translators.resolve import resolve_field


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translates a DatabricksNotebook activity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        A :class:`NotebookActivity` IR node.
    """
    type_properties = activity.type_properties or {}

    notebook_path = resolve_field(type_properties.get("notebookPath", ""), context)
    raw_params = type_properties.get("baseParameters") or {}
    libraries = type_properties.get("libraries")

    # Resolve base_parameters at translate time so ADF expressions like
    # @variables('runTimestamp') are inlined to DAB refs while the full
    # translation context (with variable_value_cache) is available.  Any
    # caveat notes the resolver emits (e.g. utcnow() approximations) are
    # captured into parameter_approximations so the bundler can surface
    # them in SETUP.md.
    resolved_params: dict[str, Any] = {}
    approximations: list[dict[str, str]] = []
    for key, value in raw_params.items():
        result = resolve_expression(value, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            resolved_params[key] = result.value
            for note in result.notes:
                approximations.append(
                    {
                        "widget_name": key,
                        "raw_expression": _raw_expression_text(value),
                        "replacement": result.value,
                        "note": note,
                    }
                )
        else:
            # Keep original for downstream handling (notebook_code or unresolvable)
            resolved_params[key] = value

    return NotebookActivity(
        **base_kwargs,
        notebook_path=notebook_path,
        base_parameters=resolved_params,
        libraries=libraries,
        parameter_approximations=approximations,
    )


def _raw_expression_text(value: Any) -> str:
    """Returns the original ADF expression text from a base_parameter value."""
    if isinstance(value, dict) and value.get("type") == "Expression" and "value" in value:
        return str(value["value"])
    return str(value)
