"""Translates ADF ExecutePipeline activities to Databricks ExecutePipelineActivity IR."""

from __future__ import annotations

from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, ExecutePipelineActivity, TranslationContext
from orchestra.parser.expression_parser import resolve_expression
from orchestra.translator.activity_translators.resolve import resolve_field


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translates an ExecutePipeline activity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing pipelines.

    Returns:
        An :class:`ExecutePipelineActivity` IR node.
    """
    type_properties = activity.type_properties or {}

    pipeline_ref = type_properties.get("pipeline", {})
    pipeline_name = (
        resolve_field(pipeline_ref.get("referenceName", ""), context)
        if isinstance(pipeline_ref, dict)
        else str(pipeline_ref)
    )

    raw_parameters = type_properties.get("parameters") or {}
    parameters: dict[str, str] = {}
    approximations: list[dict[str, str]] = list(base_kwargs.get("parameter_approximations") or [])
    for name, value in raw_parameters.items():
        resolved_value = _resolve_execute_pipeline_parameter(name, value, context, approximations)
        if resolved_value is not None:
            parameters[name] = resolved_value

    wait_on_completion = type_properties.get("waitOnCompletion", True)

    if approximations:
        # Stamp the approximations onto the activity so the bundler can
        # surface them in SETUP.md.
        base_kwargs = {**base_kwargs, "parameter_approximations": approximations}

    return ExecutePipelineActivity(
        **base_kwargs,
        pipeline_name=pipeline_name,
        parameters=parameters,
        wait_on_completion=wait_on_completion,
    )


def _resolve_execute_pipeline_parameter(
    name: str,
    value: Any,
    context: TranslationContext,
    approximations: list[dict[str, str]],
) -> str | None:
    """Resolves a single ExecutePipeline parameter or drops it with a SETUP note.

    C-09 (VAREX-001): when the value resolves to ``notebook_code`` the
    parameter cannot ride through ``job_parameters`` as a literal Python
    source string -- the sub-job's widget would receive the code text.
    Drop the parameter and record an approximation so the bundler surfaces
    it in SETUP.md for the user to supply manually.
    """
    if value is None:
        return ""
    result = resolve_expression(value, context)
    if result is None:
        # Fallback to the legacy resolve_field for plain strings / dicts.
        return resolve_field(value, context)
    if result.kind in ("literal", "dab_ref"):
        return result.value
    if result.kind == "notebook_code":
        raw = value
        if isinstance(value, dict) and "value" in value:
            raw = value["value"]
        approximations.append(
            {
                "widget_name": name,
                "raw_expression": str(raw),
                "replacement": "",
                "note": (
                    "ExecutePipeline parameter dropped: the value resolves to a "
                    "notebook_code expression which cannot ride through DAB "
                    "job_parameters as a literal. Supply manually in SETUP.md or "
                    "synthesise a generator task that publishes a task value."
                ),
            }
        )
        return None
    return None
