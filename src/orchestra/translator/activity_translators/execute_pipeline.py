"""Translates ADF ExecutePipeline activities to Databricks ExecutePipelineActivity IR."""

from __future__ import annotations

from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDefinitions
from flowx.models.ir import Activity, ExecutePipelineActivity, TranslationContext
from flowx.translator.activity_translators.resolve import resolve_dict_values, resolve_field


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

    parameters = resolve_dict_values(type_properties.get("parameters"), context) or {}
    wait_on_completion = type_properties.get("waitOnCompletion", True)

    return ExecutePipelineActivity(
        **base_kwargs,
        pipeline_name=pipeline_name,
        parameters=parameters,
        wait_on_completion=wait_on_completion,
    )
