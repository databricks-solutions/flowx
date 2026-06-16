"""Translates ADF DatabricksJob activities to Databricks RunJobActivity IR."""

from __future__ import annotations

from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDefinitions
from flowx.models.ir import Activity, RunJobActivity, TranslationContext
from flowx.translator.activity_translators.resolve import resolve_dict_values, resolve_field


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translates a DatabricksJob activity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        A :class:`RunJobActivity` IR node.
    """
    type_properties = activity.type_properties or {}

    job_name_raw = type_properties.get("jobName") or type_properties.get("jobId")
    job_name = resolve_field(job_name_raw, context) if job_name_raw else activity.name
    existing_job_id = type_properties.get("jobId")
    job_parameters = (
        resolve_dict_values(type_properties.get("jobParameters") or type_properties.get("baseParameters"), context)
        or None
    )

    return RunJobActivity(
        **base_kwargs,
        job_name=job_name,
        existing_job_id=str(existing_job_id) if existing_job_id else None,
        job_parameters=job_parameters,
    )
