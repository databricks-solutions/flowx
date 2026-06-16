"""Preparer for ExecutePipelineActivity -> run_job_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowx.models.ir import TranslationContext
from flowx.parser.expression_parser import resolve_expression, resolve_interpolated_string
from flowx.preparer.workflow_preparer import PreparedActivity, build_common_task_fields
from flowx.utils import normalize_task_key

if TYPE_CHECKING:
    from flowx.models.ir import ExecutePipelineActivity


def _resolve_param_value(value: str) -> str:
    """Resolves an ADF expression parameter value to a DAB ref.

    Args:
        value: A parameter value string that may contain ADF expressions.

    Returns:
        Resolved value string.
    """
    s = str(value)
    context = TranslationContext()

    if "@{" in s:
        return resolve_interpolated_string(s, context)

    if s.startswith("@"):
        result = resolve_expression(s, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value

    return s


def prepare(activity: ExecutePipelineActivity, *, scope: str = "") -> PreparedActivity:
    """Converts an ExecutePipelineActivity into a DAB run_job_task definition.

    Args:
        activity: The translated execute-pipeline activity from the IR.

    Returns:
        A PreparedActivity containing the run_job_task dict.
    """
    task = build_common_task_fields(activity)
    resource_key = normalize_task_key(activity.pipeline_name)
    task["run_job_task"] = {
        "job_id": f"${{resources.jobs.{resource_key}.id}}",
    }
    if activity.parameters:
        task["run_job_task"]["job_parameters"] = {k: _resolve_param_value(v) for k, v in activity.parameters.items()}
    return PreparedActivity(task=task)
