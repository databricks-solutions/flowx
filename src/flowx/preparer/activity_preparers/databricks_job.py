"""Preparer for RunJobActivity -> run_job_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowx.models.ir import TranslationContext
from flowx.parser.expression_parser import resolve_expression, resolve_interpolated_string
from flowx.preparer.workflow_preparer import PreparedActivity, build_common_task_fields

if TYPE_CHECKING:
    from flowx.models.ir import RunJobActivity


def _resolve_param_value(value: str) -> str:
    """Resolves an ADF expression parameter value to a DAB ref.

    Args:
        value: A parameter value string that may contain ADF expressions.

    Returns:
        Resolved value string.
    """
    context = TranslationContext()

    if "@{" in value:
        return resolve_interpolated_string(value, context)

    if value.startswith("@"):
        result = resolve_expression(value, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value

    return value


def prepare(activity: RunJobActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a RunJobActivity into a DAB run_job_task definition.

    Args:
        activity: The translated run-job activity from the IR.

    Returns:
        A PreparedActivity containing the run_job_task dict.
    """
    task = build_common_task_fields(activity)
    run_job: dict = {}

    if activity.existing_job_id:
        run_job["job_id"] = activity.existing_job_id
    elif activity.job_name:
        run_job["job_id"] = f"${{resources.jobs.{activity.job_name}.id}}"

    if activity.job_parameters:
        run_job["job_parameters"] = {k: _resolve_param_value(str(v)) for k, v in activity.job_parameters.items()}

    task["run_job_task"] = run_job
    return PreparedActivity(task=task)
