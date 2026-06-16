"""Preparer for AppendVariableActivity -> notebook_task with generated notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowx.preparer.activity_preparers.helpers import build_notebook_activity_task
from flowx.preparer.activity_preparers.naming import notebook_filename
from flowx.preparer.code_generator import generate_append_variable_notebook
from flowx.preparer.workflow_preparer import PreparedActivity

if TYPE_CHECKING:
    from flowx.models.ir import AppendVariableActivity


def prepare(
    activity: AppendVariableActivity,
    *,
    scope: str = "",
    variable_task_keys: dict[str, str] | None = None,
) -> PreparedActivity:
    """Converts an AppendVariableActivity into a notebook_task that appends to an array."""
    base_parameters: dict[str, str] = {
        "variable_name": activity.variable_name,
        "source_task_key": (variable_task_keys or {}).get(activity.variable_name, ""),
    }
    if activity.value_kind in ("literal", "dab_ref"):
        base_parameters["value"] = activity.append_value
    for widget_name, dab_ref in activity.required_parameters.items():
        base_parameters.setdefault(widget_name, dab_ref)

    task, notebooks = build_notebook_activity_task(
        activity,
        notebook_relative_path=f"notebooks/{notebook_filename(activity.task_key, activity.name)}",
        notebook_content=generate_append_variable_notebook(activity),
        base_parameters=base_parameters,
    )
    return PreparedActivity(task=task, notebooks=notebooks)
