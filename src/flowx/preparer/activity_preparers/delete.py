"""Preparer for DeleteActivity -> notebook_task with generated delete notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowx.preparer.activity_preparers.helpers import (
    build_notebook_activity_task,
    resolve_param_value,
)
from flowx.preparer.activity_preparers.naming import notebook_filename
from flowx.preparer.code_generator import generate_delete_notebook
from flowx.preparer.workflow_preparer import PreparedActivity

if TYPE_CHECKING:
    from flowx.models.ir import DeleteActivity


def prepare(activity: DeleteActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a DeleteActivity into a notebook_task with a generated delete notebook."""
    base_parameters = {
        "dataset_name": resolve_param_value(activity.dataset_name),
        "recursive": str(activity.recursive).lower(),
    }
    if activity.folder_path:
        base_parameters["folder_path"] = resolve_param_value(activity.folder_path)

    task, notebooks = build_notebook_activity_task(
        activity,
        notebook_relative_path=f"notebooks/{notebook_filename(activity.task_key, activity.name)}",
        notebook_content=generate_delete_notebook(activity),
        base_parameters=base_parameters,
    )
    return PreparedActivity(task=task, notebooks=notebooks)
