"""Preparer for WaitActivity -> notebook_task with generated sleep notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowx.preparer.activity_preparers.helpers import build_notebook_activity_task
from flowx.preparer.activity_preparers.naming import notebook_filename
from flowx.preparer.code_generator import generate_wait_notebook
from flowx.preparer.workflow_preparer import PreparedActivity

if TYPE_CHECKING:
    from flowx.models.ir import WaitActivity


def prepare(activity: WaitActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a WaitActivity into a notebook_task that sleeps for N seconds."""
    task, notebooks = build_notebook_activity_task(
        activity,
        notebook_relative_path=f"notebooks/{notebook_filename(activity.task_key, activity.name)}",
        notebook_content=generate_wait_notebook(activity),
        base_parameters={"wait_seconds": str(activity.wait_time_seconds)},
    )
    return PreparedActivity(task=task, notebooks=notebooks)
