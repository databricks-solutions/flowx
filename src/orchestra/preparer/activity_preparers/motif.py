"""Preparer for MotifActivity -> notebook_task with motif scaffold notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowx.preparer.activity_preparers.helpers import build_notebook_activity_task
from flowx.preparer.code_generator import generate_motif_notebook
from flowx.preparer.workflow_preparer import PreparedActivity

if TYPE_CHECKING:
    from flowx.models.ir import MotifActivity


def prepare(activity: MotifActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a MotifActivity into a notebook_task with the motif scaffold."""
    task, notebooks = build_notebook_activity_task(
        activity,
        notebook_relative_path=f"notebooks/{activity.task_key}.py",
        notebook_content=generate_motif_notebook(activity),
    )
    return PreparedActivity(task=task, notebooks=notebooks)
