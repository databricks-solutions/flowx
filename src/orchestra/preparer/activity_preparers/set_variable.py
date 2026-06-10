"""Preparer for SetVariableActivity -> notebook_task with generated notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import SetupTask
from orchestra.preparer.activity_preparers.helpers import build_notebook_activity_task
from orchestra.preparer.activity_preparers.naming import notebook_filename
from orchestra.preparer.code_generator import generate_set_variable_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity

if TYPE_CHECKING:
    from orchestra.models.ir import SetVariableActivity


def prepare(activity: SetVariableActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a SetVariableActivity into a notebook_task that sets a task value."""
    base_parameters: dict[str, str] = {"variable_name": activity.variable_name}
    if activity.value_kind in ("literal", "dab_ref"):
        base_parameters["value"] = activity.variable_value
    elif activity.value_kind == "unresolved":
        base_parameters["value"] = ""
    for widget_name, dab_ref in activity.required_parameters.items():
        base_parameters.setdefault(widget_name, dab_ref)

    task, notebooks = build_notebook_activity_task(
        activity,
        notebook_relative_path=f"notebooks/{notebook_filename(activity.task_key, activity.name)}",
        notebook_content=generate_set_variable_notebook(activity),
        base_parameters=base_parameters,
    )

    setup_tasks: list[SetupTask] = []
    # C-33 (VAREX4-001 / CF4-003): emit a manual_variable_init SetupTask so
    # SETUP.md flags the variable as needing a runtime value.
    if activity.value_kind == "unresolved" and activity.raw_expression:
        setup_tasks.append(
            SetupTask(
                type="manual_variable_init",
                config={
                    "task_key": activity.task_key,
                    "variable_name": activity.variable_name,
                    "expression": activity.raw_expression,
                },
            )
        )

    return PreparedActivity(task=task, notebooks=notebooks, setup_tasks=setup_tasks)
