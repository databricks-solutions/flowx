"""Preparer for WebActivity -> notebook_task with generated HTTP notebook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowx.models.dab import SecretInstruction
from flowx.preparer.activity_preparers.helpers import (
    build_notebook_activity_task,
    resolve_param_value,
)
from flowx.preparer.activity_preparers.naming import notebook_filename
from flowx.preparer.code_generator import generate_web_activity_notebook
from flowx.preparer.workflow_preparer import PreparedActivity

if TYPE_CHECKING:
    from flowx.models.ir import WebActivity


def prepare(activity: WebActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a WebActivity into a notebook_task with a generated HTTP notebook."""
    task, notebooks = build_notebook_activity_task(
        activity,
        notebook_relative_path=f"notebooks/{notebook_filename(activity.task_key, activity.name)}",
        notebook_content=generate_web_activity_notebook(activity, scope=scope),
        base_parameters={
            "url": resolve_param_value(activity.url),
            "method": resolve_param_value(activity.method),
        },
    )

    secrets: list[SecretInstruction] = []
    if activity.authentication:
        auth_type = activity.authentication.get("type", "unknown")
        secrets.append(
            SecretInstruction(
                scope=scope or activity.task_key,
                key="auth-credential",
                value_source=f"Authentication credential ({auth_type}) for web activity '{activity.name}'",
            )
        )

    return PreparedActivity(task=task, notebooks=notebooks, secrets=secrets)
