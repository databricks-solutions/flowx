"""Lower agent-authored bundle components through the existing output channels."""

from __future__ import annotations

import base64

from flowx.models.dab import DabNotebook
from flowx.models.ir import AgenticComponentActivity
from flowx.preparer.workflow_preparer import PreparedActivity, build_common_task_fields


def prepare(activity: AgenticComponentActivity, *, scope: str = "") -> PreparedActivity:
    """Pass authored files, resources, and task wiring to the bundle writer."""
    del scope
    notebooks = [
        DabNotebook(
            relative_path=str(file["path"]),
            content=str(file.get("content", "")),
            binary_content=(base64.b64decode(str(file["binary_content"])) if "binary_content" in file else None),
        )
        for file in activity.files
    ]
    task = {**build_common_task_fields(activity), **activity.task, "task_key": activity.task_key}
    return PreparedActivity(
        task=task,
        notebooks=notebooks,
        pipeline_resources=list(activity.resources),
    )
