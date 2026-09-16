"""Lower agent-authored bundle components through the existing output channels."""

from __future__ import annotations

import base64
from pathlib import PurePosixPath

from flowx.models.dab import DabNotebook
from flowx.models.ir import AgenticComponentActivity
from flowx.preparer.workflow_preparer import PreparedActivity, build_common_task_fields


def _source_relative_path(raw_path: object) -> str:
    """Return a safe path that the shared writer always treats as relative to ``src``."""
    relative_path = PurePosixPath(str(raw_path))
    if relative_path.is_absolute() or relative_path.as_posix() == "." or ".." in relative_path.parts:
        raise ValueError(f"Agentic component file path {raw_path!r} must be relative to the bundle src directory")
    # The bundle writer reserves resources/* and pyproject.toml for PyDABs root artifacts.
    # A leading ./ preserves the authored path while keeping agentic files on its src channel.
    return f"./{relative_path.as_posix()}"


def prepare(activity: AgenticComponentActivity, *, scope: str = "") -> PreparedActivity:
    """Pass authored files, resources, and task wiring to the bundle writer."""
    del scope
    notebooks = [
        DabNotebook(
            relative_path=_source_relative_path(file["path"]),
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
