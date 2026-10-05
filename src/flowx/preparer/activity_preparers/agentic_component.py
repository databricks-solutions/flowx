"""Lower agent-authored bundle components through the existing output channels."""

from __future__ import annotations

import base64
from pathlib import PurePosixPath, PureWindowsPath

from flowx.models.dab import DabNotebook
from flowx.models.ir import AgenticComponentActivity
from flowx.preparer.workflow_preparer import PreparedActivity, build_common_task_fields

# flowx owns a task's identity, its upstream wiring, and its run policy; these come from
# the activity itself, so an authored task fragment may only say what the task runs.
FLOWX_OWNED_TASK_FIELDS = frozenset(
    {
        "task_key",
        "depends_on",
        "run_if",
        "timeout_seconds",
        "max_retries",
        "min_retry_interval_millis",
        "retry_on_timeout",
    }
)


def _source_relative_path(raw_path: object) -> str:
    """Return a safe path relative to the bundle's ``src`` directory."""
    relative_path = PurePosixPath(str(raw_path))
    windows_path = PureWindowsPath(str(raw_path))
    if (
        relative_path.is_absolute()
        or windows_path.is_absolute()
        or relative_path.as_posix() == "."
        or ".." in relative_path.parts
        or ".." in windows_path.parts
    ):
        raise ValueError(f"Agentic component file path {raw_path!r} must be relative to the bundle src directory")
    return relative_path.as_posix()


def prepare(activity: AgenticComponentActivity, *, scope: str = "") -> PreparedActivity:
    """Pass authored files, resources, and the authored task payload to the bundle writer.

    The task's key, dependencies, run condition, timeout, and retries always come from
    the activity, never from the authored fragment, so an agent cannot rewire or re-time
    a task behind flowx's back. A fragment that tries to set any of them is rejected.

    Raises:
        ValueError: The authored task fragment sets a flowx-owned field, or a file
            path escapes the bundle's ``src`` directory.
    """
    del scope
    owned_fields = sorted(FLOWX_OWNED_TASK_FIELDS & activity.task.keys())
    if owned_fields:
        raise ValueError(
            f"Agentic component {activity.task_key!r} task wiring sets flowx-owned field(s) "
            f"{', '.join(owned_fields)}; set dependencies and run policy on the activity instead"
        )
    notebooks = [
        DabNotebook(
            relative_path=_source_relative_path(file["path"]),
            content=str(file.get("content", "")),
            binary_content=(base64.b64decode(str(file["binary_content"])) if "binary_content" in file else None),
            write_to_bundle_root=False,
        )
        for file in activity.files
    ]
    owned_task_fields = build_common_task_fields(activity)
    task = {**owned_task_fields, **activity.task}
    task.update(owned_task_fields)
    return PreparedActivity(
        task=task,
        notebooks=notebooks,
        pipeline_resources=list(activity.resources),
    )
