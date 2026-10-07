"""Lower agent-authored bundle components through the existing output channels."""

from __future__ import annotations

import base64
from pathlib import PurePosixPath, PureWindowsPath

from flowx.models.dab import DabNotebook
from flowx.models.ir import AgenticComponentActivity
from flowx.preparer.workflow_preparer import PreparedActivity, build_common_task_fields
from flowx.utils import normalize_task_key

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
        # A drive or root without the other ("C:x.py", "\\tmp\\x.py") is not absolute on Windows but
        # still resolves outside the bundle once joined to the output directory.
        or windows_path.drive
        or windows_path.root
        or relative_path.as_posix() == "."
        or ".." in relative_path.parts
        or ".." in windows_path.parts
    ):
        raise ValueError(f"Agentic component file path {raw_path!r} must be relative to the bundle src directory")
    return relative_path.as_posix()


def _check_task_payload(task_key: str, task: dict[str, object]) -> None:
    """Require exactly one executable task payload (``notebook_task``, ``pipeline_task``, ...).

    Databricks names every task type ``<kind>_task``. A fragment with none packages fine but is
    rejected at deploy time, and one with two leaves which runs undefined, so both fail here.
    """
    payloads = sorted(key for key in task if key.endswith("_task"))
    if len(payloads) != 1:
        found = ", ".join(payloads) if payloads else "none"
        raise ValueError(
            f"Agentic component {task_key!r} task must contain exactly one executable payload such as "
            f"pipeline_task or notebook_task (found: {found})"
        )


def _check_resource_key(task_key: str, resource_key: object) -> None:
    """Reject a resource key that is not a plain identifier.

    Each resource is written to ``resources/<resource_key>.yml``, so a key with path characters
    could land outside the ``resources`` directory or overwrite another bundle file.
    """
    if not isinstance(resource_key, str) or not resource_key or resource_key != normalize_task_key(resource_key):
        raise ValueError(
            f"Agentic component {task_key!r} resource key {resource_key!r} must be a plain identifier "
            "of lowercase letters, digits, and single underscores"
        )


def prepare(activity: AgenticComponentActivity, *, scope: str = "") -> PreparedActivity:
    """Pass authored files, resources, and the authored task payload to the bundle writer.

    The task's key, dependencies, run condition, timeout, and retries always come from
    the activity, never from the authored fragment, so an agent cannot rewire or re-time
    a task behind flowx's back. A fragment that tries to set any of them is rejected.

    Raises:
        ValueError: The authored task fragment sets a flowx-owned field, a file path
            escapes the bundle's ``src`` directory, a resource key is not a plain
            identifier, a binary file is not valid base64, or the task does not carry exactly
            one executable payload.
    """
    del scope
    owned_fields = sorted(FLOWX_OWNED_TASK_FIELDS & activity.task.keys())
    if owned_fields:
        raise ValueError(
            f"Agentic component {activity.task_key!r} task wiring sets flowx-owned field(s) "
            f"{', '.join(owned_fields)}; set dependencies and run policy on the activity instead"
        )
    _check_task_payload(activity.task_key, activity.task)
    for resource in activity.resources:
        _check_resource_key(activity.task_key, resource.get("resource_key"))
    notebooks = [
        DabNotebook(
            relative_path=_source_relative_path(file["path"]),
            content=str(file.get("content", "")),
            binary_content=(
                base64.b64decode(str(file["binary_content"]), validate=True) if "binary_content" in file else None
            ),
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
