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
    if not isinstance(task[payloads[0]], dict):
        raise ValueError(f"Agentic component {task_key!r} task payload {payloads[0]} must be a mapping")


def _check_resource(task_key: str, resource: object) -> None:
    """Reject a resource that is not a mapping of a plain-identifier key to a definition mapping.

    Each resource is written to ``resources/<resource_key>.yml``, so a key with path characters
    could land outside the ``resources`` directory or overwrite another bundle file. The definition
    becomes the body of ``resources.pipelines.<resource_key>``, which the bundle expects to be a mapping.
    """
    if not isinstance(resource, dict):
        raise ValueError(
            f"Agentic component {task_key!r} resource {resource!r} must be a mapping with resource_key and definition"
        )
    resource_key = resource.get("resource_key")
    if not isinstance(resource_key, str) or not resource_key or resource_key != normalize_task_key(resource_key):
        raise ValueError(
            f"Agentic component {task_key!r} resource key {resource_key!r} must be a plain identifier "
            "of lowercase letters, digits, and single underscores"
        )
    if not isinstance(resource.get("definition"), dict):
        raise ValueError(f"Agentic component {task_key!r} resource {resource_key!r} definition must be a mapping")


def _authored_file(task_key: str, file: object) -> DabNotebook:
    """Turn one authored ``files`` entry into a file the bundle writer keeps below ``src``.

    A file carries text ``content`` or base64 ``binary_content``, never both, and each must
    already be a string so the file is written exactly as authored.
    """
    if not isinstance(file, dict) or not isinstance(file.get("path"), str):
        raise ValueError(f"Agentic component {task_key!r} file {file!r} must be a mapping with a string path")
    relative_path = _source_relative_path(file["path"])
    if "content" in file and "binary_content" in file:
        raise ValueError(
            f"Agentic component {task_key!r} file {relative_path!r} must set content or binary_content, not both"
        )
    if "binary_content" in file:
        encoded = file["binary_content"]
        if not isinstance(encoded, str):
            raise ValueError(
                f"Agentic component {task_key!r} file {relative_path!r} binary_content must be a base64 string"
            )
        return DabNotebook(
            relative_path=relative_path, binary_content=base64.b64decode(encoded, validate=True), authored=True
        )
    content = file.get("content", "")
    if not isinstance(content, str):
        raise ValueError(f"Agentic component {task_key!r} file {relative_path!r} content must be a string")
    return DabNotebook(relative_path=relative_path, content=content, authored=True)


def prepare(activity: AgenticComponentActivity, *, scope: str = "") -> PreparedActivity:
    """Pass authored files, resources, and the authored task payload to the bundle writer.

    The task's key, dependencies, run condition, timeout, and retries always come from
    the activity, never from the authored fragment, so an agent cannot rewire or re-time
    a task behind flowx's back. A fragment that tries to set any of them is rejected.
    flowx never overrides or removes any other value the fragment sets. Where the fragment
    leaves one out, flowx adds only the plumbing the task needs: the ForEach ``item``
    parameter, the source activity's notifications, or a cluster for a task that names no
    compute. The task is marked ``_authored`` so the bundle writer leaves its parameters as
    given; the marker is stripped before the job YAML is written.

    Raises:
        ValueError: The authored task fragment sets a flowx-owned field, a file entry is
            malformed or its path escapes the bundle's ``src`` directory, a resource is
            malformed or its key is not a plain identifier, a binary file is not valid
            base64, or the task does not carry exactly one executable payload mapping.
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
        _check_resource(activity.task_key, resource)
    notebooks = [_authored_file(activity.task_key, file) for file in activity.files]
    owned_task_fields = {
        field: value for field, value in build_common_task_fields(activity).items() if field in FLOWX_OWNED_TASK_FIELDS
    }
    return PreparedActivity(
        task={**owned_task_fields, **activity.task, "_authored": True},
        notebooks=notebooks,
        pipeline_resources=list(activity.resources),
    )
