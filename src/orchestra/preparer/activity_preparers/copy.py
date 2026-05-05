"""Preparer for CopyActivity -> notebook_task with generated copy notebook."""

from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass

from flowx.models.dab import SecretInstruction, SetupTask
from flowx.models.ir import CopyActivity
from flowx.models.source_types import FILE_SOURCE_TYPES, JDBC_SOURCE_TYPES
from flowx.preparer.activity_preparers.helpers import (
    build_notebook_activity_task,
    make_jdbc_secrets,
)
from flowx.preparer.activity_preparers.naming import notebook_filename
from flowx.preparer.code_generator import generate_copy_notebook
from flowx.preparer.workflow_preparer import PreparedActivity

_ABFSS_URL_RE = re.compile(r"abfss://([^@]+)@([^/]+)/?(.*)")
_VOLUME_NAME_SANITIZE_RE = re.compile(r"[^a-zA-Z0-9_]")


@dataclass(frozen=True, slots=True)
class _VolumeBinding:
    """Resolved UC volume info derived from a file source's resolved ABFSS URL."""

    volume_name: str
    container_location: str
    volume_base: str
    source_path: str


def _resolve_volume_binding(source_properties: dict) -> _VolumeBinding | None:
    """Derives UC volume info from a file source's resolved ABFSS URL."""
    resolved_path = source_properties.get("resolved_path", "")
    if not resolved_path:
        return None

    match = _ABFSS_URL_RE.match(resolved_path)
    if not match:
        return None

    container, storage_account, folder_path = match.group(1), match.group(2), match.group(3).rstrip("/")
    volume_name = _VOLUME_NAME_SANITIZE_RE.sub("_", container)
    volume_base = f"/Volumes/${{var.catalog}}/${{var.schema}}/{volume_name}"
    return _VolumeBinding(
        volume_name=volume_name,
        container_location=f"abfss://{container}@{storage_account}",
        volume_base=volume_base,
        source_path=f"{volume_base}/{folder_path}" if folder_path else volume_base,
    )


def _augment_with_volume_paths(activity: CopyActivity, binding: _VolumeBinding) -> CopyActivity:
    """Returns a copy of *activity* with volume paths threaded into source_properties."""
    augmented_properties = {
        **(activity.source_properties or {}),
        "volume_path": binding.source_path,
        "volume_base": binding.volume_base,
    }
    return dataclasses.replace(activity, source_properties=augmented_properties)


def prepare(activity: CopyActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a CopyActivity into a notebook_task with a generated copy notebook."""
    source_type = activity.source_type or ""
    volume_binding: _VolumeBinding | None = None
    if source_type in FILE_SOURCE_TYPES:
        volume_binding = _resolve_volume_binding(activity.source_properties or {})
        if volume_binding is not None:
            activity = _augment_with_volume_paths(activity, volume_binding)

    base_parameters: dict[str, str] = {}
    if activity.source_type:
        base_parameters["source_type"] = activity.source_type
    if activity.sink_type:
        base_parameters["sink_type"] = activity.sink_type
    source_path = (
        volume_binding.source_path
        if volume_binding is not None
        else (activity.source_properties or {}).get("resolved_path")
    )
    if source_path:
        base_parameters["source_path"] = source_path

    task, notebooks = build_notebook_activity_task(
        activity,
        notebook_relative_path=f"notebooks/{notebook_filename(activity.task_key, activity.name)}",
        notebook_content=generate_copy_notebook(activity, scope=scope),
        base_parameters=base_parameters,
    )

    scope_name = scope or activity.task_key
    secrets = _build_secrets(activity, source_type, scope_name)
    setup_tasks = _build_setup_tasks(activity, source_type, volume_binding)

    return PreparedActivity(task=task, notebooks=notebooks, secrets=secrets, setup_tasks=setup_tasks)


def _build_secrets(activity: CopyActivity, source_type: str, scope_name: str) -> list[SecretInstruction]:
    """Returns the SecretInstructions a Copy activity needs to deploy."""
    if source_type in JDBC_SOURCE_TYPES:
        return make_jdbc_secrets(
            scope_name=scope_name,
            source_type=source_type,
            activity_name=activity.name,
            role="source",
        )
    if source_type not in FILE_SOURCE_TYPES:
        return []
    source_properties = activity.source_properties or {}
    if not (source_properties.get("connection_string") or source_properties.get("sasUri")):
        return []
    return [
        SecretInstruction(
            scope=scope_name,
            key="connection-string",
            value_source=f"Connection string for {source_type} source in activity '{activity.name}'",
        )
    ]


def _build_setup_tasks(
    activity: CopyActivity,
    source_type: str,
    volume_binding: _VolumeBinding | None,
) -> list[SetupTask]:
    """Returns the SetupTasks (UC volumes etc.) a Copy activity needs."""
    setup_tasks: list[SetupTask] = []
    if source_type in FILE_SOURCE_TYPES and volume_binding is not None:
        setup_tasks.append(
            SetupTask(
                type="volume",
                config={
                    "volume_name": volume_binding.volume_name,
                    "volume_type": "EXTERNAL",
                    "location": volume_binding.container_location,
                },
            )
        )

    sink_volume = _build_sink_volume_setup_task(activity)
    if sink_volume is not None:
        setup_tasks.append(sink_volume)

    return setup_tasks


def _build_sink_volume_setup_task(activity: CopyActivity) -> SetupTask | None:
    """Returns a UC volume SetupTask for the sink side of *activity*, or None.

    Sink-side volume info is populated by the Copy translator when the
    output dataset resolves to a cloud-storage location.  The setup task
    creates the External Location and External Volume so the generated
    notebook can write into ``/Volumes/<catalog>/<schema>/<volume>/...``.
    """
    sink_properties = activity.sink_properties or {}
    volume_name = sink_properties.get("volume_name")
    external_location = sink_properties.get("volume_external_location")
    if not volume_name or not external_location:
        return None
    return SetupTask(
        type="volume",
        config={
            "volume_name": volume_name,
            "volume_type": "EXTERNAL",
            "location": external_location,
            # ``location_type`` drives the storage-credential DDL the setup
            # notebook emits (Azure managed identity vs S3 IAM vs GCS service
            # account); omitting it leaves the user a manual TODO.
            "location_type": sink_properties.get("volume_location_type", ""),
            "storage_account": sink_properties.get("volume_storage_account", ""),
        },
    )
