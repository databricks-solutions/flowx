"""Preparer for MotifActivity -> notebook_task or consolidated pipeline_task."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orchestra.models.dab import SetupTask
from orchestra.preparer.activity_preparers.helpers import build_notebook_activity_task
from orchestra.preparer.code_generator import generate_motif_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import MotifActivity


def prepare(activity: MotifActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a MotifActivity into a DAB task.

    Args:
        activity: The motif activity stamped by the pipeline modifier.
        scope: Secret scope name (defaults to the activity task_key).

    Returns:
        A :class:`PreparedActivity` whose shape depends on the motif:
        metadata-driven motifs marked for consolidation emit a
        consolidated Lakeflow Connect pipeline resource and a
        ``pipeline_task``; every other motif keeps the legacy scaffold
        notebook task.
    """
    if activity.consolidate_metadata_driven and activity.lookup_values:
        return _prepare_consolidated_metadata_driven(activity)
    task, notebooks = build_notebook_activity_task(
        activity,
        notebook_relative_path=f"notebooks/{activity.task_key}.py",
        notebook_content=generate_motif_notebook(activity),
    )
    return PreparedActivity(task=task, notebooks=notebooks)


def _prepare_consolidated_metadata_driven(activity: MotifActivity) -> PreparedActivity:
    """Returns a PreparedActivity that materialises a consolidated ingestion pipeline.

    Args:
        activity: Motif activity carrying ``lookup_values`` and the
            ``consolidate_metadata_driven`` flag.

    Returns:
        A :class:`PreparedActivity` whose task is a ``pipeline_task``
        referencing a single Lakeflow Connect pipeline resource.  The
        pipeline's ``objects`` list contains one entry per lookup row.
    """
    resource_key = _consolidated_resource_key(activity.task_key)
    connection_name = _consolidated_connection_name(activity.task_key)
    pipeline_definition = _build_consolidated_pipeline_definition(activity, connection_name, resource_key)
    task = build_common_task_fields(activity)
    task["pipeline_task"] = {"pipeline_id": f"${{resources.pipelines.{resource_key}.id}}"}
    setup_tasks = [
        SetupTask(
            type="connection",
            config={
                "connection_name": connection_name,
                "connection_type": "SQLSERVER",
                "host": "PLACEHOLDER_HOST",
                "port": "1433",
            },
        )
    ]
    return PreparedActivity(
        task=task,
        setup_tasks=setup_tasks,
        pipeline_resources=[{"resource_key": resource_key, "definition": pipeline_definition}],
    )


def _consolidated_resource_key(task_key: str) -> str:
    """Returns the DAB resource key for a consolidated metadata-driven pipeline.

    Args:
        task_key: Sanitised task key of the source motif activity.

    Returns:
        A resource key suffixed with ``_consolidated`` so it does not
        collide with other pipeline resources in the bundle.
    """
    return f"{task_key}_consolidated"


def _consolidated_connection_name(task_key: str) -> str:
    """Returns the Unity Catalog connection name for a consolidated pipeline.

    Args:
        task_key: Sanitised task key of the source motif activity.

    Returns:
        A connection name namespaced under ``orchestra_`` so the setup
        notebook can recreate it idempotently.
    """
    return f"orchestra_{task_key}_connection"


def _build_consolidated_pipeline_definition(
    activity: MotifActivity,
    connection_name: str,
    resource_key: str,
) -> dict[str, Any]:
    """Builds the consolidated Lakeflow Connect pipeline definition.

    Args:
        activity: Motif activity carrying the lookup rows.
        connection_name: Name of the Unity Catalog connection the
            pipeline will read through.
        resource_key: Resource key the pipeline will be emitted under.

    Returns:
        A dict matching the DAB ``resources.pipelines`` schema with one
        ingestion object per row in ``activity.lookup_values``.
    """
    return {
        "name": resource_key,
        "catalog": "${var.catalog}",
        "target": "${var.schema}",
        "ingestion_definition": {
            "connection_name": connection_name,
            "objects": [_build_object_from_lookup_row(row) for row in activity.lookup_values],
        },
    }


def _build_object_from_lookup_row(row: dict[str, Any]) -> dict[str, Any]:
    """Builds a single ``objects[]`` entry from one lookup row.

    Args:
        row: Dict with optional ``source_catalog``, ``source_schema``,
            ``source_table``, and ``destination_table`` keys.  Common
            ADF aliases (``schema_name``, ``table_name``) are also
            accepted.

    Returns:
        A dict suitable for direct YAML serialisation under
        ``objects[].table``.  Bundle variables back-fill any field the
        lookup row did not supply.
    """
    source_table = row.get("source_table") or row.get("table_name") or row.get("table") or "${var.source_table}"
    return {
        "table": {
            "source_catalog": row.get("source_catalog") or row.get("catalog_name") or "${var.source_catalog}",
            "source_schema": row.get("source_schema") or row.get("schema_name") or "${var.source_schema}",
            "source_table": source_table,
            "destination_catalog": "${var.catalog}",
            "destination_schema": "${var.schema}",
            "destination_table": row.get("destination_table") or source_table,
        }
    }
