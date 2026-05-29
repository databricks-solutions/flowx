"""Preparer for CopyActivity -> notebook_task with generated copy notebook."""

from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass
from typing import Any

from orchestra.models.dab import SecretInstruction, SetupTask
from orchestra.models.ir import CopyActivity
from orchestra.models.source_types import FILE_SOURCE_TYPES, JDBC_SOURCE_TYPES
from orchestra.preparer.activity_preparers.helpers import (
    build_notebook_activity_task,
    make_jdbc_secrets,
)
from orchestra.preparer.activity_preparers.naming import notebook_filename
from orchestra.preparer.code_generator import generate_copy_notebook
from orchestra.preparer.workflow_preparer import PreparedActivity, build_common_task_fields

_LAKEFLOW_SOURCE_TYPE_TO_CONNECTION_TYPE: dict[str, str] = {
    "SqlServerSource": "SQLSERVER",
    "AzureSqlSource": "SQLSERVER",
    "MicrosoftSqlServerSource": "SQLSERVER",
    "MySqlSource": "MYSQL",
    "AzureMySqlSource": "MYSQL",
    "PostgreSqlSource": "POSTGRESQL",
    "AzurePostgreSqlSource": "POSTGRESQL",
}

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
    """Converts a CopyActivity into a notebook_task with a generated copy notebook.

    Args:
        activity: The Copy activity IR node, already stamped by the
            pipeline modifier with ``target_format`` and
            ``use_lakeflow_connector``.
        scope: Secret scope name (defaults to ``activity.task_key`` when empty).

    Returns:
        A :class:`PreparedActivity` whose shape depends on the modifier's
        choice: a ``pipeline_task`` plus pipeline resource and connection
        setup notebook when ``use_lakeflow_connector`` is true, otherwise
        a ``notebook_task`` with the legacy generated copy notebook.
    """
    if activity.use_lakeflow_connector:
        return _prepare_lakeflow_connect_copy(activity, scope=scope)
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


def _prepare_lakeflow_connect_copy(activity: CopyActivity, *, scope: str) -> PreparedActivity:
    """Returns a PreparedActivity that materialises a Lakeflow Connect ingestion.

    Args:
        activity: Copy activity stamped with ``use_lakeflow_connector=True``.
        scope: Secret scope name (unused for the LFC path; accepted for
            uniformity with the legacy preparer).

    Returns:
        A :class:`PreparedActivity` whose ``task`` is a ``pipeline_task``
        referencing the emitted Lakeflow Connect pipeline resource and
        whose ``setup_tasks`` create the matching Unity Catalog
        connection.  The connection is keyed by the source linked
        service name so multiple Copies sharing a source emit one
        shared connection.  ``notebooks`` is empty: Lakeflow Connect is
        driven purely by the pipeline resource and the connection setup
        script.
    """
    del scope
    resource_key = _lakeflow_pipeline_resource_key(activity.task_key)
    connection_name = _lakeflow_connection_name_for_activity(activity)
    pipeline_definition = _build_lakeflow_pipeline_definition(activity, connection_name)
    task = _build_common_pipeline_task(activity, resource_key)
    setup_tasks = [_build_lakeflow_connection_setup_task(activity, connection_name)]
    return PreparedActivity(
        task=task,
        setup_tasks=setup_tasks,
        pipeline_resources=[{"resource_key": resource_key, "definition": pipeline_definition}],
    )


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


def _lakeflow_pipeline_resource_key(task_key: str) -> str:
    """Returns the DAB resource key for a Copy activity's Lakeflow Connect pipeline.

    Args:
        task_key: Sanitised task key of the source Copy activity.

    Returns:
        A resource key suffixed with ``_lfc`` so it does not collide with
        any other pipeline or job resource emitted by the bundle.
    """
    return f"{task_key}_lfc"


def _lakeflow_connection_name_for_activity(activity: CopyActivity) -> str:
    """Returns the Unity Catalog connection name for a Lakeflow Connect Copy.

    Args:
        activity: Source Copy activity carrying source-side metadata.

    Returns:
        A connection name namespaced under ``orchestra_`` and derived
        from the source linked service name when available, so multiple
        Copies that share a source linked service emit one connection.
        Falls back to the activity task key when the IR does not record
        a linked service name.
    """
    source_properties = activity.source_properties or {}
    linked_service_name = source_properties.get("linked_service_name") or activity.task_key
    return f"orchestra_{_sanitize_identifier(linked_service_name)}_connection"


def _sanitize_identifier(value: str) -> str:
    """Coerces a value into a valid Unity Catalog identifier.

    Args:
        value: Source string (typically a linked service name).

    Returns:
        The input with non-alphanumeric characters replaced by
        underscores and adjacent underscores collapsed.
    """
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", value)
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned.strip("_") or "connection"


def _build_common_pipeline_task(activity: CopyActivity, resource_key: str) -> dict[str, Any]:
    """Builds the ``pipeline_task`` dict referencing a Lakeflow Connect pipeline.

    Args:
        activity: Copy activity whose IR carries dependency metadata.
        resource_key: Resource key the pipeline will be emitted under.

    Returns:
        A task dict with the base activity fields plus a
        ``pipeline_task`` pointing at ``${resources.pipelines.<key>.id}``.
    """
    task = build_common_task_fields(activity)
    task["pipeline_task"] = {"pipeline_id": f"${{resources.pipelines.{resource_key}.id}}"}
    return task


def _build_lakeflow_pipeline_definition(activity: CopyActivity, connection_name: str) -> dict[str, Any]:
    """Builds the Lakeflow Connect pipeline resource definition.

    Args:
        activity: Source Copy activity carrying source and sink metadata
            plus the connector type the modifier resolved.
        connection_name: Name of the Unity Catalog connection the
            pipeline will read through.

    Returns:
        A dict matching the DAB ``resources.pipelines`` schema.  Emits a
        ``query_based_connector_config`` ingestion definition for Copy
        activities marked as query-based; otherwise emits the
        table-based CDC ingestion definition.
    """
    return {
        "name": _lakeflow_pipeline_resource_key(activity.task_key),
        "catalog": "${var.catalog}",
        "target": "${var.schema}",
        "ingestion_definition": {
            "connection_name": connection_name,
            "objects": [_build_lakeflow_ingestion_object(activity)],
        },
    }


def _build_lakeflow_ingestion_object(activity: CopyActivity) -> dict[str, Any]:
    """Builds the per-object entry under ``ingestion_definition.objects``.

    Args:
        activity: Source Copy activity stamped with
            ``lakeflow_connector_type``.

    Returns:
        Either a ``table_configuration`` object that drives the
        query-based connector or a ``table`` object that drives the CDC
        connector, depending on the stamped connector type.
    """
    if activity.lakeflow_connector_type == "query_based":
        return {"table_configuration": _build_query_based_table_configuration(activity)}
    return {"table": _build_cdc_table_configuration(activity)}


def _build_cdc_table_configuration(activity: CopyActivity) -> dict[str, Any]:
    """Builds the CDC-connector ``table`` ingestion entry.

    Args:
        activity: Source Copy activity supplying table metadata.

    Returns:
        A dict suitable for direct YAML serialisation under
        ``objects[].table``.  Source catalog/schema/table values come
        from the translator-resolved ``source_properties`` keys; when
        the IR does not carry literal values, bundle variables stand in
        so the user can fill them at deploy time.
    """
    source_properties = activity.source_properties or {}
    sink_properties = activity.sink_properties or {}
    source_schema = source_properties.get("source_schema") or "${var.source_schema}"
    source_table = source_properties.get("source_table") or sink_properties.get("table") or activity.task_key
    source_catalog = (
        (
            source_properties.get("connection", {}).get("database")
            if isinstance(source_properties.get("connection"), dict)
            else None
        )
        or source_properties.get("sourceCatalog")
        or "${var.source_catalog}"
    )
    destination_table = sink_properties.get("table") or source_table
    return {
        "source_catalog": source_catalog,
        "source_schema": source_schema,
        "source_table": source_table,
        "destination_catalog": "${var.catalog}",
        "destination_schema": "${var.schema}",
        "destination_table": destination_table,
    }


def _build_query_based_table_configuration(activity: CopyActivity) -> dict[str, Any]:
    """Builds the ``table_configuration`` entry for the query-based connector.

    Args:
        activity: Source Copy activity stamped by the translator's
            query analyzer with ``query_cursor_column``,
            ``query_row_filter``, and ``query_include_columns``.

    Returns:
        A dict suitable for direct YAML serialisation under
        ``objects[].table_configuration``.  Emits the structured fields
        the Lakeflow Connect query-based connector accepts (``cursor``,
        ``row_filter``, ``include_columns``, ``exclude_columns``)
        rather than an arbitrary ``query`` string.  Source table info
        (``source_catalog``, ``source_schema``, ``source_table``) is
        carried alongside so the connector knows which physical table
        to scan.
    """
    source_properties = activity.source_properties or {}
    sink_properties = activity.sink_properties or {}
    raw_connection = source_properties.get("connection")
    connection: dict[str, Any] = raw_connection if isinstance(raw_connection, dict) else {}
    destination_table = sink_properties.get("table") or activity.task_key
    source_schema = source_properties.get("source_schema") or "${var.source_schema}"
    source_table = source_properties.get("source_table") or destination_table
    source_catalog = connection.get("database") or source_properties.get("sourceCatalog") or "${var.source_catalog}"

    config: dict[str, Any] = {}
    cursor_column = source_properties.get("query_cursor_column")
    if cursor_column:
        config["cursor"] = cursor_column
    row_filter = source_properties.get("query_row_filter")
    if row_filter:
        config["row_filter"] = row_filter
    include_columns = source_properties.get("query_include_columns")
    if include_columns:
        config["include_columns"] = list(include_columns)
    exclude_columns = source_properties.get("query_exclude_columns")
    if exclude_columns:
        config["exclude_columns"] = list(exclude_columns)

    return {
        "source_catalog": source_catalog,
        "source_schema": source_schema,
        "source_table": source_table,
        "destination_catalog": "${var.catalog}",
        "destination_schema": "${var.schema}",
        "destination_table": destination_table,
        "query_based_connector_config": config,
    }


def _build_lakeflow_connection_setup_task(activity: CopyActivity, connection_name: str) -> SetupTask:
    """Builds the Unity Catalog connection :class:`SetupTask` for a Lakeflow Copy.

    Args:
        activity: Source Copy activity supplying the connector type hint
            and any host/port the translator pulled from the linked
            service's connection string.
        connection_name: Name the setup notebook should create.

    Returns:
        A :class:`SetupTask` of type ``connection`` whose config is
        consumed by the existing connection setup notebook generator.
        Host and port come from ``source_properties.connection`` when
        the translator resolved them; otherwise placeholders force the
        user to fill in real values before running setup.
    """
    source_type = activity.source_type or ""
    connection_type = _LAKEFLOW_SOURCE_TYPE_TO_CONNECTION_TYPE.get(source_type, "SQLSERVER")
    source_properties = activity.source_properties or {}
    raw_connection = source_properties.get("connection")
    connection: dict[str, Any] = raw_connection if isinstance(raw_connection, dict) else {}
    host = connection.get("host", "PLACEHOLDER_HOST")
    port = str(connection.get("port", _default_port_for(connection_type)))
    return SetupTask(
        type="connection",
        config={
            "connection_name": connection_name,
            "connection_type": connection_type,
            "host": host,
            "port": port,
        },
    )


def _default_port_for(connection_type: str) -> str:
    """Returns a sensible default port for a Lakeflow Connect source type.

    Args:
        connection_type: One of ``SQLSERVER``, ``MYSQL``, ``POSTGRESQL``.

    Returns:
        The canonical default port for the protocol as a string, falling
        back to ``"1433"`` (SQL Server) for unknown types.
    """
    return {"SQLSERVER": "1433", "MYSQL": "3306", "POSTGRESQL": "5432"}.get(connection_type, "1433")


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
