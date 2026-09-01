"""Translates ADF Copy activities to Databricks CopyActivity IR."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDefinitions
from flowx.models.ir import Activity, CopyActivity, TranslationContext
from flowx.parser.dataset_resolvers import (
    dataset_props,
    resolve_dataset_linked_service_name,
    resolve_dataset_path,
    resolve_param_value,
    resolve_storage_account,
    resolve_table_reference,
)
from flowx.sources.adf.query_analysis import analyze_copy_query, dialect_for_source_type

_DATASET_TYPE_TO_SPARK_FORMAT: dict[str, str] = {
    "DelimitedText": "csv",
    "Parquet": "parquet",
    "Json": "json",
    "Avro": "avro",
    "Orc": "orc",
    "Binary": "binaryFile",
    "DeltaLakeDataset": "delta",
}

# Maps ADF dataset location types to (uri-scheme, host-template) pairs for the external-volume URL;
# {account} -> storage account (or ${var.storage_account}) and {bucket} -> bucket name for AWS/GCS.
_LOCATION_URL_TEMPLATE: dict[str, str] = {
    "AzureBlobFSLocation": "abfss://{container}@{account}.dfs.core.windows.net/",
    "AzureBlobStorageLocation": "abfss://{container}@{account}.dfs.core.windows.net/",
    "AmazonS3Location": "s3://{bucket}/",
    "GoogleCloudStorageLocation": "gs://{bucket}/",
}

# Database connection-string parsers: pull host/port/database from an ADF linked service's
# connectionString. Tolerant of casing/whitespace (ADF accepts Server= and server=).
_AZURE_SQL_SERVER_RE = re.compile(r"\bServer=(?:tcp:)?([^,;]+?)(?:,(\d+))?(?:;|$)", re.IGNORECASE)
_AZURE_SQL_DATABASE_RE = re.compile(r"\b(?:Initial Catalog|Database)=([^;]+)", re.IGNORECASE)
_MYSQL_SERVER_RE = re.compile(r"\b(?:Server|Host)=([^;]+)", re.IGNORECASE)
_MYSQL_PORT_RE = re.compile(r"\bPort=(\d+)", re.IGNORECASE)
_POSTGRES_SERVER_RE = re.compile(r"\b(?:Server|Host)=([^;]+)", re.IGNORECASE)
_POSTGRES_PORT_RE = re.compile(r"\bPort=(\d+)", re.IGNORECASE)

_DATABASE_DEFAULT_PORTS: dict[str, int] = {
    "AzureSqlDatabase": 1433,
    "AzureSqlMI": 1433,
    "SqlServer": 1433,
    "AzureMySql": 3306,
    "MySql": 3306,
    "AzurePostgreSql": 5432,
    "PostgreSql": 5432,
    "Oracle": 1521,
}


@dataclass(slots=True)
class SinkPathInfo:
    """Resolved sink dataset location for an external UC volume.

    Attributes:
        location_type: ADF location type (``AzureBlobStorageLocation``, ...).
        container: Storage container or bucket name (e.g. ``exports``).
        folder: Folder path inside the container (already expression-resolved).
        filename: File name at the leaf, may be empty for "folder of files".
        storage_account: Account name when the linked service exposed it,
            otherwise ``None`` so the bundler emits a ``${var.storage_account}``
            placeholder.
        external_location_url: ``abfss://...`` / ``s3://...`` / ``gs://...``
            URL for the external location and volume.
        volume_name: Sanitised UC volume name (typically the container name).
        volume_relative_path: Path inside the volume root, ready to append
            to ``/Volumes/<catalog>/<schema>/<volume_name>/``.
        uc_volume_path: Full ``/Volumes/${var.catalog}/${var.schema}/...``
            path the generated notebook should write to.
    """

    location_type: str
    container: str
    folder: str
    filename: str
    storage_account: str | None
    external_location_url: str
    volume_name: str
    volume_relative_path: str
    uc_volume_path: str


def _sanitize_volume_name(value: str) -> str:
    """Sanitises an ADF container name for use as a UC volume name."""
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", value or "default_volume").strip("_")
    return cleaned or "default_volume"


def _resolve_path_info(
    dataset_ref: Any,
    dataset_props: dict[str, Any],
    definitions: AdfDefinitions,
    context: TranslationContext,
) -> SinkPathInfo | None:
    """Resolves a (possibly parameterised) file-on-cloud-storage dataset."""
    type_props = dataset_props.get("typeProperties") or dataset_props
    location = type_props.get("location") or {}
    location_type = location.get("type")
    if not location_type or location_type not in _LOCATION_URL_TEMPLATE:
        return None

    declared = dataset_props.get("parameters") or {}
    effective: dict[str, Any] = {}
    for name, spec in declared.items():
        if isinstance(spec, dict) and "defaultValue" in spec:
            effective[name] = spec["defaultValue"]
    if dataset_ref is not None and getattr(dataset_ref, "parameters", None):
        effective.update(dict(dataset_ref.parameters))

    # Container name goes in the volume URL (no expressions allowed); other path components flow into
    # the notebook write as f-string fragments so date/time expressions evaluate at runtime.
    container = resolve_param_value(
        location.get("container") or location.get("fileSystem") or location.get("bucketName"),
        effective,
        context,
    )
    folder = resolve_param_value(location.get("folderPath"), effective, context, for_notebook=True).strip("/")
    filename = resolve_param_value(location.get("fileName"), effective, context, for_notebook=True).strip("/")

    if not container:
        return None

    linked_service_ref = dataset_props.get("linkedServiceName") or {}
    if isinstance(linked_service_ref, dict):
        linked_service_name = linked_service_ref.get("referenceName", "")
    else:
        linked_service_name = str(linked_service_ref)
    linked_service = definitions.linked_services.get(linked_service_name) if linked_service_name else None
    storage_account = resolve_storage_account(linked_service) if linked_service else None

    if location_type in ("AmazonS3Location", "GoogleCloudStorageLocation"):
        external_url = _LOCATION_URL_TEMPLATE[location_type].format(bucket=container)
    else:
        account_token = storage_account or "${var.storage_account}"
        external_url = _LOCATION_URL_TEMPLATE[location_type].format(container=container, account=account_token)

    volume_name = _sanitize_volume_name(container)
    parts = [folder, filename]
    relative = "/".join(part for part in parts if part)
    volume_path = f"/Volumes/${{var.catalog}}/${{var.schema}}/{volume_name}"
    if relative:
        volume_path = f"{volume_path}/{relative}"

    return SinkPathInfo(
        location_type=location_type,
        container=container,
        folder=folder,
        filename=filename,
        storage_account=storage_account,
        external_location_url=external_url,
        volume_name=volume_name,
        volume_relative_path=relative,
        uc_volume_path=volume_path,
    )


def _extract_source_query_text(source_properties: dict[str, Any]) -> str | None:
    """Returns the SQL query a Copy source executes, when one is supplied.

    Args:
        source_properties: ``source_properties`` dict on the Copy IR
            (still carrying raw ADF field names).

    Returns:
        First non-empty value across the well-known query keys
        (``sqlReaderQuery``, ``query``, ``sql_query``).  ADF expression
        wrappers (``{type: "Expression", value: "..."}``) are unwrapped
        to their inner string.  ``None`` when the source reads a table
        directly.
    """
    for key in ("sqlReaderQuery", "query", "sql_query"):
        value = source_properties.get(key)
        if value is None:
            continue
        if isinstance(value, dict) and value.get("type") == "Expression":
            value = value.get("value")
        if isinstance(value, str) and value.strip():
            return value
    return None


def _resolve_database_connection(
    linked_service_name: str,
    definitions: AdfDefinitions,
) -> dict[str, Any]:
    """Pulls host / port / database from a database linked service.

    Args:
        linked_service_name: Linked service name (referenced by a dataset).
        definitions: Full ADF definitions for lookups.

    Returns:
        Dict with optional keys ``host``, ``port``, ``database``, and
        ``type``.  Empty dict when the linked service is missing or has
        no parseable connection string.
    """
    linked_service = definitions.linked_services.get(linked_service_name) if linked_service_name else None
    if linked_service is None:
        return {}
    properties = linked_service.properties or {}
    type_props = properties.get("typeProperties") or properties
    ls_type = properties.get("type") or ""
    connection_string = type_props.get("connectionString")
    if isinstance(connection_string, dict):
        connection_string = connection_string.get("value", "")
    if not isinstance(connection_string, str):
        connection_string = ""
    host: str | None = type_props.get("server") or type_props.get("host")
    port: int | None = type_props.get("port")
    database: str | None = type_props.get("database") or type_props.get("databaseName") or type_props.get("catalog")
    if connection_string:
        host = host or _extract_connection_host(ls_type, connection_string)
        port = port or _extract_connection_port(ls_type, connection_string)
        database = database or _extract_connection_database(ls_type, connection_string)
    default_port = _DATABASE_DEFAULT_PORTS.get(ls_type)
    if port is None and default_port is not None:
        port = default_port
    out: dict[str, Any] = {}
    if host:
        out["host"] = str(host).strip()
    if port:
        out["port"] = int(port)
    if database:
        out["database"] = str(database).strip()
    if ls_type:
        out["type"] = ls_type
    return out


def _extract_connection_host(ls_type: str, connection_string: str) -> str | None:
    """Returns the host substring from a database connection string.

    Args:
        ls_type: Linked service type (e.g. ``AzureSqlDatabase``).
        connection_string: Raw connection-string value.

    Returns:
        Host string with surrounding whitespace stripped, or ``None``
        when the pattern for this database family does not match.
    """
    pattern = (
        _AZURE_SQL_SERVER_RE if "Sql" in ls_type else _MYSQL_SERVER_RE if "MySql" in ls_type else _POSTGRES_SERVER_RE
    )
    match = pattern.search(connection_string)
    return match.group(1).strip() if match else None


def _extract_connection_port(ls_type: str, connection_string: str) -> int | None:
    """Returns the port number from a database connection string.

    Args:
        ls_type: Linked service type.
        connection_string: Raw connection-string value.

    Returns:
        Integer port, or ``None`` when absent.
    """
    if "Sql" in ls_type:
        match = _AZURE_SQL_SERVER_RE.search(connection_string)
        if match and match.group(2):
            return int(match.group(2))
        return None
    pattern = _MYSQL_PORT_RE if "MySql" in ls_type else _POSTGRES_PORT_RE
    match = pattern.search(connection_string)
    return int(match.group(1)) if match else None


def _extract_connection_database(ls_type: str, connection_string: str) -> str | None:
    """Returns the database name from a database connection string.

    Args:
        ls_type: Linked service type.
        connection_string: Raw connection-string value.

    Returns:
        Database name with surrounding whitespace stripped, or ``None``.
    """
    del ls_type
    match = _AZURE_SQL_DATABASE_RE.search(connection_string)
    return match.group(1).strip() if match else None


def _resolve_source_path(activity: AdfActivity, definitions: AdfDefinitions) -> str | None:
    """Resolves the full storage path from the activity's input dataset."""
    if not activity.inputs:
        return None
    props = dataset_props(activity.inputs[0], definitions)
    if not props:
        return None
    return resolve_dataset_path(props, definitions)


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translates a Copy activity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing datasets.

    Returns:
        A :class:`CopyActivity` IR node.
    """
    type_properties = activity.type_properties or {}

    source_raw = type_properties.get("source", {})
    source_type = source_raw.get("type")
    source_properties = {k: v for k, v in source_raw.items() if k != "type"} if source_raw else {}

    resolved_path = _resolve_source_path(activity, definitions)
    if resolved_path:
        source_properties["resolved_path"] = resolved_path

    if activity.inputs:
        source_dataset_ref = activity.inputs[0]
        source_dataset_props = dataset_props(source_dataset_ref, definitions)
        source_schema, source_table = resolve_table_reference(source_dataset_ref, source_dataset_props, context)
        source_ls_name = resolve_dataset_linked_service_name(source_dataset_props)
        if source_schema:
            source_properties["source_schema"] = source_schema
        if source_table:
            source_properties["source_table"] = source_table
        if source_ls_name:
            source_properties["linked_service_name"] = source_ls_name
            connection = _resolve_database_connection(source_ls_name, definitions)
            if connection:
                source_properties["connection"] = connection

    raw_query = _extract_source_query_text(source_properties)
    if raw_query:
        dialect = dialect_for_source_type(source_type)
        analysis = analyze_copy_query(raw_query, dialect=dialect)
        if dialect:
            source_properties["query_dialect"] = dialect
        source_properties["query_parseable_for_lfc"] = analysis.parseable
        if analysis.cursor_column:
            source_properties["query_cursor_column"] = analysis.cursor_column
        if analysis.row_filter:
            source_properties["query_row_filter"] = analysis.row_filter
        if analysis.include_columns:
            source_properties["query_include_columns"] = list(analysis.include_columns)
        if analysis.rejection_reasons:
            source_properties["query_rejection_reasons"] = list(analysis.rejection_reasons)

    sink_raw = type_properties.get("sink", {})
    sink_type = sink_raw.get("type")
    sink_properties = {k: v for k, v in sink_raw.items() if k != "type"} if sink_raw else {}

    column_mapping: list[dict[str, str]] = []
    translator_raw = type_properties.get("translator")
    if translator_raw and isinstance(translator_raw, dict):
        mappings = translator_raw.get("mappings", [])
        for mapping in mappings:
            source_col = mapping.get("source", {})
            sink_col = mapping.get("sink", {})
            if source_col and sink_col:
                column_mapping.append(
                    {
                        "source_name": source_col.get("name", ""),
                        "source_type": source_col.get("type", ""),
                        "sink_name": sink_col.get("name", ""),
                        "sink_type": sink_col.get("type", ""),
                    }
                )

    # Resolve sink dataset metadata so the code generator can write to the
    # actual target format and location instead of always defaulting to Delta.
    sink_dataset_type: str | None = None
    sink_format: str | None = None
    sink_resolved_path: str | None = None
    sink_table_name: str | None = None
    if activity.outputs:
        sink_dataset_ref = activity.outputs[0]
        sink_dataset_props = dataset_props(sink_dataset_ref, definitions)
        if sink_dataset_props:
            sink_dataset_type = sink_dataset_props.get("type")
            sink_format = _DATASET_TYPE_TO_SPARK_FORMAT.get(sink_dataset_type or "")

            # File-on-cloud-storage sinks: compose a UC external-volume path so the notebook writes
            # through Unity Catalog and the bundler emits the matching SetupTask.
            sink_path_info = _resolve_path_info(sink_dataset_ref, sink_dataset_props, definitions, context)
            if sink_path_info is not None:
                sink_resolved_path = sink_path_info.uc_volume_path
                sink_properties = {
                    **sink_properties,
                    "volume_name": sink_path_info.volume_name,
                    "volume_external_location": sink_path_info.external_location_url,
                    "volume_relative_path": sink_path_info.volume_relative_path,
                    "volume_location_type": sink_path_info.location_type,
                }
                if sink_path_info.storage_account:
                    sink_properties["volume_storage_account"] = sink_path_info.storage_account
            else:
                # Non-file sinks: fall back to the simpler resolver (returns
                # an abfss:// path or None for tables).
                sink_resolved_path = resolve_dataset_path(sink_dataset_props, definitions)

            sink_schema, sink_table_name = resolve_table_reference(sink_dataset_ref, sink_dataset_props, context)
            sink_ls_name = resolve_dataset_linked_service_name(sink_dataset_props)
            if sink_schema:
                sink_properties["schema"] = sink_schema
            if sink_ls_name:
                sink_properties["linked_service_name"] = sink_ls_name

    if sink_table_name:
        sink_properties = {**sink_properties, "table": sink_table_name}
    if sink_resolved_path:
        sink_properties = {**sink_properties, "resolved_path": sink_resolved_path}

    return CopyActivity(
        **base_kwargs,
        source_type=source_type,
        sink_type=sink_type,
        source_properties=source_properties,
        sink_properties=sink_properties,
        sink_dataset_type=sink_dataset_type,
        sink_format=sink_format,
        sink_resolved_path=sink_resolved_path,
        column_mapping=column_mapping if column_mapping else None,
    )
