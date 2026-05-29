"""Translates ADF Copy activities to Databricks CopyActivity IR."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, CopyActivity, TranslationContext
from orchestra.parser.expression_parser import (
    resolve_expression,
    resolve_interpolated_string,
    resolve_interpolated_string_for_notebook,
)
from orchestra.translator.query_analysis import analyze_copy_query, dialect_for_source_type

_DATASET_TYPE_TO_SPARK_FORMAT: dict[str, str] = {
    "DelimitedText": "csv",
    "Parquet": "parquet",
    "Json": "json",
    "Avro": "avro",
    "Orc": "orc",
    "Binary": "binaryFile",
    "DeltaLakeDataset": "delta",
}

# Map ADF dataset location types to (uri-scheme, host-template) pairs used
# when constructing the external-volume URL.  ``{account}`` is replaced with
# the storage account name (or a ``${var.storage_account}`` placeholder when
# the linked service does not expose one) and ``{bucket}`` with the bucket
# name for AWS / GCS sinks.
_LOCATION_URL_TEMPLATE: dict[str, str] = {
    "AzureBlobFSLocation": "abfss://{container}@{account}.dfs.core.windows.net/",
    "AzureBlobStorageLocation": "abfss://{container}@{account}.dfs.core.windows.net/",
    "AmazonS3Location": "s3://{bucket}/",
    "GoogleCloudStorageLocation": "gs://{bucket}/",
}

# Regex to pull AccountName=... from an Azure storage connection string when
# the secret value is plaintext (rare in az exports, but supported).
_ACCOUNT_NAME_RE = re.compile(r"AccountName=([A-Za-z0-9]+)", re.IGNORECASE)
_DATASET_PARAM_RE = re.compile(r"^@dataset\(\)\.([A-Za-z_][A-Za-z0-9_]*)$")

# Database connection-string parsers: each picks up the canonical
# host/port/database fields from an ADF linked service's
# ``connectionString``.  The patterns are intentionally tolerant of
# casing and surrounding whitespace because ADF accepts both
# ``Server=`` and ``server=`` etc.
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


def _dataset_props(dataset_ref: Any, definitions: AdfDefinitions) -> dict[str, Any] | None:
    """Return the ``properties`` dict for an input/output dataset reference."""
    dataset = definitions.datasets.get(dataset_ref.reference_name)
    if not dataset:
        return None
    return dict(dataset.properties or {})


def _sanitize_volume_name(value: str) -> str:
    """Sanitises an ADF container name for use as a UC volume name."""
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", value or "default_volume").strip("_")
    return cleaned or "default_volume"


def _resolve_param_value(
    raw: Any,
    dataset_params: dict[str, Any],
    context: TranslationContext,
    *,
    for_notebook: bool = False,
) -> str:
    """Resolves a single ADF location field to a string."""
    if raw is None:
        return ""
    if isinstance(raw, dict) and raw.get("type") == "Expression":
        raw = raw.get("value", "")
    if isinstance(raw, (list, dict)):
        return ""
    if not isinstance(raw, str):
        return str(raw)
    text = raw

    match = _DATASET_PARAM_RE.match(text.strip())
    if match:
        param_name = match.group(1)
        return _resolve_param_value(
            dataset_params.get(param_name, ""), dataset_params, context, for_notebook=for_notebook
        )

    if "@{" in text:
        if for_notebook:
            return resolve_interpolated_string_for_notebook(text, context)
        return resolve_interpolated_string(text, context)

    if text.startswith("@"):
        result = resolve_expression(text, context)
        if result is not None and result.kind in ("literal", "dab_ref"):
            return result.value
        return text

    return text


def _resolve_storage_account(linked_service: Any) -> str | None:
    """Tries to pull a storage account name out of a linked service, if present."""
    if linked_service is None:
        return None
    type_props = linked_service.properties.get("typeProperties") or linked_service.properties

    url = type_props.get("url") or ""
    if isinstance(url, str) and url:
        host = url.replace("https://", "").split("/", 1)[0]
        host_no_port = host.split(":", 1)[0]
        if "." in host_no_port:
            return host_no_port.split(".", 1)[0]

    sas_uri = type_props.get("sasUri") or ""
    if isinstance(sas_uri, str) and sas_uri:
        host = sas_uri.split("?", 1)[0].replace("https://", "").split("/", 1)[0]
        if "." in host:
            return host.split(".", 1)[0]

    # Plaintext connection string (rare in az exports — usually masked).
    conn_string = type_props.get("connectionString")
    if isinstance(conn_string, str):
        match = _ACCOUNT_NAME_RE.search(conn_string)
        if match:
            return match.group(1)
    if isinstance(conn_string, dict):
        value = conn_string.get("value", "")
        match = _ACCOUNT_NAME_RE.search(value)
        if match:
            return match.group(1)

    # AWS — bucket name lives on the dataset, account is implicit.
    # Nothing useful to return at the linked-service level for S3/GCS.
    return None


def _resolve_dataset_path(dataset_props: dict[str, Any], definitions: AdfDefinitions) -> str | None:
    """Resolves a dataset's storage path using its location + linked service."""
    type_props = dataset_props.get("typeProperties") or dataset_props
    location = type_props.get("location") or {}

    file_system = location.get("fileSystem") or location.get("container") or ""
    folder_path = location.get("folderPath") or ""
    if isinstance(file_system, dict) or isinstance(folder_path, dict):
        return None  # parameterised; caller handles via _resolve_path_info

    linked_service_ref = dataset_props.get("linkedServiceName") or {}
    if isinstance(linked_service_ref, dict):
        linked_service_name = linked_service_ref.get("referenceName", "")
    else:
        linked_service_name = str(linked_service_ref)
    linked_service = definitions.linked_services.get(linked_service_name) if linked_service_name else None
    account = _resolve_storage_account(linked_service)
    if not account:
        return None

    return f"abfss://{file_system}@{account}.dfs.core.windows.net/{folder_path}".rstrip("/")


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

    # Container name is used in the volume URL (no expressions allowed); the
    # other path components flow into the notebook write call as f-string
    # fragments so date/time expressions evaluate at runtime.
    container = _resolve_param_value(
        location.get("container") or location.get("fileSystem") or location.get("bucketName"),
        effective,
        context,
    )
    folder = _resolve_param_value(location.get("folderPath"), effective, context, for_notebook=True).strip("/")
    filename = _resolve_param_value(location.get("fileName"), effective, context, for_notebook=True).strip("/")

    if not container:
        return None

    linked_service_ref = dataset_props.get("linkedServiceName") or {}
    if isinstance(linked_service_ref, dict):
        linked_service_name = linked_service_ref.get("referenceName", "")
    else:
        linked_service_name = str(linked_service_ref)
    linked_service = definitions.linked_services.get(linked_service_name) if linked_service_name else None
    storage_account = _resolve_storage_account(linked_service) if linked_service else None

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


def _effective_dataset_params(dataset_ref: Any, dataset_props: dict[str, Any]) -> dict[str, Any]:
    """Returns the effective parameter map for a dataset reference.

    Args:
        dataset_ref: Activity-side dataset reference (carries parameter
            overrides supplied at the call site).
        dataset_props: Full properties dict of the referenced dataset.

    Returns:
        Mapping of parameter name to resolved value: dataset declared
        defaults first, then activity-side overrides win.
    """
    declared = dataset_props.get("parameters") or {}
    effective: dict[str, Any] = {}
    for name, spec in declared.items():
        if isinstance(spec, dict) and "defaultValue" in spec:
            effective[name] = spec["defaultValue"]
    if dataset_ref is not None and getattr(dataset_ref, "parameters", None):
        effective.update(dict(dataset_ref.parameters))
    return effective


def _resolve_table_reference(
    dataset_ref: Any,
    dataset_props: dict[str, Any] | None,
    context: TranslationContext,
) -> tuple[str | None, str | None]:
    """Resolves the schema and table name from a dataset reference.

    Args:
        dataset_ref: Activity-side dataset reference.
        dataset_props: Full properties dict of the referenced dataset.
        context: Translation context for expression resolution.

    Returns:
        Tuple of ``(schema, table)`` strings.  Either may be ``None``
        when the dataset does not carry that field.  ADF parameter
        expressions are resolved against the dataset reference's
        effective parameter map.  Handles both the nested
        ``typeProperties`` shape and the ``schemaTypePropertiesSchema``
        flattened form ``az datafactory dataset show`` emits.
    """
    if not dataset_props:
        return None, None
    type_props = dataset_props.get("typeProperties") if isinstance(dataset_props.get("typeProperties"), dict) else None
    effective_params = _effective_dataset_params(dataset_ref, dataset_props)
    schema_raw = _pick_dataset_field(
        type_props,
        dataset_props,
        ("schema", "database"),
        ("schemaTypePropertiesSchema", "database"),
    )
    table_raw = _pick_dataset_field(
        type_props,
        dataset_props,
        ("table", "tableName"),
        ("table", "tableName"),
    )
    schema = _resolve_param_value(schema_raw, effective_params, context) if schema_raw is not None else None
    table = _resolve_param_value(table_raw, effective_params, context) if table_raw is not None else None
    return (schema or None), (table or None)


def _pick_dataset_field(
    type_props: dict[str, Any] | None,
    dataset_props: dict[str, Any],
    nested_keys: tuple[str, ...],
    flat_keys: tuple[str, ...],
) -> Any:
    """Returns the first populated dataset field across nested and flat shapes.

    Args:
        type_props: ``typeProperties`` dict when present, ``None``
            when the dataset is in the az-flattened shape.
        dataset_props: Top-level dataset properties dict.
        nested_keys: Keys to try inside ``type_props`` (nested ADF shape).
        flat_keys: Keys to try at the top level (az flattened shape).

    Returns:
        The first non-empty value found.  Empty strings, empty lists,
        and ``None`` are skipped so column-schema artifacts like
        ``schema: []`` don't shadow the actual database schema stored
        under a flattened key.
    """
    candidates: list[Any] = []
    if type_props is not None:
        candidates.extend(type_props.get(key) for key in nested_keys)
    candidates.extend(dataset_props.get(key) for key in flat_keys)
    for value in candidates:
        if value is None:
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _resolve_dataset_linked_service_name(dataset_props: dict[str, Any] | None) -> str | None:
    """Returns the linked service name a dataset references.

    Args:
        dataset_props: Full properties dict of the referenced dataset.

    Returns:
        Linked service name string, or ``None`` when not present.
    """
    if not dataset_props:
        return None
    raw = dataset_props.get("linkedServiceName") or {}
    if isinstance(raw, dict):
        return raw.get("referenceName") or None
    return str(raw) or None


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
    props = _dataset_props(activity.inputs[0], definitions)
    if not props:
        return None
    return _resolve_dataset_path(props, definitions)


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
        source_dataset_props = _dataset_props(source_dataset_ref, definitions)
        source_schema, source_table = _resolve_table_reference(source_dataset_ref, source_dataset_props, context)
        source_ls_name = _resolve_dataset_linked_service_name(source_dataset_props)
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
        sink_dataset_props = _dataset_props(sink_dataset_ref, definitions)
        if sink_dataset_props:
            sink_dataset_type = sink_dataset_props.get("type")
            sink_format = _DATASET_TYPE_TO_SPARK_FORMAT.get(sink_dataset_type or "")

            # File-on-cloud-storage sinks: compose a UC external volume
            # path so the notebook writes through Unity Catalog and the
            # bundler can emit the matching SetupTask (storage credential
            # + external location + external volume).
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
                sink_resolved_path = _resolve_dataset_path(sink_dataset_props, definitions)

            sink_schema, sink_table_name = _resolve_table_reference(sink_dataset_ref, sink_dataset_props, context)
            sink_ls_name = _resolve_dataset_linked_service_name(sink_dataset_props)
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
