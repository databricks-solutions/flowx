"""Deterministic dataset-identity resolvers.

Resolve an ADF dataset reference to its physical identity (``schema.table`` or
storage path) and backing linked service, independent of convert-time state.
Shared by convert (``copy.py``) and discover (``lineage.py``) so both phases use
one implementation instead of duplicating the resolution logic.
"""

from __future__ import annotations

import re
from typing import Any

from flowx.models.adf_ast import AdfDefinitions
from flowx.models.ir import TranslationContext
from flowx.parser.expression_parser import (
    resolve_expression,
    resolve_interpolated_string,
    resolve_interpolated_string_for_notebook,
)

_ACCOUNT_NAME_RE = re.compile(r"AccountName=([A-Za-z0-9]+)", re.IGNORECASE)
_DATASET_PARAM_RE = re.compile(r"^@dataset\(\)\.([A-Za-z_][A-Za-z0-9_]*)$")


def dataset_props(dataset_ref: Any, definitions: AdfDefinitions) -> dict[str, Any] | None:
    """Return the ``properties`` dict for an input/output dataset reference."""
    dataset = definitions.datasets.get(dataset_ref.reference_name)
    if not dataset:
        return None
    return dict(dataset.properties or {})


def resolve_param_value(
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
        return resolve_param_value(
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


def resolve_storage_account(linked_service: Any) -> str | None:
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


def resolve_dataset_path(dataset_props: dict[str, Any], definitions: AdfDefinitions) -> str | None:
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
    account = resolve_storage_account(linked_service)
    if not account:
        return None

    return f"abfss://{file_system}@{account}.dfs.core.windows.net/{folder_path}".rstrip("/")


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


def resolve_table_reference(
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
    schema = resolve_param_value(schema_raw, effective_params, context) if schema_raw is not None else None
    table = resolve_param_value(table_raw, effective_params, context) if table_raw is not None else None
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


def resolve_dataset_linked_service_name(dataset_props: dict[str, Any] | None) -> str | None:
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


def _is_physical(value: str) -> bool:
    """Return True only when *value* is a literal (physical) identifier.

    A value is NOT physical when it still contains an unresolved marker:
    - a DAB-ref placeholder  ``{{`` … ``}}``
    - a leftover ADF interpolation fragment  ``@{``
    - a bare ADF expression  (starts with ``@`` after stripping)
    """
    stripped = value.lstrip()
    return not ("{{" in value or "@{" in value or stripped.startswith("@"))


def resolve_dataset_identity(
    dataset_ref: Any,
    definitions: AdfDefinitions,
    context: TranslationContext | None = None,
) -> str | None:
    """Deterministic physical identity for a dataset reference.

    Returns ``"schema.table"`` when a table is resolvable, else a storage path,
    else ``None`` (never a guess). Used to join producers to consumers on the
    same physical asset even when their ADF dataset names differ.

    Parameterized values (ADF expressions or DAB-ref placeholders) are treated
    as unresolvable and return ``None`` — they must never be used as identity
    keys because two unrelated pipelines sharing the same parameter name would
    collide on the same placeholder string.
    """
    if dataset_ref is None:
        return None
    ctx = context if context is not None else TranslationContext()
    props = dataset_props(dataset_ref, definitions)
    if props is None:
        return None
    schema, table = resolve_table_reference(dataset_ref, props, ctx)
    if table:
        identity = f"{schema}.{table}" if schema else table
        return identity if _is_physical(identity) else None
    path = resolve_dataset_path(props, definitions)
    return path if (path and _is_physical(path)) else None
