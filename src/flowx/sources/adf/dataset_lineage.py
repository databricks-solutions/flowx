"""Resolve ADF dataset references into source-neutral :class:`DataAsset` values.

This is the ADF half of data-lineage population (#62b). It re-homes the
dataset-identity resolver and the path-signature logic first written for the
closed #36 ADF-only lineage attempt, but emits the shared #61 two-tier
:class:`~flowx.models.ir.DataAsset` (``identity`` + ``signature``) instead of a
bespoke edge type, so the source-neutral join in :mod:`flowx.lineage` /
:mod:`flowx.discovery_lineage` does the matching.

Two tiers, exactly as #36 established them:

* **identity** -- the resolved physical location of the asset (a store-qualified
  ``<server>/schema.table`` or a concrete ``abfss://`` path down to the literal
  file name). Present only when it resolves *deterministically*
  from literals; a parameterised reference is never guessed at and leaves
  ``identity`` unset. This is the strong join key.
* **signature** -- the *path-derived* weak key. For an asset with a resolved
  identity the signature mirrors that identity, so the weak tier can never join a
  resolved asset to an unresolved one on a coincidence. For an unresolved reference
  the signature is the *structural path signature* (#36's "expression" tier: the
  literal path skeleton plus its parameter-slot count) when the path has a literal
  anchor, prefixed ``ST[<store>]/`` when the dataset's store is known (see
  :func:`_resolve_dataset_store`), so two datasets on different stores that merely
  share a folder shape never join. An unknown store is never guessed: the signature
  stays unqualified, and a qualified and an unqualified signature never join.
  It is **never** the bare dataset reference name: two unrelated opaque
  references that merely share a name must not join (#36's explicit rule), so when
  neither a physical identity nor a path-anchored signature is available the
  signature is left empty and the asset cannot participate in signature matching.
  The signature is also left empty when the activity overrides the dataset's
  location at run time (a source query, stored procedure or ``storeSettings`` path
  override on a read; a sink stored procedure on a write), because the dataset's
  path is then not what the activity touches.

Only literal, provable values ever become an ``identity`` -- the resolver returns
``None`` rather than guessing, which is what stopped #36's spurious edges.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterator
from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDatasetReference, AdfDefinitions
from flowx.models.ir import DataAsset, TranslationContext
from flowx.parser.expression_parser import resolve_expression, resolve_interpolated_string

_ACCOUNT_NAME_RE = re.compile(r"AccountName=([A-Za-z0-9]+)", re.IGNORECASE)
_DATASET_PARAM_RE = re.compile(r"^@dataset\(\)\.([A-Za-z_][A-Za-z0-9_]*)$")
_ARM_EXPRESSION_RE = re.compile(r"^\[\s*[A-Za-z_][A-Za-z0-9_]*\s*\(.*\]$", re.DOTALL)
_RUN_TIME_PATH_OVERRIDES = ("wildcardFolderPath", "wildcardFileName", "fileListPath", "prefix")

# Runtime references inside a path expression. Each is a value only knowable at
# run time; for a *structural* signature we collapse them all to one slot token so
# that, e.g., a writer's ``pipeline().parameters.entityID`` and a reader's
# ``item().entityID`` (the same value passed down a ForEach) share the same shape.
_PARAM_REF_RE = re.compile(
    r"pipeline\(\)\.parameters\.\w+"
    r"|item\(\)(?:\.\w+)*"
    r"|variables\('[^']*'\)"
    r"|dataset\(\)\.\w+"
    r"|activity\('[^']*'\)\.[\w.]+"
)
_QUOTED_LITERAL_RE = re.compile(r"'([^']*)'")


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #


def activity_data_assets(
    activity: AdfActivity,
    definitions: AdfDefinitions,
    context: TranslationContext | None = None,
) -> tuple[list[DataAsset], list[DataAsset]]:
    """Resolve the assets an ADF activity reads and writes.

    Captures **every** input and output, not just index 0: a Copy reads its
    ``source`` dataset(s) and writes its ``sink`` dataset(s), a Lookup reads its
    ``dataset``, and any activity-level ``inputs`` / ``outputs`` slots are all
    included. A dataset named in both an activity-level slot and ``typeProperties``
    is counted once per side so an activity does not emit two identical assets.

    When the activity overrides the dataset's physical source or target at run time,
    the dataset's own location is not what the activity touches, so that side gets
    neither an identity nor a signature and cannot join on either tier. Reads are overridden by
    a source query or stored procedure, or by ``storeSettings`` that give a wildcard
    folder or file name, a file list, or a prefix; writes by a sink stored procedure,
    which decides the table itself.

    Args:
        activity: The ADF activity to resolve.
        definitions: All loaded ADF definitions (datasets + linked services),
            needed to resolve a reference to its physical identity.
        context: Optional translation context for expression resolution; a default
            (empty) context is used when none is given, mirroring the deterministic
            discover-time resolution the closed #36 attempt used.

    Returns:
        ``(data_reads, data_writes)`` as lists of :class:`DataAsset`.
    """
    resolution_context = context if context is not None else TranslationContext()
    reads_overridden = _location_overridden(activity, produced=False)
    reads = [
        _dataset_ref_to_asset(reference, definitions, resolution_context, location_overridden=reads_overridden)
        for reference in _activity_dataset_refs(activity, produced=False)
    ]
    writes_overridden = _location_overridden(activity, produced=True)
    writes = [
        _dataset_ref_to_asset(reference, definitions, resolution_context, location_overridden=writes_overridden)
        for reference in _activity_dataset_refs(activity, produced=True)
    ]
    return reads, writes


def _location_overridden(activity: AdfActivity, *, produced: bool) -> bool:
    """Say whether the activity replaces its datasets' physical target (produced) or source (not) at run time.

    A Copy sink that names a stored procedure writes wherever the procedure decides.
    A Copy or Lookup ``source`` that names a query or stored procedure reads what it
    returns. Connectors name these keys differently (``sqlReaderQuery``,
    ``oracleReaderQuery``, ``sqlReaderStoredProcedureName``, ...), so they are
    matched by kind: ``query`` or any key ending in ``Query`` or
    ``StoredProcedureName``. ``storeSettings`` with a wildcard, file list or prefix
    replace the read path: Copy and Lookup carry them on ``source``, Delete and
    GetMetadata directly on ``typeProperties``.
    """
    type_properties = activity.type_properties or {}
    if produced:
        return _names_any(type_properties.get("sink"), lambda key: key.endswith("StoredProcedureName"))
    source = type_properties.get("source")
    store_settings_candidates = (
        source.get("storeSettings") if isinstance(source, dict) else None,
        type_properties.get("storeSettings"),
    )
    return _names_any(source, _is_source_query_key) or any(
        _names_any(store_settings, lambda key: key in _RUN_TIME_PATH_OVERRIDES)
        for store_settings in store_settings_candidates
    )


def _is_source_query_key(key: str) -> bool:
    """``True`` for a ``source`` key that names a query or stored procedure to read through."""
    return key == "query" or key.endswith("Query") or key.endswith("StoredProcedureName")


def _names_any(settings: object, is_override_key: Callable[[str], bool]) -> bool:
    """``True`` when *settings* is a dict with a non-empty value under a key *is_override_key* accepts."""
    return isinstance(settings, dict) and any(value and is_override_key(key) for key, value in settings.items())


def _dataset_ref_to_asset(
    dataset_ref: AdfDatasetReference,
    definitions: AdfDefinitions,
    context: TranslationContext,
    *,
    location_overridden: bool,
) -> DataAsset:
    """Turn one dataset reference into a two-tier :class:`DataAsset`.

    Signature is derived only from the resolved *physical* location -- the identity
    when it resolves, else the structural path signature. It is **never** the bare
    dataset reference name (#36's hard rule): two unrelated opaque references that
    merely share a name must not join, so when neither a physical identity nor a
    path-anchored signature is available the signature is left empty. An empty
    signature is falsy, so :func:`~flowx.lineage._match_assets` cannot use it as a
    join key -- the asset is still captured as a read / write for reporting, it just
    cannot manufacture a signature-tier edge. When *location_overridden* is set the
    dataset's location is not what the activity touches, so neither an identity nor a
    signature is derived from it.

    A path signature is prefixed with the dataset's store when that store is known
    (see :func:`_resolve_dataset_store`), so two datasets on different containers or
    accounts that merely share a folder shape never join.
    """
    if location_overridden:
        return DataAsset(signature="", identity=None, asset_type=_asset_type(dataset_ref, definitions))
    identity = resolve_dataset_identity(dataset_ref, definitions, context)
    if identity is not None:
        # Mirror the identity into the signature so the weak tier never joins a
        # resolved asset to an unresolved one that merely shares a physical value.
        return DataAsset(signature=identity, identity=identity, asset_type=_asset_type(dataset_ref, definitions))
    path_signature = _path_signature(dataset_ref.parameters)
    store = _resolve_dataset_store(dataset_ref, definitions, context) if path_signature is not None else None
    if path_signature is not None and store:
        path_signature = f"ST[{store}]/{path_signature}"
    return DataAsset(
        signature=path_signature if path_signature is not None else "",
        identity=None,
        asset_type=_asset_type(dataset_ref, definitions),
    )


# --------------------------------------------------------------------------- #
# Dataset reference gathering (all inputs / outputs)
# --------------------------------------------------------------------------- #


def _typeprops_dataset_ref(candidate: object) -> AdfDatasetReference | None:
    """Build a dataset reference from a ``typeProperties`` source/sink/dataset slot.

    Carries the slot's ``parameters`` (Lookup / Delete / GetMetadata put the
    dataset call-site params here) so the path signature can be computed.
    """
    if isinstance(candidate, dict):
        name = candidate.get("referenceName")
        if isinstance(name, str) and name:
            parameters = candidate.get("parameters")
            return AdfDatasetReference(
                reference_name=name,
                parameters=parameters if isinstance(parameters, dict) else None,
            )
    return None


def _activity_dataset_refs(activity: AdfActivity, *, produced: bool) -> Iterator[AdfDatasetReference]:
    """Yield the dataset references an activity writes (produced) or reads (not).

    Gathers activity-level ``inputs`` / ``outputs`` **and** the ``typeProperties``
    ``source`` / ``sink`` / ``dataset`` slots. De-duplication is by
    ``(reference_name, parameter binding)``, not by name alone: a dataset named in
    both an activity slot and ``typeProperties`` with the *same* call-site params is
    the same physical asset and is yielded once, but two uses of the *same*
    parameterised dataset with *different* params (``ds(tbl=orders)`` vs
    ``ds(tbl=customers)``) resolve to distinct physical assets and are both kept.
    """
    type_properties = activity.type_properties or {}
    candidates: list[AdfDatasetReference] = []
    if produced:
        candidates.extend(activity.outputs or [])
        sink_reference = _typeprops_dataset_ref(type_properties.get("sink"))
        if sink_reference is not None:
            candidates.append(sink_reference)
    else:
        candidates.extend(activity.inputs or [])
        for key in ("source", "dataset"):
            read_reference = _typeprops_dataset_ref(type_properties.get(key))
            if read_reference is not None:
                candidates.append(read_reference)

    seen: set[tuple[str, str]] = set()
    for reference in candidates:
        dedupe_key = (reference.reference_name, _parameter_binding_key(reference))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        yield reference


def _parameter_binding_key(reference: AdfDatasetReference) -> str:
    """Stable key for a reference's call-site parameter binding.

    Two references with the same name collapse only when their parameters match, so
    distinct bindings that resolve to distinct physical assets survive. Sorted keys
    make the string order-independent; ``default=str`` keeps it total for any value
    an ADF export can carry.
    """
    return json.dumps(reference.parameters or {}, sort_keys=True, default=str)


# --------------------------------------------------------------------------- #
# Physical identity resolution (tier 1)
# --------------------------------------------------------------------------- #


def resolve_dataset_identity(
    dataset_ref: AdfDatasetReference,
    definitions: AdfDefinitions,
    context: TranslationContext | None = None,
) -> str | None:
    """Deterministic physical identity for a dataset reference.

    Returns ``"<store>/schema.table"`` when a table is resolvable, else a storage
    path down to the literal file name, else ``None`` (never a guess). Used to join
    producers to consumers on the same physical asset even when their ADF dataset
    names differ.

    A table name is only unique within the store that holds it, so the table is
    prefixed with the backing linked service's server (and database, when given),
    or with the linked service name when the server is hidden in a secret.
    Otherwise a source ``dbo.Orders`` and a warehouse ``dbo.Orders`` would look like
    one table.

    Parameterised values (ADF expressions or DAB-ref placeholders), including a
    parameterised linked service, are treated as unresolvable and return ``None``
    -- they must never be used as identity keys because two unrelated pipelines
    sharing a parameter name would collide on the same placeholder string.
    """
    resolution_context = context if context is not None else TranslationContext()
    properties = _dataset_props(dataset_ref, definitions)
    if properties is None:
        return None
    schema, table = _resolve_table_reference(dataset_ref, properties, resolution_context)
    if table:
        if not all(_is_physical(part) for part in (schema, table) if part):
            return None
        qualified_table = f"{schema}.{table}" if schema else table
        store = _resolve_table_store(properties, definitions)
        return f"{store}/{qualified_table}" if store else None
    return _resolve_dataset_path(
        properties,
        _backing_linked_service(properties, definitions),
        _effective_dataset_params(dataset_ref, properties),
        resolution_context,
    )


def _dataset_props(dataset_ref: AdfDatasetReference, definitions: AdfDefinitions) -> dict[str, Any] | None:
    """Return the ``properties`` dict for a dataset reference, or ``None``."""
    dataset = definitions.get_dataset(dataset_ref.reference_name)
    if not dataset:
        return None
    return dict(dataset.properties or {})


def _resolve_param_value(
    raw: Any,
    dataset_params: dict[str, Any],
    context: TranslationContext,
) -> str:
    """Resolve a single ADF location / table field to a string."""
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
        parameter_name = match.group(1)
        return _resolve_param_value(dataset_params.get(parameter_name, ""), dataset_params, context)

    if "@{" in text:
        return resolve_interpolated_string(text, context)

    if text.startswith("@"):
        result = resolve_expression(text, context)
        if result is not None and result.kind in ("literal", "dab_ref"):
            return result.value
        return text

    return text


def _effective_dataset_params(dataset_ref: AdfDatasetReference, dataset_props: dict[str, Any]) -> dict[str, Any]:
    """Effective parameter map: declared dataset defaults first, call-site overrides win."""
    declared = dataset_props.get("parameters") or {}
    effective: dict[str, Any] = {}
    for name, spec in declared.items():
        if isinstance(spec, dict) and "defaultValue" in spec:
            effective[name] = spec["defaultValue"]
    if dataset_ref.parameters:
        effective.update(dict(dataset_ref.parameters))
    return effective


def _resolve_table_reference(
    dataset_ref: AdfDatasetReference,
    dataset_props: dict[str, Any] | None,
    context: TranslationContext,
) -> tuple[str | None, str | None]:
    """Resolve ``(schema, table)`` from a dataset reference.

    Handles both the nested ``typeProperties`` shape and the
    ``schemaTypePropertiesSchema`` flattened form ``az datafactory dataset show``
    emits. ADF parameter expressions resolve against the reference's effective
    parameter map.
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
    """First populated dataset field across the nested and az-flattened shapes.

    Empty strings, empty lists, and ``None`` are skipped so a column-schema
    artifact like ``schema: []`` does not shadow the real database schema stored
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


def _linked_service_reference(dataset_props: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Return the dataset's linked service name and the parameters its reference binds."""
    linked_service_reference = dataset_props.get("linkedServiceName") or {}
    if not isinstance(linked_service_reference, dict):
        return str(linked_service_reference), {}
    parameters = linked_service_reference.get("parameters")
    return linked_service_reference.get("referenceName") or "", parameters if isinstance(parameters, dict) else {}


def _backing_linked_service(dataset_props: dict[str, Any], definitions: AdfDefinitions) -> Any:
    """Return the loaded definition of the dataset's linked service, or ``None``."""
    linked_service_name, _ = _linked_service_reference(dataset_props)
    return definitions.get_linked_service(linked_service_name) if linked_service_name else None


def _resolve_table_store(dataset_props: dict[str, Any], definitions: AdfDefinitions) -> str | None:
    """Name the store that holds a dataset's table, or ``None`` when it is not provable.

    Uses the literal ``server`` (plus ``database`` when given) from the backing
    linked service's properties or plaintext connection string, exactly as written.
    When the server is hidden (a Key Vault reference or a masked secret) the linked
    service name stands in for it. A parameterised server, database, or connection
    string, or a linked service that takes parameters, gives ``None``: each binding
    may point at a different store, so no single name identifies it.
    """
    linked_service_name, _ = _linked_service_reference(dataset_props)
    if not linked_service_name:
        return None
    linked_service = definitions.get_linked_service(linked_service_name)
    linked_service_properties = linked_service.properties if linked_service is not None else {}
    type_props = linked_service_properties.get("typeProperties") or linked_service_properties
    connection_string = type_props.get("connectionString")
    if isinstance(connection_string, dict):
        connection_string = connection_string.get("value")
    if isinstance(connection_string, str) and not _is_physical(connection_string):
        return None
    connection_fields = _connection_string_fields(connection_string) if isinstance(connection_string, str) else {}

    server = type_props.get("server") or connection_fields.get("server") or connection_fields.get("data source")
    database = (
        type_props.get("database") or connection_fields.get("database") or connection_fields.get("initial catalog")
    )
    store_parts = [part for part in (server, database) if part]
    if not all(isinstance(part, str) and part.strip() and _is_physical(part) for part in store_parts):
        return None
    if server:
        return "/".join(part.strip() for part in store_parts)
    return _unparameterised_linked_service_name(dataset_props, definitions)


def _unparameterised_linked_service_name(dataset_props: dict[str, Any], definitions: AdfDefinitions) -> str | None:
    """The dataset's linked service name when that linked service takes no parameters, else ``None``.

    A linked service with no parameters points at the same store on every use, so its
    name can stand in for a server or account it hides in a secret. One that takes
    parameters, on its reference or in its own definition, may point somewhere else
    for each binding.
    """
    linked_service_name, reference_parameters = _linked_service_reference(dataset_props)
    if not linked_service_name or reference_parameters:
        return None
    linked_service = definitions.get_linked_service(linked_service_name)
    if linked_service is not None and linked_service.properties.get("parameters"):
        return None
    return linked_service_name


def _resolve_dataset_store(
    dataset_ref: AdfDatasetReference,
    definitions: AdfDefinitions,
    context: TranslationContext,
) -> str | None:
    """Name the store a dataset lives in, even when its full path or table does not resolve.

    A table dataset's store is the one :func:`_resolve_table_store` names. A file
    dataset's store is ``<file system>@<account>`` when both are literal. When the
    file system is literal but the account cannot be read (a Key Vault or masked
    connection string, for example), the linked service name stands in for the
    account, as long as the linked service takes no parameters. ``None`` whenever
    the store is not provable, so the caller leaves the signature unqualified rather
    than guess.
    """
    properties = _dataset_props(dataset_ref, definitions)
    if properties is None:
        return None
    _, table = _resolve_table_reference(dataset_ref, properties, context)
    if table:
        return _resolve_table_store(properties, definitions)
    type_props = properties.get("typeProperties") or properties
    location = type_props.get("location")
    if not isinstance(location, dict):
        return None
    file_system, account = _resolve_file_store(
        location,
        _backing_linked_service(properties, definitions),
        _effective_dataset_params(dataset_ref, properties),
        context,
    )
    if file_system is None:
        return None
    store = account or _unparameterised_linked_service_name(properties, definitions)
    return f"{file_system}@{store}" if store else None


def _resolve_file_store(
    location: dict[str, Any],
    linked_service: Any,
    dataset_params: dict[str, Any],
    context: TranslationContext,
) -> tuple[str | None, str | None]:
    """Resolve a file dataset's ``(file system, storage account)``.

    Each part is ``None`` unless it resolves to a literal. The path identity and the
    signature's store qualifier both read the store here, so they always agree on
    what counts as a known file system and account.
    """
    file_system = _resolve_param_value(location.get("fileSystem") or location.get("container"), dataset_params, context)
    account = _resolve_storage_account(linked_service)
    return (
        file_system if file_system and _is_physical(file_system) else None,
        account if account and _is_physical(account) else None,
    )


def _connection_string_fields(connection_string: str) -> dict[str, str]:
    """Split a ``key=value;`` connection string into a lower-cased key map."""
    fields: dict[str, str] = {}
    for part in connection_string.split(";"):
        key, separator, value = part.partition("=")
        if separator:
            fields[key.strip().lower()] = value.strip()
    return fields


def _resolve_dataset_path(
    dataset_props: dict[str, Any],
    linked_service: Any,
    dataset_params: dict[str, Any],
    context: TranslationContext,
) -> str | None:
    """Resolve a dataset's storage path from its location + backing linked service.

    The path runs down to the literal ``fileName`` when the dataset names one, so
    two files in the same folder stay distinct. Location parts written as
    ``@dataset().x`` resolve against the reference's effective parameters, as table
    names do, so a parameterised dataset bound to literals still gets its identity.
    Any part that stays parameterised or unresolved (file system, folder, file name,
    or storage account) yields ``None``.
    """
    type_props = dataset_props.get("typeProperties") or dataset_props
    location = type_props.get("location") or {}
    if not isinstance(location, dict):
        return None

    def _resolved_part(raw: Any) -> str | None:
        if raw is None or raw == "":
            return ""
        resolved = _resolve_param_value(raw, dataset_params, context)
        # A part that was written but resolves to nothing (an unbound parameter) is unknown, not absent.
        if not resolved or not _is_physical(resolved):
            return None
        return resolved

    file_system, account = _resolve_file_store(location, linked_service, dataset_params, context)
    folder_path = _resolved_part(location.get("folderPath"))
    file_name = _resolved_part(location.get("fileName"))
    if file_system is None or account is None or folder_path is None or file_name is None:
        return None

    relative_path = "/".join(part.strip("/") for part in (folder_path, file_name) if part.strip("/"))
    return f"abfss://{file_system}@{account}.dfs.core.windows.net/{relative_path}".rstrip("/")


def _resolve_storage_account(linked_service: Any) -> str | None:
    """Pull a storage account name out of a linked service, if present.

    The whole ``url``, ``sasUri`` or connection string must be physical before the
    account is cut out of it: cutting first would drop the closing ``]`` of an ARM
    expression or the tail of an ``@{...}`` and leave a fragment that looks literal.
    """
    if linked_service is None:
        return None
    type_props = linked_service.properties.get("typeProperties") or linked_service.properties

    url = type_props.get("url") or ""
    if isinstance(url, str) and url:
        if not _is_physical(url):
            return None
        host = url.replace("https://", "").split("/", 1)[0]
        host_no_port = host.split(":", 1)[0]
        if "." in host_no_port:
            return host_no_port.split(".", 1)[0]

    sas_uri = type_props.get("sasUri") or ""
    if isinstance(sas_uri, str) and sas_uri:
        if not _is_physical(sas_uri):
            return None
        host = sas_uri.split("?", 1)[0].replace("https://", "").split("/", 1)[0]
        if "." in host:
            return host.split(".", 1)[0]

    # Plaintext connection string (rare in az exports -- usually masked).
    connection_string = type_props.get("connectionString")
    if isinstance(connection_string, dict):
        connection_string = connection_string.get("value", "")
    if isinstance(connection_string, str):
        if not _is_physical(connection_string):
            return None
        match = _ACCOUNT_NAME_RE.search(connection_string)
        if match:
            return match.group(1)

    # AWS -- bucket name lives on the dataset, account is implicit; nothing useful
    # to return at the linked-service level for S3 / GCS.
    return None


def _is_physical(value: str) -> bool:
    """Return ``True`` only when *value* is a literal (physical) identifier.

    A value is NOT physical when it still contains an unresolved marker -- a
    DAB-ref placeholder (``{{`` ... ``}}``), a leftover ADF interpolation
    fragment (``@{``), a bare ADF expression (starts with ``@``), or an ARM
    template expression the export left unevaluated (``[parameters('x')]``,
    ``[concat(...)]``). An ARM expression always opens with a function call, so
    bracket-quoted SQL names such as ``[dbo].[Orders]`` stay physical, and so
    does ARM's ``[[`` escape for a literal that starts with ``[``.
    """
    stripped = value.strip()
    if "{{" in value or "@{" in value or stripped.startswith("@"):
        return False
    return not _ARM_EXPRESSION_RE.match(stripped)


# --------------------------------------------------------------------------- #
# Structural path signature (tier 2, #36's "expression" tier)
# --------------------------------------------------------------------------- #


def _path_signature(parameters: dict[str, Any] | None) -> str | None:
    """Structural signature of a reference's parameterised folderPath / fileName.

    Requires a resolvable **folderPath** literal anchor. A file name alone is too
    weak a discriminator: many unrelated activities write ``.csv`` / ``.json`` files
    to opaque parameterised folders, so a signature built only from a file extension
    would join them all -- re-creating the explosion the identity-only join avoids.
    Anchoring on the literal folder segment keeps the match specific to a real,
    named location.

    ``None`` when the folder path has no literal segment to anchor on.
    """
    if not parameters:
        return None
    folder_signature = _normalize_path_expression(parameters.get("folderPath"))
    if folder_signature is None:
        return None
    file_signature = _normalize_path_expression(parameters.get("fileName"))
    return f"FP[{folder_signature}]/FN[{file_signature}]"


def _normalize_path_expression(expression: Any) -> str | None:
    """Reduce a (possibly parameterised) ADF path expression to a structural signature.

    Keeps the literal path segments and collapses every runtime reference to a
    single ``<P>`` slot, so the result captures the path *shape* (literal skeleton
    plus slot count) without guessing the runtime value. Returns ``None`` when
    there is no literal segment to anchor on (a signature of only slots is too weak
    a join key -- never guess).
    """
    if isinstance(expression, dict):
        expression = expression.get("value", "")
    if not isinstance(expression, str) or not expression.strip():
        return None
    text = expression.strip()
    if "@" not in text:
        # A bare literal value (no ADF expression): the whole string is the literal
        # path / filename, with no runtime slots.
        literal = re.sub(r"/+", "/", text).strip("/")
        return f"{literal}|slots=0" if literal else None
    marked = _PARAM_REF_RE.sub("<P>", text)
    literal = re.sub(r"/+", "/", "".join(_QUOTED_LITERAL_RE.findall(marked))).strip("/")
    if not literal:
        return None
    return f"{literal}|slots={marked.count('<P>')}"


# --------------------------------------------------------------------------- #
# Asset-type classification (best-effort neutral kind)
# --------------------------------------------------------------------------- #

_TABLE_HINTS = ("table", "sql", "database")
_FILE_HINTS = ("delimited", "parquet", "orc", "avro", "json", "binary", "excel", "xml", "blob", "adls", "file")


def _asset_type(dataset_ref: AdfDatasetReference, definitions: AdfDefinitions) -> str | None:
    """Best-effort neutral asset kind (``"table"`` / ``"file"``), or ``None``.

    Prefers the shape of the dataset's typeProperties (a ``location`` block means a
    file, a ``table`` / ``schema`` means a table) and falls back to keyword hints in
    the dataset's ADF type string. Returns ``None`` when nothing is conclusive
    rather than guessing.
    """
    dataset = definitions.get_dataset(dataset_ref.reference_name)
    if dataset is None:
        return None
    type_props = dataset.properties.get("typeProperties")
    if isinstance(type_props, dict):
        if isinstance(type_props.get("location"), dict):
            return "file"
        if type_props.get("table") or type_props.get("tableName") or type_props.get("schema"):
            return "table"
    dataset_type = (dataset.type or "").lower()
    if any(hint in dataset_type for hint in _TABLE_HINTS):
        return "table"
    if any(hint in dataset_type for hint in _FILE_HINTS):
        return "file"
    return None
