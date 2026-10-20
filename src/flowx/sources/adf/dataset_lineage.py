"""Resolve ADF dataset references into source-neutral :class:`DataAsset` values.

Each read or write gets two keys, and :mod:`flowx.lineage` joins on them:

* **identity**, the strong key: the physical table (``<server>/schema.table``) or
  file path (``abfss://...`` down to the file name). It is set only when every part
  is a literal; anything parameterised or unresolved leaves it ``None``.
* **signature**, the weak key. It equals the identity when there is one. Otherwise
  it is the structural path signature (:func:`_path_signature`): the folder and file
  name with literal text kept and one ``<P>`` slot per run-time value, prefixed
  ``ST[<store>]/`` when the store is known. It is empty when nothing literal anchors
  the path, when the activity overrides the dataset's location at run time, and it
  is never the bare dataset name, so two opaque references never join by name.
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
_ARM_CALL_RE = re.compile(r"^\[\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\s*\(.*\]$", re.DOTALL)
# ARM template functions (compared lower-cased, as ARM does). A bracketed value is an unevaluated ARM
# expression only when it calls one of these, a ``list*`` function, or a ``namespace.function`` of the
# template's own, so a bracket-quoted SQL name such as ``[Sales (2024)]`` stays a literal.
_ARM_FUNCTIONS = frozenset(
    name.lower()
    for name in (
        "add and array base64 base64ToJson base64ToString bool cidrHost cidrSubnet coalesce concat contains "
        "copyIndex createArray createObject dataUri dataUriToString dateTimeAdd dateTimeFromEpoch "
        "dateTimeToEpoch deployer deployment div empty endsWith environment equals extensionResourceId "
        "filter first float format greater greaterOrEquals groupBy guid if indexFromEnd indexOf int "
        "intersection items join json last lastIndexOf length less lessOrEquals managementGroup "
        "managementGroupResourceId map mapValues max min mod mul newGuid not null objectKeys or padLeft "
        "parameters parseCidr pickZones providers range reduce reference references replace resourceGroup "
        "resourceId shallowMerge skip sort split startsWith string sub subscription subscriptionResourceId "
        "substring take tenant tenantResourceId toLower toObject toUpper trim tryGet tryIndexFromEnd union "
        "uniqueString uri uriComponent uriComponentToString utcNow variables"
    ).split()
)
_RUN_TIME_PATH_OVERRIDES = ("wildcardFolderPath", "wildcardFileName", "fileListPath", "prefix")
# Location types (lower-cased) that name no store of their own: the server, share, lakehouse or
# account is set on the linked service.
_LINKED_SERVICE_STORE_LOCATIONS = frozenset(
    {
        "sftplocation",
        "fileserverlocation",
        "ftpserverlocation",
        "azurefilestoragelocation",
        "hdfslocation",
        "httpserverlocation",
        "lakehouselocation",
        "azuredatalakestorelocation",
    }
)

_QUOTED_LITERAL_RE = re.compile(r"'((?:[^']|'')*)'", re.DOTALL)
_CONCAT_CALL_RE = re.compile(r"concat\s*\((?P<arguments>.*)\)", re.IGNORECASE | re.DOTALL)
_DATASET_PARAMETER_RE = re.compile(r"dataset\(\)\??\.(\w+)")
_NUMBER_LITERAL_RE = re.compile(r"-?\d+(?:\.\d+)?")
_RUNTIME_SLOT_RE = re.compile(r"<P(?::(?:[^<>]|<P>)*)?>")


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #


def activity_data_assets(
    activity: AdfActivity,
    definitions: AdfDefinitions,
    context: TranslationContext | None = None,
) -> tuple[list[DataAsset], list[DataAsset]]:
    """Resolve the assets an ADF activity reads and writes, as ``(data_reads, data_writes)``.

    Every input and output counts, not just the first: a Copy's ``source`` and
    ``sink`` datasets, a Lookup's ``dataset``, and any activity-level ``inputs`` /
    ``outputs``. A dataset named in two of those places counts once per side.

    When the activity replaces the dataset's location at run time (a source query or
    stored procedure, a ``storeSettings`` wildcard, file list or prefix on a read; a
    sink stored procedure on a write), that side gets neither an identity nor a
    signature, since the dataset's location is not what it touches. *context* is the
    translation context for expressions; an empty one is used by default.
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
    """``True`` when *settings* is a dict with a non-empty value under a key *is_override_key* accepts.

    An Expression object counts by its ``value``, so ``{"type": "Expression", "value": ""}``
    is as empty as a bare ``""``.
    """
    return isinstance(settings, dict) and any(
        _expression_value(value) and is_override_key(key) for key, value in settings.items()
    )


def _expression_value(value: Any) -> Any:
    """*value* itself, or the ``value`` of an ADF Expression object (``None`` when it has none)."""
    return value.get("value") if isinstance(value, dict) else value


def _dataset_ref_to_asset(
    dataset_ref: AdfDatasetReference,
    definitions: AdfDefinitions,
    context: TranslationContext,
    *,
    location_overridden: bool,
) -> DataAsset:
    """Turn one dataset reference into a two-tier :class:`DataAsset`.

    The signature mirrors the identity when there is one, else it is the store-qualified
    path signature, else empty. It is never the bare dataset name, so two opaque
    references cannot join by name; an empty signature joins nothing but still
    records the read or write. With *location_overridden* neither key is derived.
    A dataset that names its store but cannot prove it (see
    :func:`_resolve_dataset_store`) gets no signature, because a folder shape alone
    cannot tell two such stores apart.
    """
    if location_overridden:
        return DataAsset(signature="", identity=None, asset_type=_asset_type(dataset_ref, definitions))
    identity = resolve_dataset_identity(dataset_ref, definitions, context)
    if identity is not None:
        # Mirror the identity into the signature so the weak tier never joins a
        # resolved asset to an unresolved one that merely shares a physical value.
        return DataAsset(signature=identity, identity=identity, asset_type=_asset_type(dataset_ref, definitions))
    path_signature = _path_signature(dataset_ref, definitions)
    if path_signature is not None:
        store, location_names_store = _resolve_dataset_store(dataset_ref, definitions, context)
        if store:
            path_signature = f"ST[{store}]/{path_signature}"
        elif location_names_store:
            path_signature = None
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
    """Physical identity for a dataset reference, or ``None`` when it is not provable.

    A table is ``<store>/schema.table``, where the store is the linked service's
    server (and database), or its name when the server is hidden in a secret, so a
    source ``dbo.Orders`` and a warehouse ``dbo.Orders`` stay apart. A file is its
    path down to the literal file name. Any ADF expression or DAB placeholder left
    unresolved, a parameterised linked service included, gives ``None``: two
    pipelines sharing a parameter name must never collide on the placeholder text.
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

    stripped = text.strip()
    match = _DATASET_PARAMETER_RE.fullmatch(stripped[1:]) if stripped.startswith("@") else None
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
    """Effective parameter map: declared dataset defaults first, call-site overrides win.

    Declared or call-site ``parameters`` that are not a map (a list, say) bind nothing,
    rather than crashing the whole discover run.
    """
    declared = dataset_props.get("parameters")
    effective: dict[str, Any] = {}
    if isinstance(declared, dict):
        for name, spec in declared.items():
            if isinstance(spec, dict) and "defaultValue" in spec:
                effective[name] = spec["defaultValue"]
    effective.update(_call_site_parameters(dataset_ref))
    return effective


def _call_site_parameters(dataset_ref: AdfDatasetReference) -> dict[str, Any]:
    """The parameters the reference passes, or an empty map when they are missing or not a map."""
    return dict(dataset_ref.parameters) if isinstance(dataset_ref.parameters, dict) else {}


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
    """Return the dataset's linked service name and the parameters its reference binds.

    The name is ``""`` unless it is a plain name: an unevaluated ``[parameters('ls')]``
    or any other expression names no linked service, even if a definition happens to be
    loaded under that key, so it can never prove a store.
    """
    linked_service_reference = dataset_props.get("linkedServiceName") or {}
    if isinstance(linked_service_reference, dict):
        name = linked_service_reference.get("referenceName")
        parameters = linked_service_reference.get("parameters")
    else:
        name, parameters = linked_service_reference, None
    if not isinstance(name, str) or not name or not _is_physical(name):
        return "", {}
    return name, parameters if isinstance(parameters, dict) else {}


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
    connection_string = _expression_value(type_props.get("connectionString"))
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
    for each binding. A name that is not plain, such as an unevaluated
    ``[parameters('ls')]``, gives ``None`` too.
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
) -> tuple[str | None, bool]:
    """Return ``(store, location_names_store)`` for a dataset; the store is ``None`` unless provable.

    * A table's store is the one :func:`_resolve_table_store` names.
    * A file's store is ``<file system>@<account>``. When the account is hidden (a Key
      Vault or masked connection string), a plain, unparameterised linked service
      name stands in for it.
    * A location whose server, share, lakehouse or account lives on the linked
      service (SFTP, FTP, a file share, HDFS, HTTP, a Fabric Lakehouse, ADLS Gen1;
      type compared case-insensitively) is named by that linked service name alone.

    ``location_names_store`` says whether the location tells its store apart at all,
    provable or not: a literal file system, container or bucket, or one of those
    linked-service locations. A parameterised file system names no store.
    """
    properties = _dataset_props(dataset_ref, definitions)
    if properties is None:
        return None, False
    type_props = properties.get("typeProperties") or properties
    location = type_props.get("location")
    file_system: str | None = None
    account: str | None = None
    location_names_store = False
    if isinstance(location, dict):
        location_type = location.get("type")
        location_names_store = (
            isinstance(location_type, str) and location_type.lower() in _LINKED_SERVICE_STORE_LOCATIONS
        )
        if not location_names_store:
            file_system, account = _resolve_file_store(
                location,
                _backing_linked_service(properties, definitions),
                _effective_dataset_params(dataset_ref, properties),
                context,
            )
            location_names_store = file_system is not None
    _, table = _resolve_table_reference(dataset_ref, properties, context)
    if table:
        return _resolve_table_store(properties, definitions), location_names_store
    if not location_names_store:
        return None, False
    if file_system is None:
        return _unparameterised_linked_service_name(properties, definitions), True
    store = account or _unparameterised_linked_service_name(properties, definitions)
    return (f"{file_system}@{store}" if store else None), True


def _resolve_file_store(
    location: dict[str, Any],
    linked_service: Any,
    dataset_params: dict[str, Any],
    context: TranslationContext,
) -> tuple[str | None, str | None]:
    """Resolve a file dataset's ``(file system, storage account)``.

    The file system is the ADLS ``fileSystem``, the Blob ``container``, or the S3 /
    GCS ``bucketName``. Each part is ``None`` unless it resolves to a literal. The
    path identity and the signature's store qualifier both read the store here, so
    they always agree on what counts as a known file system and account.
    """
    file_system = _resolve_param_value(
        location.get("fileSystem") or location.get("container") or location.get("bucketName"), dataset_params, context
    )
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

    Blob and ADLS Gen2 datasets share one ``abfss://<container>@<account>.dfs...``
    form on purpose. A storage account name is unique across Azure and both
    connectors are endpoints onto that one account, so a Blob writer and an ADLS
    Gen2 reader of the same container and path touch the same file and should link.
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
    return not _is_arm_expression(stripped)


def _is_arm_expression(text: str) -> bool:
    """``True`` when *text* is an unevaluated ARM template expression such as ``[parameters('x')]``."""
    call = _ARM_CALL_RE.match(text)
    if call is None:
        return False
    function = call.group(1).lower()
    return "." in function or function.startswith("list") or function in _ARM_FUNCTIONS


# --------------------------------------------------------------------------- #
# Structural path signature (tier 2)
# --------------------------------------------------------------------------- #
#
# The rule: only provably literal text forms the path. Any other value, whatever
# function or reference it is, is one placeholder slot where it sits, so no link is
# ever made from a non-literal value standing in for a folder. A function over a bound
# dataset() value is a slot keyed by that value, never path text.
# tests/unit/test_adf_path_signature_corpus.py checks this over generated
# expressions: add a new ADF construct there rather than patching the shape for it.


def _path_signature(dataset_ref: AdfDatasetReference, definitions: AdfDefinitions) -> str | None:
    """Structural signature of the folder and file name a parameterised reference points at.

    Both of these must hold, or the result is ``None``:

    1. The reference passes a ``folderPath`` at the call site with literal text in it:
       quoted text or a number in an expression, or text outside ``@{...}``, so
       ``landing/@{item().run}`` qualifies just as ``@concat('landing/', item().run)``
       does. The dataset's own location is not enough on its own: a reference that
       passes no anchored ``folderPath`` gets no signature. A file name alone never
       qualifies, since many unrelated activities write ``.csv`` files to opaque folders.
    2. The folder the reference resolves to has a literal segment. That folder is the
       dataset's own ``location.folderPath`` with each ``dataset().<name>`` filled in
       from the reference's parameters (call-site values, then declared defaults), so
       ``bronze/`` and ``silver/`` datasets called with the same value stay apart. When
       the location sets no folder, or the dataset is not in the export, the call-site
       ``folderPath`` is used instead.

    The file name is read the same way; one with no literal text gives ``FN[]``.
    """
    call_site = _call_site_parameters(dataset_ref)
    passed_folder = _path_shape(call_site.get("folderPath"), {}, require_physical=True)
    if passed_folder is None or not _has_literal_anchor(passed_folder):
        return None
    properties = _dataset_props(dataset_ref, definitions) or {}
    type_props = properties.get("typeProperties") or properties
    written_location = type_props.get("location")
    location: dict[str, Any] = written_location if isinstance(written_location, dict) else {}
    bindings = {
        name: value
        for name, value in _effective_dataset_params(dataset_ref, properties).items()
        if _reads_as_text(value)
    }

    def _shape_of(key: str, *, require_physical: bool) -> str | None:
        written = _expression_value(location.get(key))
        if isinstance(written, str) and written.strip():
            return _path_shape(written, bindings, require_physical=require_physical)
        return _path_shape(call_site.get(key), {}, require_physical=require_physical)

    folder_shape = _shape_of("folderPath", require_physical=True)
    if folder_shape is None or not _has_literal_anchor(folder_shape):
        return None
    file_shape = _shape_of("fileName", require_physical=False)
    names_a_file = file_shape and (_has_literal_anchor(file_shape) or "<P:" in file_shape)
    return f"FP[{folder_shape}]/FN[{file_shape if names_a_file else ''}]"


def _reads_as_text(value: Any) -> bool:
    """``True`` when a bound parameter *value* is text ADF can splice into a path, even ``""``.

    That is a string, number or boolean, given bare or as an Expression's ``value``. A
    ``null``, a list, or an Expression with no ``value`` says nothing about the path,
    so it is no more known than a parameter the call site never bound.
    """
    return isinstance(_expression_value(value), (str, bool, int, float))


def _path_shape(expression: Any, bindings: dict[str, Any], *, require_physical: bool) -> str | None:
    """Reduce a (possibly parameterised) ADF path expression to its structural shape.

    Keeps the literal path text in place and writes ``<P>`` where each runtime value
    sits, so ``landing/<P>`` and ``<P>/landing`` stay apart, without guessing the
    runtime value. A ``dataset().<name>`` bound in *bindings* is replaced by its bound
    value's shape instead. Repeated slashes collapse, and leading and trailing ones go,
    only once the whole shape is put together, so a bound ``bronze/`` followed by
    ``orders`` reads ``bronze/orders``. Gives ``""`` for a missing or empty value; see
    :func:`_raw_path_shape` for how each part of the value is read.
    """
    shape = _raw_path_shape(expression, bindings, require_physical=require_physical)
    return re.sub(r"/+", "/", shape).strip("/") if shape is not None else None


def _raw_path_shape(expression: Any, bindings: dict[str, Any], *, require_physical: bool) -> str | None:
    """Shape of an ADF path value with its slashes left exactly as written.

    A value that opens with ``@`` (but not ``@{``) is one whole ADF expression, so a
    quoted ``'@{x}'`` inside it is text. A value holding ``@{`` is interpolated: the
    text outside each ``@{...}`` is literal and each block is an expression. A value
    that opens with ``@@`` is the literal text after the first ``@``, as ADF reads it.
    A number or boolean is its text. Any other value, and an unevaluated ARM
    expression, is kept verbatim.

    With *require_physical*, an unevaluated ARM expression such as
    ``[parameters('folder')]`` or a ``{{`` placeholder, here or in a bound value,
    gives ``None``, even when a quoted literal inside it holds ``@``; an ADF ``@``
    expression is still reduced to its shape. Without it that value is kept verbatim,
    and so is an ARM expression holding a quoted ``@``, so two different ones never
    match.
    """
    if isinstance(expression, dict):
        expression = expression.get("value", "")
    if isinstance(expression, (bool, int, float)):
        return str(expression)
    if not isinstance(expression, str):
        return ""
    text = expression.strip()
    is_arm_expression = bool(_is_arm_expression(text))
    if require_physical and ("{{" in text or is_arm_expression):
        return None
    if is_arm_expression:
        return text
    if text.startswith("@@"):
        return text[1:]
    if text.startswith("@") and not text.startswith("@{"):
        return _expression_shape(text[1:], bindings, require_physical=require_physical)
    if "@{" in text:
        return _interpolation_shape(text, bindings, require_physical=require_physical)
    return text


def _interpolation_shape(text: str, bindings: dict[str, Any], *, require_physical: bool) -> str | None:
    """Shape of an interpolated string: text outside ``@{...}`` is literal, each ``@{...}`` is an expression."""
    pieces: list[str] = []
    literal_start = 0
    for block_start, block_end in _interpolation_blocks(text):
        expression_shape = _expression_shape(
            text[block_start + 2 : block_end - 1], bindings, require_physical=require_physical
        )
        if expression_shape is None:
            return None
        pieces.extend((text[literal_start:block_start], expression_shape))
        literal_start = block_end
    pieces.append(text[literal_start:])
    return "".join(pieces)


def _interpolation_blocks(text: str) -> list[tuple[int, int]]:
    """The ``(start, end)`` of each ``@{...}`` block in *text*, where a ``}`` inside quoted text does not close one.

    An ``@{`` with no closing ``}`` is left as literal text, as ADF would print it.
    """
    blocks: list[tuple[int, int]] = []
    search_from = 0
    while (block_start := text.find("@{", search_from)) != -1:
        in_quote = False
        for index in range(block_start + 2, len(text)):
            if text[index] == "'":
                in_quote = not in_quote
            elif text[index] == "}" and not in_quote:
                blocks.append((block_start, index + 1))
                search_from = index + 1
                break
        else:
            break
    return blocks


def _expression_shape(expression: str, bindings: dict[str, Any], *, require_physical: bool) -> str | None:
    """Shape of one ADF expression body, read so that no run-time value is ever dropped.

    A quoted ``'text'`` (``''`` is an escaped quote) is that text, and a number is its
    digits. ``concat(...)`` is its arguments' shapes joined in order. A
    ``dataset().<name>`` (or ``dataset()?.<name>``) bound in *bindings* is its bound
    value's shape, slashes and all. Anything else -- any other function call,
    ``pipeline()``, ``item()``, ``activity(...)``, a subscript -- is one slot (see
    :func:`_runtime_slot`), so none of its argument text ever reads as part of the
    path.
    """
    expression = expression.strip()
    literal = _QUOTED_LITERAL_RE.fullmatch(expression)
    if literal is not None:
        return literal.group(1).replace("''", "'")
    if _NUMBER_LITERAL_RE.fullmatch(expression):
        return expression
    arguments = _concat_arguments(expression)
    if arguments is not None:
        pieces: list[str] = []
        for argument in arguments:
            argument_shape = _expression_shape(argument, bindings, require_physical=require_physical)
            if argument_shape is None:
                return None
            pieces.append(argument_shape)
        return "".join(pieces)
    dataset_parameter = _DATASET_PARAMETER_RE.fullmatch(expression)
    if dataset_parameter is None or dataset_parameter.group(1) not in bindings:
        return _runtime_slot(expression, bindings, require_physical=require_physical)
    return _raw_path_shape(bindings[dataset_parameter.group(1)], {}, require_physical=require_physical)


def _runtime_slot(expression: str, bindings: dict[str, Any], *, require_physical: bool) -> str | None:
    """The one slot a run-time expression leaves, keyed by any bound ``dataset()`` value it uses.

    A plain run-time value is ``<P>``. A call over a ``dataset().<name>`` the call site
    bound, such as ``toLower(dataset().folderPath)`` called with ``orders``, is
    ``<P:tolower('orders')>``: still one slot that is never path text, but two of them
    match only when the call and its bound inputs match, so ``orders`` and
    ``customers`` never join. ``None`` when a bound value is not physical.
    """
    pieces: list[str] = []
    piece_start = 0
    for match in _DATASET_PARAMETER_RE.finditer(expression):
        if match.group(1) not in bindings or expression.count("'", 0, match.start()) % 2:
            continue
        bound_shape = _raw_path_shape(bindings[match.group(1)], {}, require_physical=require_physical)
        if bound_shape is None:
            return None
        quoted_bound_shape = bound_shape.replace("'", "''")
        pieces.extend((expression[piece_start : match.start()], f"'{quoted_bound_shape}'"))
        piece_start = match.end()
    if not pieces:
        return "<P>"
    pieces.append(expression[piece_start:])
    return f"<P:{_canonical_slot_key(''.join(pieces))}>"


def _canonical_slot_key(text: str) -> str:
    """*text* lower-cased and without whitespace outside quotes.

    So neither spacing nor the case of an ADF function name (ADF reads ``toLower`` and
    ``TOLOWER`` alike) tells two keyed slots apart; quoted text keeps its case.
    """
    return "".join(
        part if index % 2 else re.sub(r"\s+", "", part).lower()
        for index, part in enumerate(re.split(r"('(?:[^']|'')*')", text))
    )


def _concat_arguments(expression: str) -> list[str] | None:
    """The top-level arguments of *expression* when it is exactly one ``concat(...)`` call, else ``None``.

    Commas split arguments only outside quotes and outside any nested parentheses or
    brackets, so ``concat('a,b', f(x, y))`` has two arguments.
    """
    match = _CONCAT_CALL_RE.fullmatch(expression)
    if match is None:
        return None
    body = match.group("arguments")
    arguments: list[str] = []
    argument_start = 0
    depth = 0
    in_quote = False
    for index, character in enumerate(body):
        if character == "'":
            in_quote = not in_quote
        elif in_quote:
            continue
        elif character in "([":
            depth += 1
        elif character in ")]":
            depth -= 1
            if depth < 0:
                return None
        elif character == "," and depth == 0:
            arguments.append(body[argument_start:index])
            argument_start = index + 1
    if in_quote or depth:
        return None
    arguments.append(body[argument_start:])
    return arguments


def _has_literal_anchor(shape: str) -> bool:
    """``True`` when *shape* holds literal text besides slots and slashes; only slots is too weak a key.

    A keyed slot such as ``<P:tolower('orders')>`` is still a slot, so its key never counts as literal text.
    """
    return bool(_RUNTIME_SLOT_RE.sub("", shape).replace("/", "").strip())


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
