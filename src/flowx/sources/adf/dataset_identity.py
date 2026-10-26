"""Physical identity of an ADF dataset (the strong lineage key), and the value helpers it shares.

An identity is a store-qualified table (``<server>/schema.table``) or a file path
(``abfss://<container>@<account>.dfs.core.windows.net/...``) built only from
literals. Anything parameterised or unresolved gives ``None`` rather than a guess.
"""

from __future__ import annotations

import re
from typing import Any

from flowx.models.adf_ast import AdfDatasetReference, AdfDefinitions
from flowx.models.ir import TranslationContext
from flowx.parser.expression_parser import resolve_expression, resolve_interpolated_string

_ACCOUNT_NAME_RE = re.compile(r"AccountName=([^;]*)", re.IGNORECASE)
_STORAGE_ACCOUNT_RE = re.compile(r"[A-Za-z0-9]+")
# The object types whose ``value`` is the field's own value; any other typed object is left whole.
_VALUE_OBJECT_TYPES = frozenset({"Expression", "SecureString"})
_ARM_CALL_RE = re.compile(
    r"^\[\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\s*\((?P<arguments>.*)\]$", re.DOTALL
)
_NESTED_CALL_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\s*\(")


# ARM template functions (compared lower-cased, as ARM does). A bracketed call is an unevaluated ARM
# expression when it calls one of these, a ``list*`` function, or a ``namespace.function`` of the
# template's own, or when its arguments hold a quoted string or another call, as only ARM's do. So a
# bracket-quoted SQL name such as ``[Sales (2024)]`` stays a literal.
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


_DATASET_PARAMETER_RE = re.compile(r"dataset\(\)\??\.(\w+)")


def _expression_value(value: Any) -> Any:
    """*value* itself, or the ``value`` of an ADF Expression-shaped object (``None`` when it has none).

    Only an object typed ``Expression`` or ``SecureString``, or an untyped one carrying a
    ``value``, is unwrapped. Any other object, such as a Key Vault secret reference or a
    type ADF does not define, is returned whole, so a caller never mistakes it for an
    empty value or reads a value it cannot vouch for.
    """
    if isinstance(value, dict) and (
        value.get("type") in _VALUE_OBJECT_TYPES or ("type" not in value and "value" in value)
    ):
        return value.get("value")
    return value


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
    A ``server`` or ``database`` wrapped in an Expression or SecureString object is
    read from its ``value`` and checked like a plain string. Neither name can hold
    ``*``, so one that does is a masked secret (an export writes ``**********``). When the connection string is hidden
    (a Key Vault reference or a masked secret) the linked service name stands in for
    the server. A parameterised or masked server or database, a parameterised
    connection string, or a linked service that takes parameters, gives ``None``:
    each binding may point at a different store, so no single name identifies it.
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

    server = (
        _expression_value(type_props.get("server"))
        or connection_fields.get("server")
        or connection_fields.get("data source")
    )
    database = (
        _expression_value(type_props.get("database"))
        or connection_fields.get("database")
        or connection_fields.get("initial catalog")
    )
    store_parts = [part for part in (server, database) if part]
    if not all(
        isinstance(part, str) and part.strip() and _is_physical(part) and "*" not in part for part in store_parts
    ):
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

    Each of ``url``, ``sasUri`` and the connection string is read from its ``value``
    when wrapped in an Expression or SecureString object. The whole of it must be
    physical before the account is cut out of it: cutting first would drop the
    closing ``]`` of an ARM expression or the tail of an ``@{...}`` and leave a
    fragment that looks literal. A storage account name is only letters and digits,
    so a cut that holds anything else (a masked ``*``, a ``-``) names no account.
    """
    if linked_service is None:
        return None
    type_props = linked_service.properties.get("typeProperties") or linked_service.properties

    url = _expression_value(type_props.get("url")) or ""
    if isinstance(url, str) and url:
        if not _is_physical(url):
            return None
        host = url.replace("https://", "").split("/", 1)[0]
        host_no_port = host.split(":", 1)[0]
        if "." in host_no_port:
            return _storage_account_name(host_no_port.split(".", 1)[0])

    sas_uri = _expression_value(type_props.get("sasUri")) or ""
    if isinstance(sas_uri, str) and sas_uri:
        if not _is_physical(sas_uri):
            return None
        host = sas_uri.split("?", 1)[0].replace("https://", "").split("/", 1)[0]
        if "." in host:
            return _storage_account_name(host.split(".", 1)[0])

    # Plaintext connection string (rare in az exports -- usually masked).
    connection_string = _expression_value(type_props.get("connectionString"))
    if isinstance(connection_string, str):
        if not _is_physical(connection_string):
            return None
        match = _ACCOUNT_NAME_RE.search(connection_string)
        if match:
            return _storage_account_name(match.group(1).strip())

    # AWS -- bucket name lives on the dataset, account is implicit; nothing useful
    # to return at the linked-service level for S3 / GCS.
    return None


def _storage_account_name(text: str) -> str | None:
    """*text* when it can be a storage account name (letters and digits only), else ``None``."""
    return text if _STORAGE_ACCOUNT_RE.fullmatch(text) else None


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
    if "." in function or function.startswith("list") or function in _ARM_FUNCTIONS:
        return True
    arguments = call.group("arguments")
    return "'" in arguments or bool(_NESTED_CALL_RE.search(arguments))
