"""Resolve ADF dataset references into source-neutral :class:`DataAsset` values.

This is the ADF half of data-lineage population (#62b). It re-homes the
dataset-identity resolver and the path-signature logic first written for the
closed #36 ADF-only lineage attempt, but emits the shared #61 two-tier
:class:`~flowx.models.ir.DataAsset` (``identity`` + ``signature``) instead of a
bespoke edge type, so the source-neutral join in :mod:`flowx.lineage` /
:mod:`flowx.discovery_lineage` does the matching.

Two tiers, exactly as #36 established them:

* **identity** -- the resolved physical location of the asset (``schema.table`` or
  a concrete ``abfss://`` path). Present only when it resolves *deterministically*
  from literals; a parameterised reference is never guessed at and leaves
  ``identity`` unset. This is the strong join key.
* **signature** -- the *path-derived* weak key. For an asset with a resolved
  identity the signature mirrors that identity, so the weak tier can never join a
  resolved asset to an unresolved one on a coincidence. For an unresolved reference
  the signature is the *structural path signature* (#36's "expression" tier: the
  literal path skeleton plus its parameter-slot count) when the path has a literal
  anchor. It is **never** the bare dataset reference name: two unrelated opaque
  references that merely share a name must not join (#36's explicit rule), so when
  neither a physical identity nor a path-anchored signature is available the
  signature is left empty and the asset cannot participate in signature matching.

Only literal, provable values ever become an ``identity`` -- the resolver returns
``None`` rather than guessing, which is what stopped #36's spurious edges.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDatasetReference, AdfDefinitions
from flowx.models.ir import DataAsset, TranslationContext
from flowx.parser.expression_parser import resolve_expression, resolve_interpolated_string

_ACCOUNT_NAME_RE = re.compile(r"AccountName=([A-Za-z0-9]+)", re.IGNORECASE)
_DATASET_PARAM_RE = re.compile(r"^@dataset\(\)\.([A-Za-z_][A-Za-z0-9_]*)$")

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
    reads = [
        _dataset_ref_to_asset(reference, definitions, resolution_context)
        for reference in _activity_dataset_refs(activity, produced=False)
    ]
    writes = [
        _dataset_ref_to_asset(reference, definitions, resolution_context)
        for reference in _activity_dataset_refs(activity, produced=True)
    ]
    return reads, writes


def _dataset_ref_to_asset(
    dataset_ref: AdfDatasetReference,
    definitions: AdfDefinitions,
    context: TranslationContext,
) -> DataAsset:
    """Turn one dataset reference into a two-tier :class:`DataAsset`.

    Signature is derived only from the resolved *physical* location -- the identity
    when it resolves, else the structural path signature. It is **never** the bare
    dataset reference name (#36's hard rule): two unrelated opaque references that
    merely share a name must not join, so when neither a physical identity nor a
    path-anchored signature is available the signature is left empty. An empty
    signature is falsy, so :func:`~flowx.lineage._match_assets` cannot use it as a
    join key -- the asset is still captured as a read / write for reporting, it just
    cannot manufacture a signature-tier edge.
    """
    identity = resolve_dataset_identity(dataset_ref, definitions, context)
    if identity is not None:
        # Mirror the identity into the signature so the weak tier never joins a
        # resolved asset to an unresolved one that merely shares a physical value.
        return DataAsset(signature=identity, identity=identity, asset_type=_asset_type(dataset_ref, definitions))
    path_signature = _path_signature(dataset_ref.parameters)
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

    Returns ``"schema.table"`` when a table is resolvable, else a storage path,
    else ``None`` (never a guess). Used to join producers to consumers on the same
    physical asset even when their ADF dataset names differ.

    Parameterised values (ADF expressions or DAB-ref placeholders) are treated as
    unresolvable and return ``None`` -- they must never be used as identity keys
    because two unrelated pipelines sharing a parameter name would collide on the
    same placeholder string.
    """
    resolution_context = context if context is not None else TranslationContext()
    properties = _dataset_props(dataset_ref, definitions)
    if properties is None:
        return None
    schema, table = _resolve_table_reference(dataset_ref, properties, resolution_context)
    if table:
        identity = f"{schema}.{table}" if schema else table
        return identity if _is_physical(identity) else None
    path = _resolve_dataset_path(properties, definitions)
    return path if (path and _is_physical(path)) else None


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


def _resolve_dataset_path(dataset_props: dict[str, Any], definitions: AdfDefinitions) -> str | None:
    """Resolve a dataset's storage path from its location + backing linked service."""
    type_props = dataset_props.get("typeProperties") or dataset_props
    location = type_props.get("location") or {}
    if not isinstance(location, dict):
        return None

    file_system = location.get("fileSystem") or location.get("container") or ""
    folder_path = location.get("folderPath") or ""
    if isinstance(file_system, dict) or isinstance(folder_path, dict):
        return None  # parameterised location; not a deterministic identity

    linked_service_reference = dataset_props.get("linkedServiceName") or {}
    if isinstance(linked_service_reference, dict):
        linked_service_name = linked_service_reference.get("referenceName", "")
    else:
        linked_service_name = str(linked_service_reference)
    linked_service = definitions.get_linked_service(linked_service_name) if linked_service_name else None
    account = _resolve_storage_account(linked_service)
    if not account:
        return None

    return f"abfss://{file_system}@{account}.dfs.core.windows.net/{folder_path}".rstrip("/")


def _resolve_storage_account(linked_service: Any) -> str | None:
    """Pull a storage account name out of a linked service, if present."""
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

    # Plaintext connection string (rare in az exports -- usually masked).
    connection_string = type_props.get("connectionString")
    if isinstance(connection_string, str):
        match = _ACCOUNT_NAME_RE.search(connection_string)
        if match:
            return match.group(1)
    if isinstance(connection_string, dict):
        value = connection_string.get("value", "")
        match = _ACCOUNT_NAME_RE.search(value)
        if match:
            return match.group(1)

    # AWS -- bucket name lives on the dataset, account is implicit; nothing useful
    # to return at the linked-service level for S3 / GCS.
    return None


def _is_physical(value: str) -> bool:
    """Return ``True`` only when *value* is a literal (physical) identifier.

    A value is NOT physical when it still contains an unresolved marker -- a
    DAB-ref placeholder (``{{`` ... ``}}``), a leftover ADF interpolation
    fragment (``@{``), or a bare ADF expression (starts with ``@``).
    """
    stripped = value.lstrip()
    return not ("{{" in value or "@{" in value or stripped.startswith("@"))


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
