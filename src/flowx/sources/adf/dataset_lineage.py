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

The identity lives in :mod:`~flowx.sources.adf.dataset_identity`, the store that
qualifies a signature in :mod:`~flowx.sources.adf.dataset_store`, and the
signature itself in :mod:`~flowx.sources.adf.path_signature`. This module puts
them together for each read and write.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator

from flowx.models.adf_ast import AdfActivity, AdfDatasetReference, AdfDefinitions
from flowx.models.ir import DataAsset, TranslationContext
from flowx.sources.adf.dataset_identity import (
    _expression_value,
    resolve_dataset_identity,
)
from flowx.sources.adf.dataset_store import _resolve_dataset_store
from flowx.sources.adf.path_signature import _path_signature

__all__ = ["activity_data_assets", "resolve_dataset_identity"]


_RUN_TIME_PATH_OVERRIDES = ("wildcardFolderPath", "wildcardFileName", "fileListPath", "prefix")


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
