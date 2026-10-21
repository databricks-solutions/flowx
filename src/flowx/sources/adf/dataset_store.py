"""The store an ADF dataset lives in, used to qualify its path signature.

Two datasets on different containers, accounts or servers that share a folder
shape must not join, so the signature names the store when it can be proven.
"""

from __future__ import annotations

from flowx.models.adf_ast import AdfDatasetReference, AdfDefinitions
from flowx.models.ir import TranslationContext
from flowx.sources.adf.dataset_identity import (
    _backing_linked_service,
    _dataset_props,
    _effective_dataset_params,
    _resolve_file_store,
    _resolve_table_reference,
    _resolve_table_store,
    _unparameterised_linked_service_name,
)

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
