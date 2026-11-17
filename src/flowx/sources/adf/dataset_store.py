"""The store an ADF dataset lives in, used to qualify its path signature.

Two datasets on different containers, accounts or servers that share a folder
shape must not join, so the signature names the store when it can be proven.
"""

from __future__ import annotations

from flowx.models.adf_ast import AdfDatasetReference, AdfDefinitions
from flowx.models.ir import TranslationContext
from flowx.sources.adf.dataset_identity import (
    backing_linked_service,
    dataset_props,
    effective_dataset_params,
    expression_value,
    resolve_file_store,
    resolve_table_reference,
    resolve_table_store,
    unparameterised_linked_service_name,
    written_file_system,
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


def resolve_dataset_store(
    dataset_ref: AdfDatasetReference,
    definitions: AdfDefinitions,
    context: TranslationContext,
) -> tuple[str | None, bool]:
    """Return ``(store, location_names_store)`` for a dataset; the store is ``None`` unless provable.

    * A table's store is the one :func:`resolve_table_store` names.
    * A file's store is ``<file system>@<account>``. When the account is hidden (a Key
      Vault or masked connection string), a plain, unparameterised linked service
      name stands in for it.
    * A location whose server, share, lakehouse or account lives on the linked
      service (SFTP, FTP, a file share, HDFS, HTTP, a Fabric Lakehouse, ADLS Gen1;
      type compared case-insensitively) is named by that linked service name alone.

    ``location_names_store`` says whether the location tells its store apart at all,
    provable or not: a literal file system, container or bucket, or one of those
    linked-service locations. A masked file system (holding ``*``) names a store that
    cannot be proven, so it gives ``(None, True)``.

    A file system that is written but only known at run time (``@dataset().fs`` bound
    to a pipeline parameter, say) still sits inside a known account, so the store is
    ``<P>@<account>``, or ``<P>@<linked service name>`` when the account is hidden.
    Two such datasets then join only within one account. When neither the account nor
    a plain linked service is known, the location names no store.
    """
    properties = dataset_props(dataset_ref, definitions)
    if properties is None:
        return None, False
    type_props = properties.get("typeProperties") or properties
    location = type_props.get("location")
    file_system: str | None = None
    account: str | None = None
    location_names_store = False
    file_system_is_dynamic = False
    if isinstance(location, dict):
        location_type = location.get("type")
        location_names_store = (
            isinstance(location_type, str) and location_type.lower() in _LINKED_SERVICE_STORE_LOCATIONS
        )
        if not location_names_store:
            dataset_params = effective_dataset_params(dataset_ref, properties)
            file_system, account = resolve_file_store(
                location, backing_linked_service(properties, definitions), dataset_params, context
            )
            if file_system is None and "*" in written_file_system(location, dataset_params, context):
                return None, True
            location_names_store = file_system is not None
            file_system_is_dynamic = file_system is None and _writes_file_system(location)
    _, table = resolve_table_reference(dataset_ref, properties, context)
    if table:
        return resolve_table_store(properties, definitions), location_names_store
    if file_system_is_dynamic:
        known_boundary = account or unparameterised_linked_service_name(properties, definitions)
        return (f"<P>@{known_boundary}", True) if known_boundary else (None, False)
    if not location_names_store:
        return None, False
    if file_system is None:
        return unparameterised_linked_service_name(properties, definitions), True
    store = account or unparameterised_linked_service_name(properties, definitions)
    return (f"{file_system}@{store}" if store else None), True


def _writes_file_system(location: dict) -> bool:
    """``True`` when the location writes a file system, container or bucket at all, resolvable or not.

    An empty value counts as unwritten, bare or as the ``value`` of an Expression object.
    """
    return any(expression_value(location.get(key)) for key in ("fileSystem", "container", "bucketName"))
