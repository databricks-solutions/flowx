"""Tests for the ADF dataset-lineage resolver (:mod:`flowx.sources.adf.dataset_lineage`).

Proves the two-tier identity / signature model re-homed from the closed #36
attempt: a table or a concrete path resolves to a physical ``identity``; a
parameterised reference does not (it never guesses) and falls back to the
structural path signature or the dataset name; and every input / output slot is
captured, not just index 0.
"""

from __future__ import annotations

from flowx.models.adf_ast import (
    AdfActivity,
    AdfDataset,
    AdfDatasetReference,
    AdfDefinitions,
    AdfLinkedService,
)
from flowx.sources.adf.dataset_lineage import activity_data_assets, resolve_dataset_identity


def _definitions(**datasets: AdfDataset) -> AdfDefinitions:
    return AdfDefinitions(pipelines=[], datasets=dict(datasets))


def _table_dataset(name: str, *, schema: str, table: str, linked_service: str = "ls_sql") -> AdfDataset:
    return AdfDataset(
        name=name,
        type="AzureSqlTable",
        properties={
            "typeProperties": {"schema": schema, "table": table},
            "linkedServiceName": {"referenceName": linked_service},
        },
    )


def _adls_dataset(
    name: str, *, file_system: str, folder_path: str, linked_service: str, file_name: object = None
) -> AdfDataset:
    location: dict = {"fileSystem": file_system, "folderPath": folder_path}
    if file_name is not None:
        location["fileName"] = file_name
    return AdfDataset(
        name=name,
        type="DelimitedText",
        properties={
            "typeProperties": {"location": location},
            "linkedServiceName": {"referenceName": linked_service},
        },
    )


def _adls_linked_service(name: str, *, account: str) -> AdfLinkedService:
    return AdfLinkedService(
        name=name,
        type="AzureBlobFS",
        properties={"typeProperties": {"url": f"https://{account}.dfs.core.windows.net"}},
    )


def _sql_linked_service(name: str, *, connection_string: object) -> AdfLinkedService:
    return AdfLinkedService(
        name=name,
        type="AzureSqlDatabase",
        properties={"typeProperties": {"connectionString": connection_string}},
    )


def _copy(name: str, *, source: str, sink: str) -> AdfActivity:
    return AdfActivity(
        name=name,
        type="Copy",
        type_properties={"source": {"referenceName": source}, "sink": {"referenceName": sink}},
    )


# --------------------------------------------------------------------------- #
# Identity tier
# --------------------------------------------------------------------------- #


def test_identity_resolves_schema_and_table() -> None:
    definitions = _definitions(ds_orders=_table_dataset("ds_orders", schema="curated", table="orders"))
    identity = resolve_dataset_identity(AdfDatasetReference(reference_name="ds_orders"), definitions)
    assert identity == "ls_sql/curated.orders"


def test_identity_resolves_storage_path_from_linked_service() -> None:
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_raw": _adls_dataset("ds_raw", file_system="data", folder_path="raw/customers", linked_service="ls")
        },
        linked_services={"ls": _adls_linked_service("ls", account="contosolake")},
    )
    identity = resolve_dataset_identity(AdfDatasetReference(reference_name="ds_raw"), definitions)
    assert identity == "abfss://data@contosolake.dfs.core.windows.net/raw/customers"


def test_files_in_the_same_folder_resolve_to_distinct_identities() -> None:
    """``raw/orders.csv`` and ``raw/customers.csv`` are different assets, so no identity edge joins them."""
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_orders": _adls_dataset(
                "ds_orders", file_system="data", folder_path="raw", linked_service="ls", file_name="orders.csv"
            ),
            "ds_customers": _adls_dataset(
                "ds_customers", file_system="data", folder_path="raw", linked_service="ls", file_name="customers.csv"
            ),
            "ds_orders_again": _adls_dataset(
                "ds_orders_again", file_system="data", folder_path="raw", linked_service="ls", file_name="orders.csv"
            ),
        },
        linked_services={"ls": _adls_linked_service("ls", account="acct")},
    )
    _, copy1_writes = activity_data_assets(_copy("Copy1", source="ds_orders_again", sink="ds_orders"), definitions)
    copy2_reads, _ = activity_data_assets(_copy("Copy2", source="ds_customers", sink="ds_orders_again"), definitions)

    assert copy1_writes[0].identity == "abfss://data@acct.dfs.core.windows.net/raw/orders.csv"
    assert copy2_reads[0].identity == "abfss://data@acct.dfs.core.windows.net/raw/customers.csv"
    assert copy1_writes[0].signature != copy2_reads[0].signature
    same_file_identity = resolve_dataset_identity(AdfDatasetReference(reference_name="ds_orders_again"), definitions)
    assert same_file_identity == copy1_writes[0].identity


def test_parameterised_file_name_has_no_path_identity() -> None:
    """A per-entity file name is only known at run time, so the folder alone is never used as its identity."""
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_expression": _adls_dataset(
                "ds_expression",
                file_system="data",
                folder_path="raw",
                linked_service="ls",
                file_name={"value": "@dataset().entity", "type": "Expression"},
            ),
            "ds_bare_expression": _adls_dataset(
                "ds_bare_expression",
                file_system="data",
                folder_path="raw",
                linked_service="ls",
                file_name="@dataset().entity",
            ),
        },
        linked_services={"ls": _adls_linked_service("ls", account="acct")},
    )
    assert resolve_dataset_identity(AdfDatasetReference(reference_name="ds_expression"), definitions) is None
    assert resolve_dataset_identity(AdfDatasetReference(reference_name="ds_bare_expression"), definitions) is None


def test_same_table_name_on_different_servers_resolves_to_distinct_identities() -> None:
    """A source ``dbo.Orders`` and a warehouse ``dbo.Orders`` are different tables, so no identity edge joins them."""
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_source_orders": _table_dataset(
                "ds_source_orders", schema="dbo", table="Orders", linked_service="ls_src"
            ),
            "ds_warehouse_orders": _table_dataset(
                "ds_warehouse_orders", schema="dbo", table="Orders", linked_service="ls_dw"
            ),
            "ds_lake": _adls_dataset("ds_lake", file_system="lake", folder_path="orders", linked_service="ls_lake"),
        },
        linked_services={
            "ls_src": _sql_linked_service(
                "ls_src", connection_string="Server=tcp:onprem-sql.contoso.local,1433;Database=Sales;User ID=etl"
            ),
            "ls_dw": _sql_linked_service(
                "ls_dw", connection_string={"type": "SecureString", "value": "Data Source=dw.contoso.net;"}
            ),
            "ls_lake": _adls_linked_service("ls_lake", account="lake"),
        },
    )
    copy_a_reads, _ = activity_data_assets(_copy("Copy A", source="ds_source_orders", sink="ds_lake"), definitions)
    _, copy_b_writes = activity_data_assets(_copy("Copy B", source="ds_lake", sink="ds_warehouse_orders"), definitions)

    assert copy_a_reads[0].identity == "onprem-sql.contoso.local/Sales/dbo.Orders"
    assert copy_b_writes[0].identity == "dw.contoso.net/dbo.Orders"
    assert copy_a_reads[0].signature != copy_b_writes[0].signature


def test_table_identity_falls_back_to_linked_service_name_when_server_is_secret() -> None:
    """A Key Vault connection string hides the server, so the linked service name qualifies the table."""
    key_vault_connection = {
        "type": "AzureKeyVaultSecret",
        "store": {"referenceName": "ls_key_vault", "type": "LinkedServiceReference"},
        "secretName": "sql-connection",
    }
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={"ds_orders": _table_dataset("ds_orders", schema="dbo", table="Orders", linked_service="ls_vaulted")},
        linked_services={"ls_vaulted": _sql_linked_service("ls_vaulted", connection_string=key_vault_connection)},
    )
    identity = resolve_dataset_identity(AdfDatasetReference(reference_name="ds_orders"), definitions)
    assert identity == "ls_vaulted/dbo.Orders"


def test_table_without_linked_service_has_no_identity() -> None:
    """With no backing store named, a bare ``schema.table`` is not provably one physical table."""
    dataset = AdfDataset(
        name="ds_orphan",
        type="AzureSqlTable",
        properties={"typeProperties": {"schema": "dbo", "table": "Orders"}},
    )
    definitions = _definitions(ds_orphan=dataset)
    assert resolve_dataset_identity(AdfDatasetReference(reference_name="ds_orphan"), definitions) is None


def test_identity_resolves_dataset_param_from_call_site_literal() -> None:
    """A ``@dataset().table`` expression resolves when the call site passes a literal."""
    dataset = AdfDataset(
        name="ds_param",
        type="AzureSqlTable",
        properties={
            "typeProperties": {"schema": "dbo", "table": "@dataset().tbl"},
            "parameters": {"tbl": {"type": "String"}},
            "linkedServiceName": {"referenceName": "ls_sql"},
        },
    )
    definitions = _definitions(ds_param=dataset)
    reference = AdfDatasetReference(reference_name="ds_param", parameters={"tbl": "shipments"})
    assert resolve_dataset_identity(reference, definitions) == "ls_sql/dbo.shipments"


def test_parameterised_table_is_not_guessed() -> None:
    """An unresolved ``@pipeline()`` expression yields no identity (never a guess, per #36)."""
    dataset = AdfDataset(
        name="ds_dyn",
        type="AzureSqlTable",
        properties={
            "typeProperties": {"schema": "dbo", "table": "@pipeline().parameters.tableName"},
            "linkedServiceName": {"referenceName": "ls_sql"},
        },
    )
    definitions = _definitions(ds_dyn=dataset)
    assert resolve_dataset_identity(AdfDatasetReference(reference_name="ds_dyn"), definitions) is None


def test_unknown_dataset_reference_has_no_identity() -> None:
    assert resolve_dataset_identity(AdfDatasetReference(reference_name="missing"), _definitions()) is None


# --------------------------------------------------------------------------- #
# activity_data_assets: reads/writes + the two-tier signature
# --------------------------------------------------------------------------- #


def test_copy_captures_source_read_and_sink_write_with_identity() -> None:
    definitions = _definitions(
        ds_src=_table_dataset("ds_src", schema="raw", table="orders"),
        ds_dst=_table_dataset("ds_dst", schema="curated", table="orders"),
    )
    activity = AdfActivity(
        name="Copy Orders",
        type="Copy",
        type_properties={
            "source": {"referenceName": "ds_src"},
            "sink": {"referenceName": "ds_dst"},
        },
    )
    reads, writes = activity_data_assets(activity, definitions)
    assert [(asset.identity, asset.signature) for asset in reads] == [("ls_sql/raw.orders", "ls_sql/raw.orders")]
    assert [(asset.identity, asset.signature) for asset in writes] == [
        ("ls_sql/curated.orders", "ls_sql/curated.orders")
    ]
    assert reads[0].asset_type == "table"


def test_captures_all_inputs_and_outputs_not_just_index_zero() -> None:
    """Every ``inputs`` / ``outputs`` slot is captured, not only the first."""
    definitions = _definitions(
        ds_in_a=_table_dataset("ds_in_a", schema="raw", table="a"),
        ds_in_b=_table_dataset("ds_in_b", schema="raw", table="b"),
        ds_out_a=_table_dataset("ds_out_a", schema="curated", table="a"),
        ds_out_b=_table_dataset("ds_out_b", schema="curated", table="b"),
    )
    activity = AdfActivity(
        name="Multi IO",
        type="Copy",
        inputs=[AdfDatasetReference(reference_name="ds_in_a"), AdfDatasetReference(reference_name="ds_in_b")],
        outputs=[AdfDatasetReference(reference_name="ds_out_a"), AdfDatasetReference(reference_name="ds_out_b")],
    )
    reads, writes = activity_data_assets(activity, definitions)
    assert sorted(asset.identity for asset in reads) == ["ls_sql/raw.a", "ls_sql/raw.b"]
    assert sorted(asset.identity for asset in writes) == ["ls_sql/curated.a", "ls_sql/curated.b"]


def test_dataset_named_in_both_slot_and_typeproperties_counted_once() -> None:
    definitions = _definitions(ds_src=_table_dataset("ds_src", schema="raw", table="orders"))
    activity = AdfActivity(
        name="Lookup",
        type="Lookup",
        inputs=[AdfDatasetReference(reference_name="ds_src")],
        type_properties={"dataset": {"referenceName": "ds_src"}},
    )
    reads, _ = activity_data_assets(activity, definitions)
    assert [asset.identity for asset in reads] == ["ls_sql/raw.orders"]


def test_unresolved_reference_falls_back_to_path_signature() -> None:
    """A parameterised file reference with a literal folder anchor gets a structural signature."""
    dataset = AdfDataset(
        name="ds_wm",
        type="DelimitedText",
        properties={"typeProperties": {"location": {"fileSystem": "@dataset().fs"}}},
    )
    definitions = _definitions(ds_wm=dataset)
    reference = AdfDatasetReference(
        reference_name="ds_wm",
        parameters={"folderPath": "@concat('watermark/', item().entity)", "fileName": "version.txt"},
    )
    activity = AdfActivity(name="Read WM", type="Lookup", type_properties={"dataset": _ref_dict(reference)})
    reads, _ = activity_data_assets(activity, definitions)
    assert reads[0].identity is None
    assert reads[0].signature == "FP[watermark|slots=1]/FN[version.txt|slots=0]"


def test_unresolved_reference_without_path_anchor_has_empty_signature() -> None:
    """No identity and no path anchor -> empty signature, so it never joins (#36 name rule)."""
    dataset = AdfDataset(
        name="ds_generic",
        type="AzureSqlTable",
        properties={"typeProperties": {"table": "@pipeline().parameters.t"}},
    )
    definitions = _definitions(ds_generic=dataset)
    activity = AdfActivity(
        name="Copy",
        type="Copy",
        type_properties={"source": {"referenceName": "ds_generic"}},
    )
    reads, _ = activity_data_assets(activity, definitions)
    assert reads[0].identity is None
    # Never the bare dataset name: an empty signature cannot produce a signature-tier join.
    assert reads[0].signature == ""


def test_distinct_parameter_bindings_of_same_dataset_are_all_retained() -> None:
    """Same dataset ref, different params -> distinct physical assets, both kept (not collapsed)."""
    dataset = AdfDataset(
        name="ds",
        type="AzureSqlTable",
        properties={
            "typeProperties": {"schema": "raw", "table": "@dataset().tbl"},
            "parameters": {"tbl": {"type": "String"}},
            "linkedServiceName": {"referenceName": "ls_sql"},
        },
    )
    definitions = _definitions(ds=dataset)
    activity = AdfActivity(
        name="Multi Bind",
        type="Copy",
        inputs=[
            AdfDatasetReference(reference_name="ds", parameters={"tbl": "orders"}),
            AdfDatasetReference(reference_name="ds", parameters={"tbl": "customers"}),
        ],
    )
    reads, _ = activity_data_assets(activity, definitions)
    assert sorted(asset.identity for asset in reads) == ["ls_sql/raw.customers", "ls_sql/raw.orders"]


def test_same_ref_same_params_in_slot_and_typeproperties_still_collapses() -> None:
    """The de-dup only fires on an identical binding: one asset, not two."""
    definitions = _definitions(ds_src=_table_dataset("ds_src", schema="raw", table="orders"))
    activity = AdfActivity(
        name="Lookup",
        type="Lookup",
        inputs=[AdfDatasetReference(reference_name="ds_src")],
        type_properties={"dataset": {"referenceName": "ds_src"}},
    )
    reads, _ = activity_data_assets(activity, definitions)
    assert [asset.identity for asset in reads] == ["ls_sql/raw.orders"]


def _ref_dict(reference: AdfDatasetReference) -> dict:
    """Render a dataset reference the way ADF ``typeProperties`` nests it."""
    payload: dict = {"referenceName": reference.reference_name}
    if reference.parameters:
        payload["parameters"] = reference.parameters
    return payload
