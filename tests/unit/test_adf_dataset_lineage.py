"""Tests for the ADF dataset-lineage resolver (:mod:`flowx.sources.adf.dataset_lineage`).

Proves the two-tier identity / signature model re-homed from the closed #36
attempt: a table or a concrete path resolves to a physical ``identity``; a
parameterised reference does not (it never guesses) and falls back to the
structural path signature or the dataset name; and every input / output slot is
captured, not just index 0.
"""

from __future__ import annotations

from pathlib import Path

from flowx.lineage import data_edges_from_endpoints
from flowx.models.adf_ast import (
    AdfActivity,
    AdfDataset,
    AdfDatasetReference,
    AdfDefinitions,
    AdfLinkedService,
)
from flowx.sources.adf.dataset_lineage import activity_data_assets, resolve_dataset_identity
from flowx.sources.adf.loader import load_adf_definitions

FIXTURES_DIR = Path(__file__).parent.parent / "resources" / "json"


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

    assert copy_a_reads[0].identity == "tcp:onprem-sql.contoso.local,1433/Sales/dbo.Orders"
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


def test_parameterised_storage_account_has_no_path_identity() -> None:
    """A generic ADLS linked service names its account per binding, so the account is never part of an identity."""
    generic_lake = AdfLinkedService(
        name="ls_generic_lake",
        type="AzureBlobFS",
        properties={
            "typeProperties": {"url": "https://@{linkedService().accountName}.dfs.core.windows.net"},
            "parameters": {"accountName": {"type": "String"}},
        },
    )
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_orders": _adls_dataset(
                "ds_orders",
                file_system="data",
                folder_path="raw",
                linked_service="ls_generic_lake",
                file_name="orders.csv",
            )
        },
        linked_services={"ls_generic_lake": generic_lake},
    )
    assert resolve_dataset_identity(AdfDatasetReference(reference_name="ds_orders"), definitions) is None


def test_parameterised_database_has_no_table_identity() -> None:
    """A literal server with a parameterised database does not say which database holds the table."""
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_property": _table_dataset("ds_property", schema="dbo", table="Orders", linked_service="ls_property"),
            "ds_connection": _table_dataset(
                "ds_connection", schema="dbo", table="Orders", linked_service="ls_connection"
            ),
        },
        linked_services={
            "ls_property": AdfLinkedService(
                name="ls_property",
                type="SqlServer",
                properties={"typeProperties": {"server": "sql.contoso.net", "database": "@{linkedService().dbName}"}},
            ),
            "ls_connection": _sql_linked_service(
                "ls_connection", connection_string="Server=sql.contoso.net;Initial Catalog=@{linkedService().dbName}"
            ),
        },
    )
    assert resolve_dataset_identity(AdfDatasetReference(reference_name="ds_property"), definitions) is None
    assert resolve_dataset_identity(AdfDatasetReference(reference_name="ds_connection"), definitions) is None


def test_generic_linked_service_called_with_different_bindings_has_no_table_identity() -> None:
    """One generic SQL linked service bound to a source and a warehouse server never yields one shared identity."""
    generic_sql = AdfLinkedService(
        name="ls_generic_sql",
        type="AzureSqlDatabase",
        properties={
            "typeProperties": {
                "connectionString": {
                    "type": "AzureKeyVaultSecret",
                    "store": {"referenceName": "ls_key_vault", "type": "LinkedServiceReference"},
                    "secretName": "@{linkedService().secretName}",
                }
            },
            "parameters": {"secretName": {"type": "String"}},
        },
    )

    def _bound_orders(name: str, secret_name: str) -> AdfDataset:
        return AdfDataset(
            name=name,
            type="AzureSqlTable",
            properties={
                "typeProperties": {"schema": "dbo", "table": "Orders"},
                "linkedServiceName": {
                    "referenceName": "ls_generic_sql",
                    "parameters": {"secretName": secret_name},
                },
            },
        )

    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_source_orders": _bound_orders("ds_source_orders", "source-sql"),
            "ds_warehouse_orders": _bound_orders("ds_warehouse_orders", "warehouse-sql"),
        },
        linked_services={"ls_generic_sql": generic_sql},
    )
    copy_reads, copy_writes = activity_data_assets(
        _copy("Copy", source="ds_source_orders", sink="ds_warehouse_orders"), definitions
    )

    assert copy_reads[0].identity is None
    assert copy_writes[0].identity is None


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


def test_unevaluated_arm_expressions_never_become_identities() -> None:
    """An ARM template expression the export left unevaluated is unresolved, so it never forms an identity."""
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_table": _table_dataset("ds_table", schema="[parameters('schemaName')]", table="Orders"),
            "ds_file": _adls_dataset(
                "ds_file", file_system="[concat('raw', parameters('env'))]", folder_path="in", linked_service="ls"
            ),
            "ds_escaped": _adls_dataset("ds_escaped", file_system="[[literal]", folder_path="in", linked_service="ls"),
        },
        linked_services={
            "ls_sql": AdfLinkedService(
                name="ls_sql", type="AzureSqlDatabase", properties={"typeProperties": {"server": "sql.example.net"}}
            ),
            "ls": _adls_linked_service("ls", account="acct"),
        },
    )
    assert resolve_dataset_identity(AdfDatasetReference(reference_name="ds_table"), definitions) is None
    assert resolve_dataset_identity(AdfDatasetReference(reference_name="ds_file"), definitions) is None
    # ARM's "[[" escape is a literal value that starts with "[", so it stays physical.
    assert resolve_dataset_identity(AdfDatasetReference(reference_name="ds_escaped"), definitions) is not None


def test_bracket_quoted_sql_names_keep_their_identity() -> None:
    """Bracket-quoted SQL names are literal table names, not ARM expressions, so they keep their identity."""
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_legacy": AdfDataset(
                name="ds_legacy",
                type="AzureSqlTable",
                properties={
                    "typeProperties": {"tableName": "[dbo].[Orders]"},
                    "linkedServiceName": {"referenceName": "ls_sql"},
                },
            ),
            "ds_spaced": _table_dataset("ds_spaced", schema="dbo", table="[Order Details]"),
        },
        linked_services={
            "ls_sql": AdfLinkedService(
                name="ls_sql", type="AzureSqlDatabase", properties={"typeProperties": {"server": "sql.example.net"}}
            ),
        },
    )
    assert (
        resolve_dataset_identity(AdfDatasetReference(reference_name="ds_legacy"), definitions)
        == "sql.example.net/[dbo].[Orders]"
    )
    assert (
        resolve_dataset_identity(AdfDatasetReference(reference_name="ds_spaced"), definitions)
        == "sql.example.net/dbo.[Order Details]"
    )


def test_arm_expression_storage_url_has_no_path_identity() -> None:
    """An unevaluated ARM ``url`` is checked whole, so a cut-off fragment never stands in for the account."""
    arm_lake = AdfLinkedService(
        name="ls_arm_lake",
        type="AzureBlobFS",
        properties={
            "typeProperties": {"url": "[concat('https://', parameters('storageAccountName'), '.dfs.core.windows.net')]"}
        },
    )
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_orders": _adls_dataset(
                "ds_orders", file_system="raw", folder_path="in", linked_service="ls_arm_lake", file_name="orders.csv"
            )
        },
        linked_services={"ls_arm_lake": arm_lake},
    )
    assert resolve_dataset_identity(AdfDatasetReference(reference_name="ds_orders"), definitions) is None


def _lake_definitions() -> AdfDefinitions:
    return AdfDefinitions(
        pipelines=[],
        datasets={"ds_lake": _adls_dataset("ds_lake", file_system="data", folder_path="landing", linked_service="ls")},
        linked_services={"ls": _adls_linked_service("ls", account="acct")},
    )


def test_store_settings_path_override_read_gets_no_identity_edge() -> None:
    """A Copy that reads through a wildcard override does not read the dataset's folder, so no identity edge forms."""
    definitions = _lake_definitions()
    ingest = AdfActivity(
        name="Ingest",
        type="Copy",
        outputs=[AdfDatasetReference(reference_name="ds_lake")],
        type_properties={"sink": {"type": "DelimitedTextSink"}},
    )
    publish = AdfActivity(
        name="Publish",
        type="Copy",
        inputs=[AdfDatasetReference(reference_name="ds_lake")],
        type_properties={
            "source": {
                "type": "DelimitedTextSource",
                "storeSettings": {"wildcardFolderPath": "archive/2023", "wildcardFileName": "*.csv"},
            }
        },
    )
    _, ingest_writes = activity_data_assets(ingest, definitions)
    publish_reads, _ = activity_data_assets(publish, definitions)

    assert ingest_writes[0].identity == "abfss://data@acct.dfs.core.windows.net/landing"
    assert publish_reads[0].identity is None
    edges = data_edges_from_endpoints(
        [("Ingest", asset) for asset in ingest_writes], [("Publish", asset) for asset in publish_reads]
    )
    assert edges == []


def test_store_settings_path_override_covers_lookup_and_dataset_activities() -> None:
    """Lookup overrides on its ``source``; Delete and GetMetadata override directly on ``typeProperties``."""
    definitions = _lake_definitions()
    lookup = AdfActivity(
        name="Lookup",
        type="Lookup",
        type_properties={
            "source": {"type": "DelimitedTextSource", "storeSettings": {"prefix": "orders_"}},
            "dataset": {"referenceName": "ds_lake"},
        },
    )
    get_metadata = AdfActivity(
        name="GetMetadata",
        type="GetMetadata",
        type_properties={"dataset": {"referenceName": "ds_lake"}, "storeSettings": {"fileListPath": "lists/today.txt"}},
    )
    recursive_delete = AdfActivity(
        name="Delete",
        type="Delete",
        type_properties={"dataset": {"referenceName": "ds_lake"}, "storeSettings": {"recursive": True}},
    )

    assert [asset.identity for asset in activity_data_assets(lookup, definitions)[0]] == [None]
    assert [asset.identity for asset in activity_data_assets(get_metadata, definitions)[0]] == [None]
    assert [asset.identity for asset in activity_data_assets(recursive_delete, definitions)[0]] == [
        "abfss://data@acct.dfs.core.windows.net/landing"
    ]


def test_source_query_read_gets_no_identity_edge() -> None:
    """A Lookup that runs its own query reads what the query returns, not the dataset's table."""
    definitions = load_adf_definitions(FIXTURES_DIR)
    load_orders = AdfActivity(
        name="LoadOrders",
        type="Copy",
        outputs=[AdfDatasetReference(reference_name="ds_azure_sql_orders")],
        type_properties={"sink": {"type": "AzureSqlSink"}},
    )
    get_watermark = AdfActivity(
        name="GetWatermark",
        type="Lookup",
        type_properties={
            "source": {"type": "AzureSqlSource", "sqlReaderQuery": "SELECT MAX(ts) AS wm FROM dbo.watermarks"},
            "dataset": {"referenceName": "ds_azure_sql_orders"},
        },
    )
    _, load_orders_writes = activity_data_assets(load_orders, definitions)
    get_watermark_reads, _ = activity_data_assets(get_watermark, definitions)

    assert load_orders_writes[0].identity == "ls_azure_sql/dbo.orders"
    assert get_watermark_reads[0].identity is None
    edges = data_edges_from_endpoints(
        [("LoadOrders", asset) for asset in load_orders_writes],
        [("GetWatermark", asset) for asset in get_watermark_reads],
    )
    assert edges == []


def test_stored_procedure_source_and_sink_get_no_identity() -> None:
    """A stored procedure decides what is read or written, so neither side keeps the dataset's table identity."""
    definitions = _definitions(ds_orders=_table_dataset("ds_orders", schema="dbo", table="Orders"))
    lookup = AdfActivity(
        name="LookupStoredProc",
        type="Lookup",
        type_properties={
            "source": {"type": "SqlSource", "sqlReaderStoredProcedureName": "dbo.usp_get_orders"},
            "dataset": {"referenceName": "ds_orders"},
        },
    )
    copy = AdfActivity(
        name="UpsertOrders",
        type="Copy",
        inputs=[AdfDatasetReference(reference_name="ds_orders")],
        outputs=[AdfDatasetReference(reference_name="ds_orders")],
        type_properties={
            "source": {"type": "SqlSource"},
            "sink": {"type": "SqlSink", "sqlWriterStoredProcedureName": "dbo.usp_upsert_orders"},
        },
    )
    copy_reads, copy_writes = activity_data_assets(copy, definitions)

    assert [asset.identity for asset in activity_data_assets(lookup, definitions)[0]] == [None]
    assert [asset.identity for asset in copy_writes] == [None]
    assert [asset.identity for asset in copy_reads] == ["ls_sql/dbo.Orders"]


def test_connector_specific_query_read_gets_no_identity_edge() -> None:
    """An Oracle ``oracleReaderQuery`` overrides the read like ``sqlReaderQuery``; an empty one does not."""
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_oracle_control": AdfDataset(
                name="ds_oracle_control",
                type="OracleTable",
                properties={
                    "typeProperties": {"schema": "HR", "table": "ETL_CONTROL"},
                    "linkedServiceName": {"referenceName": "ls_oracle"},
                },
            )
        },
        linked_services={
            "ls_oracle": AdfLinkedService(
                name="ls_oracle",
                type="Oracle",
                properties={"typeProperties": {"connectionString": "host=ora.example.net;port=1521;serviceName=HR"}},
            )
        },
    )
    stage_control = AdfActivity(
        name="StageControl",
        type="Copy",
        outputs=[AdfDatasetReference(reference_name="ds_oracle_control")],
        type_properties={"sink": {"type": "OracleSink"}},
    )

    def _lookup(name: str, query: str) -> AdfActivity:
        return AdfActivity(
            name=name,
            type="Lookup",
            type_properties={
                "source": {"type": "OracleSource", "oracleReaderQuery": query},
                "dataset": {"referenceName": "ds_oracle_control"},
            },
        )

    _, stage_control_writes = activity_data_assets(stage_control, definitions)
    get_watermark_reads, _ = activity_data_assets(
        _lookup("GetWatermark", "SELECT MAX(loaded_at) FROM HR.ETL_AUDIT"), definitions
    )
    read_control_reads, _ = activity_data_assets(_lookup("ReadControl", ""), definitions)

    assert stage_control_writes[0].identity == "ls_oracle/HR.ETL_CONTROL"
    assert get_watermark_reads[0].identity is None
    assert (
        data_edges_from_endpoints(
            [("StageControl", asset) for asset in stage_control_writes],
            [("GetWatermark", asset) for asset in get_watermark_reads],
        )
        == []
    )
    assert read_control_reads[0].identity == "ls_oracle/HR.ETL_CONTROL"


def test_parameterised_file_location_bound_to_literals_resolves_to_its_path() -> None:
    """``@dataset().x`` location parts resolve against the call-site bindings, as table names do."""
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_param": _adls_dataset(
                "ds_param",
                file_system="data",
                folder_path={"value": "@dataset().folder", "type": "Expression"},
                linked_service="ls",
                file_name="@dataset().entity",
            ),
        },
        linked_services={"ls": _adls_linked_service("ls", account="acct")},
    )
    orders = AdfDatasetReference(reference_name="ds_param", parameters={"folder": "raw", "entity": "orders.csv"})
    customers = AdfDatasetReference(reference_name="ds_param", parameters={"folder": "raw", "entity": "customers.csv"})
    unbound = AdfDatasetReference(reference_name="ds_param", parameters={"folder": "raw"})
    assert resolve_dataset_identity(orders, definitions) == "abfss://data@acct.dfs.core.windows.net/raw/orders.csv"
    assert (
        resolve_dataset_identity(customers, definitions) == "abfss://data@acct.dfs.core.windows.net/raw/customers.csv"
    )
    # An unbound file-name parameter is unknown, so the folder alone is never used as the identity.
    assert resolve_dataset_identity(unbound, definitions) is None


def test_overridden_read_carries_no_signature_so_it_cannot_join_on_the_weak_tier() -> None:
    """With the writer's identity unresolved, a shared dataset binding must not join a wildcard reader by signature."""
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_param": _adls_dataset(
                "ds_param",
                file_system="data",
                folder_path="@dataset().folderPath",
                linked_service="ls_secret",
            )
        },
        linked_services={
            "ls_secret": AdfLinkedService(name="ls_secret", type="AzureBlobFS", properties={"typeProperties": {}})
        },
    )
    binding = {"folderPath": {"value": "@concat('landing/', pipeline().parameters.run)", "type": "Expression"}}
    ingest = AdfActivity(
        name="Ingest",
        type="Copy",
        outputs=[AdfDatasetReference(reference_name="ds_param", parameters=binding)],
        type_properties={"sink": {"type": "DelimitedTextSink"}},
    )
    publish = AdfActivity(
        name="Publish",
        type="Copy",
        inputs=[AdfDatasetReference(reference_name="ds_param", parameters=binding)],
        type_properties={"source": {"type": "DelimitedTextSource", "storeSettings": {"wildcardFileName": "*.csv"}}},
    )
    _, ingest_writes = activity_data_assets(ingest, definitions)
    publish_reads, _ = activity_data_assets(publish, definitions)

    assert ingest_writes[0].identity is None
    assert ingest_writes[0].signature  # the writer still has a path-anchored signature to join on
    assert publish_reads[0].identity is None
    assert publish_reads[0].signature == ""
    edges = data_edges_from_endpoints(
        [("Ingest", asset) for asset in ingest_writes], [("Publish", asset) for asset in publish_reads]
    )
    assert edges == []
