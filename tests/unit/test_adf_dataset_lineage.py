"""Tests for the ADF dataset-lineage resolver (:mod:`flowx.sources.adf.dataset_lineage`).

Proves the two-tier identity / signature model re-homed from the closed #36
attempt: a table or a concrete path resolves to a physical ``identity``; a
parameterised reference does not (it never guesses) and falls back to the
structural path signature; and every input / output slot is captured, not just
index 0.
"""

from __future__ import annotations

from pathlib import Path

import pytest

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
    assert reads[0].signature == "FP[watermark/<P>]/FN[version.txt]"


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


def test_arm_expression_folder_path_is_not_a_signature_anchor() -> None:
    """An unevaluated ARM expression bound as the folder is not a literal anchor, so it never joins two datasets."""
    dataset = AdfDataset(
        name="ds_landing",
        type="DelimitedText",
        properties={
            "typeProperties": {"location": {"fileSystem": "@dataset().fs", "folderPath": "@dataset().folderPath"}}
        },
    )
    definitions = _definitions(ds_landing=dataset)
    reference = AdfDatasetReference(reference_name="ds_landing", parameters={"folderPath": "[parameters('landing')]"})
    _, writes = activity_data_assets(AdfActivity(name="Write", type="Copy", outputs=[reference]), definitions)
    reads, _ = activity_data_assets(AdfActivity(name="Read", type="Copy", inputs=[reference]), definitions)

    assert writes[0].signature == reads[0].signature == ""
    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])
    assert edges == []


def test_arm_expression_folder_path_with_a_quoted_at_sign_is_not_a_signature_anchor() -> None:
    """An ARM folder expression is rejected even when a quoted literal inside it holds ``@``."""
    dataset = AdfDataset(
        name="ds_landing",
        type="DelimitedText",
        properties={
            "typeProperties": {"location": {"fileSystem": "@dataset().fs", "folderPath": "@dataset().folderPath"}}
        },
    )
    definitions = _definitions(ds_landing=dataset)
    reference = AdfDatasetReference(
        reference_name="ds_landing", parameters={"folderPath": "[concat(parameters('root'), '@archive')]"}
    )
    _, writes = activity_data_assets(AdfActivity(name="Write", type="Copy", outputs=[reference]), definitions)
    reads, _ = activity_data_assets(AdfActivity(name="Read", type="Copy", inputs=[reference]), definitions)

    assert writes[0].signature == reads[0].signature == ""
    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])
    assert edges == []


def test_different_arm_expression_file_names_under_one_folder_never_join() -> None:
    """An unevaluated ARM file name stays in the signature, so two different ones under one folder never join."""
    dataset = _adls_dataset(
        "ds_landing",
        file_system="raw",
        folder_path="@dataset().folderPath",
        linked_service="ls_lake",
        file_name="@dataset().fileName",
    )
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={"ds_landing": dataset},
        linked_services={"ls_lake": _adls_linked_service("ls_lake", account="acct")},
    )

    def _reference(file_name: str) -> AdfDatasetReference:
        return AdfDatasetReference(
            reference_name="ds_landing", parameters={"folderPath": "landing", "fileName": file_name}
        )

    _, writes = activity_data_assets(
        AdfActivity(name="Write", type="Copy", outputs=[_reference("[parameters('a')]")]), definitions
    )
    reads, _ = activity_data_assets(
        AdfActivity(name="Read", type="Copy", inputs=[_reference("[parameters('b')]")]), definitions
    )

    assert writes[0].identity is None and reads[0].identity is None
    assert writes[0].signature != reads[0].signature
    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])
    assert edges == []


def test_different_arm_file_names_with_a_quoted_at_sign_never_join() -> None:
    """An ARM file name holding a quoted ``@`` is not reduced to an ADF shape, so two different ones never join."""
    dataset = _adls_dataset(
        "ds_landing",
        file_system="raw",
        folder_path="@dataset().folderPath",
        linked_service="ls_lake",
        file_name="@dataset().fileName",
    )
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={"ds_landing": dataset},
        linked_services={"ls_lake": _adls_linked_service("ls_lake", account="acct")},
    )

    def _reference(file_name: str) -> AdfDatasetReference:
        return AdfDatasetReference(
            reference_name="ds_landing", parameters={"folderPath": "landing", "fileName": file_name}
        )

    _, writes = activity_data_assets(
        AdfActivity(name="Write", type="Copy", outputs=[_reference("[concat(variables('a'), '@x.csv')]")]), definitions
    )
    reads, _ = activity_data_assets(
        AdfActivity(name="Read", type="Copy", inputs=[_reference("[concat(variables('b'), '@x.csv')]")]), definitions
    )

    assert writes[0].signature != reads[0].signature
    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])
    assert edges == []


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


def _landing_copy(name: str, *, dataset: str, produced: bool) -> AdfActivity:
    """A Copy that writes (or reads) *dataset* bound to a run-specific folder under ``landing/``."""
    reference = AdfDatasetReference(
        reference_name=dataset,
        parameters={"folderPath": {"value": "@concat('landing/', pipeline().parameters.run)", "type": "Expression"}},
    )
    if produced:
        return AdfActivity(name=name, type="Copy", outputs=[reference])
    return AdfActivity(name=name, type="Copy", inputs=[reference])


def _landing_definitions(**stores: tuple[str, str]) -> AdfDefinitions:
    """One parameterised-folder dataset per entry, on container ``stores[name][0]`` of account ``stores[name][1]``."""
    return AdfDefinitions(
        pipelines=[],
        datasets={
            name: _adls_dataset(
                name, file_system=container, folder_path="@dataset().folderPath", linked_service=f"ls_{account}"
            )
            for name, (container, account) in stores.items()
        },
        linked_services={
            f"ls_{account}": _adls_linked_service(f"ls_{account}", account=account) for _, account in stores.values()
        },
    )


def test_same_folder_shape_on_different_stores_does_not_join_by_signature() -> None:
    """A write to sales@acctA and a read of hr@acctB share only a folder skeleton, so no edge joins them."""
    definitions = _landing_definitions(ds_sales=("sales", "acctA"), ds_hr=("hr", "acctB"))
    _, sales_writes = activity_data_assets(_landing_copy("WriteSales", dataset="ds_sales", produced=True), definitions)
    hr_reads, _ = activity_data_assets(_landing_copy("ReadHR", dataset="ds_hr", produced=False), definitions)

    assert sales_writes[0].identity is None
    assert hr_reads[0].identity is None
    edges = data_edges_from_endpoints(
        [("WriteSales", asset) for asset in sales_writes], [("ReadHR", asset) for asset in hr_reads]
    )
    assert edges == []


def test_same_folder_shape_on_the_same_store_still_joins_by_signature() -> None:
    """Two datasets on one literal container keep their advisory signature edge, now naming that store."""
    definitions = _landing_definitions(ds_out=("sales", "acctA"), ds_in=("sales", "acctA"))
    _, writes = activity_data_assets(_landing_copy("Write", dataset="ds_out", produced=True), definitions)
    reads, _ = activity_data_assets(_landing_copy("Read", dataset="ds_in", produced=False), definitions)

    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])
    assert [(edge.match_kind, edge.match_key) for edge in edges] == [
        ("signature", "ST[sales@acctA]/FP[landing/<P>]/FN[]")
    ]


def _vaulted_blob_definitions(**containers: str) -> AdfDefinitions:
    """One parameterised-folder dataset per entry, on container ``containers[name]`` of the Key Vault blob fixture."""
    vaulted_blob = load_adf_definitions(FIXTURES_DIR).get_linked_service("ls_azure_blob")
    assert vaulted_blob is not None
    return AdfDefinitions(
        pipelines=[],
        datasets={
            name: _adls_dataset(
                name, file_system=container, folder_path="@dataset().folderPath", linked_service="ls_azure_blob"
            )
            for name, container in containers.items()
        },
        linked_services={"ls_azure_blob": vaulted_blob},
    )


def test_same_folder_shape_on_different_containers_of_a_hidden_account_does_not_join() -> None:
    """With the account in Key Vault, the linked service name still tells ``staging`` and ``archive`` apart."""
    definitions = _vaulted_blob_definitions(ds_staging="staging", ds_archive="archive")
    _, staging_writes = activity_data_assets(_landing_copy("Stage", dataset="ds_staging", produced=True), definitions)
    archive_reads, _ = activity_data_assets(_landing_copy("Archive", dataset="ds_archive", produced=False), definitions)

    edges = data_edges_from_endpoints(
        [("Stage", asset) for asset in staging_writes], [("Archive", asset) for asset in archive_reads]
    )
    assert edges == []


def test_same_folder_shape_on_one_container_of_a_hidden_account_still_joins() -> None:
    """Two datasets on the same container of a Key Vault blob linked service join on that container and service."""
    definitions = _vaulted_blob_definitions(ds_out="staging", ds_in="staging")
    _, writes = activity_data_assets(_landing_copy("Write", dataset="ds_out", produced=True), definitions)
    reads, _ = activity_data_assets(_landing_copy("Read", dataset="ds_in", produced=False), definitions)

    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])
    assert [(edge.match_kind, edge.match_key) for edge in edges] == [
        ("signature", "ST[staging@ls_azure_blob]/FP[landing/<P>]/FN[]")
    ]


def _unprovable_store_definitions(
    location_type: str, file_system_key: str, linked_service: str, **file_systems: str
) -> AdfDefinitions:
    """One parameterised-folder dataset per entry, on file system ``file_systems[name]`` of *linked_service*.

    The only loaded linked service, ``ls_generic_store``, takes its account as a parameter, and an ARM-named
    *linked_service* is not loaded at all, so neither can prove which account holds the file system.
    """
    return AdfDefinitions(
        pipelines=[],
        datasets={
            name: AdfDataset(
                name=name,
                type="DelimitedText",
                properties={
                    "typeProperties": {
                        "location": {
                            "type": location_type,
                            file_system_key: file_system,
                            "folderPath": "@dataset().folderPath",
                        }
                    },
                    "linkedServiceName": {"referenceName": linked_service},
                },
            )
            for name, file_system in file_systems.items()
        },
        linked_services={
            "ls_generic_store": AdfLinkedService(
                name="ls_generic_store",
                type="AzureBlobFS",
                properties={
                    "typeProperties": {"url": "https://@{linkedService().accountName}.dfs.core.windows.net"},
                    "parameters": {"accountName": {"type": "String"}},
                },
            )
        },
    )


_LITERAL_FILE_SYSTEM_LOCATIONS = [
    ("AzureBlobFSLocation", "fileSystem"),
    ("AzureBlobStorageLocation", "container"),
    ("AmazonS3Location", "bucketName"),
    ("GoogleCloudStorageLocation", "bucketName"),
]


@pytest.mark.parametrize(("location_type", "file_system_key"), _LITERAL_FILE_SYSTEM_LOCATIONS)
@pytest.mark.parametrize("linked_service", ["ls_generic_store", "[parameters('ls')]"])
def test_literal_file_systems_on_an_unprovable_store_give_no_signature(
    location_type: str, file_system_key: str, linked_service: str
) -> None:
    """``sales`` and ``hr`` are different stores even with the account unknown, so they never join on folder shape."""
    definitions = _unprovable_store_definitions(
        location_type, file_system_key, linked_service, ds_sales="sales", ds_hr="hr"
    )
    _, sales_writes = activity_data_assets(_landing_copy("WriteSales", dataset="ds_sales", produced=True), definitions)
    hr_reads, _ = activity_data_assets(_landing_copy("ReadHR", dataset="ds_hr", produced=False), definitions)

    assert sales_writes[0].identity is None and hr_reads[0].identity is None
    assert sales_writes[0].signature == hr_reads[0].signature == ""
    edges = data_edges_from_endpoints(
        [("WriteSales", asset) for asset in sales_writes], [("ReadHR", asset) for asset in hr_reads]
    )
    assert edges == []


@pytest.mark.parametrize("linked_service", ["ls_generic_store", "[parameters('ls')]"])
def test_parameterised_file_system_on_an_unprovable_store_keeps_its_unqualified_signature(
    linked_service: str,
) -> None:
    """A file system left as ``@dataset().fs`` names no store, so the folder shape still joins, unqualified."""
    definitions = _unprovable_store_definitions(
        "AzureBlobFSLocation", "fileSystem", linked_service, ds_out="@dataset().fs", ds_in="@dataset().fs"
    )
    _, writes = activity_data_assets(_landing_copy("Write", dataset="ds_out", produced=True), definitions)
    reads, _ = activity_data_assets(_landing_copy("Read", dataset="ds_in", produced=False), definitions)

    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])
    assert [(edge.match_kind, edge.match_key) for edge in edges] == [("signature", "FP[landing/<P>]/FN[]")]


def test_arm_expression_linked_service_name_never_qualifies_an_identity() -> None:
    """A linked service named by an unevaluated ARM expression is not a plain name, so it gives no table identity."""
    definitions = _definitions(
        ds_orders=_table_dataset("ds_orders", schema="dbo", table="Orders", linked_service="[parameters('ls')]")
    )
    assert resolve_dataset_identity(AdfDatasetReference(reference_name="ds_orders"), definitions) is None


def _bucket_definitions(location_type: str, **buckets: str) -> AdfDefinitions:
    """One parameterised-folder dataset per entry, in bucket ``buckets[name]`` of a single S3 or GCS linked service."""
    return AdfDefinitions(
        pipelines=[],
        datasets={
            name: AdfDataset(
                name=name,
                type="DelimitedText",
                properties={
                    "typeProperties": {
                        "location": {"type": location_type, "bucketName": bucket, "folderPath": "@dataset().folderPath"}
                    },
                    "linkedServiceName": {"referenceName": "ls_bucket_store"},
                },
            )
            for name, bucket in buckets.items()
        },
        linked_services={
            "ls_bucket_store": AdfLinkedService(
                name="ls_bucket_store", type="AmazonS3", properties={"typeProperties": {"accessKeyId": "AKIAEXAMPLE"}}
            )
        },
    )


@pytest.mark.parametrize("location_type", ["AmazonS3Location", "GoogleCloudStorageLocation"])
def test_same_folder_shape_in_different_buckets_does_not_join_by_signature(location_type: str) -> None:
    """S3 and GCS name the bucket in ``bucketName``, so two buckets with one folder layout get distinct signatures."""
    definitions = _bucket_definitions(location_type, ds_sales="sales-bucket", ds_hr="hr-bucket")
    _, sales_writes = activity_data_assets(_landing_copy("WriteSales", dataset="ds_sales", produced=True), definitions)
    hr_reads, _ = activity_data_assets(_landing_copy("ReadHR", dataset="ds_hr", produced=False), definitions)

    assert sales_writes[0].signature == "ST[sales-bucket@ls_bucket_store]/FP[landing/<P>]/FN[]"
    assert hr_reads[0].signature == "ST[hr-bucket@ls_bucket_store]/FP[landing/<P>]/FN[]"
    edges = data_edges_from_endpoints(
        [("WriteSales", asset) for asset in sales_writes], [("ReadHR", asset) for asset in hr_reads]
    )
    assert edges == []


_LINKED_SERVICE_STORE_LOCATIONS = [
    "SftpLocation",
    "FileServerLocation",
    "FtpServerLocation",
    "AzureFileStorageLocation",
    "HdfsLocation",
    "HttpServerLocation",
]


def _server_definitions(
    location_type: str, linked_service_parameters: dict | None = None, **servers: str
) -> AdfDefinitions:
    """One parameterised-folder dataset per entry, on linked service ``servers[name]``; the location names no store."""
    linked_service_properties: dict = {"typeProperties": {"host": "files.example.com"}}
    if linked_service_parameters:
        linked_service_properties["parameters"] = linked_service_parameters
    return AdfDefinitions(
        pipelines=[],
        datasets={
            name: AdfDataset(
                name=name,
                type="DelimitedText",
                properties={
                    "typeProperties": {"location": {"type": location_type, "folderPath": "@dataset().folderPath"}},
                    "linkedServiceName": {"referenceName": linked_service},
                },
            )
            for name, linked_service in servers.items()
        },
        linked_services={
            linked_service: AdfLinkedService(name=linked_service, type="Sftp", properties=linked_service_properties)
            for linked_service in servers.values()
        },
    )


@pytest.mark.parametrize("location_type", _LINKED_SERVICE_STORE_LOCATIONS)
def test_same_folder_shape_on_different_servers_does_not_join_by_signature(location_type: str) -> None:
    """When only the linked service names the server or share, its plain name qualifies the signature."""
    definitions = _server_definitions(location_type, ds_sales="ls_sales_server", ds_hr="ls_hr_server")
    _, sales_writes = activity_data_assets(_landing_copy("WriteSales", dataset="ds_sales", produced=True), definitions)
    hr_reads, _ = activity_data_assets(_landing_copy("ReadHR", dataset="ds_hr", produced=False), definitions)

    assert sales_writes[0].signature == "ST[ls_sales_server]/FP[landing/<P>]/FN[]"
    assert hr_reads[0].signature == "ST[ls_hr_server]/FP[landing/<P>]/FN[]"
    edges = data_edges_from_endpoints(
        [("WriteSales", asset) for asset in sales_writes], [("ReadHR", asset) for asset in hr_reads]
    )
    assert edges == []


@pytest.mark.parametrize("location_type", _LINKED_SERVICE_STORE_LOCATIONS)
def test_same_folder_shape_on_one_server_still_joins_by_signature(location_type: str) -> None:
    """Two datasets on the same plain linked service keep their signature edge, naming that linked service."""
    definitions = _server_definitions(location_type, ds_out="ls_server", ds_in="ls_server")
    _, writes = activity_data_assets(_landing_copy("Write", dataset="ds_out", produced=True), definitions)
    reads, _ = activity_data_assets(_landing_copy("Read", dataset="ds_in", produced=False), definitions)

    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])
    assert [(edge.match_kind, edge.match_key) for edge in edges] == [
        ("signature", "ST[ls_server]/FP[landing/<P>]/FN[]")
    ]


@pytest.mark.parametrize("location_type", _LINKED_SERVICE_STORE_LOCATIONS)
@pytest.mark.parametrize(
    ("linked_service", "linked_service_parameters"),
    [("[parameters('ls')]", None), ("ls_any_server", {"host": {"type": "String"}})],
)
def test_server_store_without_a_plain_linked_service_gives_no_signature(
    location_type: str, linked_service: str, linked_service_parameters: dict | None
) -> None:
    """An ARM-named or parameterised linked service cannot name the server, so no signature can join on folder shape."""
    definitions = _server_definitions(
        location_type, linked_service_parameters, ds_out=linked_service, ds_in=linked_service
    )
    _, writes = activity_data_assets(_landing_copy("Write", dataset="ds_out", produced=True), definitions)
    reads, _ = activity_data_assets(_landing_copy("Read", dataset="ds_in", produced=False), definitions)

    assert writes[0].signature == reads[0].signature == ""
    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])
    assert edges == []


def _wrapped_folder_definitions(**folder_paths: str) -> AdfDefinitions:
    """One dataset per entry on container ``sales`` of ``acctA``, its folder written as ``folder_paths[name]``."""
    return AdfDefinitions(
        pipelines=[],
        datasets={
            name: _adls_dataset(
                name,
                file_system="sales",
                folder_path={"value": folder_path, "type": "Expression"},
                linked_service="ls_acctA",
            )
            for name, folder_path in folder_paths.items()
        },
        linked_services={"ls_acctA": _adls_linked_service("ls_acctA", account="acctA")},
    )


def _orders_copy(name: str, *, dataset: str, produced: bool) -> AdfActivity:
    """A Copy that writes (or reads) *dataset* with ``folderPath`` bound to ``orders``."""
    reference = AdfDatasetReference(reference_name=dataset, parameters={"folderPath": "orders"})
    if produced:
        return AdfActivity(name=name, type="Copy", outputs=[reference])
    return AdfActivity(name=name, type="Copy", inputs=[reference])


def test_datasets_that_wrap_the_same_folder_value_in_different_folders_never_join() -> None:
    """``bronze/orders`` and ``silver/orders`` are different folders, though both are called with ``orders``."""
    definitions = _wrapped_folder_definitions(
        ds_bronze="@concat('bronze/', dataset().folderPath)", ds_silver="@concat('silver/', dataset().folderPath)"
    )
    _, bronze_writes = activity_data_assets(
        _orders_copy("WriteBronze", dataset="ds_bronze", produced=True), definitions
    )
    silver_reads, _ = activity_data_assets(_orders_copy("ReadSilver", dataset="ds_silver", produced=False), definitions)

    assert bronze_writes[0].identity is None and silver_reads[0].identity is None
    assert bronze_writes[0].signature == "ST[sales@acctA]/FP[bronze/orders]/FN[]"
    assert silver_reads[0].signature == "ST[sales@acctA]/FP[silver/orders]/FN[]"
    edges = data_edges_from_endpoints(
        [("WriteBronze", asset) for asset in bronze_writes], [("ReadSilver", asset) for asset in silver_reads]
    )
    assert edges == []


def test_datasets_that_wrap_the_same_folder_value_in_one_folder_still_join() -> None:
    """Two datasets that both put ``orders`` under ``bronze/`` keep their signature edge on that folder."""
    definitions = _wrapped_folder_definitions(
        ds_out="@concat('bronze/', dataset().folderPath)", ds_in="@concat('bronze/', dataset().folderPath)"
    )
    _, writes = activity_data_assets(_orders_copy("Write", dataset="ds_out", produced=True), definitions)
    reads, _ = activity_data_assets(_orders_copy("Read", dataset="ds_in", produced=False), definitions)

    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])
    assert [(edge.match_kind, edge.match_key) for edge in edges] == [
        ("signature", "ST[sales@acctA]/FP[bronze/orders]/FN[]")
    ]


def test_interpolated_dataset_folders_keep_their_own_literal_text() -> None:
    """``bronze/@{dataset().folderPath}`` keeps ``bronze/``, so it never matches ``silver/@{dataset().folderPath}``."""
    definitions = _wrapped_folder_definitions(
        ds_bronze="bronze/@{dataset().folderPath}", ds_silver="silver/@{dataset().folderPath}"
    )
    _, bronze_writes = activity_data_assets(
        _orders_copy("WriteBronze", dataset="ds_bronze", produced=True), definitions
    )
    silver_reads, _ = activity_data_assets(_orders_copy("ReadSilver", dataset="ds_silver", produced=False), definitions)

    assert bronze_writes[0].signature == "ST[sales@acctA]/FP[bronze/orders]/FN[]"
    assert silver_reads[0].signature == "ST[sales@acctA]/FP[silver/orders]/FN[]"


def _folder_binding_copy(name: str, *, dataset: str, folder_path: str, produced: bool) -> AdfActivity:
    """A Copy that writes (or reads) *dataset* with ``folderPath`` bound to the ADF expression *folder_path*."""
    reference = AdfDatasetReference(
        reference_name=dataset, parameters={"folderPath": {"value": folder_path, "type": "Expression"}}
    )
    if produced:
        return AdfActivity(name=name, type="Copy", outputs=[reference])
    return AdfActivity(name=name, type="Copy", inputs=[reference])


def test_folder_signature_keeps_where_each_runtime_value_sits() -> None:
    """``landing/<env>`` and ``<env>/landing`` are different folders, so they never join on folder shape."""
    definitions = _landing_definitions(ds_out=("sales", "acctA"), ds_in=("sales", "acctA"))
    _, writes = activity_data_assets(
        _folder_binding_copy(
            "Write", dataset="ds_out", folder_path="@concat('landing/', pipeline().parameters.env)", produced=True
        ),
        definitions,
    )
    reads, _ = activity_data_assets(
        _folder_binding_copy(
            "Read", dataset="ds_in", folder_path="@concat(pipeline().parameters.env, '/landing')", produced=False
        ),
        definitions,
    )

    assert writes[0].signature == "ST[sales@acctA]/FP[landing/<P>]/FN[]"
    assert reads[0].signature == "ST[sales@acctA]/FP[<P>/landing]/FN[]"
    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])
    assert edges == []


def test_a_dataset_whose_parameters_are_not_a_map_does_not_crash() -> None:
    """Declared ``parameters`` written as a list are ignored rather than crashing the whole discover run."""
    dataset = _adls_dataset("ds_odd", file_system="sales", folder_path="@dataset().folderPath", linked_service="ls")
    dataset.properties["parameters"] = [{"name": "folderPath", "type": "String"}]
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={"ds_odd": dataset},
        linked_services={"ls": _adls_linked_service("ls", account="acctA")},
    )
    reads, _ = activity_data_assets(_landing_copy("Read", dataset="ds_odd", produced=False), definitions)

    assert reads[0].identity is None
    assert reads[0].signature == "ST[sales@acctA]/FP[landing/<P>]/FN[]"


def test_call_site_parameters_that_are_not_a_map_do_not_crash() -> None:
    """Call-site ``parameters`` written as a list bind nothing, so the reference gets no signature and no crash."""
    definitions = _landing_definitions(ds_in=("sales", "acctA"))
    reference = AdfDatasetReference(reference_name="ds_in", parameters=[{"folderPath": "landing"}])  # type: ignore[arg-type]
    reads, _ = activity_data_assets(AdfActivity(name="Read", type="Copy", inputs=[reference]), definitions)

    assert reads[0].identity is None
    assert reads[0].signature == ""


def test_a_reference_that_passes_no_folder_gets_no_signature_from_the_dataset_folder() -> None:
    """Only a reference that passes a ``folderPath`` joins on folder shape; the dataset's own folder never adds one."""
    definitions = _wrapped_folder_definitions(ds_out="archive", ds_in="archive")
    for definition in definitions.datasets.values():
        definition.properties["typeProperties"]["location"]["fileName"] = "@dataset().fileName"

    def _file_copy(name: str, *, dataset: str, produced: bool) -> AdfActivity:
        reference = AdfDatasetReference(
            reference_name=dataset, parameters={"fileName": {"value": "@item().name", "type": "Expression"}}
        )
        if produced:
            return AdfActivity(name=name, type="Copy", outputs=[reference])
        return AdfActivity(name=name, type="Copy", inputs=[reference])

    _, writes = activity_data_assets(_file_copy("Write", dataset="ds_out", produced=True), definitions)
    reads, _ = activity_data_assets(_file_copy("Read", dataset="ds_in", produced=False), definitions)

    assert writes[0].signature == reads[0].signature == ""


def test_a_quoted_interpolation_marker_inside_a_whole_expression_stays_literal_text() -> None:
    """In ``@concat('foo/', '@{left}')`` the quoted ``'@{left}'`` is text, so it never matches ``'@{right}'``."""
    definitions = _landing_definitions(ds_out=("sales", "acctA"), ds_in=("sales", "acctA"))
    _, writes = activity_data_assets(
        _folder_binding_copy("Write", dataset="ds_out", folder_path="@concat('foo/', '@{left}')", produced=True),
        definitions,
    )
    reads, _ = activity_data_assets(
        _folder_binding_copy("Read", dataset="ds_in", folder_path="@concat('foo/', '@{right}')", produced=False),
        definitions,
    )

    assert writes[0].signature == "ST[sales@acctA]/FP[foo/@{left}]/FN[]"
    assert reads[0].signature == "ST[sales@acctA]/FP[foo/@{right}]/FN[]"
    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])
    assert edges == []


@pytest.mark.parametrize(
    "runtime_value", ["pipeline().RunId", "pipeline().TriggerTime", "utcNow()", "utcnow('yyyy')", "trigger().startTime"]
)
def test_a_system_runtime_value_in_a_folder_is_a_slot_not_nothing(runtime_value: str) -> None:
    """``landing/<run id>`` is not the literal folder ``landing``, so the two never join on folder shape."""
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            name: _adls_dataset(
                name,
                file_system="sales",
                folder_path="@dataset().folderPath",
                linked_service="ls_acctA",
                file_name="@dataset().fileName",
            )
            for name in ("ds_out", "ds_in")
        },
        linked_services={"ls_acctA": _adls_linked_service("ls_acctA", account="acctA")},
    )

    def _reference(dataset: str, folder_path: str) -> AdfDatasetReference:
        return AdfDatasetReference(
            reference_name=dataset,
            parameters={
                "folderPath": {"value": folder_path, "type": "Expression"},
                "fileName": {"value": "@item().name", "type": "Expression"},
            },
        )

    _, writes = activity_data_assets(
        AdfActivity(name="Write", type="Copy", outputs=[_reference("ds_out", f"@concat('landing/', {runtime_value})")]),
        definitions,
    )
    reads, _ = activity_data_assets(
        AdfActivity(name="Read", type="Copy", inputs=[_reference("ds_in", "landing")]), definitions
    )

    assert writes[0].signature == "ST[sales@acctA]/FP[landing/<P>]/FN[]"
    assert reads[0].signature == "ST[sales@acctA]/FP[landing]/FN[]"
    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])
    assert edges == []


def _sales_definitions(**folder_paths: str) -> AdfDefinitions:
    """One dataset per entry on container ``sales`` of ``acctA``, its folder written as ``folder_paths[name]``.

    Each dataset takes its file name as ``@dataset().fileName``.
    """
    return AdfDefinitions(
        pipelines=[],
        datasets={
            name: _adls_dataset(
                name,
                file_system="sales",
                folder_path=folder_path,
                linked_service="ls_acctA",
                file_name="@dataset().fileName",
            )
            for name, folder_path in folder_paths.items()
        },
        linked_services={"ls_acctA": _adls_linked_service("ls_acctA", account="acctA")},
    )


def _sales_copy(name: str, *, dataset: str, folder_path: str, produced: bool) -> AdfActivity:
    """A Copy that writes (or reads) *dataset* with ``folderPath`` bound to *folder_path* and a run-time file name."""
    reference = AdfDatasetReference(
        reference_name=dataset,
        parameters={
            "folderPath": {"value": folder_path, "type": "Expression"},
            "fileName": {"value": "@item().name", "type": "Expression"},
        },
    )
    if produced:
        return AdfActivity(name=name, type="Copy", outputs=[reference])
    return AdfActivity(name=name, type="Copy", inputs=[reference])


def _write_then_read(
    definitions: AdfDefinitions, *, written: tuple[str, str], read: tuple[str, str]
) -> tuple[str, str, list[tuple[str, str]]]:
    """Write and read each ``(dataset, folderPath)`` pair; give both signatures and the edges between them."""
    _, writes = activity_data_assets(
        _sales_copy("Write", dataset=written[0], folder_path=written[1], produced=True), definitions
    )
    reads, _ = activity_data_assets(
        _sales_copy("Read", dataset=read[0], folder_path=read[1], produced=False), definitions
    )
    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])
    return writes[0].signature, reads[0].signature, [(edge.match_kind, edge.match_key) for edge in edges]


@pytest.mark.parametrize(
    "folder_path",
    ["@concat('landing/', rand(1,100))", "landing/@{rand(1,100)}", "@concat('landing/', pipeline()?.TriggerTime)"],
)
def test_any_runtime_function_in_a_folder_is_one_slot(folder_path: str) -> None:
    """``rand()`` or a ``?.`` system value is a slot under ``landing``, so it never joins the folder ``landing``."""
    definitions = _sales_definitions(ds_out="@dataset().folderPath", ds_in="@dataset().folderPath")
    written, read, edges = _write_then_read(definitions, written=("ds_out", folder_path), read=("ds_in", "landing"))

    assert written == "ST[sales@acctA]/FP[landing/<P>]/FN[]"
    assert read == "ST[sales@acctA]/FP[landing]/FN[]"
    assert edges == []


_ROOTED_FOLDERS = [
    "@concat(dataset().root, dataset().folderPath)",
    "@{dataset().root}@{dataset().folderPath}",
    "@concat(dataset()?.root, dataset()?.folderPath)",
]


def _rooted_definitions(rooted_folder: str, **other_folder_paths: str) -> AdfDefinitions:
    """``ds_rooted`` writes its folder as *rooted_folder*, with ``root`` defaulting to ``bronze/``; plus the others."""
    definitions = _sales_definitions(ds_rooted=rooted_folder, **other_folder_paths)
    definitions.datasets["ds_rooted"].properties["parameters"] = {
        "root": {"type": "String", "defaultValue": "bronze/"},
        "folderPath": {"type": "String"},
        "fileName": {"type": "String"},
    }
    return definitions


@pytest.mark.parametrize("rooted_folder", _ROOTED_FOLDERS)
def test_a_bound_value_keeps_its_own_slash_when_spliced_into_the_folder(rooted_folder: str) -> None:
    """A ``root`` of ``bronze/`` followed by ``orders`` is the folder ``bronze/orders``, never ``bronzeorders``."""
    definitions = _rooted_definitions(rooted_folder, ds_plain="@dataset().folderPath")
    written, read, edges = _write_then_read(
        definitions, written=("ds_rooted", "orders"), read=("ds_plain", "bronzeorders")
    )

    assert written == "ST[sales@acctA]/FP[bronze/orders]/FN[]"
    assert read == "ST[sales@acctA]/FP[bronzeorders]/FN[]"
    assert edges == []


@pytest.mark.parametrize("rooted_folder", _ROOTED_FOLDERS)
def test_a_bound_root_links_to_the_same_folder_spelled_with_a_literal_root(rooted_folder: str) -> None:
    """A ``root`` of ``bronze/`` plus ``orders`` joins a dataset that writes ``'bronze/'`` itself plus ``orders``."""
    definitions = _rooted_definitions(rooted_folder, ds_bronze="@concat('bronze/', dataset().folderPath)")
    _, _, edges = _write_then_read(definitions, written=("ds_rooted", "orders"), read=("ds_bronze", "orders"))

    assert edges == [("signature", "ST[sales@acctA]/FP[bronze/orders]/FN[]")]


def test_a_subscript_is_one_slot_so_its_key_never_becomes_folder_text() -> None:
    """``item()['zone']`` is one run-time value: ``raw/<P>`` never matches ``raw/<P>zone``."""
    definitions = _sales_definitions(ds_out="@dataset().folderPath", ds_in="@dataset().folderPath")
    written, read, edges = _write_then_read(
        definitions,
        written=("ds_out", "@concat('raw/', item()['zone'])"),
        read=("ds_in", "@concat('raw/', item().region, 'zone')"),
    )

    assert written == "ST[sales@acctA]/FP[raw/<P>]/FN[]"
    assert read == "ST[sales@acctA]/FP[raw/<P>zone]/FN[]"
    assert edges == []


@pytest.mark.parametrize("read_folder", ["landing/@{item().run}", "@concat('landing/', pipeline().parameters.run)"])
def test_an_interpolated_folder_links_like_its_concat_spelling(read_folder: str) -> None:
    """``landing/@{pipeline().parameters.run}`` has a literal ``landing``, so it joins that folder either way."""
    definitions = _sales_definitions(ds_out="@dataset().folderPath", ds_in="@dataset().folderPath")
    _, _, edges = _write_then_read(
        definitions, written=("ds_out", "landing/@{pipeline().parameters.run}"), read=("ds_in", read_folder)
    )

    assert edges == [("signature", "ST[sales@acctA]/FP[landing/<P>]/FN[]")]


def test_a_folder_that_is_only_a_formatted_date_gets_no_signature() -> None:
    """``formatDateTime(utcnow(), 'yyyy/MM/dd')`` is one run-time value with no literal folder, so it never joins."""
    folder_path = "@formatDateTime(utcnow(), 'yyyy/MM/dd')"
    definitions = _sales_definitions(ds_out="@dataset().folderPath", ds_in="@dataset().folderPath")
    written, read, edges = _write_then_read(definitions, written=("ds_out", folder_path), read=("ds_in", folder_path))

    assert written == read == ""
    assert edges == []


_WRAPPED_BOUND_FOLDERS = [
    "@concat('bronze/', toLower(dataset().folderPath))",
    "@concat('bronze/', string(dataset().folderPath))",
    "@concat('bronze/', trim(dataset().folderPath))",
    "@concat('bronze/', replace(dataset().folderPath, '-', '_'))",
    "@concat('bronze/', if(equals(dataset().folderPath, 'archive'), 'old', dataset().folderPath))",
    "bronze/@{toLower(dataset().folderPath)}",
]


@pytest.mark.parametrize("wrapped_folder", _WRAPPED_BOUND_FOLDERS)
def test_a_function_over_different_bound_folders_never_links(wrapped_folder: str) -> None:
    """``toLower('orders')`` and ``toLower('customers')`` are different folders, so the call keeps its bound value."""
    definitions = _sales_definitions(ds_out=wrapped_folder, ds_in=wrapped_folder)
    written, read, edges = _write_then_read(definitions, written=("ds_out", "orders"), read=("ds_in", "customers"))

    assert written != read
    assert "orders" in written and "customers" in read
    assert edges == []


@pytest.mark.parametrize("wrapped_folder", _WRAPPED_BOUND_FOLDERS)
def test_a_function_over_the_same_bound_folder_still_links(wrapped_folder: str) -> None:
    """The same function over the same bound ``orders`` is the same folder on both sides, so it still joins."""
    definitions = _sales_definitions(ds_out=wrapped_folder, ds_in=wrapped_folder)
    written, read, edges = _write_then_read(definitions, written=("ds_out", "orders"), read=("ds_in", "orders"))

    assert written == read
    assert edges == [("signature", written)]


def test_a_function_over_a_bound_folder_is_a_keyed_slot_not_folder_text() -> None:
    """``bronze/`` + ``toLower(orders)`` is a slot keyed by its input, never the literal folder ``bronze/orders``."""
    definitions = _sales_definitions(
        ds_out="@concat('bronze/', toLower(dataset().folderPath))", ds_in="@dataset().folderPath"
    )
    written, read, edges = _write_then_read(definitions, written=("ds_out", "orders"), read=("ds_in", "bronze/orders"))

    assert written == "ST[sales@acctA]/FP[bronze/<P:tolower('orders')>]/FN[]"
    assert edges == []


def test_a_keyed_slot_alone_cannot_anchor_a_folder_signature() -> None:
    """A folder that is only ``toLower(dataset().folderPath)`` has no literal text, so it gets no signature."""
    definitions = _sales_definitions(ds_out="@toLower(dataset().folderPath)", ds_in="@toLower(dataset().folderPath)")
    written, read, edges = _write_then_read(definitions, written=("ds_out", "orders"), read=("ds_in", "orders"))

    assert written == read == ""
    assert edges == []


def test_a_function_over_different_bound_file_names_never_links() -> None:
    """``toLower('A') + '.csv'`` and ``toLower('B') + '.csv'`` are different files, so they never join."""
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            name: _adls_dataset(
                name,
                file_system="sales",
                folder_path="@dataset().folderPath",
                linked_service="ls_acctA",
                file_name="@concat(toLower(dataset().fileName), '.csv')",
            )
            for name in ("ds_out", "ds_in")
        },
        linked_services={"ls_acctA": _adls_linked_service("ls_acctA", account="acctA")},
    )

    def _file_copy(name: str, *, dataset: str, file_name: str, produced: bool) -> AdfActivity:
        reference = AdfDatasetReference(
            reference_name=dataset,
            parameters={
                "folderPath": {"value": "@concat('landing/', item().run)", "type": "Expression"},
                "fileName": file_name,
            },
        )
        if produced:
            return AdfActivity(name=name, type="Copy", outputs=[reference])
        return AdfActivity(name=name, type="Copy", inputs=[reference])

    _, writes = activity_data_assets(_file_copy("Write", dataset="ds_out", file_name="A", produced=True), definitions)
    reads, _ = activity_data_assets(_file_copy("Read", dataset="ds_in", file_name="B", produced=False), definitions)
    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])

    assert writes[0].signature == "ST[sales@acctA]/FP[landing/<P>]/FN[<P:tolower('A')>.csv]"
    assert edges == []


def test_a_function_over_the_same_bound_file_name_still_links() -> None:
    """The same function over the same bound file name names the same file, so the two still join."""
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            name: _adls_dataset(
                name,
                file_system="sales",
                folder_path="@dataset().folderPath",
                linked_service="ls_acctA",
                file_name="@concat(toLower(dataset().fileName), '.csv')",
            )
            for name in ("ds_out", "ds_in")
        },
        linked_services={"ls_acctA": _adls_linked_service("ls_acctA", account="acctA")},
    )
    reference = {
        name: AdfDatasetReference(
            reference_name=name,
            parameters={
                "folderPath": {"value": "@concat('landing/', item().run)", "type": "Expression"},
                "fileName": "A",
            },
        )
        for name in ("ds_out", "ds_in")
    }
    _, writes = activity_data_assets(AdfActivity(name="Write", type="Copy", outputs=[reference["ds_out"]]), definitions)
    reads, _ = activity_data_assets(AdfActivity(name="Read", type="Copy", inputs=[reference["ds_in"]]), definitions)
    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])

    assert [edge.match_key for edge in edges] == ["ST[sales@acctA]/FP[landing/<P>]/FN[<P:tolower('A')>.csv]"]


def test_a_file_name_that_is_only_a_function_over_a_bound_value_keeps_its_key() -> None:
    """``toLower('A')`` and ``toLower('B')`` name different files, so even with no literal text they never join."""
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            name: _adls_dataset(
                name,
                file_system="sales",
                folder_path="@dataset().folderPath",
                linked_service="ls_acctA",
                file_name="@toLower(dataset().fileName)",
            )
            for name in ("ds_out", "ds_in")
        },
        linked_services={"ls_acctA": _adls_linked_service("ls_acctA", account="acctA")},
    )

    def _reference(name: str, file_name: str) -> AdfDatasetReference:
        return AdfDatasetReference(
            reference_name=name,
            parameters={
                "folderPath": {"value": "@concat('landing/', item().run)", "type": "Expression"},
                "fileName": file_name,
            },
        )

    _, writes = activity_data_assets(
        AdfActivity(name="Write", type="Copy", outputs=[_reference("ds_out", "A")]), definitions
    )
    reads, _ = activity_data_assets(
        AdfActivity(name="Read", type="Copy", inputs=[_reference("ds_in", "B")]), definitions
    )
    edges = data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])

    assert writes[0].signature == "ST[sales@acctA]/FP[landing/<P>]/FN[<P:tolower('A')>]"
    assert edges == []


@pytest.mark.parametrize(
    "year_folder", ["@concat('landing/', string(dataset().year))", "@concat('landing/', dataset().year)"]
)
def test_a_numeric_bound_value_keeps_its_value_in_the_folder(year_folder: str) -> None:
    """A ``year`` bound to 2024 and one bound to 2025 are different folders, plain or wrapped in a function."""
    definitions = _sales_definitions(ds_out=year_folder, ds_in=year_folder)

    def _year_copy(name: str, *, dataset: str, year: int, produced: bool) -> AdfActivity:
        reference = AdfDatasetReference(
            reference_name=dataset,
            parameters={
                "folderPath": "landing",
                "year": year,
                "fileName": {"value": "@item().name", "type": "Expression"},
            },
        )
        if produced:
            return AdfActivity(name=name, type="Copy", outputs=[reference])
        return AdfActivity(name=name, type="Copy", inputs=[reference])

    def _edges(written_year: int, read_year: int) -> list:
        _, writes = activity_data_assets(
            _year_copy("Write", dataset="ds_out", year=written_year, produced=True), definitions
        )
        reads, _ = activity_data_assets(
            _year_copy("Read", dataset="ds_in", year=read_year, produced=False), definitions
        )
        return data_edges_from_endpoints([("Write", asset) for asset in writes], [("Read", asset) for asset in reads])

    assert _edges(2024, 2025) == []
    assert len(_edges(2024, 2024)) == 1


def test_an_apostrophe_in_a_bound_value_never_merges_two_keyed_slots() -> None:
    """``O'Brien`` and ``O' Brien`` are different folders, so the keyed slot quotes the bound value ADF's way."""
    wrapped_folder = "@concat('bronze/', toLower(dataset().folderPath))"
    definitions = _sales_definitions(ds_out=wrapped_folder, ds_in=wrapped_folder)
    written, read, edges = _write_then_read(definitions, written=("ds_out", "O'Brien"), read=("ds_in", "O' Brien"))

    assert written == "ST[sales@acctA]/FP[bronze/<P:tolower('O''Brien')>]/FN[]"
    assert written != read
    assert edges == []


def test_a_closing_brace_inside_quoted_text_does_not_end_an_interpolation_block() -> None:
    """In ``x/@{concat('}', item().x)}/y`` the quoted ``}`` is text, so the block is one expression."""
    definitions = _sales_definitions(ds_out="@dataset().folderPath", ds_in="@dataset().folderPath")
    written, _, _ = _write_then_read(
        definitions, written=("ds_out", "x/@{concat('}', item().x)}/y"), read=("ds_in", "x/y")
    )

    assert written == "ST[sales@acctA]/FP[x/}<P>/y]/FN[]"


def test_an_escaped_at_sign_is_literal_folder_text() -> None:
    """ADF reads ``@@landing`` as the literal folder ``@landing``, so it is path text, not a run-time value."""
    definitions = _sales_definitions(ds_out="@dataset().folderPath", ds_in="@dataset().folderPath")
    written, read, edges = _write_then_read(definitions, written=("ds_out", "@@landing"), read=("ds_in", "@@landing"))

    assert written == "ST[sales@acctA]/FP[@landing]/FN[]"
    assert edges == [("signature", written)]


@pytest.mark.parametrize("year_folder", ["landing/@{2024}", "@concat('landing/', 2024)"])
def test_a_number_in_an_expression_is_literal_folder_text(year_folder: str) -> None:
    """``@{2024}`` always prints ``2024``, so the folder is ``landing/2024`` and never joins a run-time year."""
    definitions = _sales_definitions(ds_out="@dataset().folderPath", ds_in="@dataset().folderPath")
    written, read, edges = _write_then_read(
        definitions, written=("ds_out", year_folder), read=("ds_in", "landing/@{item().year}")
    )

    assert written == "ST[sales@acctA]/FP[landing/2024]/FN[]"
    assert read == "ST[sales@acctA]/FP[landing/<P>]/FN[]"
    assert edges == []


def test_function_names_in_a_keyed_slot_ignore_case_as_adf_does() -> None:
    """``toLower`` and ``TOLOWER`` are the same ADF function, so over the same bound value they still join."""
    definitions = _sales_definitions(
        ds_out="@concat('bronze/', toLower(dataset().folderPath))",
        ds_in="@concat('bronze/', TOLOWER(dataset().folderPath))",
    )
    written, read, edges = _write_then_read(definitions, written=("ds_out", "Orders"), read=("ds_in", "Orders"))

    assert written == read == "ST[sales@acctA]/FP[bronze/<P:tolower('Orders')>]/FN[]"
    assert edges == [("signature", written)]


def _subfolder_definitions(folder_path: str, **sub_defaults: object) -> AdfDefinitions:
    """One dataset per entry writing its folder as *folder_path*, ``sub`` defaulting to ``sub_defaults[name]``.

    Plus ``ds_plain``, whose folder is just ``@dataset().folderPath``.
    """
    definitions = _sales_definitions(ds_plain="@dataset().folderPath", **dict.fromkeys(sub_defaults, folder_path))
    for name, sub_default in sub_defaults.items():
        definitions.datasets[name].properties["parameters"] = {
            "folderPath": {"type": "String"},
            "sub": {"type": "String", "defaultValue": sub_default},
            "fileName": {"type": "String"},
        }
    return definitions


_UNREADABLE_BOUND_VALUES = [None, ["archive"], {"type": "Expression"}]


def test_an_empty_bound_value_adds_nothing_to_the_folder() -> None:
    """``landing/`` plus a ``sub`` defaulting to ``""`` is the folder ``landing``, not a run-time subfolder of it."""
    definitions = _subfolder_definitions("@concat(dataset().folderPath, dataset().sub)", ds_sub="")
    written, _, literal_edges = _write_then_read(
        definitions, written=("ds_sub", "landing/"), read=("ds_plain", "landing")
    )
    _, _, runtime_edges = _write_then_read(
        definitions, written=("ds_sub", "landing/"), read=("ds_plain", "@concat('landing/', item().day)")
    )

    assert written == "ST[sales@acctA]/FP[landing]/FN[]"
    assert literal_edges == [("signature", written)]
    assert runtime_edges == []


@pytest.mark.parametrize("sub_default", _UNREADABLE_BOUND_VALUES)
def test_an_unreadable_bound_value_is_a_runtime_slot(sub_default: object) -> None:
    """A ``sub`` defaulting to ``null`` (or a list, or an Expression with no value) is unknown, so it is a ``<P>``."""
    definitions = _subfolder_definitions("@concat(dataset().folderPath, dataset().sub)", ds_sub=sub_default)
    written, _, edges = _write_then_read(definitions, written=("ds_sub", "landing/"), read=("ds_plain", "landing"))

    assert written == "ST[sales@acctA]/FP[landing/<P>]/FN[]"
    assert edges == []


@pytest.mark.parametrize("sub_default", _UNREADABLE_BOUND_VALUES)
def test_a_function_over_an_unreadable_bound_value_never_shares_a_key_with_an_empty_one(sub_default: object) -> None:
    """``toLower`` over a ``null`` ``sub`` is a plain ``<P>``, never the same slot as ``toLower('')``."""
    definitions = _subfolder_definitions(
        "@concat('landing/', toLower(dataset().sub))", ds_unknown=sub_default, ds_empty=""
    )
    written, read, edges = _write_then_read(
        definitions, written=("ds_unknown", "landing"), read=("ds_empty", "landing")
    )

    assert written == "ST[sales@acctA]/FP[landing/<P>]/FN[]"
    assert read == "ST[sales@acctA]/FP[landing/<P:tolower('')>]/FN[]"
    assert edges == []


def test_an_arm_named_linked_service_never_gives_an_identity_even_when_it_is_loaded() -> None:
    """``[parameters('ls')]`` is not a plain name, so even a loaded definition under that key proves no store."""
    arm_name = "[parameters('ls')]"
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_file": _adls_dataset(
                "ds_file", file_system="sales", folder_path="raw", linked_service=arm_name, file_name="orders.csv"
            ),
            "ds_table": _table_dataset("ds_table", schema="dbo", table="Orders", linked_service=arm_name),
        },
        linked_services={
            arm_name: AdfLinkedService(
                name=arm_name,
                type="AzureBlobFS",
                properties={
                    "typeProperties": {"url": "https://acctA.dfs.core.windows.net", "server": "sql.example.com"}
                },
            )
        },
    )

    assert resolve_dataset_identity(AdfDatasetReference(reference_name="ds_file"), definitions) is None
    assert resolve_dataset_identity(AdfDatasetReference(reference_name="ds_table"), definitions) is None
