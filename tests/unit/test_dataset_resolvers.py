"""Unit tests for the shared dataset-identity resolvers."""

from __future__ import annotations

from flowx.models.adf_ast import (
    AdfDataset,
    AdfDatasetReference,
    AdfDefinitions,
)
from flowx.parser.dataset_resolvers import resolve_dataset_identity


def _defs_with_table_dataset(name: str, table: str) -> AdfDefinitions:
    ds = AdfDataset(
        name=name,
        type="AzureDatabricksDeltaLakeDataset",
        properties={
            "type": "AzureDatabricksDeltaLakeDataset",
            "linkedServiceName": {"referenceName": "ls_x", "type": "LinkedServiceReference"},
            "typeProperties": {"table": table},
        },
    )
    return AdfDefinitions(pipelines=[], datasets={name: ds}, linked_services={}, triggers=[])


class TestResolveDatasetIdentity:
    def test_table_identity_resolves(self):
        defs = _defs_with_table_dataset("ds_orders", "curated.orders")
        ref = AdfDatasetReference(reference_name="ds_orders")
        assert resolve_dataset_identity(ref, defs) == "curated.orders"

    def test_unknown_dataset_returns_none(self):
        defs = AdfDefinitions(pipelines=[], datasets={}, linked_services={}, triggers=[])
        ref = AdfDatasetReference(reference_name="missing")
        assert resolve_dataset_identity(ref, defs) is None

    def test_none_ref_returns_none(self):
        defs = AdfDefinitions(pipelines=[], datasets={}, linked_services={}, triggers=[])
        assert resolve_dataset_identity(None, defs) is None

    # --- Parameterized-table regression tests ---

    def test_parameterized_table_pipeline_param_returns_none(self):
        """ADF expression referencing a pipeline parameter must return None, not a DAB placeholder."""
        defs = _defs_with_table_dataset("ds_param", "@pipeline().parameters.tableName")
        ref = AdfDatasetReference(reference_name="ds_param")
        assert resolve_dataset_identity(ref, defs) is None

    def test_parameterized_table_interpolated_returns_none(self):
        """Interpolated ADF expression (tbl_@{...}) must return None, not a DAB placeholder."""
        defs = _defs_with_table_dataset("ds_interp", "tbl_@{pipeline().parameters.suffix}")
        ref = AdfDatasetReference(reference_name="ds_interp")
        assert resolve_dataset_identity(ref, defs) is None

    def test_parameterized_table_expression_object_returns_none(self):
        """Expression-object typeProperties.table must return None, not a DAB placeholder."""
        ds = AdfDataset(
            name="ds_expr",
            type="AzureDatabricksDeltaLakeDataset",
            properties={
                "type": "AzureDatabricksDeltaLakeDataset",
                "linkedServiceName": {"referenceName": "ls_x", "type": "LinkedServiceReference"},
                "typeProperties": {"table": {"type": "Expression", "value": "@pipeline().parameters.t"}},
            },
        )
        defs = AdfDefinitions(pipelines=[], datasets={"ds_expr": ds}, linked_services={}, triggers=[])
        ref = AdfDatasetReference(reference_name="ds_expr")
        assert resolve_dataset_identity(ref, defs) is None

    def test_literal_table_still_resolves(self):
        """Literal schema.table must continue to return 'curated.orders' (non-regression)."""
        defs = _defs_with_table_dataset("ds_orders2", "curated.orders")
        ref = AdfDatasetReference(reference_name="ds_orders2")
        assert resolve_dataset_identity(ref, defs) == "curated.orders"
