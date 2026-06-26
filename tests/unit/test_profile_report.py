"""Tests for the profile-phase complexity report (CSV + T-shirt sizing + ARM export)."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from flowx.models.adf_ast import (
    AdfActivity,
    AdfDataset,
    AdfDatasetReference,
    AdfDefinitions,
    AdfLinkedService,
    AdfLinkedServiceReference,
    AdfPipeline,
)
from flowx.parser.adf_loader import (
    _activity_category,
    _complexity_score,
    _tshirt_size,
    build_profile_rows,
    write_pipeline_arm,
    write_profile_csv,
)


def test_activity_category_ordering():
    assert _activity_category("DatabricksNotebook") == "databricks"
    assert _activity_category("ForEach") == "control"
    assert _activity_category("SetVariable") == "control"
    assert _activity_category("Copy") == "other"
    assert _activity_category("WebActivity") == "other"


def test_complexity_score_weights_other_highest():
    # 1 native vs 1 other: other must score higher.
    native = _complexity_score({"databricks": 1, "control": 0, "other": 0}, 0, 0, 0)
    control = _complexity_score({"databricks": 0, "control": 1, "other": 0}, 0, 0, 0)
    other = _complexity_score({"databricks": 0, "control": 0, "other": 1}, 0, 0, 0)
    assert native < control < other


def test_tshirt_size_buckets():
    assert _tshirt_size(2) == "S"
    assert _tshirt_size(10) == "M"
    assert _tshirt_size(25) == "L"
    assert _tshirt_size(40) == "XL"


def _pipeline_with_copy() -> AdfDefinitions:
    copy = AdfActivity(
        name="Load",
        type="Copy",
        inputs=[AdfDatasetReference(reference_name="src_ds")],
        outputs=[AdfDatasetReference(reference_name="dst_ds")],
    )
    notebook = AdfActivity(
        name="Run",
        type="DatabricksNotebook",
        linked_service_name=AdfLinkedServiceReference(reference_name="adb_ls"),
    )
    pipeline = AdfPipeline(name="p1", activities=[copy, notebook], raw={"name": "p1", "properties": {}})
    return AdfDefinitions(
        pipelines=[pipeline],
        datasets={
            "src_ds": AdfDataset(name="src_ds", type="AzureSqlTable", properties={}, linked_service_name="sql_ls"),
            "dst_ds": AdfDataset(name="dst_ds", type="DelimitedText", properties={}, linked_service_name="adls_ls"),
        },
        linked_services={
            "sql_ls": AdfLinkedService(name="sql_ls", type="AzureSqlDatabase", properties={}),
            "adls_ls": AdfLinkedService(name="adls_ls", type="AzureBlobFS", properties={}),
            "adb_ls": AdfLinkedService(name="adb_ls", type="AzureDatabricks", properties={}),
        },
    )


def test_build_profile_rows_counts_datasets_and_linked_services():
    rows = build_profile_rows(_pipeline_with_copy())
    assert len(rows) == 1
    row = rows[0]
    assert row["pipeline"] == "p1"
    assert row["activities"] == 2
    assert row["datasets"] == 2  # src_ds + dst_ds
    # 2 from datasets (sql_ls, adls_ls) + 1 activity-level (adb_ls)
    assert row["linked_services"] == 3
    assert row["databricks_native_activities"] == 1
    assert row["other_activities"] == 1
    assert row["complexity_size"] in {"S", "M", "L", "XL"}


def test_write_profile_csv_roundtrip(tmp_path: Path):
    rows = build_profile_rows(_pipeline_with_copy())
    csv_path = tmp_path / "profile_report.csv"
    write_profile_csv(rows, csv_path)
    with csv_path.open() as handle:
        read_rows = list(csv.DictReader(handle))
    assert read_rows[0]["pipeline"] == "p1"
    assert read_rows[0]["activities"] == "2"


def test_write_pipeline_arm_emits_verbatim_source(tmp_path: Path):
    definitions = _pipeline_with_copy()
    written = write_pipeline_arm(definitions, tmp_path)
    assert len(written) == 1
    arm = json.loads(written[0].read_text())
    assert arm == {"name": "p1", "properties": {}}
