"""Tests for source-aware inputs prompts and airflow coverage profile columns."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from flowx.adapter.session import MigrationInputSession
from flowx.models.ir import NotebookActivity, Pipeline
from flowx.reporting.coverage import COVERAGE_METRIC_COLUMNS, build_coverage_rows
from flowx.sources.airflow.discover import _profile_row, build_inventory_dict
from flowx.sources.airflow.discover import main as discover_main


def test_inputs_discover_airflow_prompts_for_dags_not_adf():
    session = MigrationInputSession(phase="discover", source="airflow")
    options = {o.option_id: o for o in session.pending().options}
    assert "airflow_source_path" in options
    assert "adf_source_path" not in options
    assert "DAG" in options["airflow_source_path"].prompt


def test_inputs_discover_adf_unchanged():
    session = MigrationInputSession(phase="discover", source="adf")
    ids = {o.option_id for o in session.pending().options}
    assert ids == {"adf_source_path", "adf_resource_url", "output_dir"}


def test_inputs_convert_airflow_uses_airflow_source_path():
    session = MigrationInputSession(phase="convert", source="airflow")
    ids = {o.option_id for o in session.pending().options}
    assert "airflow_source_path" in ids
    assert "adf_source_path" not in ids


def test_inputs_discover_requires_source():
    # No default source: discover prompts can't be worded without one, so pending() raises.
    session = MigrationInputSession(phase="discover")
    with pytest.raises(ValueError, match="source is required"):
        session.pending()


def test_airflow_profile_csv_has_all_coverage_columns():
    dag = (
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='cov') as dag:\n"
        "    a = PythonOperator(task_id='a', python_callable=w)\n"
        "    b = SomeExoticOperator(task_id='b')\n"
        "    a >> b\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "dag.py"
        src.write_text(dag, encoding="utf-8")
        out = Path(tmp) / "out"
        assert discover_main(["--source-dir", str(src), "--output-dir", str(out)]) == 0
        rows = build_coverage_rows(out / "metadata")
        assert len(rows) == 1
        row = rows[0]
        # Every coverage metric column is present (no silent-zero KeyErrors) ...
        for column in COVERAGE_METRIC_COLUMNS:
            assert column in row
        # ... and the computable airflow columns carry real values, not zeros.
        assert row["databricks_native_activities"] == 1  # the PythonOperator
        assert row["other_activities"] == 1  # the placeholder
        assert row["complexity_score"] == 4  # 1*1 + 1*3


def test_airflow_inventory_persists_audit_status_counts_and_findings() -> None:
    finding = {
        "code": "unsupported_operator",
        "severity": "gap",
        "fingerprint": "stable123",
        "message": "manual translation required",
    }
    pipeline = Pipeline(
        name="audited",
        reconciliation_status="verified_with_gaps",
        migration_status="included",
        not_translatable=[finding],
        audit={
            "audited_activity_count": 8,
            "deterministic_count": 7,
            "agentic_count": 1,
            "failed_count": 0,
            "excluded_count": 0,
            "transformations": [{"code": "task_key_collision_resolved"}],
        },
    )

    inventory = build_inventory_dict([pipeline], "/src")
    entry = inventory["pipelines"][0]

    assert entry["audited_activity_count"] == 8
    assert entry["deterministic_count"] == 7
    assert entry["agentic_count"] == 1
    assert entry["failed_count"] == 0
    assert entry["excluded_count"] == 0
    assert entry["coverage_pct"] == 100.0
    assert entry["deterministic_coverage_pct"] == 87.5
    assert entry["reconciliation_status"] == "verified_with_gaps"
    assert entry["findings"] == [finding]
    assert entry["transformations"] == [{"code": "task_key_collision_resolved"}]
    assert inventory["summary"]["activity_count"] == 8
    assert inventory["summary"]["deterministic_coverage_pct"] == 87.5


def test_airflow_profile_categories_never_mix_audited_and_synthetic_tasks() -> None:
    pipeline = Pipeline(
        name="profile",
        tasks=[],
        audit={
            "audited_activity_count": 1,
            "deterministic_count": 0,
            "agentic_count": 0,
            "failed_count": 1,
            "excluded_count": 0,
        },
    )
    pipeline.tasks.extend(
        [
            NotebookActivity(name="first", task_key="first", notebook_path="/Shared/first"),
            NotebookActivity(name="second", task_key="second", notebook_path="/Shared/second"),
        ]
    )

    row = _profile_row(pipeline)

    assert row["other_activities"] >= 0
    assert row["complexity_score"] >= 0
    assert (
        row["databricks_native_activities"] + row["control_flow_activities"] + row["other_activities"]
        == row["activities"]
    )
