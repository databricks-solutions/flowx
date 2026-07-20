"""Tests for source-aware inputs prompts and airflow coverage profile columns."""

from __future__ import annotations

import tempfile
from pathlib import Path

from flowx.adapter.session import MigrationInputSession
from flowx.reporting.coverage import COVERAGE_METRIC_COLUMNS, build_coverage_rows
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


def test_inputs_default_source_is_adf():
    # Back-compat: no source arg -> ADF prompts.
    session = MigrationInputSession(phase="discover")
    assert any(o.option_id == "adf_source_path" for o in session.pending().options)


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
