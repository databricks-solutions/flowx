"""Unit tests for packaging files a DAG names by their Airflow-machine path from the export."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowx.models.ir import SparkPythonActivity
from flowx.sources.airflow.loader import load_airflow_dags


@pytest.mark.parametrize(
    "task",
    [
        "BashOperator(task_id='job', bash_command='spark-submit /home/jdoe/jobs/reprocess.py --ff-days 3')",
        "SparkSubmitOperator(task_id='job', application='/home/jdoe/jobs/reprocess.py', "
        "application_args=['--ff-days', '3'])",
    ],
)
def test_spark_submit_script_from_the_export_is_packaged(tmp_path: Path, task: str) -> None:
    script = tmp_path / "export" / "home" / "jdoe" / "jobs" / "reprocess.py"
    script.parent.mkdir(parents=True)
    script.write_text("import argparse\nprint('reprocess')\n", encoding="utf-8")

    pipeline = _load(tmp_path, task)
    activity = pipeline.tasks[0]

    assert isinstance(activity, SparkPythonActivity)
    assert activity.generated_source == "import argparse\nprint('reprocess')\n"
    assert activity.parameters == ["--ff-days", "3"]
    codes = [item["code"] for item in pipeline.audit["transformations"]]
    assert "spark_submit_application_packaged" in codes


def test_spark_submit_script_missing_from_the_export_keeps_its_path(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "BashOperator(task_id='job', bash_command='spark-submit /opt/jobs/etl.py')")
    activity = pipeline.tasks[0]

    assert isinstance(activity, SparkPythonActivity)
    assert activity.python_file == "/opt/jobs/etl.py"
    assert activity.generated_source is None


def test_databricks_paths_are_left_to_package(tmp_path: Path) -> None:
    script = tmp_path / "export" / "Workspace" / "jobs" / "etl.py"
    script.parent.mkdir(parents=True)
    script.write_text("print(1)\n", encoding="utf-8")

    pipeline = _load(tmp_path, "BashOperator(task_id='job', bash_command='spark-submit /Workspace/jobs/etl.py')")

    assert pipeline.tasks[0].generated_source is None


def _load(tmp_path: Path, task: str):
    root = tmp_path / "export"
    dag_path = root / "dags" / "dag.py"
    dag_path.parent.mkdir(parents=True, exist_ok=True)
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator\n"
        "with DAG(dag_id='d', schedule_interval='15 * * * *', catchup=False) as dag:\n"
        f"    {task}\n",
        encoding="utf-8",
    )
    pipelines = load_airflow_dags(dag_path, source_root=root)
    assert len(pipelines) == 1
    return pipelines[0]
