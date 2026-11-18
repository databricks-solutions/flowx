"""Tests for the Databricks job settings an Airflow DAG's schedule carries over: timezone and pause state."""

from __future__ import annotations

from pathlib import Path

from flowx.models.ir import Pipeline
from flowx.sources.airflow.loader import load_airflow_dag


def _load(
    tmp_path: Path, dag_arguments: str, *, task: str = "BashOperator(task_id='t', bash_command='echo hi')"
) -> Pipeline:
    dag_path = tmp_path / "dag.py"
    dag_path.write_text(
        "import pendulum\n"
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        f"with DAG(dag_id='d', schedule_interval='0 1 * * *'{dag_arguments}) as dag:\n"
        f"    t = {task}\n",
        encoding="utf-8",
    )
    return load_airflow_dag(dag_path)


def _transformation(pipeline: Pipeline, code: str) -> dict | None:
    return next((item for item in pipeline.audit["transformations"] if item["code"] == code), None)


def test_schedule_without_a_timezone_discloses_the_utc_default(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "")

    assert pipeline.schedule["timezone_id"] == "UTC"
    disclosure = _transformation(pipeline, "schedule_timezone_defaulted")
    assert disclosure is not None
    assert "default_timezone" in disclosure["message"]


def test_dag_start_date_timezone_is_not_disclosed(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, ", start_date=pendulum.datetime(2024, 1, 1, tz='Europe/Madrid')")

    assert pipeline.schedule["timezone_id"] == "Europe/Madrid"
    assert _transformation(pipeline, "schedule_timezone_defaulted") is None


def test_default_args_start_date_sets_the_schedule_timezone(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, ", default_args={'start_date': pendulum.datetime(2024, 1, 1, tz='America/New_York')}")

    assert pipeline.schedule["timezone_id"] == "America/New_York"
    assert _transformation(pipeline, "schedule_timezone_defaulted") is None
