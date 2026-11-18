"""Tests for Python callables that import packages Databricks compute does not provide."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowx.models.ir import NotebookActivity, PlaceholderActivity
from flowx.sources.airflow.loader import load_airflow_dag


def _task(tmp_path: Path, setup: str, body: str):
    dag_path = tmp_path / "dag.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        f"{setup}"
        "def load():\n"
        f"{body}"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='t', python_callable=load)\n",
        encoding="utf-8",
    )
    return next(task for task in load_airflow_dag(dag_path).tasks if task.task_key == "t")


@pytest.mark.parametrize(
    ("setup", "body", "missing"),
    [
        ("", "    import snowflake.connector\n    snowflake.connector.connect()\n", "snowflake"),
        ("import pyodbc\n", "    pyodbc.connect('dsn')\n", "pyodbc"),
        ("from libs.dq import run_checks\n", "    run_checks()\n", "libs"),
        ("from .helpers import clean\n", "    clean()\n", ".helpers"),
    ],
)
def test_callable_importing_a_worker_package_is_a_gap(tmp_path: Path, setup: str, body: str, missing: str) -> None:
    task = _task(tmp_path, setup, body)

    assert isinstance(task, PlaceholderActivity)
    assert f"callable imports {missing}," in task.comment


@pytest.mark.parametrize(
    ("setup", "body"),
    [
        ("import json\nimport os\n", "    print(json.dumps({'cwd': os.getcwd()}))\n"),
        ("import pandas as pd\n", "    import requests\n    print(pd.DataFrame(), requests.__name__)\n"),
        ("import pyodbc\n", "    print('pyodbc is imported by the module but unused here')\n"),
    ],
)
def test_callable_using_standard_and_runtime_packages_is_translated(tmp_path: Path, setup: str, body: str) -> None:
    assert isinstance(_task(tmp_path, setup, body), NotebookActivity)
