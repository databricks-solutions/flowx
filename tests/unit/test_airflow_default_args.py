"""Tests for reading a DAG's ``default_args``: inline, from a dict built in the DAG file, or not at all."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowx.models.ir import Pipeline
from flowx.sources.airflow.loader import load_airflow_dag


def _load(tmp_path: Path, setup: str, default_args: str = "default_args") -> Pipeline:
    dag_path = tmp_path / "dag.py"
    dag_path.write_text(
        "from datetime import timedelta\n"
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        f"{setup}\n"
        f"with DAG(dag_id='d', default_args={default_args}, schedule_interval='0 1 * * *') as dag:\n"
        "    t = BashOperator(task_id='t', bash_command='echo hi')\n",
        encoding="utf-8",
    )
    return load_airflow_dag(dag_path)


def _task(pipeline: Pipeline):
    return next(task for task in pipeline.tasks if task.task_key == "t")


def _gap_names(pipeline: Pipeline) -> list[str]:
    return [item["details"]["name"] for item in pipeline.not_translatable if item["code"] == "unsupported_dag_setting"]


@pytest.mark.parametrize(
    "setup",
    [
        "default_args = {'retries': 1, 'retry_delay': timedelta(minutes=5)}\ndefault_args['retries'] = 3",
        "default_args = dict(retries=1, retry_delay=timedelta(minutes=5))\ndefault_args.update({'retries': 3})",
        "default_args = {'retries': 3}\ndefault_args.update(retry_delay=timedelta(minutes=5))",
        "base = {'retries': 3}\ndefault_args = {**base, 'retry_delay': timedelta(minutes=5)}",
    ],
)
def test_dict_built_in_the_dag_file_carries_its_retry_policy(tmp_path: Path, setup: str) -> None:
    pipeline = _load(tmp_path, setup)

    task = _task(pipeline)
    assert task.max_retries == 3
    assert task.min_retry_interval_millis == 300000
    assert pipeline.reconciliation_status == "verified"


def test_change_after_the_dag_is_built_does_not_apply(tmp_path: Path) -> None:
    dag_path = tmp_path / "dag.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "default_args = {'retries': 1}\n"
        "dag = DAG(dag_id='d', default_args=default_args)\n"
        "default_args['retries'] = 5\n"
        "t = BashOperator(task_id='t', bash_command='echo hi', dag=dag)\n",
        encoding="utf-8",
    )

    assert _task(load_airflow_dag(dag_path)).max_retries == 1


def test_module_level_dict_settings_are_checked_like_inline_ones(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "default_args = {'depends_on_past': True, 'retries': 2}")

    assert _gap_names(pipeline) == ["default_args.depends_on_past"]


def test_deleted_key_is_not_audited(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path, "default_args = {'depends_on_past': True, 'retries': 2}\ndel default_args['depends_on_past']"
    )

    assert pipeline.reconciliation_status == "verified"


@pytest.mark.parametrize(
    ("setup", "default_args"),
    [
        ("from libs.airflow_defaults import default_args\ndefault_args['retries'] = 3", "default_args"),
        ("from libs.airflow_defaults import base", "{**base, 'retries': 3}"),
        ("default_args = {'retries': 1}\ndefault_args.setdefault('retries', 3)", "default_args"),
        ("default_args = {'retries': 1}\nalias = default_args\nalias['retries'] = 3", "default_args"),
        ("def build():\n    return {'retries': 3}\ndefault_args = build()", "default_args"),
    ],
)
def test_unreadable_default_args_is_a_gap(tmp_path: Path, setup: str, default_args: str) -> None:
    pipeline = _load(tmp_path, setup, default_args)

    assert "default_args" in _gap_names(pipeline)
    assert pipeline.reconciliation_status == "verified_with_gaps"


def test_default_args_none_is_unset(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "", "None")

    assert pipeline.reconciliation_status == "verified"


def test_key_popped_into_a_variable_is_not_audited(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path, "default_args = {'depends_on_past': True, 'retries': 2}\nflag = default_args.pop('depends_on_past')"
    )

    assert pipeline.reconciliation_status == "verified"
    assert _task(pipeline).max_retries == 2
