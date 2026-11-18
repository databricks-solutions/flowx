"""Tests for reading a DAG's ``default_args``: inline, from a dict built in the DAG file, or not at all."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowx.models.ir import Pipeline
from flowx.sources.airflow.loader import load_airflow_dag, load_airflow_dags


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


def test_dict_passed_to_a_function_before_the_dag_is_a_gap(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "from libs import mutate\ndefault_args = {'retries': 1}\nmutate(default_args)")

    assert "default_args" in _gap_names(pipeline)


def test_dict_read_by_a_builtin_is_still_followed(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "default_args = {'retries': 2}\nprint(len(default_args))")

    assert pipeline.reconciliation_status == "verified"
    assert _task(pipeline).max_retries == 2


def test_annotated_dict_is_read_like_a_plain_one(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "default_args: dict = {'retries': 3}")

    assert pipeline.reconciliation_status == "verified"
    assert _task(pipeline).max_retries == 3


def test_dict_passed_as_a_default_args_keyword_to_a_function_is_a_gap(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path, "from libs import mutate\ndefault_args = {'retries': 1}\nmutate(default_args=default_args)"
    )

    assert "default_args" in _gap_names(pipeline)


def test_change_made_in_a_module_level_loop_is_followed(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "default_args = {'retries': 1}\nfor env in ['prod']:\n    default_args['retries'] = 3")

    assert pipeline.reconciliation_status == "verified"
    assert _task(pipeline).max_retries == 3


def test_dags_generated_in_a_loop_get_the_dict_as_the_loop_changed_it(tmp_path: Path) -> None:
    dag_path = tmp_path / "dags.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "default_args = {'retries': 1}\n"
        "for team in ['a', 'b']:\n"
        "    default_args['retries'] = 4\n"
        "    with DAG(dag_id=f'etl_{team}', default_args=default_args) as dag:\n"
        "        BashOperator(task_id='t', bash_command='echo hi')\n",
        encoding="utf-8",
    )

    pipelines = load_airflow_dags(dag_path)

    assert [(pipeline.name, _task(pipeline).max_retries) for pipeline in pipelines] == [("etl_a", 4), ("etl_b", 4)]


def test_inline_dict_call_is_read_like_a_dict_literal(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "", "dict(owner='a', retries=3)")

    assert pipeline.reconciliation_status == "verified"
    assert _task(pipeline).max_retries == 3


def test_reading_a_module_list_keeps_later_loops_unrolled(tmp_path: Path) -> None:
    dag_path = tmp_path / "dags.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "TABLES = ['a', 'b']\n"
        "RETRIES = TABLES.index('b') + 1\n"
        "for table in TABLES:\n"
        "    with DAG(dag_id=f'load_{table}', default_args={'retries': 2}) as dag:\n"
        "        BashOperator(task_id='t', bash_command='echo hi')\n",
        encoding="utf-8",
    )

    pipelines = load_airflow_dags(dag_path)

    assert [(pipeline.name, _task(pipeline).max_retries) for pipeline in pipelines] == [("load_a", 2), ("load_b", 2)]


def test_change_inside_an_uncalled_helper_keeps_the_loop_unrolled(tmp_path: Path) -> None:
    dag_path = tmp_path / "dags.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "TEAMS = ['a', 'b']\n"
        "def add_team(name):\n"
        "    TEAMS.append(name)\n"
        "for team in TEAMS:\n"
        "    with DAG(dag_id=f'etl_{team}') as dag:\n"
        "        BashOperator(task_id='t', bash_command='echo hi')\n",
        encoding="utf-8",
    )

    assert sorted(pipeline.name for pipeline in load_airflow_dags(dag_path)) == ["etl_a", "etl_b"]


def test_dict_passed_to_an_aliased_dag_constructor_is_still_read(tmp_path: Path) -> None:
    dag_path = tmp_path / "dag.py"
    dag_path.write_text(
        "from airflow import DAG as Workflow\n"
        "from airflow.operators.bash import BashOperator\n"
        "default_args = {'retries': 3}\n"
        "dag = Workflow(dag_id='d', default_args=default_args)\n"
        "t = BashOperator(task_id='t', bash_command='echo hi', dag=dag)\n",
        encoding="utf-8",
    )

    pipeline = load_airflow_dag(dag_path)

    assert pipeline.reconciliation_status == "verified"
    assert _task(pipeline).max_retries == 3
