"""Tests that operators built without ``dag=`` join the DAG through dependency wiring, as in Airflow."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from flowx.models.ir import Pipeline, RunJobActivity
from flowx.sources.airflow import audit as source_audit
from flowx.sources.airflow import loader as airflow_loader


def _write(tmp_path: Path, source: str) -> Path:
    dag_path = tmp_path / "dag.py"
    dag_path.write_text(source, encoding="utf-8")
    return dag_path


def _load(tmp_path: Path, source: str) -> Pipeline:
    return airflow_loader.load_airflow_dag(_write(tmp_path, source))


def _edges(pipeline: Pipeline) -> set[tuple[str, str]]:
    return {(dependency.task_key, task.task_key) for task in pipeline.tasks for dependency in task.depends_on or []}


def _task_keys(pipeline: Pipeline) -> set[str]:
    return {task.task_key for task in pipeline.tasks}


def test_operator_without_dag_argument_joins_the_dag_through_shift_wiring(tmp_path: Path) -> None:
    source = (
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.operators.trigger_dagrun import TriggerDagRunOperator\n"
        "dag = DAG('coordinator', schedule_interval='0 3 * * 1-5')\n"
        "start = BashOperator(task_id='start', bash_command='echo start', dag=dag)\n"
        "child_a = TriggerDagRunOperator(task_id='child_a', trigger_dag_id='child_a')\n"
        "child_b = TriggerDagRunOperator(task_id='child_b', trigger_dag_id='child_b')\n"
        "start >> child_a >> child_b\n"
    )

    pipeline = _load(tmp_path, source)

    tasks = {task.task_key: task for task in pipeline.tasks}
    assert isinstance(tasks["child_a"], RunJobActivity)
    assert isinstance(tasks["child_b"], RunJobActivity)
    assert {("start", "child_a"), ("child_a", "child_b")} <= _edges(pipeline)
    assert pipeline.reconciliation_status == "verified"


def test_inline_operator_without_dag_argument_joins_the_dag_through_shift_wiring(tmp_path: Path) -> None:
    source = (
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.operators.trigger_dagrun import TriggerDagRunOperator\n"
        "dag = DAG('coordinator', schedule_interval=None)\n"
        "start = BashOperator(task_id='start', bash_command='echo start', dag=dag)\n"
        "start >> TriggerDagRunOperator(task_id='child', trigger_dag_id='child')\n"
    )

    pipeline = _load(tmp_path, source)

    assert ("start", "child") in _edges(pipeline)
    assert pipeline.reconciliation_status == "verified"


def test_wired_operator_without_dag_argument_is_never_dropped_silently(tmp_path: Path) -> None:
    # Inline operator calls inside a fan-out list aren't captured even with dag=, so this shape fails
    # reconciliation. Once fan-out inline capture lands, assert the start -> left/right edges instead.
    source = (
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.operators.trigger_dagrun import TriggerDagRunOperator\n"
        "dag = DAG('coordinator', schedule_interval=None)\n"
        "start = BashOperator(task_id='start', bash_command='echo start', dag=dag)\n"
        "start >> [TriggerDagRunOperator(task_id='left', trigger_dag_id='left'),\n"
        "          TriggerDagRunOperator(task_id='right', trigger_dag_id='right')]\n"
    )

    pipeline = _load(tmp_path, source)

    assert pipeline.reconciliation_status == "failed"


def test_operator_without_dag_argument_joins_through_chain_and_set_downstream(tmp_path: Path) -> None:
    source = (
        "from airflow import DAG\n"
        "from airflow.models.baseoperator import chain\n"
        "from airflow.operators.bash import BashOperator\n"
        "dag = DAG('coordinator', schedule_interval=None)\n"
        "start = BashOperator(task_id='start', bash_command='echo start', dag=dag)\n"
        "middle = BashOperator(task_id='middle', bash_command='echo middle')\n"
        "finish = BashOperator(task_id='finish', bash_command='echo finish')\n"
        "chain(start, middle)\n"
        "middle.set_downstream(finish)\n"
    )

    pipeline = _load(tmp_path, source)

    assert {("start", "middle"), ("middle", "finish")} <= _edges(pipeline)
    assert pipeline.reconciliation_status == "verified"


def test_unwired_operator_without_dag_argument_is_not_part_of_the_dag(tmp_path: Path) -> None:
    source = (
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "dag = DAG('coordinator', schedule_interval=None)\n"
        "start = BashOperator(task_id='start', bash_command='echo start', dag=dag)\n"
        "orphan = BashOperator(task_id='orphan', bash_command='echo orphan')\n"
    )

    pipeline = _load(tmp_path, source)

    assert "orphan" not in _task_keys(pipeline)
    assert pipeline.reconciliation_status == "verified"


def test_operator_bound_to_another_dag_is_not_adopted_through_wiring(tmp_path: Path) -> None:
    source = (
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "dag = DAG('first', schedule_interval=None)\n"
        "other_dag = DAG('second', schedule_interval=None)\n"
        "start = BashOperator(task_id='start', bash_command='echo start', dag=dag)\n"
        "elsewhere = BashOperator(task_id='elsewhere', bash_command='echo elsewhere', dag=other_dag)\n"
    )

    pipelines = airflow_loader.load_airflow_dags(_write(tmp_path, source))

    first = next(pipeline for pipeline in pipelines if pipeline.name == "first")
    assert _task_keys(first) == {"start"}


def test_reassigned_variable_does_not_pull_an_unwired_task_into_the_dag(tmp_path: Path) -> None:
    source = (
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.operators.trigger_dagrun import TriggerDagRunOperator\n"
        "dag = DAG('coordinator', schedule_interval=None)\n"
        "start = BashOperator(task_id='start', bash_command='echo start', dag=dag)\n"
        "step = BashOperator(task_id='bound_step', bash_command='echo bound', dag=dag)\n"
        "start >> step\n"
        "step = TriggerDagRunOperator(task_id='orphan', trigger_dag_id='orphan')\n"
    )

    pipeline = _load(tmp_path, source)

    assert _task_keys(pipeline) == {"start", "bound_step"}
    assert pipeline.reconciliation_status == "verified"


def test_wiring_attaches_the_binding_current_at_that_line(tmp_path: Path) -> None:
    source = (
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.operators.trigger_dagrun import TriggerDagRunOperator\n"
        "dag = DAG('coordinator', schedule_interval=None)\n"
        "start = BashOperator(task_id='start', bash_command='echo start', dag=dag)\n"
        "child = TriggerDagRunOperator(task_id='wired', trigger_dag_id='wired')\n"
        "start >> child\n"
        "child = TriggerDagRunOperator(task_id='never_wired', trigger_dag_id='never_wired')\n"
    )

    pipeline = _load(tmp_path, source)

    assert _task_keys(pipeline) == {"start", "wired"}
    assert _edges(pipeline) == {("start", "wired")}
    assert pipeline.reconciliation_status == "verified"


def test_operator_with_explicit_dag_none_joins_the_dag_through_wiring(tmp_path: Path) -> None:
    source = (
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.operators.trigger_dagrun import TriggerDagRunOperator\n"
        "dag = DAG('coordinator', schedule_interval=None)\n"
        "start = BashOperator(task_id='start', bash_command='echo start', dag=dag)\n"
        "child = TriggerDagRunOperator(task_id='child', trigger_dag_id='child', dag=None)\n"
        "start >> child\n"
    )

    pipeline = _load(tmp_path, source)

    assert ("start", "child") in _edges(pipeline)
    assert pipeline.reconciliation_status == "verified"


def test_task_adopted_through_wiring_does_not_inherit_dag_default_args(tmp_path: Path) -> None:
    # Airflow merges default_args when an operator is built; a task that joins later via >> gets none.
    source = (
        "from datetime import timedelta\n"
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "dag = DAG('coordinator', schedule_interval=None, default_args={\n"
        "    'retries': 4, 'retry_delay': timedelta(minutes=7), 'execution_timeout': timedelta(hours=1)})\n"
        "start = BashOperator(task_id='start', bash_command='echo start', dag=dag)\n"
        "child = BashOperator(task_id='child', bash_command='echo child')\n"
        "start >> child\n"
    )

    pipeline = _load(tmp_path, source)

    tasks = {task.task_key: task for task in pipeline.tasks}
    assert tasks["start"].max_retries == 4
    assert tasks["start"].min_retry_interval_millis == 420000
    assert tasks["start"].timeout_seconds == 3600
    assert tasks["child"].max_retries is None
    assert tasks["child"].min_retry_interval_millis is None
    assert tasks["child"].timeout_seconds is None


_COORDINATOR_HEADER = (
    "from airflow import DAG\n"
    "from airflow.operators.bash import BashOperator\n"
    "from airflow.operators.trigger_dagrun import TriggerDagRunOperator\n"
    "dag = DAG('coordinator', schedule_interval=None)\n"
    "start = BashOperator(task_id='start', bash_command='echo start', dag=dag)\n"
)


@pytest.mark.parametrize(
    "wiring",
    [
        pytest.param(
            "ts = []\n"
            "for n in ['a', 'b']:\n"
            "    ts.append(TriggerDagRunOperator(task_id=n, trigger_dag_id=n))\n"
            "start >> ts\n",
            id="append-in-loop",
        ),
        pytest.param(
            "triggers = [TriggerDagRunOperator(task_id=n, trigger_dag_id=n) for n in ['a', 'b']]\nstart >> triggers\n",
            id="list-comprehension",
        ),
        pytest.param(
            "a, b = (BashOperator(task_id='a', bash_command='echo a'),\n"
            "        BashOperator(task_id='b', bash_command='echo b'))\n"
            "start >> [a, b]\n",
            id="tuple-unpacking",
        ),
        pytest.param(
            "ts = {n: TriggerDagRunOperator(task_id=n, trigger_dag_id=n) for n in ['a', 'b']}\nstart >> ts['a']\n",
            id="dict-comprehension",
        ),
    ],
)
def test_dagless_operator_in_a_shape_the_scan_cannot_follow_fails_reconciliation(tmp_path: Path, wiring: str) -> None:
    pipeline = _load(tmp_path, _COORDINATOR_HEADER + wiring)

    assert pipeline.reconciliation_status == "failed"


def test_wiring_two_dagless_tasks_before_either_has_a_dag_fails_reconciliation(tmp_path: Path) -> None:
    # Airflow raises at import here: a >> b runs while neither task has a DAG yet.
    source = _COORDINATOR_HEADER + (
        "a = BashOperator(task_id='a', bash_command='echo a')\n"
        "b = BashOperator(task_id='b', bash_command='echo b')\n"
        "a >> b\n"
        "start >> a\n"
    )

    pipeline = _load(tmp_path, source)

    assert pipeline.reconciliation_status == "failed"


def test_dagless_task_wired_into_two_dags_fails_reconciliation(tmp_path: Path) -> None:
    source = (
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "dag = DAG('first', schedule_interval=None)\n"
        "other_dag = DAG('second', schedule_interval=None)\n"
        "start = BashOperator(task_id='start', bash_command='echo start', dag=dag)\n"
        "elsewhere = BashOperator(task_id='elsewhere', bash_command='echo elsewhere', dag=other_dag)\n"
        "shared = BashOperator(task_id='shared', bash_command='echo shared')\n"
        "start >> shared\n"
        "elsewhere >> shared\n"
    )

    pipelines = airflow_loader.load_airflow_dags(_write(tmp_path, source))

    first = next(pipeline for pipeline in pipelines if pipeline.name == "first")
    assert first.reconciliation_status == "failed"


def test_chain_of_dagless_tasks_joins_once_the_first_is_wired_to_the_dag(tmp_path: Path) -> None:
    source = _COORDINATOR_HEADER + (
        "a = BashOperator(task_id='a', bash_command='echo a')\n"
        "b = BashOperator(task_id='b', bash_command='echo b')\n"
        "start >> a\n"
        "a >> b\n"
    )

    pipeline = _load(tmp_path, source)

    assert {("start", "a"), ("a", "b")} <= _edges(pipeline)
    assert pipeline.reconciliation_status == "verified"


def test_audit_reports_a_dagless_task_it_cannot_tie_to_any_wiring(tmp_path: Path) -> None:
    source = _COORDINATOR_HEADER + (
        "ts = []\n"
        "for n in ['a', 'b']:\n"
        "    ts.append(TriggerDagRunOperator(task_id=n, trigger_dag_id=n))\n"
        "start >> ts\n"
    )

    audit = source_audit.audit_module(ast.parse(source), target_dag_variable="dag")

    assert "dagless_task_membership_unknown" in {candidate.code for candidate in audit.unresolved}


def test_audit_keeps_a_named_but_unwired_dagless_task_as_an_orphan(tmp_path: Path) -> None:
    source = _COORDINATOR_HEADER + "orphan = BashOperator(task_id='orphan', bash_command='echo orphan')\n"

    audit = source_audit.audit_module(ast.parse(source), target_dag_variable="dag")

    assert audit.unresolved == []
    assert {candidate.details.get("task_id") for candidate in audit.tasks} == {"start"}
