"""Tests for environment variables a shell task reads from the Airflow worker or SSH host."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowx.models.ir import NotebookActivity, PlaceholderActivity
from flowx.sources.airflow.loader import load_airflow_dag
from flowx.sources.airflow.shell_environment import environment_references


@pytest.mark.parametrize(
    ("command", "expected"),
    [
        ("$DBT_PATH run --select dim_aircraft", ["DBT_PATH"]),
        ("cd ${PROJECT_DIR:-/opt} && make", ["PROJECT_DIR"]),
        ('echo "$A $B" $A', ["A", "B"]),
        ("echo ${#ROWS} ${!REF}", ["ROWS", "REF"]),
        ("echo '$NOT_EXPANDED'", []),
        ("echo \\$ESCAPED", []),
        ("echo $1 $? $@ $$ $#", []),
        ("echo $HOME $PATH $USER", []),
        ("OUT=/tmp/x; echo $OUT", []),
        ("export TARGET=prod && dbt run --target $TARGET", []),
        ("for f in a b; do echo $f; done", []),
        ("read -r LINE < f; echo $LINE", []),
        ("echo {{ params.path }} $(date +%F)", []),
        ("cat <<'EOF'\n$LITERAL\nEOF\necho done", []),
        ("cat <<EOF\n$EXPANDED\nEOF\n", ["EXPANDED"]),
    ],
)
def test_environment_references(command: str, expected: list[str]) -> None:
    assert environment_references(command) == expected


def _task(tmp_path: Path, task: str):
    dag_path = tmp_path / "dag.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.providers.ssh.operators.ssh import SSHOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        f"    t = {task}\n",
        encoding="utf-8",
    )
    pipeline = load_airflow_dag(dag_path)
    return next(task for task in pipeline.tasks if task.task_key == "t")


def test_bash_command_reading_a_worker_variable_is_a_gap(tmp_path: Path) -> None:
    task = _task(tmp_path, "BashOperator(task_id='t', bash_command='$DBT_PATH snapshot --select snp')")

    assert isinstance(task, PlaceholderActivity)
    assert "$DBT_PATH" in task.comment
    assert "Airflow worker" in task.comment
    assert task.raw_definition["environment_variables"] == ["DBT_PATH"]


def test_ssh_command_names_the_ssh_host(tmp_path: Path) -> None:
    task = _task(tmp_path, "SSHOperator(task_id='t', ssh_conn_id='edge', command='run.sh $ENVIRONMENT')")

    assert isinstance(task, PlaceholderActivity)
    assert "SSH host" in task.comment


def test_command_that_sets_its_own_variables_is_translated(tmp_path: Path) -> None:
    task = _task(tmp_path, "BashOperator(task_id='t', bash_command='DAY=$(date +%F); echo $DAY $HOME')")

    assert isinstance(task, NotebookActivity)
