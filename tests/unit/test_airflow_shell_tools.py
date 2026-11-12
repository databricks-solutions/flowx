"""Tests for shell tasks that call programs Databricks compute does not provide."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowx.models.ir import NotebookActivity, PlaceholderActivity
from flowx.sources.airflow.loader import load_airflow_dag
from flowx.sources.airflow.shell_tools import unavailable_programs


@pytest.mark.parametrize(
    ("command", "expected"),
    [
        ('sqlcmd -S server -Q "select 1"', ["sqlcmd"]),
        ("cd /tmp && aws s3 cp a b | gzip > out.gz", ["aws"]),
        ('bash -c "jq . data.json && echo done"', ["jq"]),
        ("./load.sh --day 1", ["load.sh"]),
        ("/opt/tools/bin/loader", ["loader"]),
        ("for f in a b; do echo $f; done", []),
        ("if [ -f x ]; then rm x; fi", []),
        ("cleanup() { rm -f /tmp/x; }; cleanup", []),
        ("set -e\nmkdir -p /tmp/out && curl -s https://x | tee /tmp/out/x | wc -l", []),
        ("python3 -c 'print(1)'", []),
        ("shopt -s nullglob; mapfile -t rows < f; typeset -i n=1; hash -r; kill -0 $$", []),
        ("echo {{ ds }}", []),
    ],
)
def test_unavailable_programs(command: str, expected: list[str]) -> None:
    assert unavailable_programs(command) == expected


def _task(tmp_path: Path, task: str):
    dag_path = tmp_path / "dag.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        f"    t = {task}\n",
        encoding="utf-8",
    )
    return next(task for task in load_airflow_dag(dag_path).tasks if task.task_key == "t")


def test_command_running_a_client_tool_is_a_gap(tmp_path: Path) -> None:
    task = _task(tmp_path, "BashOperator(task_id='t', bash_command='aws s3 cp s3://bucket/x /tmp/x')")

    assert isinstance(task, PlaceholderActivity)
    assert "aws" in task.comment
    assert task.raw_definition["unavailable_programs"] == ["aws"]


def test_command_using_standard_programs_is_translated(tmp_path: Path) -> None:
    task = _task(tmp_path, "BashOperator(task_id='t', bash_command='mkdir -p /tmp/x && date > /tmp/x/run')")

    assert isinstance(task, NotebookActivity)
