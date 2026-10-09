"""Tests for Teradata work in Airflow tasks: utilities in shell commands, BteqOperator, and TeradataOperator."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from flowx.models.ir import NotebookActivity, Pipeline, PlaceholderActivity
from flowx.sources.airflow import teradata
from flowx.sources.airflow.loader import load_airflow_dag

_IMPORTS = (
    "from airflow import DAG\n"
    "from airflow.operators.bash import BashOperator\n"
    "from airflow.providers.ssh.operators.ssh import SSHOperator\n"
    "from airflow.providers.teradata.operators.bteq import BteqOperator\n"
    "from airflow.providers.teradata.operators.teradata import TeradataOperator\n"
)


def _load(tmp_path: Path, task: str, *, files: dict[str, str] | None = None) -> Pipeline:
    for name, content in (files or {}).items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    dag_path = tmp_path / "dags" / "dag.py"
    dag_path.parent.mkdir(parents=True, exist_ok=True)
    dag_path.write_text(f"{_IMPORTS}with DAG(dag_id='d') as dag:\n    t = {task}\n", encoding="utf-8")
    return load_airflow_dag(dag_path)


def _task(pipeline: Pipeline):
    return next(task for task in pipeline.tasks if task.task_key == "t")


def _teradata(pipeline: Pipeline) -> dict:
    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    return (task.raw_definition or {})["teradata"]


# --------------------------------------------------------------------------------------
# Command analysis
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("command", "utilities"),
    [
        ("bteq < /opt/scripts/load.bteq", ["bteq"]),
        ("/usr/bin/tbuild -f job.tpt && mload < m.ml", ["tbuild", "mload"]),
        ("bteq 'unterminated", ["bteq"]),
        ("echo done; ls /opt/bteq/", []),
        ("cat load.bteq.bak", []),
    ],
)
def test_finds_teradata_utilities(command: str, utilities: list[str]) -> None:
    assert teradata.find_utilities(command) == utilities


@pytest.mark.parametrize(
    ("command", "references"),
    [
        ("bteq < /opt/scripts/load.bteq > out.log", ["/opt/scripts/load.bteq"]),
        ("cat a.bteq b.bteq | bteq", ["a.bteq", "b.bteq"]),
        ("tbuild -f job.tpt -v vars.txt", ["job.tpt"]),
        ("bteq <<EOF\n.LOGON x/y,z;\nSELECT 1;\nEOF", []),
    ],
)
def test_finds_the_scripts_fed_to_utilities(command: str, references: list[str]) -> None:
    assert teradata.script_references(command) == references


def test_heredoc_body_is_the_inline_script() -> None:
    command = "bteq <<'EOF' > out.log\n.LOGON x/y,z;\nSELECT 1;\n.LOGOFF;\nEOF\necho done"

    assert teradata.heredoc_scripts(command) == [".LOGON x/y,z;\nSELECT 1;\n.LOGOFF;"]


def test_large_script_is_recorded_without_content(tmp_path: Path) -> None:
    script = tmp_path / "big.bteq"
    data = ("SELECT 1;\n" * (teradata.MAXIMUM_ATTACHED_SCRIPT_BYTES // 10 + 1)).encode()
    script.write_bytes(data)

    record = teradata.script_record(str(script), ())

    assert record["size_bytes"] == len(data)
    assert record["sha256"] == hashlib.sha256(data).hexdigest()
    assert "content" not in record


# --------------------------------------------------------------------------------------
# Lowering
# --------------------------------------------------------------------------------------


def test_bash_bteq_becomes_a_gap_with_its_script(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "BashOperator(task_id='t', bash_command='bteq < scripts/load.bteq')",
        files={"dags/scripts/load.bteq": ".LOGON x/y,z;\nSEL * FROM db.t;\n"},
    )

    details = _teradata(pipeline)
    assert details["utilities"] == ["bteq"]
    [script] = details["scripts"]
    assert script["path"] == "scripts/load.bteq"
    assert script["content"] == ".LOGON x/y,z;\nSEL * FROM db.t;\n"
    assert "Databricks has no Teradata client" in _task(pipeline).comment
    assert pipeline.reconciliation_status == "verified_with_gaps"


def test_missing_absolute_script_is_recorded_as_unresolved(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "SSHOperator(task_id='t', ssh_conn_id='edge', command='fastload < /opt/x.fl')")

    [script] = _teradata(pipeline)["scripts"]
    assert script == {"path": "/opt/x.fl", "unresolved": "was not found in /opt"}


def test_heredoc_bteq_carries_the_inline_script(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, 'BashOperator(task_id="t", bash_command="bteq <<EOF\\nSELECT 1;\\nEOF")')

    assert _teradata(pipeline)["inline_scripts"] == ["SELECT 1;"]


def test_wrapper_script_template_is_read_and_checked(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "BashOperator(task_id='t', bash_command='run_load.sh')",
        files={"dags/run_load.sh": "#!/bin/bash\nset -e\nbteq < /opt/scripts/load.bteq\n"},
    )

    assert _teradata(pipeline)["utilities"] == ["bteq"]


def test_plain_script_template_becomes_a_shell_notebook(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "BashOperator(task_id='t', bash_command='cleanup.sh')",
        files={"dags/cleanup.sh": 'echo "rows: ${#ROWS[@]}"\n'},
    )

    task = _task(pipeline)
    assert isinstance(task, NotebookActivity)
    assert 'echo "rows: ${#ROWS[@]}"' in (task.generated_source or "")


def test_trailing_space_runs_the_script_name_as_a_command(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "BashOperator(task_id='t', bash_command='cleanup.sh ')")

    task = _task(pipeline)
    assert isinstance(task, NotebookActivity)
    assert "cleanup.sh" in (task.generated_source or "")


@pytest.mark.parametrize(
    ("task", "files", "reason"),
    [
        ("BashOperator(task_id='t', bash_command='missing.sh')", {}, "was not found in"),
        ("SSHOperator(task_id='t', ssh_conn_id='e', command='job.ksh')", {}, "a shell other than bash"),
        (
            "BashOperator(task_id='t', bash_command='job.sh')",
            {"dags/job.sh": "{% for d in params.days %}echo {{ d }}{% endfor %}"},
            "Jinja statements or comments",
        ),
    ],
)
def test_unloadable_shell_script_fails_closed(tmp_path: Path, task: str, files: dict[str, str], reason: str) -> None:
    pipeline = _load(tmp_path, task, files=files)

    placeholder = _task(pipeline)
    assert isinstance(placeholder, PlaceholderActivity)
    assert reason in placeholder.comment


def test_bteq_operator_file_path_is_attached(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "BteqOperator(task_id='t', file_path='scripts/daily.bteq', teradata_conn_id='td')",
        files={"dags/scripts/daily.bteq": "SELECT DATE;\n"},
    )

    details = _teradata(pipeline)
    assert details["utilities"] == ["bteq"]
    assert details["scripts"][0]["content"] == "SELECT DATE;\n"


def test_bteq_operator_inline_sql_is_attached(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "BteqOperator(task_id='t', sql='SELECT 1;', teradata_conn_id='td')")

    assert _teradata(pipeline)["inline_scripts"] == ["SELECT 1;"]


def test_teradata_operator_sql_file_is_attached(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "TeradataOperator(task_id='t', sql='sql/load.sql', teradata_conn_id='td')",
        files={"dags/sql/load.sql": "SEL TOP 10 * FROM db.t;"},
    )

    details = _teradata(pipeline)
    assert details["utilities"] == []
    [script] = details["scripts"]
    assert (script["path"], script["content"]) == ("sql/load.sql", "SEL TOP 10 * FROM db.t;")
    assert "runs Teradata SQL with sql/load.sql" in _task(pipeline).comment


def test_teradata_operator_inline_sql_is_attached(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "TeradataOperator(task_id='t', sql='SEL 1;', teradata_conn_id='td')")

    assert _teradata(pipeline)["inline_scripts"] == ["SEL 1;"]
