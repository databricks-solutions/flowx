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


# --------------------------------------------------------------------------------------
# Review regressions
# --------------------------------------------------------------------------------------


def test_relative_dag_path_still_attaches_the_sql_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "dags" / "sql").mkdir(parents=True)
    (tmp_path / "dags" / "sql" / "load.sql").write_text("SEL 1;", encoding="utf-8")
    (tmp_path / "dags" / "dag.py").write_text(
        f"{_IMPORTS}with DAG(dag_id='d') as dag:\n    t = TeradataOperator(task_id='t', sql='sql/load.sql')\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    pipeline = load_airflow_dag(Path("dags/dag.py"))

    assert _teradata(pipeline)["scripts"][0]["content"] == "SEL 1;"


def test_later_guards_keep_the_teradata_record(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "BashOperator(task_id='t', bash_command='bteq < /opt/x.bteq', trigger_rule='one_done')")

    task = _task(pipeline)
    assert _teradata(pipeline)["utilities"] == ["bteq"]
    assert "Databricks has no Teradata client" in task.comment
    assert "trigger_rule" in task.comment


def test_jinja_in_a_script_path_stays_one_word() -> None:
    assert teradata.script_references("bteq < /opt/x_{{ prev_ds }}.bteq") == ["/opt/x_{{ prev_ds }}.bteq"]


def test_unparseable_heredoc_keeps_other_script_references() -> None:
    command = "bteq < /opt/a.bteq; bteq <<EOF\n-- don't\nSEL 1;\nEOF"

    assert teradata.script_references(command) == ["/opt/a.bteq"]
    assert teradata.heredoc_scripts(command) == ["-- don't\nSEL 1;"]


def test_unparseable_command_falls_back_to_a_segment_search() -> None:
    assert teradata.script_references("bteq < /opt/a.bteq; echo 'unterminated") == ["/opt/a.bteq"]


def test_teradata_operator_hql_string_is_inline_sql(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "TeradataOperator(task_id='t', sql='load.hql')", files={"dags/load.hql": "SEL 1;"})

    details = _teradata(pipeline)
    assert (details["scripts"], details["inline_scripts"]) == ([], ["load.hql"])


def test_teradata_operator_statement_list_is_attached(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "TeradataOperator(task_id='t', sql=['SEL 1;', 'SEL 2;'])")

    assert _teradata(pipeline)["inline_scripts"] == ["SEL 1;", "SEL 2;"]


def test_non_utf8_sql_file_keeps_its_digest(tmp_path: Path) -> None:
    script = tmp_path / "dags" / "latin.sql"
    script.parent.mkdir(parents=True)
    script.write_bytes("SEL 'é';".encode("latin-1"))

    pipeline = _load(tmp_path, "TeradataOperator(task_id='t', sql='latin.sql')")

    [record] = _teradata(pipeline)["scripts"]
    assert record["sha256"] == hashlib.sha256(script.read_bytes()).hexdigest()
    assert "content_note" in record


def test_bteq_operator_sql_naming_a_file_is_sent_as_written(tmp_path: Path) -> None:
    # BteqOperator declares no template_ext, so Airflow passes the string to bteq unchanged.
    pipeline = _load(tmp_path, "BteqOperator(task_id='t', sql='daily.sql')", files={"dags/daily.sql": "SEL 1;"})

    details = _teradata(pipeline)
    assert (details["scripts"], details["inline_scripts"]) == ([], ["daily.sql"])
