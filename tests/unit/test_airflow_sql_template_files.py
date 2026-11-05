"""Tests for Airflow SQL template files: an operator's ``sql`` that names a file Airflow loads and renders."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowx.models.ir import Pipeline, PlaceholderActivity, SqlActivity
from flowx.sources.airflow.loader import load_airflow_dag


def _load(tmp_path: Path, task: str, *, dag_arguments: str = "", files: dict[str, str] | None = None) -> Pipeline:
    for name, content in (files or {}).items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    dag_path = tmp_path / "dags" / "dag.py"
    dag_path.parent.mkdir(parents=True, exist_ok=True)
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator\n"
        "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator\n"
        "from airflow.providers.apache.hive.operators.hive import HiveOperator\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.operators.python import PythonOperator\n"
        "from airflow.providers.postgres.operators.postgres import PostgresOperator\n"
        "from airflow.providers.mysql.operators.mysql import MySqlOperator\n"
        "import os\n"
        "from pathlib import Path\n"
        f"with DAG(dag_id='d'{dag_arguments}) as dag:\n"
        f"    t = {task}\n",
        encoding="utf-8",
    )
    return load_airflow_dag(dag_path)


def _task(pipeline: Pipeline):
    return next(task for task in pipeline.tasks if task.task_key == "t")


def test_sql_file_next_to_the_dag_becomes_the_task_sql(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "DatabricksSqlOperator(task_id='t', sql='queries/load.sql')",
        files={"dags/queries/load.sql": "INSERT INTO gold.accounts SELECT * FROM silver.accounts"},
    )

    task = _task(pipeline)
    assert isinstance(task, SqlActivity)
    assert task.sql == "INSERT INTO gold.accounts SELECT * FROM silver.accounts"
    assert pipeline.reconciliation_status == "verified"


def test_sql_file_jinja_is_bound_like_inline_sql(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "DatabricksSqlOperator(task_id='t', sql='load.sql')",
        files={"dags/load.sql": "SELECT * FROM {{ params.table }} WHERE id = {{ params.id }}"},
    )

    task = _task(pipeline)
    assert isinstance(task, SqlActivity)
    assert task.sql == "SELECT * FROM IDENTIFIER(:table) WHERE id = :id"
    assert task.parameters == {"table": "{{job.parameters.table}}", "id": "{{job.parameters.id}}"}


def test_dag_folder_is_searched_before_template_searchpath(tmp_path: Path) -> None:
    sql_root = tmp_path / "sql"
    pipeline = _load(
        tmp_path,
        "SQLExecuteQueryOperator(task_id='t', sql='load.sql')",
        dag_arguments=f", template_searchpath=[{str(sql_root)!r}]",
        files={"dags/load.sql": "SELECT 'dag folder'", "sql/load.sql": "SELECT 'search path'"},
    )

    assert _task(pipeline).sql == "SELECT 'dag folder'"


def test_template_searchpath_locates_the_file(tmp_path: Path) -> None:
    sql_root = tmp_path / "sql"
    pipeline = _load(
        tmp_path,
        "SQLExecuteQueryOperator(task_id='t', sql='marts/load.sql')",
        dag_arguments=f", template_searchpath={str(sql_root)!r}",
        files={"sql/marts/load.sql": "SELECT 1"},
    )

    assert _task(pipeline).sql == "SELECT 1"
    assert pipeline.reconciliation_status == "verified"


def test_hive_hql_file_is_read(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "HiveOperator(task_id='t', hql='job.hql')", files={"dags/job.hql": "SELECT 2"})

    assert _task(pipeline).sql == "SELECT 2"


@pytest.mark.parametrize(
    ("sql", "files", "reason"),
    [
        ("missing.sql", {}, "was not found in"),
        ("../outside.sql", {"outside.sql": "SELECT 1"}, "climbs out of the template search path"),
        ("load.sql", {"dags/load.sql": "{% include 'other.sql' %}"}, "Jinja statements or comments"),
        ("load.sql", {"dags/load.sql": "SELECT 1 {# note #}"}, "Jinja statements or comments"),
    ],
)
def test_unloadable_sql_file_fails_closed(tmp_path: Path, sql: str, files: dict[str, str], reason: str) -> None:
    pipeline = _load(tmp_path, f"DatabricksSqlOperator(task_id='t', sql={sql!r})", files=files)

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert reason in task.comment


def test_json_template_file_fails_closed(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "SQLExecuteQueryOperator(task_id='t', sql='query.json')")

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert "template file of a type flowx does not read" in task.comment


def test_inline_jinja_statement_fails_closed(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "DatabricksSqlOperator(task_id='t', sql=\"{% if params.full %}TRUNCATE t{% endif %}\")")

    assert isinstance(_task(pipeline), PlaceholderActivity)


def test_extension_inside_the_sql_text_stays_inline_sql(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "HiveOperator(task_id='t', hql='SELECT * FROM logs.sql_audit')")

    assert _task(pipeline).sql == "SELECT * FROM logs.sql_audit"


def test_dynamic_template_searchpath_is_a_dag_gap(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "SQLExecuteQueryOperator(task_id='t', sql='SELECT 1')",
        dag_arguments=", template_searchpath=SQL_ROOT",
    )

    codes = {finding["code"] for finding in pipeline.not_translatable}
    assert "unsupported_dag_setting" in codes


def test_trailing_newline_is_dropped_like_airflow_jinja(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path, "DatabricksSqlOperator(task_id='t', sql='load.sql')", files={"dags/load.sql": "SELECT 1\n"}
    )

    assert _task(pipeline).sql == "SELECT 1"


def test_gap_from_a_loaded_file_carries_its_sql(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "DatabricksSqlOperator(task_id='t', sql='load.sql')",
        files={"dags/load.sql": "{% if params.full %}TRUNCATE t{% endif %}"},
    )

    template_file = _task(pipeline).raw_definition["template_file"]
    assert template_file["path"] == "load.sql"
    assert template_file["resolved_path"] == str(tmp_path / "dags" / "load.sql")
    assert template_file["content"] == "{% if params.full %}TRUNCATE t{% endif %}"


def test_relative_template_searchpath_is_not_searched(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "include" / "sql").mkdir(parents=True)
    (tmp_path / "include" / "sql" / "load.sql").write_text("SELECT 1", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    pipeline = _load(
        tmp_path,
        "DatabricksSqlOperator(task_id='t', sql='load.sql')",
        dag_arguments=", template_searchpath='include/sql'",
    )

    assert isinstance(_task(pipeline), PlaceholderActivity)
    disposition = next(
        item for item in pipeline.audit["transformations"] if item.get("setting") == "template_searchpath"
    )
    assert "include/sql (relative" in disposition["unavailable_entries"]


def test_absent_template_searchpath_directory_is_reported(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "DatabricksSqlOperator(task_id='t', sql='SELECT 1')",
        dag_arguments=", template_searchpath='/opt/airflow/include/sql'",
    )

    disposition = next(
        item for item in pipeline.audit["transformations"] if item.get("setting") == "template_searchpath"
    )
    assert disposition["unavailable_entries"] == "/opt/airflow/include/sql (not found where discover runs)"
    assert pipeline.reconciliation_status == "verified"


def test_template_searchpath_none_is_unset(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path, "DatabricksSqlOperator(task_id='t', sql='SELECT 1')", dag_arguments=", template_searchpath=None"
    )

    assert pipeline.reconciliation_status == "verified"


def test_jinja_statement_in_a_shell_command_fails_closed(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "BashOperator(task_id='t', bash_command='{% if params.full %}rm -rf /tmp/x{% endif %}')")

    assert isinstance(_task(pipeline), PlaceholderActivity)


def test_bash_length_expansion_fails_closed_like_airflow_jinja(tmp_path: Path) -> None:
    # Jinja opens a comment at the {# in ${#ROWS[@]}, so Airflow cannot render this command.
    pipeline = _load(tmp_path, "BashOperator(task_id='t', bash_command='echo ${#ROWS[@]} # done')")

    assert isinstance(_task(pipeline), PlaceholderActivity)


def test_symlinked_dag_searches_the_folder_it_is_linked_from(tmp_path: Path) -> None:
    repository = tmp_path / "repo"
    repository.mkdir()
    (repository / "load.sql").write_text("SELECT 'repo'", encoding="utf-8")
    dag_source = (
        "from airflow import DAG\n"
        "from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = DatabricksSqlOperator(task_id='t', sql='load.sql')\n"
    )
    (repository / "job.py").write_text(dag_source, encoding="utf-8")
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "load.sql").write_text("SELECT 'dags'", encoding="utf-8")
    (dags / "job.py").symlink_to(repository / "job.py")

    pipeline = load_airflow_dag(dags / "job.py")

    assert _task(pipeline).sql == "SELECT 'dags'"


def test_relative_dag_path_searches_its_folder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "load.sql").write_text("SELECT 1", encoding="utf-8")
    (dags / "dag.py").write_text(
        "from airflow import DAG\n"
        "from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = DatabricksSqlOperator(task_id='t', sql='load.sql')\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    pipeline = load_airflow_dag(Path("dags/dag.py"))

    assert _task(pipeline).sql == "SELECT 1"


def test_loaded_file_keeps_its_sql_when_a_later_check_makes_it_a_gap(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "DatabricksSqlOperator(task_id='t', sql='load.sql')",
        files={"dags/load.sql": "SELECT * FROM t WHERE id = {{ ti.xcom_pull(task_ids='a') }}"},
    )

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert (
        task.raw_definition["template_file"]["content"] == "SELECT * FROM t WHERE id = {{ ti.xcom_pull(task_ids='a') }}"
    )


def test_loaded_sql_task_records_its_source_file(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path, "DatabricksSqlOperator(task_id='t', sql='load.sql')", files={"dags/load.sql": "SELECT 1"}
    )

    assert _task(pipeline).template_file == {
        "path": "load.sql",
        "resolved_path": str(tmp_path / "dags" / "load.sql"),
        "content": "SELECT 1",
    }


def test_non_utf8_sql_file_fails_closed(tmp_path: Path) -> None:
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags" / "latin.sql").write_bytes("SELECT 'é'".encode("latin-1"))

    pipeline = _load(tmp_path, "DatabricksSqlOperator(task_id='t', sql='latin.sql')")

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert "could not be read as UTF-8 text" in task.comment


@pytest.mark.parametrize("operator", ["PostgresOperator", "MySqlOperator"])
def test_postgres_and_mysql_read_sql_files(tmp_path: Path, operator: str) -> None:
    pipeline = _load(tmp_path, f"{operator}(task_id='t', sql='load.sql')", files={"dags/load.sql": "SELECT 3"})

    assert _task(pipeline).sql == "SELECT 3"


def test_absolute_template_name_is_looked_up_inside_the_search_path(tmp_path: Path) -> None:
    # Jinja drops the leading slash, so '/x/a.sql' is x/a.sql under each search folder.
    pipeline = _load(tmp_path, "DatabricksSqlOperator(task_id='t', sql='/x/a.sql')", files={"dags/x/a.sql": "SELECT 4"})

    assert _task(pipeline).sql == "SELECT 4"


def test_symlink_leading_outside_the_folder_fails_closed(tmp_path: Path) -> None:
    (tmp_path / "secret.sql").write_text("SELECT 'secret'", encoding="utf-8")
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags" / "load.sql").symlink_to(tmp_path / "secret.sql")

    pipeline = _load(tmp_path, "DatabricksSqlOperator(task_id='t', sql='load.sql')")

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert "symbolic link that leads outside" in task.comment


def test_dag_decorator_template_searchpath_is_used(tmp_path: Path) -> None:
    sql_root = tmp_path / "sql"
    sql_root.mkdir()
    (sql_root / "load.sql").write_text("SELECT 5", encoding="utf-8")
    dag_path = tmp_path / "dags" / "dag.py"
    dag_path.parent.mkdir()
    dag_path.write_text(
        "from airflow.decorators import dag\n"
        "from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator\n"
        f"@dag(dag_id='d', template_searchpath={str(sql_root)!r})\n"
        "def build():\n"
        "    DatabricksSqlOperator(task_id='t', sql='load.sql')\n"
        "build()\n",
        encoding="utf-8",
    )

    pipeline = load_airflow_dag(dag_path)

    assert _task(pipeline).sql == "SELECT 5"


@pytest.mark.parametrize(
    "searchpath",
    [
        "os.path.join(os.path.dirname(__file__), 'sql')",
        "Path(__file__).parent / 'sql'",
        "[str(Path(__file__).parent / 'sql')]",
        "os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sql')",
    ],
)
def test_template_searchpath_built_from_dunder_file_is_resolved(tmp_path: Path, searchpath: str) -> None:
    pipeline = _load(
        tmp_path,
        "DatabricksSqlOperator(task_id='t', sql='load.sql')",
        dag_arguments=f", template_searchpath={searchpath}",
        files={"dags/sql/load.sql": "SELECT 6"},
    )

    assert _task(pipeline).sql == "SELECT 6"
    assert pipeline.reconciliation_status == "verified"


def test_unusable_template_searchpath_is_reported_as_ignored(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "DatabricksSqlOperator(task_id='t', sql='SELECT 1')",
        dag_arguments=", template_searchpath='include/sql'",
    )

    entry = next(item for item in pipeline.audit["transformations"] if item.get("setting") == "template_searchpath")
    assert entry["code"] == "dag_setting_ignored"
    assert "include/sql (relative" in entry["unavailable_entries"]


def test_python_callable_building_its_own_jinja_is_not_flagged(tmp_path: Path) -> None:
    (tmp_path / "dags").mkdir()
    dag_path = tmp_path / "dags" / "dag.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def render():\n"
        "    template = '{% for row in rows %}row{% endfor %} {# note #}'\n"
        "    print(template)\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='t', python_callable=render)\n",
        encoding="utf-8",
    )

    pipeline = load_airflow_dag(dag_path)

    assert not isinstance(_task(pipeline), PlaceholderActivity)
