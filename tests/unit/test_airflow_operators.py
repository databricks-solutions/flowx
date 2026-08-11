"""Unit tests for Airflow operator -> flowx IR coverage (Tier 1-4)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from flowx.models.ir import (
    DbtFactoryActivity,
    ForEachActivity,
    NotebookActivity,
    PlaceholderActivity,
    RunJobActivity,
    SparkJarActivity,
    SparkPythonActivity,
    SqlActivity,
)
from flowx.sources.airflow.loader import load_airflow_dag, load_airflow_dags


def _load(dag_source: str):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "dag.py"
        path.write_text(dag_source, encoding="utf-8")
        return load_airflow_dag(path)


def _load_all(dag_source: str):
    """Loads every DAG declared in one module (the multi-DAG form)."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "dag.py"
        path.write_text(dag_source, encoding="utf-8")
        return load_airflow_dags(path)


def _by_key(pipeline):
    return {t.task_key: t for t in pipeline.tasks}


# --------------------------------------------------------------------------------------
# Tier 1
# --------------------------------------------------------------------------------------


def test_python_operator_becomes_generated_notebook():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def work():\n    spark.sql('select 1')\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='work', python_callable=work)\n"
    )
    task = _by_key(p)["work"]
    assert isinstance(task, NotebookActivity)
    assert "spark.sql('select 1')" in task.generated_source


def test_python_operator_notebook_is_valid_python():
    # A callable with an early return, a helper, a constant, and a non-Airflow import must
    # produce a notebook that compiles (no top-level return, no undefined names).
    p = _load(
        "from datetime import datetime\n"
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "CONST = 10\n"
        "def _double(x):\n    return x * 2\n"
        "def process(factor=1):\n"
        "    n = _double(factor) + CONST\n"
        "    if n > 100:\n        return 'big'\n"
        "    return datetime.now().isoformat()\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='proc', python_callable=process, op_kwargs={'factor': 5})\n"
    )
    nb = _by_key(p)["proc"].generated_source
    compile(nb, "<nb>", "exec")  # raises SyntaxError if invalid
    assert "def process(factor=1):" in nb  # def preserved (early returns stay legal)
    assert "def _double(x):" in nb  # transitive helper carried
    assert "CONST = 10" in nb  # constant carried
    assert "from datetime import datetime" in nb  # non-Airflow import carried
    assert "from airflow" not in nb  # Airflow imports dropped
    assert "result = process(**op_kwargs)" in nb  # invoked with op_kwargs
    assert "taskValues.set" in nb  # return value captured


def test_python_callable_dependencies_are_emitted_in_definition_order():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "CONST = 7\n"
        "def helper(value=CONST):\n    return value\n"
        "def work():\n    return helper()\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='work', python_callable=work)\n"
    )

    source = _by_key(p)["work"].generated_source
    assert source.index("CONST = 7") < source.index("def helper")


def test_python_callable_carries_annotated_module_constant():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "LIMIT: int = 7\n"
        "def work():\n    return LIMIT\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='work', python_callable=work)\n"
    )

    assert "LIMIT: int = 7" in _by_key(p)["work"].generated_source


def test_python_operator_with_context_kwarg_becomes_placeholder():
    # A callable taking **context can't run without the Airflow runtime; route to a gap
    # rather than emitting a notebook that fails at runtime.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def work(**context):\n    print(context['ds'])\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='work', python_callable=work)\n"
    )
    task = _by_key(p)["work"]
    assert isinstance(task, PlaceholderActivity)
    assert "context" in task.comment
    assert task.raw_definition is not None  # carries source for the agentic round


def test_python_operator_with_named_airflow_context_becomes_placeholder():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def work(ds):\n    print(ds)\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='work', python_callable=work)\n"
    )

    assert isinstance(_by_key(p)["work"], PlaceholderActivity)


def test_python_operator_with_ti_param_becomes_placeholder():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def work(ti):\n    ti.xcom_push(key='k', value=1)\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='work', python_callable=work)\n"
    )
    task = _by_key(p)["work"]
    assert isinstance(task, PlaceholderActivity)


def test_python_operator_with_module_constant_arguments_is_rendered():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "ARGS = {'value': 3}\n"
        "def work(value):\n    return value\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='work', python_callable=work, op_kwargs=ARGS)\n"
    )

    task = _by_key(p)["work"]
    assert isinstance(task, NotebookActivity)
    assert task.base_parameters == {"__flowx_op_kwargs": '{"value": 3}'}


def test_python_operator_with_xcom_pull_becomes_placeholder():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def work(data=None):\n"
        "    prev = work.xcom_pull(task_ids='up')\n"
        "    print(prev)\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='work', python_callable=work)\n"
    )
    task = _by_key(p)["work"]
    assert isinstance(task, PlaceholderActivity)
    assert "XCom" in task.comment


def test_bash_operator_becomes_sh_notebook():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = BashOperator(task_id='clean', bash_command='rm -rf /tmp/x')\n"
    )
    task = _by_key(p)["clean"]
    assert isinstance(task, NotebookActivity)
    assert "%sh" in task.generated_source
    # No macros -> no widget prelude, just the %sh cell.
    assert "dbutils.widgets" not in task.generated_source


def test_bash_operator_macros_thread_through_shell_env_vars():
    # A BashOperator with Airflow macros must resolve them at run time: each macro becomes a $var fed
    # by a job-parameter widget exported to the shell env, not a literal left in the command.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d', schedule_interval='0 6 * * *') as dag:\n"
        "    t = BashOperator(task_id='run',\n"
        "                     bash_command='python /opt/etl.py --date {{ ds }} --env {{ params.env }}')\n"
    )
    task = _by_key(p)["run"]
    assert isinstance(task, NotebookActivity)
    # Macros converted to shell variables; the raw {{ ... }} is gone from the %sh cell.
    assert "--date ${__flowx_airflow_run_date}" in task.generated_source
    assert "--env ${env}" in task.generated_source
    assert "{{ ds }}" not in task.generated_source
    # The widgets are declared and exported to the environment before the %sh cell.
    assert (
        "os.environ['__flowx_airflow_run_date'] = dbutils.widgets.get('__flowx_airflow_run_date')"
    ) in task.generated_source
    compile("\n".join(task.generated_source.split("# MAGIC %sh")[0].splitlines()), "<pre>", "exec")
    # Each widget must be BOUND to its job parameter: an unbound widget is backfilled with an empty
    # string by the bundler, so the command would silently run with blank values.
    assert task.base_parameters == {
        "__flowx_airflow_run_date": "{{job.parameters.__flowx_airflow_run_date}}",
        "env": "{{job.parameters.env}}",
    }
    # run_date declared as a job parameter with the schedule-aware default (backfill-overridable).
    params = {param["name"]: param["default"] for param in p.parameters}
    assert params["__flowx_airflow_run_date"] == "{{job.trigger.time.iso_date}}"
    assert params["env"] == ""


def test_python_operator_with_unresolvable_callable_becomes_placeholder():
    # python_callable imported from another module has no source to render -- it must become a
    # placeholder (a real gaps.json entry), not a bodyless notebook counted as deterministic.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "import my_module\n"
        "with DAG(dag_id='d') as dag:\n"
        "    a = PythonOperator(task_id='a', python_callable=my_module.etl_step)\n"
    )
    task = _by_key(p)["a"]
    assert isinstance(task, PlaceholderActivity)
    assert "could not be resolved" in task.comment


def test_bash_operator_run_id_macro_defaults_to_run_id_ref():
    # run_id has no user default: threaded through a widget whose default resolves to the run id.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = BashOperator(task_id='run', bash_command='echo {{ run_id }}')\n"
    )
    task = _by_key(p)["run"]
    assert "echo ${__flowx_airflow_run_id}" in task.generated_source
    params = {param["name"]: param["default"] for param in p.parameters}
    assert params["__flowx_airflow_run_id"] == "{{job.run_id}}"


def test_bash_operator_wrapping_spark_submit_becomes_spark_task():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = BashOperator(task_id='j', bash_command='spark-submit --master yarn /opt/etl.py --date x')\n"
    )
    task = _by_key(p)["j"]
    assert isinstance(task, SparkPythonActivity)
    assert task.python_file == "/opt/etl.py"
    assert task.parameters == ["--date", "x"]


def test_spark_submit_python_and_jar():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    a = SparkSubmitOperator(task_id='py', application='/o/e.py', application_args=['--d','1'])\n"
        "    b = SparkSubmitOperator(task_id='jar', application='/o/a.jar', java_class='com.X')\n"
    )
    tasks = _by_key(p)
    assert isinstance(tasks["py"], SparkPythonActivity)
    assert tasks["py"].parameters == ["--d", "1"]
    assert isinstance(tasks["jar"], SparkJarActivity)
    assert tasks["jar"].main_class_name == "com.X"
    assert tasks["jar"].libraries == [{"jar": "/o/a.jar"}]


def test_ssh_operator_spark_submit_drops_the_hop():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.ssh.operators.ssh import SSHOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = SSHOperator(task_id='r', command='spark-submit --class com.E /o/e.jar --date x')\n"
    )
    task = _by_key(p)["r"]
    assert isinstance(task, SparkJarActivity)
    assert task.main_class_name == "com.E"


def test_sql_operator_becomes_sql_task():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = SQLExecuteQueryOperator(task_id='rep', sql='CREATE TABLE g AS SELECT 1')\n"
    )
    task = _by_key(p)["rep"]
    assert isinstance(task, SqlActivity)
    assert task.sql == "CREATE TABLE g AS SELECT 1"


def test_sql_identifier_template_uses_identifier_marker():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = SQLExecuteQueryOperator(task_id='rep', "
        "sql='SELECT * FROM {{ params.table }} WHERE id = {{ params.id }}')\n"
    )

    task = _by_key(p)["rep"]
    assert task.sql == "SELECT * FROM IDENTIFIER(:table) WHERE id = :id"
    assert task.parameters == {
        "table": "{{job.parameters.table}}",
        "id": "{{job.parameters.id}}",
    }
    assert task.warehouse_ref == "${var.warehouse_id}"


def test_hive_operator_reads_hql_into_sql_task():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.apache.hive.operators.hive import HiveOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = HiveOperator(task_id='h', hql='SELECT * FROM t')\n"
    )
    task = _by_key(p)["h"]
    assert isinstance(task, SqlActivity)
    assert task.sql == "SELECT * FROM t"


def test_copy_into_operator_becomes_sql_task():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.databricks.operators.databricks_sql import DatabricksCopyIntoOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = DatabricksCopyIntoOperator(task_id='c', table_name='bronze.raw',\n"
        "                                   file_location='s3://l/', file_format='CSV')\n"
    )
    task = _by_key(p)["c"]
    assert isinstance(task, SqlActivity)
    assert "COPY INTO bronze.raw" in task.sql


def test_table_sensor_lifts_to_table_update_trigger():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.databricks.sensors.databricks_partition import DatabricksPartitionSensor\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    wait = DatabricksPartitionSensor(task_id='wait', table_name='main.silver.events')\n"
        "    go = PythonOperator(task_id='go', python_callable=w)\n"
        "    wait >> go\n"
    )
    assert set(_by_key(p)) == {"go"}  # sensor is not a task
    assert p.schedule == {
        "kind": "table_update",
        "table_names": ["main.silver.events"],
        "condition": "ANY_UPDATED",
        "pause_status": "UNPAUSED",
    }


def test_sql_condition_sensor_becomes_polling_task():
    # A SqlSensor checking an arbitrary condition (no table_name) must NOT vanish and must NOT lift
    # to a table trigger; it becomes a polling notebook task running the query on a poke loop.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.common.sql.sensors.sql import SqlSensor\n"
        "with DAG(dag_id='d') as dag:\n"
        "    s = SqlSensor(task_id='chk', sql='SELECT COUNT(*) FROM t WHERE ready', poke_interval=30, timeout=600)\n"
    )
    task = _by_key(p)["chk"]
    assert isinstance(task, NotebookActivity)
    assert p.schedule is None
    src = task.generated_source
    assert "SELECT COUNT(*) FROM t WHERE ready" in src
    assert "POKE_INTERVAL = 30" in src
    assert "TIMEOUT = 600" in src
    # Generated polling notebook must be valid Python.
    compile(src, "<chk>", "exec")


def test_sql_condition_sensor_without_literal_sql_stays_placeholder():
    # No literal sql/table_name to poll -> a placeholder task with guidance, never a silent drop.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.common.sql.sensors.sql import SqlSensor\n"
        "with DAG(dag_id='d') as dag:\n"
        "    s = SqlSensor(task_id='chk', sql=build_query())\n"
    )
    task = _by_key(p)["chk"]
    assert isinstance(task, PlaceholderActivity)
    assert p.schedule is None


def test_external_task_sensor_becomes_manual_cross_dag_placeholder():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.sensors.external_task import ExternalTaskSensor\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    wait = ExternalTaskSensor(task_id='wait_up', external_dag_id='upstream dag',\n"
        "                              poke_interval=45, timeout=900)\n"
        "    go = PythonOperator(task_id='go', python_callable=w)\n"
        "    wait >> go\n"
    )
    task = _by_key(p)["wait_up"]
    assert isinstance(task, PlaceholderActivity)
    assert "logical run" in task.comment


def test_http_sensor_with_relative_endpoint_becomes_placeholder():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.http.sensors.http import HttpSensor\n"
        "with DAG(dag_id='d') as dag:\n"
        "    s = HttpSensor(task_id='h', endpoint='api/ready', poke_interval=10, timeout=120)\n"
    )
    task = _by_key(p)["h"]
    assert isinstance(task, PlaceholderActivity)
    assert "http_conn_id" in task.comment


def test_http_sensor_with_absolute_endpoint_becomes_polling_task():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.http.sensors.http import HttpSensor\n"
        "with DAG(dag_id='d') as dag:\n"
        "    s = HttpSensor(task_id='h', endpoint='https://example.com/api/ready')\n"
    )
    task = _by_key(p)["h"]
    assert isinstance(task, NotebookActivity)
    assert "requests.get" in task.generated_source


def test_python_sensor_polls_callable_without_eager_call():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.sensors.python import PythonSensor\n"
        "def is_ready():\n    return spark.table('t').count() > 0\n"
        "with DAG(dag_id='d') as dag:\n"
        "    s = PythonSensor(task_id='chk', python_callable=is_ready, poke_interval=25, timeout=500)\n"
    )
    task = _by_key(p)["chk"]
    assert isinstance(task, NotebookActivity)
    src = task.generated_source
    assert "def is_ready():" in src  # callable carried
    assert "return is_ready()" in src  # polled inside the loop
    assert "taskValues.set" not in src  # NOT invoked eagerly as a one-shot
    compile(src, "<chk>", "exec")


def test_python_sensor_with_context_stays_placeholder():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.sensors.python import PythonSensor\n"
        "def is_ready(**context):\n    return context['ti'].xcom_pull('x')\n"
        "with DAG(dag_id='d') as dag:\n"
        "    s = PythonSensor(task_id='chk', python_callable=is_ready)\n"
    )
    assert isinstance(_by_key(p)["chk"], PlaceholderActivity)


def test_datetime_sensor_becomes_wait_until_task():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.sensors.date_time import DateTimeSensor\n"
        "with DAG(dag_id='d') as dag:\n"
        "    s = DateTimeSensor(task_id='wait', target_time='2026-01-01T00:00:00+00:00')\n"
    )
    task = _by_key(p)["wait"]
    assert isinstance(task, NotebookActivity)
    src = task.generated_source
    assert "datetime.fromisoformat" in src
    assert "2026-01-01T00:00:00+00:00" in src
    compile(src, "<wait>", "exec")


def test_time_delta_sensor_is_retained_as_manual_placeholder():
    p = _load(
        "from datetime import timedelta\n"
        "from airflow import DAG\n"
        "from airflow.sensors.time_delta import TimeDeltaSensor\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d', schedule='0 0 * * *') as dag:\n"
        "    wait = TimeDeltaSensor(task_id='wait', delta=timedelta(hours=2))\n"
        "    work = BashOperator(task_id='work', bash_command='echo work')\n"
        "    wait >> work\n"
    )

    tasks = _by_key(p)
    assert isinstance(tasks["wait"], PlaceholderActivity)
    assert tasks["work"].depends_on[0].task_key == "wait"


def test_databricks_run_now_becomes_run_job():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = DatabricksRunNowOperator(task_id='dn', job_id=999)\n"
    )
    task = _by_key(p)["dn"]
    assert isinstance(task, RunJobActivity)
    assert task.existing_job_id == "999"


def test_trigger_dag_run_becomes_run_job_by_name():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.trigger_dagrun import TriggerDagRunOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = TriggerDagRunOperator(task_id='f', trigger_dag_id='other_dag', conf={'k': 'v'})\n"
    )
    task = _by_key(p)["f"]
    assert isinstance(task, RunJobActivity)
    assert task.job_name == "other_dag"
    assert task.job_parameters == {"k": "v"}


def test_trigger_dag_run_job_name_matches_target_job_resource_key():
    # job_name becomes ${resources.jobs.<job_name>.id}; it must equal normalize_task_key(dag_id)
    # (how write_bundle keys the target job), or the cross-DAG ref dangles for hyphenated/mixed-case ids.
    from flowx.utils import normalize_task_key

    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.trigger_dagrun import TriggerDagRunOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = TriggerDagRunOperator(task_id='f', trigger_dag_id='Upstream-DAG')\n"
    )
    task = _by_key(p)["f"]
    assert task.job_name == normalize_task_key("Upstream-DAG") == "upstream_dag"


def test_databricks_submit_run_reads_notebook_from_json():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = DatabricksSubmitRunOperator(task_id='s', json={'notebook_task': {'notebook_path': '/W/etl'}})\n"
    )
    task = _by_key(p)["s"]
    assert isinstance(task, NotebookActivity)
    assert task.notebook_path == "/W/etl"


# --------------------------------------------------------------------------------------
# Tier 2
# --------------------------------------------------------------------------------------


def test_dummy_operators_dropped_and_rewired():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.empty import EmptyOperator\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    start = EmptyOperator(task_id='start')\n"
        "    mid = PythonOperator(task_id='mid', python_callable=w)\n"
        "    end = EmptyOperator(task_id='end')\n"
        "    start >> mid >> end\n"
    )
    keys = set(_by_key(p))
    assert keys == {"mid"}  # start/end dropped
    assert p.tasks[0].depends_on is None  # mid's dropped upstream rewired away


def test_dummy_rewire_bridges_dependencies():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.empty import EmptyOperator\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    a = PythonOperator(task_id='a', python_callable=w)\n"
        "    gate = EmptyOperator(task_id='gate')\n"
        "    b = PythonOperator(task_id='b', python_callable=w)\n"
        "    a >> gate >> b\n"
    )
    tasks = _by_key(p)
    assert set(tasks) == {"a", "b"}
    assert [d.task_key for d in tasks["b"].depends_on] == ["a"]  # bridged through dropped gate


def test_structural_only_dag_emits_completion_sentinel():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.empty import EmptyOperator\n"
        "with DAG(dag_id='structural_only') as dag:\n"
        "    start = EmptyOperator(task_id='start')\n"
        "    end = EmptyOperator(task_id='end')\n"
        "    start >> end\n"
    )

    assert p.reconciliation_status == "verified"
    assert len(p.tasks) == 1
    sentinel = p.tasks[0]
    assert isinstance(sentinel, NotebookActivity)
    assert sentinel.task_key == "__flowx_empty_dag"
    assert "completed without executable tasks" in (sentinel.generated_source or "")
    assert any(item["code"] == "empty_dag_sentinel_emitted" for item in p.audit["transformations"])


def test_cosmos_dbt_task_group_becomes_dbt_factory():
    p = _load(
        "from airflow import DAG\n"
        "from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig\n"
        "with DAG(dag_id='d') as dag:\n"
        "    dbt = DbtTaskGroup(group_id='t', project_config=ProjectConfig('/opt/proj'),\n"
        "                       profile_config=ProfileConfig(profile_name='p', target_name='prod'))\n"
    )
    task = _by_key(p)["t"]
    assert isinstance(task, DbtFactoryActivity)
    assert task.project_dir == "/opt/proj"
    assert task.target == "prod"
    assert task.render_mode == "static"


def test_dbt_mode_pydabs_sets_render_mode():
    # `--dbt-mode pydabs` (threaded through load_airflow_dag) makes the factory reachable in PyDABs mode.
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "dag.py"
        path.write_text(
            "from airflow import DAG\n"
            "from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig\n"
            "with DAG(dag_id='d') as dag:\n"
            "    dbt = DbtTaskGroup(group_id='t', project_config=ProjectConfig('/opt/proj'),\n"
            "                       profile_config=ProfileConfig(profile_name='p', target_name='prod'))\n",
            encoding="utf-8",
        )
        p = load_airflow_dag(path, dbt_mode="pydabs")
    task = _by_key(p)["t"]
    assert isinstance(task, DbtFactoryActivity)
    assert task.render_mode == "pydabs"


def test_dbt_cli_operators_collapse_to_one_factory():
    p = _load(
        "from airflow import DAG\n"
        "from airflow_dbt.operators.dbt_operator import DbtSeedOperator, DbtRunOperator, DbtTestOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    s = DbtSeedOperator(task_id='seed', dir='/opt/proj')\n"
        "    r = DbtRunOperator(task_id='run', dir='/opt/proj')\n"
        "    t = DbtTestOperator(task_id='test', dir='/opt/proj')\n"
        "    s >> r >> t\n"
    )
    dbt_tasks = [t for t in p.tasks if isinstance(t, DbtFactoryActivity)]
    assert len(dbt_tasks) == 1  # the seed>>run>>test chain collapses into one factory job
    assert dbt_tasks[0].project_dir == "/opt/proj"
    # A manifest_path must be set or the static preparer would explode zero tasks (empty child job).
    assert dbt_tasks[0].manifest_path == "/opt/proj/target/manifest.json"


def test_single_dbt_run_operator_limits_factory_to_models():
    p = _load(
        "from airflow import DAG\n"
        "from airflow_dbt.operators.dbt_operator import DbtRunOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    run = DbtRunOperator(task_id='run', dir='/opt/proj')\n"
    )

    task = _by_key(p)["run"]
    assert isinstance(task, DbtFactoryActivity)
    assert task.resource_types == ["model"]


def test_dbt_operator_preserves_command_options_and_standard_manifest_path():
    p = _load(
        "from airflow import DAG\n"
        "from airflow_dbt.operators.dbt_operator import DbtRunOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    run = DbtRunOperator(task_id='run', dir='/opt/proj', select=['tag:daily'], "
        "exclude=['tag:slow'], vars={'region': 'west'}, full_refresh=True)\n"
    )

    task = _by_key(p)["run"]
    assert isinstance(task, DbtFactoryActivity)
    assert task.manifest_path == "/opt/proj/target/manifest.json"
    assert task.selectors == ["tag:daily"]
    assert task.exclude_selectors == ["tag:slow"]
    assert task.variables == {"region": "west"}
    assert task.full_refresh is True


def test_dbt_deps_operator_runs_only_dependency_installation(tmp_path):
    from flowx.preparer.workflow_preparer import prepare_workflow

    project = tmp_path / "project"
    profiles = tmp_path / "profiles"
    project.mkdir()
    profiles.mkdir()
    (project / "dbt_project.yml").write_text("name: demo\nprofile: demo\n")
    (profiles / "profiles.yml").write_text("demo:\n  target: dev\n  outputs: {}\n")
    p = _load(
        "from airflow import DAG\n"
        "from airflow_dbt.operators.dbt_operator import DbtDepsOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        f"    deps = DbtDepsOperator(task_id='deps', dir={str(project)!r}, profiles_dir={str(profiles)!r})\n"
    )

    task = _by_key(p)["deps"]
    assert isinstance(task, DbtFactoryActivity)
    assert task.resource_types == ["dependency"]

    prepared = prepare_workflow(p)
    assert prepared.tasks[0]["run_job_task"]
    assert prepared.inner_workflows[0].tasks[0]["notebook_task"]["base_parameters"]["dbt_command"] == "deps"


def test_dbt_chain_downstream_dep_rewired_to_factory_key():
    # A non-dbt task depending on the LAST dbt op (`test`) must point at the single collapsed
    # factory task (`seed`), not the vanished `test` key (which would dangle at package time).
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "from airflow_dbt.operators.dbt_operator import DbtSeedOperator, DbtRunOperator, DbtTestOperator\n"
        "def pub():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    s = DbtSeedOperator(task_id='seed', dir='/opt/proj')\n"
        "    r = DbtRunOperator(task_id='run', dir='/opt/proj')\n"
        "    t = DbtTestOperator(task_id='test', dir='/opt/proj')\n"
        "    p2 = PythonOperator(task_id='publish', python_callable=pub)\n"
        "    s >> r >> t >> p2\n"
    )
    tasks = _by_key(p)
    factory_key = next(t.task_key for t in p.tasks if isinstance(t, DbtFactoryActivity))
    assert [d.task_key for d in tasks["publish"].depends_on] == [factory_key]


def test_dbt_chain_absorbs_every_dbt_ops_upstream():
    # An external task feeding a LATER dbt op must gate the single collapsed factory -- not be dropped
    # because only the first dbt op's upstreams were absorbed.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    ingest = PythonOperator(task_id='ingest', python_callable=w)\n"
        "    seed_src = PythonOperator(task_id='seed_src', python_callable=w)\n"
        "    r = DbtRunOperator(task_id='run', dir='/opt/proj')\n"
        "    t = DbtTestOperator(task_id='test', dir='/opt/proj')\n"
        "    ingest >> r\n"
        "    seed_src >> t\n"
        "    r >> t\n"
    )
    factory = next(t for t in p.tasks if isinstance(t, DbtFactoryActivity))
    assert sorted(d.task_key for d in factory.depends_on) == ["ingest", "seed_src"]


def test_dbt_chain_preserves_sandwiched_task_ordering():
    # A non-dbt task between two dbt ops (seed >> mid >> run) is downstream of the collapsed factory,
    # so a task consuming the later dbt op must still wait for it -- and no cycle is formed.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "from airflow_dbt.operators.dbt_operator import DbtSeedOperator, DbtRunOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    s = DbtSeedOperator(task_id='seed', dir='/opt/proj')\n"
        "    mid = PythonOperator(task_id='mid', python_callable=w)\n"
        "    r = DbtRunOperator(task_id='run', dir='/opt/proj')\n"
        "    tail = PythonOperator(task_id='tail', python_callable=w)\n"
        "    s >> mid >> r >> tail\n"
    )
    tasks = _by_key(p)
    factory_key = next(t.task_key for t in p.tasks if isinstance(t, DbtFactoryActivity))
    # `mid` gates on the factory; `tail` (consumer of the vanished `run`) waits for BOTH the factory
    # and `mid`, preserving the mid->tail ordering without depending on itself (no cycle).
    assert [d.task_key for d in tasks["mid"].depends_on] == [factory_key]
    assert sorted(d.task_key for d in tasks["tail"].depends_on) == sorted([factory_key, "mid"])


def test_table_sensor_escapes_quotes_in_table_name():
    # A table_name (or file path) carrying a double quote must not break the generated notebook: the
    # value goes through repr(), so the source still compiles.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.databricks.sensors.databricks_partition import DatabricksPartitionSensor\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    prep = PythonOperator(task_id='prep', python_callable=w)\n"
        "    wait = DatabricksPartitionSensor(task_id='wait', table_name='main.silver.we\"ird')\n"
        "    prep >> wait\n"
    )
    wait = _by_key(p)["wait"]
    assert isinstance(wait, NotebookActivity)
    compile(wait.generated_source, "<wait>", "exec")  # would raise before the repr() fix


def test_dbt_factory_explodes_manifest_into_tasks(tmp_path):
    # End-to-end: a real (synthetic) manifest must explode into per-node tasks, not an empty job.
    import json

    from flowx.preparer.workflow_preparer import prepare_workflow

    manifest = {
        "nodes": {
            "seed.p.codes": {
                "resource_type": "seed",
                "name": "codes",
                "fqn": ["p", "codes"],
                "depends_on": {"nodes": []},
            },
            "model.p.stg": {
                "resource_type": "model",
                "name": "stg",
                "fqn": ["p", "stg"],
                "depends_on": {"nodes": ["seed.p.codes"]},
            },
        },
        "unit_tests": {},
    }
    project_dir = tmp_path / "proj"
    (project_dir / "dbt_project.yml").parent.mkdir(parents=True)
    (project_dir / "dbt_project.yml").write_text("name: p\nprofile: p\n", encoding="utf-8")
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    (profiles_dir / "profiles.yml").write_text("p:\n  target: dev\n  outputs: {}\n", encoding="utf-8")
    proj = project_dir / "target"
    proj.mkdir(parents=True)
    (proj / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    dag = (
        "from airflow import DAG\n"
        "from airflow_dbt.operators.dbt_operator import DbtRunOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        f"    r = DbtRunOperator(task_id='run', dir={str(tmp_path / 'proj')!r}, "
        f"profiles_dir={str(profiles_dir)!r})\n"
    )
    dag_file = tmp_path / "dag.py"
    dag_file.write_text(dag, encoding="utf-8")
    p = load_airflow_dag(dag_file)
    wf = prepare_workflow(p)
    inner_task_keys = {t["task_key"] for inner in wf.inner_workflows for t in inner.tasks}
    assert inner_task_keys == {"model_stg"}


# --------------------------------------------------------------------------------------
# Tier 3 — sensors
# --------------------------------------------------------------------------------------


def test_file_sensor_lifts_to_file_arrival_trigger():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    wait = S3KeySensor(task_id='wait', bucket_key='s3://landing/in/')\n"
        "    go = PythonOperator(task_id='go', python_callable=w)\n"
        "    wait >> go\n"
    )
    assert set(_by_key(p)) == {"go"}  # sensor is not a task
    assert p.schedule == {"kind": "file_arrival", "url": "s3://landing/in/", "pause_status": "UNPAUSED"}


def test_s3_sensor_trigger_combines_bucket_and_relative_key():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor\n"
        "with DAG(dag_id='d') as dag:\n"
        "    wait = S3KeySensor(task_id='wait', bucket_name='landing', bucket_key='incoming/')\n"
    )

    assert p.schedule == {
        "kind": "file_arrival",
        "url": "s3://landing/incoming/",
        "pause_status": "UNPAUSED",
    }


def test_cron_and_sensor_keeps_both_schedule_and_polling_task():
    # cron AND-THEN wait: the cron becomes the schedule and the sensor is retained as a polling
    # task (never silently dropped), because schedule and file_arrival triggers are mutually exclusive.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d', schedule_interval='0 6 * * *') as dag:\n"
        "    wait = S3KeySensor(task_id='wait', bucket_key='s3://x/in/', poke_interval=30, timeout=600)\n"
        "    go = PythonOperator(task_id='go', python_callable=w)\n"
        "    wait >> go\n"
    )
    assert p.schedule["kind"] == "schedule"
    assert p.schedule["quartz_cron_expression"] == "0 0 6 ? * *"
    tasks = _by_key(p)
    assert set(tasks) == {"wait", "go"}  # sensor retained as a task
    wait = tasks["wait"]
    assert isinstance(wait, NotebookActivity)
    assert "dbutils.fs.ls" in wait.generated_source
    assert "s3://x/in/" in wait.generated_source
    assert "POKE_INTERVAL = 30" in wait.generated_source
    assert "TIMEOUT = 600" in wait.generated_source
    compile(wait.generated_source, "<wait>", "exec")
    # `go` still depends on the retained sensor task (ordering preserved).
    assert [d.task_key for d in tasks["go"].depends_on] == ["wait"]


def test_mid_dag_sensor_retained_as_polling_task():
    # A sensor that is not the DAG's entry gate (has an upstream) is an ordering gate within the run,
    # so it stays a polling task rather than lifting to a job-level trigger.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.databricks.sensors.databricks_partition import DatabricksPartitionSensor\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    prep = PythonOperator(task_id='prep', python_callable=w)\n"
        "    wait = DatabricksPartitionSensor(task_id='wait', table_name='main.silver.events')\n"
        "    go = PythonOperator(task_id='go', python_callable=w)\n"
        "    prep >> wait >> go\n"
    )
    assert p.schedule is None  # mid-DAG sensor does not become a trigger
    tasks = _by_key(p)
    assert set(tasks) == {"prep", "wait", "go"}
    wait = tasks["wait"]
    assert isinstance(wait, NotebookActivity)
    assert "spark.catalog.tableExists('main.silver.events')" in wait.generated_source
    compile(wait.generated_source, "<wait>", "exec")


def test_dag_params_supply_job_parameter_defaults():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.models.param import Param\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d', params={'env': 'prod', 'threshold': Param(10)}) as dag:\n"
        "    t = PythonOperator(task_id='t', python_callable=w,\n"
        "                       op_kwargs={'e': '{{ params.env }}'})\n"
    )
    params = {entry["name"]: entry["default"] for entry in (p.parameters or [])}
    # Referenced param picks up its params={...} default; a declared-but-unreferenced param is
    # still emitted (with its default) so the job parameter validates.
    assert params["env"] == "prod"
    assert params["threshold"] == 10


# --------------------------------------------------------------------------------------
# Tier 4 — fallback
# --------------------------------------------------------------------------------------


def test_unknown_operator_becomes_placeholder():
    p = _load("from airflow import DAG\nwith DAG(dag_id='d') as dag:\n    t = SomeExoticOperator(task_id='mystery')\n")
    task = _by_key(p)["mystery"]
    assert isinstance(task, PlaceholderActivity)
    assert task.original_type == "SomeExoticOperator"


def test_placeholder_carries_operator_source_for_agentic_round():
    # The placeholder must carry the operator's raw source so a reviewed resolution workflow can
    # reason from it without reparsing or executing the DAG.
    p = _load(
        "from airflow import DAG\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = KubernetesPodOperator(task_id='pod', image='python:3.11')\n"
    )
    task = _by_key(p)["pod"]
    assert isinstance(task, PlaceholderActivity)
    assert task.raw_definition is not None
    assert task.raw_definition["operator"] == "KubernetesPodOperator"
    assert "image='python:3.11'" in task.raw_definition["source"]


def test_convert_emits_gaps_json_for_unmapped_operators():
    import json

    from flowx.sources.airflow.convert import main

    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "dag.py"
        src.write_text(
            "from airflow import DAG\n"
            "with DAG(dag_id='d') as dag:\n"
            "    t = KubernetesPodOperator(task_id='pod', image='x')\n",
            encoding="utf-8",
        )
        out = Path(tmp) / "out"
        assert main(["--source-dir", str(src), "--output-dir", str(out)]) == 0
        gaps = json.loads((out / ".work" / "gaps.json").read_text())
        assert len(gaps) == 1
        assert gaps[0]["activity_type"] == "KubernetesPodOperator"
        assert gaps[0]["raw_definition"]["source"]


def test_convert_rejects_legacy_agentic_merge_without_modifying_report(tmp_path):
    import json

    from flowx.sources.airflow.convert import main

    report = tmp_path / "translation_report.json"
    report.write_text(
        json.dumps(
            {
                "name": "example",
                "tasks": [
                    {
                        "type": "PlaceholderActivity",
                        "name": "b",
                        "task_key": "b",
                        "depends_on": [{"task_key": "a"}],
                        "max_retries": 1,
                        "original_type": "KubernetesPodOperator",
                    }
                ],
            }
        )
    )
    results = tmp_path / "results"
    results.mkdir()
    (results / "pod.json").write_text(
        json.dumps(
            {
                "activity_name": "b",
                "task": {
                    "type": "NotebookActivity",
                    "name": "b",
                    "task_key": "HIJACKED_KEY",
                    "depends_on": [],
                    "max_retries": 99,
                    "notebook_path": "/Workspace/evil",
                },
            }
        )
    )

    original = report.read_text()

    assert main(["--merge-agentic", "--report", str(report), "--agentic-results", str(results)]) == 2
    assert report.read_text() == original


# --------------------------------------------------------------------------------------
# Cross-cutting: Jinja templating, default_args, trigger_rule
# --------------------------------------------------------------------------------------


def test_jinja_macros_convert_to_dab_refs_and_collect_params():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w(date=None, env=None):\n    pass\n"
        "with DAG(dag_id='d', schedule_interval='0 6 * * *') as dag:\n"
        "    t = PythonOperator(task_id='t', python_callable=w,\n"
        "                       op_kwargs={'date': '{{ ds }}', 'env': '{{ params.env }}'})\n"
    )
    task = _by_key(p)["t"]
    # op_kwargs are JSON-encoded into the internal __flowx_op_kwargs widget; Jinja inside the
    # values is still converted to DAB refs. {{ ds }} routes through a run_date job parameter (so a
    # native backfill can override it), not an inline start_time ref.
    kwargs_json = task.base_parameters["__flowx_op_kwargs"]
    assert "{{job.parameters.__flowx_airflow_run_date}}" in kwargs_json
    assert "{{job.parameters.env}}" in kwargs_json
    # Referenced params are declared with Databricks-required defaults; the reserved logical-date
    # parameter defaults to the scheduled trigger time on a cron job.
    assert p.parameters == [
        {"name": "__flowx_airflow_run_date", "default": "{{job.trigger.time.iso_date}}"},
        {"name": "env", "default": ""},
    ]


def test_execution_date_on_event_triggered_job_defaults_to_start_time():
    # A cron+sensor collapses to a file_arrival trigger -- no scheduled trigger time exists, so the
    # The reserved execution-date parameter approximates with the run start time.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor\n"
        "def w(date=None):\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    wait = S3KeySensor(task_id='wait', bucket_key='s3://b/landing/')\n"
        "    t = PythonOperator(task_id='t', python_callable=w, op_kwargs={'date': '{{ execution_date }}'})\n"
        "    wait >> t\n"
    )
    assert (p.schedule or {}).get("kind") == "file_arrival"
    assert p.parameters == [{"name": "__flowx_airflow_execution_date", "default": "{{job.start_time.iso_datetime}}"}]


def test_catchup_true_tags_pipeline_for_native_backfill():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d', schedule_interval='0 6 * * *', catchup=True) as dag:\n"
        "    t = PythonOperator(task_id='t', python_callable=w)\n"
    )
    assert p.tags.get("airflow_catchup") == "true"


def test_catchup_false_leaves_no_backfill_tag():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d', schedule_interval='0 6 * * *', catchup=False) as dag:\n"
        "    t = PythonOperator(task_id='t', python_callable=w)\n"
    )
    assert "airflow_catchup" not in p.tags


def test_dag_param_named_run_date_remains_distinct_from_logical_date():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.models.param import Param\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w(date=None):\n    pass\n"
        "with DAG(dag_id='d', schedule_interval='0 6 * * *', params={'run_date': Param('2024-01-01')}) as dag:\n"
        "    t = PythonOperator(task_id='t', python_callable=w, op_kwargs={'date': '{{ ds }}'})\n"
    )
    parameters = {param["name"]: param["default"] for param in p.parameters}
    assert parameters["run_date"] == "2024-01-01"
    assert parameters["__flowx_airflow_run_date"] == "{{job.trigger.time.iso_date}}"
    task = _by_key(p)["t"]
    assert "{{job.parameters.__flowx_airflow_run_date}}" in task.base_parameters["__flowx_op_kwargs"]


def test_sql_embedded_string_template_becomes_placeholder():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = SQLExecuteQueryOperator(task_id='report', sql=\"SELECT 'partition_{{ ds }}'\")\n"
    )

    task = _by_key(p)["report"]
    assert isinstance(task, PlaceholderActivity)
    assert any(finding["code"] == "unresolved_airflow_template" for finding in p.not_translatable)


def test_unsupported_airflow_macro_becomes_placeholder():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w(value=None):\n    return value\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='t', python_callable=w, op_kwargs={'value': '{{ ds_nodash }}'})\n"
    )

    task = _by_key(p)["t"]
    assert isinstance(task, PlaceholderActivity)
    assert "ds_nodash" in task.comment


def test_default_args_apply_retries_timeout_retry_delay():
    p = _load(
        "from datetime import timedelta\n"
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d', default_args={'retries': 3, 'retry_delay': timedelta(minutes=5),\n"
        "         'execution_timeout': timedelta(hours=2)}) as dag:\n"
        "    t = PythonOperator(task_id='t', python_callable=w)\n"
    )
    task = _by_key(p)["t"]
    assert task.max_retries == 3
    assert task.timeout_seconds == 7200
    assert task.min_retry_interval_millis == 300000


def test_subsecond_timeout_rounds_up_not_dropped():
    # A sub-second execution_timeout must round up to 1s, not truncate to 0 (which reads as "unset").
    p = _load(
        "from datetime import timedelta\n"
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d', default_args={'execution_timeout': timedelta(milliseconds=500)}) as dag:\n"
        "    t = PythonOperator(task_id='t', python_callable=w)\n"
    )
    assert _by_key(p)["t"].timeout_seconds == 1


def test_timedelta_positional_arguments_are_preserved():
    p = _load(
        "from datetime import timedelta\n"
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d', dagrun_timeout=timedelta(1, 30), "
        "default_args={'execution_timeout': timedelta(0, 45)}) as dag:\n"
        "    t = PythonOperator(task_id='t', python_callable=w)\n"
    )

    assert p.timeout_seconds == 86430
    assert _by_key(p)["t"].timeout_seconds == 45


def test_per_task_retries_override_default_args():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d', default_args={'retries': 3}) as dag:\n"
        "    t = BashOperator(task_id='t', bash_command='echo hi', retries=7)\n"
    )
    assert _by_key(p)["t"].max_retries == 7


def test_trigger_rule_maps_to_run_if_constant():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    a = PythonOperator(task_id='a', python_callable=w)\n"
        "    cleanup = PythonOperator(task_id='cleanup', python_callable=w, trigger_rule='all_done')\n"
        "    fail_only = PythonOperator(task_id='fail_only', python_callable=w, trigger_rule='one_failed')\n"
        "    only_fail = PythonOperator(task_id='only_fail', python_callable=w, trigger_rule='all_failed')\n"
        "    any_ok = PythonOperator(task_id='any_ok', python_callable=w, trigger_rule='one_success')\n"
        "    no_fail = PythonOperator(task_id='no_fail', python_callable=w, trigger_rule='none_failed')\n"
        "    no_fail_with_success = PythonOperator(task_id='no_fail_with_success', python_callable=w,\n"
        "        trigger_rule='none_failed_min_one_success')\n"
        "    a >> cleanup\n"
        "    a >> fail_only\n"
        "    a >> only_fail\n"
        "    a >> any_ok\n"
        "    a >> no_fail\n"
        "    a >> no_fail_with_success\n"
    )
    tasks = _by_key(p)
    # trigger_rule maps straight to the DAB run_if constant, carried as the dependency outcome.
    assert tasks["cleanup"].depends_on[0].outcome == "ALL_DONE"
    assert tasks["fail_only"].depends_on[0].outcome == "AT_LEAST_ONE_FAILED"
    assert tasks["only_fail"].depends_on[0].outcome == "ALL_FAILED"
    assert tasks["any_ok"].depends_on[0].outcome == "AT_LEAST_ONE_SUCCESS"
    assert tasks["no_fail"].depends_on[0].outcome == "NONE_FAILED"
    assert tasks["no_fail_with_success"].depends_on[0].outcome == "NONE_FAILED"
    assert tasks["a"].depends_on is None  # default all_success -> no outcome


def test_trigger_rule_enum_member_maps_to_run_if_constant():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "from airflow.utils.trigger_rule import TriggerRule\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    a = PythonOperator(task_id='a', python_callable=w)\n"
        "    cleanup = PythonOperator(task_id='cleanup', python_callable=w, trigger_rule=TriggerRule.ALL_DONE)\n"
        "    a >> cleanup\n"
    )

    assert _by_key(p)["cleanup"].depends_on[0].outcome == "ALL_DONE"


# --------------------------------------------------------------------------------------
# Bare (unassigned) operator statements
# --------------------------------------------------------------------------------------


def test_bare_operator_statements_are_registered():
    # Airflow registers a task when the operator is instantiated inside a DAG context; assigning it to
    # a name is optional. Unassigned operators must not vanish (the Airflow example DAGs use this form).
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    BashOperator(task_id='alpha', bash_command='echo alpha')\n"
        "    BashOperator(task_id='beta', bash_command='echo beta')\n"
        "    assigned = BashOperator(task_id='gamma', bash_command='echo gamma')\n"
    )
    assert set(_by_key(p)) == {"alpha", "beta", "gamma"}


def test_bare_operator_chain_keeps_tasks_and_edge():
    # `Op() >> Op()` with no assignments: both tasks register and the dependency edge survives.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    BashOperator(task_id='first', bash_command='echo 1')"
        " >> BashOperator(task_id='second', bash_command='echo 2')\n"
    )
    tasks = _by_key(p)
    assert set(tasks) == {"first", "second"}
    assert [d.task_key for d in tasks["second"].depends_on] == ["first"]


def test_bare_operator_without_literal_task_id_gets_synthetic_key():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    BashOperator(bash_command='echo x')\n"
    )
    assert list(_by_key(p)) == ["bare_task1"]


def test_bare_operators_stay_scoped_to_their_own_dag():
    # Two DAGs in one module: each keeps only its own bare tasks.
    p = _load_all(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='one') as dag1:\n"
        "    BashOperator(task_id='a', bash_command='echo a')\n"
        "with DAG(dag_id='two') as dag2:\n"
        "    BashOperator(task_id='b', bash_command='echo b')\n"
        "    BashOperator(task_id='c', bash_command='echo c')\n"
    )
    by_name = {pipeline.name: sorted(t.task_key for t in pipeline.tasks) for pipeline in p}
    assert by_name == {"one": ["a"], "two": ["b", "c"]}


# --------------------------------------------------------------------------------------
# Dynamic mapping (.expand), TaskGroup prefixing, timezone/timedelta schedules
# --------------------------------------------------------------------------------------


def test_expand_becomes_for_each_task():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w(i=None):\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    m = PythonOperator.partial(task_id='proc', python_callable=w).expand(\n"
        "        op_kwargs=[{'i': 1}, {'i': 2}])\n"
    )
    task = _by_key(p)["proc"]
    assert isinstance(task, ForEachActivity)
    assert task.items_expression == '[{"i": 1}, {"i": 2}]'
    assert task.inner_activities[0].task_key == "proc_iteration"


def test_expand_direct_call_form():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    m = BashOperator(task_id='run', bash_command='echo').expand(env=[{'a': 1}])\n"
    )
    assert isinstance(_by_key(p)["run"], ForEachActivity)


def test_for_each_inputs_come_from_expand_not_partial():
    # A list-valued .partial() arg is a FIXED value; only the .expand() kwarg is fanned out. Taking the
    # partial list would iterate the wrong values (and the wrong count).
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    m = BashOperator.partial(task_id='t', env=['FIXED1', 'FIXED2']).expand(\n"
        "        bash_command=['echo a', 'echo b', 'echo c'])\n"
    )
    task = _by_key(p)["t"]
    assert isinstance(task, ForEachActivity)
    assert task.items_expression == '["echo a", "echo b", "echo c"]'


def test_placeholder_nested_in_for_each_is_collected_as_a_gap():
    # A mapped operator whose command isn't a literal becomes a PlaceholderActivity INSIDE the
    # for_each; gaps.json and the inventory must see it, or the guidance is generated then dropped.
    from flowx.sources.airflow.convert import _collect_gaps
    from flowx.sources.airflow.discover import _classify

    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    m = BashOperator.partial(task_id='t').expand(bash_command=['echo a', 'echo b'])\n"
    )
    outer = _by_key(p)["t"]
    assert isinstance(outer, ForEachActivity)
    assert isinstance(outer.inner_activities[0], PlaceholderActivity)
    assert len(_collect_gaps([p])) == 1
    assert [item["strategy"] for item in _classify(p)].count("agentic") == 1


def test_mixed_shift_directions_preserve_each_operator_direction():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        "    a = BashOperator(task_id='a', bash_command='a')\n"
        "    b = BashOperator(task_id='b', bash_command='b')\n"
        "    c = BashOperator(task_id='c', bash_command='c')\n"
        "    a >> b << c\n"
    )

    tasks = _by_key(p)
    assert tasks["a"].depends_on is None
    assert {dependency.task_key for dependency in tasks["b"].depends_on} == {"a", "c"}
    assert tasks["c"].depends_on is None


def test_task_group_prefixes_member_keys():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "from airflow.utils.task_group import TaskGroup\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    with TaskGroup('extract') as extract:\n"
        "        r = PythonOperator(task_id='run', python_callable=w)\n"
        "    with TaskGroup('load') as load:\n"
        "        r2 = PythonOperator(task_id='run', python_callable=w)\n"
    )
    keys = set(_by_key(p))
    assert keys == {"extract__run", "load__run"}  # no collision


def test_task_group_level_dependencies_expand_to_boundary_tasks():
    # `start >> etl >> pub >> end` where etl/pub are TaskGroups: the group-level edges must expand to
    # leaf(upstream) -> root(downstream), not be silently dropped.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "from airflow.utils.task_group import TaskGroup\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    start = PythonOperator(task_id='start', python_callable=w)\n"
        "    with TaskGroup('etl') as etl:\n"
        "        a = PythonOperator(task_id='a', python_callable=w)\n"
        "        b = PythonOperator(task_id='b', python_callable=w)\n"
        "        a >> b\n"
        "    with TaskGroup('publish') as pub:\n"
        "        c = PythonOperator(task_id='c', python_callable=w)\n"
        "    end = PythonOperator(task_id='end', python_callable=w)\n"
        "    start >> etl >> pub >> end\n"
    )
    deps = {k: sorted(d.task_key for d in (t.depends_on or [])) for k, t in _by_key(p).items()}
    assert deps["etl__a"] == ["start"]  # start -> root of etl
    assert deps["etl__b"] == ["etl__a"]  # intra-group edge preserved
    assert deps["publish__c"] == ["etl__b"]  # leaf of etl -> root of publish
    assert deps["end"] == ["publish__c"]  # leaf of publish -> end


def test_timedelta_schedule_becomes_periodic():
    p = _load(
        "from datetime import timedelta\n"
        "from airflow import DAG\n"
        "with DAG(dag_id='d', schedule_interval=timedelta(days=2)) as dag:\n"
        "    pass\n"
    )
    assert p.schedule == {"kind": "periodic", "interval": 2, "unit": "DAYS", "pause_status": "UNPAUSED"}


def test_subhour_timedelta_schedule_becomes_quartz_cron():
    p = _load(
        "from datetime import timedelta\n"
        "from airflow import DAG\n"
        "with DAG(dag_id='d', schedule=timedelta(minutes=30)) as dag:\n"
        "    pass\n"
    )
    assert p.schedule == {
        "kind": "schedule",
        "quartz_cron_expression": "0 0/30 * * * ?",
        "timezone_id": "UTC",
        "pause_status": "UNPAUSED",
    }


def test_continuous_schedule_becomes_continuous_job_mode():
    p = _load("from airflow import DAG\nwith DAG(dag_id='d', schedule='@continuous') as dag:\n    pass\n")
    assert p.schedule == {"kind": "continuous", "pause_status": "UNPAUSED"}


def test_dag_timezone_extracted_into_cron_schedule():
    p = _load(
        "from datetime import datetime\n"
        "import pendulum\n"
        "from airflow import DAG\n"
        "with DAG(dag_id='d', schedule_interval='0 6 * * *',\n"
        "         start_date=datetime(2024, 1, 1, tzinfo=pendulum.timezone('Europe/Madrid'))) as dag:\n"
        "    pass\n"
    )
    assert p.schedule["timezone_id"] == "Europe/Madrid"


# --------------------------------------------------------------------------------------
# Variables / Connections in notebook bodies
# --------------------------------------------------------------------------------------


def test_variable_get_rewritten_to_widget_and_declared_as_param():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "from airflow.models import Variable\n"
        "def ingest():\n"
        "    env = Variable.get('target_env')\n"
        "    print(env)\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='ingest', python_callable=ingest)\n"
    )
    task = _by_key(p)["ingest"]
    assert 'dbutils.widgets.get("__flowx_airflow_variable_target_env")' in task.generated_source
    assert "Variable.get" not in task.generated_source
    assert {"name": "__flowx_airflow_variable_target_env", "default": ""} in (p.parameters or [])


@pytest.mark.parametrize(
    "expression",
    [
        "Variable.get('target_env', 'prod')",
        "Variable.get('target_env', default_var='prod')",
        "Variable.get('target_env', deserialize_json=True)",
        "Variable.get(variable_name)",
    ],
)
def test_variable_get_forms_that_need_airflow_runtime_become_placeholders(expression: str):
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "from airflow.models import Variable\n"
        "variable_name = 'target_env'\n"
        "def ingest():\n"
        f"    print({expression})\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='ingest', python_callable=ingest)\n"
    )

    task = _by_key(p)["ingest"]
    assert isinstance(task, PlaceholderActivity)
    assert "Variable.get" in task.comment


def test_aliased_airflow_runtime_import_in_callable_becomes_placeholder():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "from airflow.models import Variable as AirflowVariable\n"
        "def ingest():\n"
        "    print(AirflowVariable.get('target_env'))\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='ingest', python_callable=ingest)\n"
    )

    task = _by_key(p)["ingest"]
    assert isinstance(task, PlaceholderActivity)
    assert "Airflow runtime import" in task.comment


def test_function_local_airflow_import_becomes_placeholder():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def ingest():\n"
        "    from airflow.models import Variable\n"
        "    print(Variable.get('target_env'))\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='ingest', python_callable=ingest)\n"
    )

    task = _by_key(p)["ingest"]
    assert isinstance(task, PlaceholderActivity)
    assert "Airflow runtime import" in task.comment


def test_reserved_flowx_dag_parameter_becomes_an_explicit_gap():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d', params={'__flowx_airflow_run_date': 'spoofed'}) as dag:\n"
        "    t = BashOperator(task_id='t', bash_command='echo ok')\n"
    )

    assert p.reconciliation_status == "verified_with_gaps"
    assert any(item["code"] == "reserved_airflow_parameter_name" for item in p.not_translatable)
    assert "__flowx_airflow_run_date" not in {param["name"] for param in p.parameters or []}


def test_connection_get_becomes_placeholder_for_connection_object_mapping():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "from airflow.hooks.base import BaseHook\n"
        "def ingest():\n"
        "    conn = BaseHook.get_connection('snowflake_default')\n"
        "    print(conn.host)\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='ingest', python_callable=ingest)\n"
    )
    task = _by_key(p)["ingest"]
    assert isinstance(task, PlaceholderActivity)
    assert "snowflake_default" in task.comment


def test_connection_get_in_carried_helper_becomes_placeholder():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "from airflow.hooks.base import BaseHook\n"
        "def connection_host():\n"
        "    conn = BaseHook.get_connection('warehouse')\n"
        "    return conn.host\n"
        "def ingest():\n"
        "    print(connection_host())\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='ingest', python_callable=ingest)\n"
    )

    task = _by_key(p)["ingest"]
    assert isinstance(task, PlaceholderActivity)
    assert "warehouse" in task.comment


def test_airflow_host_detection_from_dag_source():
    from flowx.sources.airflow.loader import detect_hosts

    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "dag.py"
        src.write_text(
            "from airflow import DAG\n"
            "from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator\n"
            "with DAG(dag_id='h') as dag:\n"
            "    t = DatabricksNotebookOperator(task_id='n', notebook_path='/W/e',\n"
            "        host='https://ws.cloud.databricks.com/')\n",
            encoding="utf-8",
        )
        assert detect_hosts(src) == ["ws.cloud.databricks.com"]


# --------------------------------------------------------------------------------------
# TaskFlow API (@dag / @task)
# --------------------------------------------------------------------------------------


def test_taskflow_dag_and_tasks_are_detected():
    # A pure-TaskFlow DAG must yield tasks (not silently drop them), with the @dag config picked up.
    p = _load(
        "from airflow.decorators import dag, task\n"
        "from datetime import datetime\n"
        "@task\n"
        "def extract():\n    return [1, 2, 3]\n"
        "@task\n"
        "def transform(data):\n    return [x * 2 for x in data]\n"
        "@task\n"
        "def load(data):\n    print(sum(data))\n"
        "@dag(schedule='0 6 * * *', start_date=datetime(2024, 1, 1), dag_id='etl_flow')\n"
        "def pipeline():\n"
        "    load(transform(extract()))\n"
        "pipeline()\n"
    )
    assert p.name == "etl_flow"
    assert p.schedule["quartz_cron_expression"] == "0 0 6 ? * *"
    kinds = sorted(type(t).__name__ for t in p.tasks)
    assert kinds == ["NotebookActivity", "NotebookActivity", "NotebookActivity"]
    # The nested chain load(transform(extract())) wires extract -> transform -> load.
    transform_task = next(t for t in p.tasks if t.task_key.startswith("transform"))
    extract_key = next(t.task_key for t in p.tasks if t.task_key.startswith("extract"))
    load_task = next(t for t in p.tasks if t.task_key.startswith("load"))
    assert transform_task.depends_on[0].task_key == extract_key
    assert load_task.depends_on[0].task_key == transform_task.task_key


def test_taskflow_data_flow_reads_upstream_taskvalue():
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@task\n"
        "def extract():\n    return 5\n"
        "@task\n"
        "def transform(data):\n    return data * 2\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    raw = extract()\n"
        "    transform(raw)\n"
        "pipeline()\n"
    )
    tasks = _by_key(p)
    assert set(tasks) == {"raw", "transform"}
    src = tasks["transform"].generated_source
    compile(src, "<transform>", "exec")
    assert "def transform(data):" in src  # callable carried
    assert "dbutils.jobs.taskValues.get(taskKey='raw', key='return_value'" in src  # reads upstream
    assert "result = transform(_upstream_0)" in src  # bound to positional arg
    assert "dbutils.jobs.taskValues.set(key='return_value', value=result)" in src  # publishes


def test_taskflow_literal_arguments_are_preserved():
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@task\n"
        "def add(x, y):\n    return x + y\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    add(1, y=2)\n"
        "pipeline()\n"
    )

    source = p.tasks[0].generated_source
    assert "result = add(1, y=2)" in source


def test_taskflow_nonliteral_argument_becomes_placeholder():
    p = _load(
        "from airflow.decorators import dag, task\n"
        "VALUE = 3\n"
        "@task\n"
        "def work(value):\n    return value\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    work(VALUE)\n"
        "pipeline()\n"
    )

    assert isinstance(p.tasks[0], PlaceholderActivity)
    assert "VALUE" in p.tasks[0].comment


def test_taskflow_override_call_is_preserved():
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@task\n"
        "def work(value):\n    return value\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    work.override(task_id='renamed')(2)\n"
        "pipeline()\n"
    )

    assert len(p.tasks) == 1
    assert p.tasks[0].task_key == "renamed"
    assert "result = work(2)" in p.tasks[0].generated_source


def test_nested_taskflow_callable_carries_module_imports():
    p = _load(
        "from datetime import datetime\n"
        "from airflow.decorators import dag, task\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    @task\n"
        "    def now():\n"
        "        return datetime.now().isoformat()\n"
        "    now()\n"
        "pipeline()\n"
    )

    source = p.tasks[0].generated_source
    assert "from datetime import datetime" in source
    compile(source, "<now>", "exec")


def test_nested_taskflow_callable_carries_literal_closure_bindings():
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    factor = 3\n"
        "    @task\n"
        "    def scale(value):\n"
        "        return value * factor\n"
        "    scale(2)\n"
        "pipeline()\n"
    )

    source = p.tasks[0].generated_source
    assert "factor = 3" in source
    assert "result = scale(2)" in source


def test_nested_taskflow_callable_with_dynamic_closure_becomes_placeholder():
    p = _load(
        "from airflow.decorators import dag, task\n"
        "def get_factor():\n"
        "    return 3\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    factor = get_factor()\n"
        "    @task\n"
        "    def scale(value):\n"
        "        return value * factor\n"
        "    scale(2)\n"
        "pipeline()\n"
    )

    assert isinstance(p.tasks[0], PlaceholderActivity)
    assert "factor" in p.tasks[0].comment


def test_taskflow_branch_decorator_becomes_placeholder():
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@task\n"
        "def extract():\n    return 1\n"
        "@task.branch\n"
        "def choose(data):\n    return 'a' if data else 'b'\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    choose(extract())\n"
        "pipeline()\n"
    )
    choose = next(t for t in p.tasks if t.task_key.startswith("choose"))
    assert isinstance(choose, PlaceholderActivity)
    assert "condition_task" in choose.comment


def test_taskflow_task_with_context_becomes_placeholder():
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@task\n"
        "def work(**context):\n    print(context['ds'])\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    work()\n"
        "pipeline()\n"
    )
    work = next(t for t in p.tasks if t.task_key.startswith("work"))
    assert isinstance(work, PlaceholderActivity)


def test_taskflow_mixed_with_classic_operator():
    # A @dag body mixing a classic operator and a @task: both become tasks, wired by >>.
    p = _load(
        "from airflow.decorators import dag, task\n"
        "from airflow.operators.bash import BashOperator\n"
        "@task\n"
        "def finalize():\n    print('done')\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    prep = BashOperator(task_id='prep', bash_command='echo hi')\n"
        "    prep >> finalize()\n"
        "pipeline()\n"
    )
    tasks = _by_key(p)
    assert "prep" in tasks
    finalize = next(t for t in p.tasks if t.task_key.startswith("finalize"))
    assert finalize.depends_on[0].task_key == "prep"


def test_taskflow_expand_literal_list_becomes_for_each():
    # @task.expand over a literal list -> for_each_task; the inner notebook reads the per-iteration
    # element from the `item` widget (Tier 1, deterministic).
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@task\n"
        "def process(item):\n    return item * 2\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    process.expand(item=[1, 2, 3])\n"
        "pipeline()\n"
    )
    task = next(t for t in p.tasks if t.task_key.startswith("process"))
    assert isinstance(task, ForEachActivity)
    # Each element is JSON-encoded individually so the inner notebook's json.loads recovers the exact
    # value (ints stay ints, JSON-looking strings stay strings) regardless of {{input}} serialization.
    assert task.items_expression == '["1", "2", "3"]'
    inner = task.inner_activities[0]
    assert isinstance(inner, NotebookActivity)
    assert "dbutils.widgets.get('item')" in inner.generated_source
    assert "item=_expand_item" in inner.generated_source
    compile(inner.generated_source, "<proc>", "exec")


def test_taskflow_mapped_output_consumer_becomes_placeholder():
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@task\n"
        "def add_one(value):\n"
        "    return value + 1\n"
        "@task\n"
        "def total(values):\n"
        "    return sum(values)\n"
        "@dag(dag_id='mapped_output')\n"
        "def pipeline():\n"
        "    added = add_one.expand(value=[1, 2, 3])\n"
        "    total(added)\n"
        "pipeline()\n"
    )

    tasks = _by_key(p)
    assert isinstance(tasks["added"], ForEachActivity)
    assert isinstance(tasks["total"], PlaceholderActivity)
    assert [dependency.task_key for dependency in tasks["total"].depends_on or []] == ["added"]
    assert p.reconciliation_status == "verified_with_gaps"
    assert any(finding["code"] == "taskflow_mapped_output_unavailable" for finding in p.not_translatable)


def test_airflow_non_execution_metadata_does_not_create_runtime_gap():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(\n"
        "    dag_id='metadata',\n"
        "    tags=['demo', 'daily'],\n"
        "    description='Customer-facing description',\n"
        "    doc_md='Long Airflow documentation',\n"
        "    default_args={'owner': 'data-platform'},\n"
        ") as dag:\n"
        "    work = BashOperator(task_id='work', bash_command='echo work')\n"
    )

    assert p.reconciliation_status == "verified"
    assert p.description == "Customer-facing description"
    assert p.tags["airflow_tag_1"] == "demo"
    assert p.tags["airflow_tag_2"] == "daily"
    assert p.tags["airflow_owner"] == "data-platform"
    assert any(
        item["code"] == "dag_setting_ignored" and item["setting"] == "doc_md" for item in p.audit["transformations"]
    )
    assert not any(finding["code"] == "unsupported_dag_setting" for finding in p.not_translatable)


def test_airflow_dagrun_timeout_and_failure_email_map_to_job_policy():
    p = _load(
        "import datetime\n"
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(\n"
        "    dag_id='job_policy',\n"
        "    dagrun_timeout=datetime.timedelta(minutes=45),\n"
        "    default_args={\n"
        "        'email': ['alerts@example.com'],\n"
        "        'email_on_failure': True,\n"
        "        'email_on_retry': False,\n"
        "    },\n"
        ") as dag:\n"
        "    work = BashOperator(task_id='work', bash_command='echo work')\n"
    )

    assert p.reconciliation_status == "verified"
    assert p.timeout_seconds == 2700
    assert p.email_notifications == {"on_failure": ["alerts@example.com"]}
    assert {
        (item["setting"], item["code"])
        for item in p.audit["transformations"]
        if item.get("setting") in {"dagrun_timeout", "default_args.email", "default_args.email_on_failure"}
    } == {
        ("dagrun_timeout", "dag_setting_mapped"),
        ("default_args.email", "dag_setting_mapped"),
        ("default_args.email_on_failure", "dag_setting_mapped"),
    }


def test_airflow_disabled_default_args_are_intentional_noops():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(\n"
        "    dag_id='disabled_defaults',\n"
        "    max_consecutive_failed_dag_runs=0,\n"
        "    sla_miss_callback=None,\n"
        "    default_args={\n"
        "        'depends_on_past': False,\n"
        "        'email': ['unused@example.com'],\n"
        "        'email_on_failure': False,\n"
        "        'email_on_retry': False,\n"
        "        'env': {},\n"
        "    },\n"
        ") as dag:\n"
        "    work = BashOperator(task_id='work', bash_command='echo work')\n"
    )

    assert p.reconciliation_status == "verified"
    assert p.email_notifications == {}
    ignored = {item["setting"] for item in p.audit["transformations"] if item.get("code") == "dag_setting_ignored"}
    assert {
        "max_consecutive_failed_dag_runs",
        "sla_miss_callback",
        "default_args.depends_on_past",
        "default_args.email",
        "default_args.email_on_failure",
        "default_args.email_on_retry",
        "default_args.env",
    } <= ignored
    assert not any(finding["code"] == "unsupported_dag_setting" for finding in p.not_translatable)


def test_airflow_sla_email_target_is_preserved_but_remains_an_explicit_gap():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "def notify(*args):\n"
        "    return None\n"
        "with DAG(\n"
        "    dag_id='sla_email',\n"
        "    sla_miss_callback=notify,\n"
        "    default_args={'email': 'alerts@example.com'},\n"
        ") as dag:\n"
        "    work = BashOperator(task_id='work', bash_command='echo work')\n"
    )

    assert p.email_notifications == {"on_failure": ["alerts@example.com"]}
    assert p.reconciliation_status == "verified_with_gaps"
    finding = next(
        item
        for item in p.not_translatable
        if item["code"] == "unsupported_dag_setting" and item["details"]["name"] == "default_args.email"
    )
    assert "SLA email" in finding["message"]
    assert any(
        item["code"] == "dag_setting_partially_mapped" and item["setting"] == "default_args.email"
        for item in p.audit["transformations"]
    )


def test_airflow_retry_email_accounts_for_task_level_retries():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(\n"
        "    dag_id='task_retry_email',\n"
        "    default_args={'email': 'ops@example.com', 'email_on_retry': True},\n"
        ") as dag:\n"
        "    work = BashOperator(task_id='work', bash_command='echo work', retries=2)\n"
    )

    assert p.email_notifications == {"on_failure": ["ops@example.com"]}
    assert p.reconciliation_status == "verified_with_gaps"
    findings = {
        item["details"]["name"]: item for item in p.not_translatable if item["code"] == "unsupported_dag_setting"
    }
    assert "retry notification" in findings["default_args.email"]["message"]
    assert "retry notification" in findings["default_args.email_on_retry"]["message"]


@pytest.mark.parametrize(
    ("dag_argument", "expected_reason"),
    [
        ("default_args={'depends_on_past': True}", "prior DAG run"),
        ("max_consecutive_failed_dag_runs=3", "automatically pause"),
        ("sla_miss_callback=notify", "SLA callback"),
        ("default_args={'env': {'TOKEN': '{{ conn.api.password }}'}}", "task environment"),
        (
            "default_args={'email': 'ops@example.com', 'email_on_retry': True, 'retries': 1}",
            "retry notification",
        ),
        ("dagrun_timeout=runtime_timeout", "static positive timedelta"),
    ],
)
def test_airflow_unrepresentable_dag_runtime_semantics_remain_blocking_gaps(dag_argument, expected_reason):
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "runtime_timeout = object()\n"
        "def notify(*args):\n"
        "    return None\n"
        f"with DAG(dag_id='runtime_semantics', {dag_argument}) as dag:\n"
        "    work = BashOperator(task_id='work', bash_command='echo work')\n"
    )

    assert p.reconciliation_status == "verified_with_gaps"
    assert p.tasks[0].task_key == "__flowx_source_gaps"
    finding = next(item for item in p.not_translatable if item["code"] == "unsupported_dag_setting")
    assert expected_reason in finding["message"]


def test_positional_dag_id_is_preserved_as_job_identity_metadata():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG('positional_dag') as dag:\n"
        "    work = BashOperator(task_id='work', bash_command='echo work')\n"
    )

    assert p.name == "positional_dag"
    assert p.tags["dag_id"] == "positional_dag"


def test_airflow_tags_respect_the_databricks_job_tag_limit():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        f"with DAG(dag_id='many_tags', tags={[f'tag_{index}' for index in range(30)]!r}, "
        "catchup=True, default_args={'owner': 'data-platform'}) as dag:\n"
        "    work = BashOperator(task_id='work', bash_command='echo work')\n"
    )

    assert len(p.tags) == 25
    assert p.tags["source"] == "airflow"
    assert p.tags["dag_id"] == "many_tags"
    assert p.tags["airflow_catchup"] == "true"
    assert p.tags["airflow_owner"] == "data-platform"
    assert any(
        item["code"] == "dag_setting_partially_mapped" and item["setting"] == "tags"
        for item in p.audit["transformations"]
    )


def test_taskflow_partial_expand_gap_carries_the_mapping_call():
    # .partial() fixed args can't ride on a for_each inner task, so the task becomes a placeholder --
    # but the gap must carry the mapping call, or the fixed argument values are lost and the agentic
    # round can't reconstruct the invocation (the callable's own source doesn't contain them).
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@task\n"
        "def get_astronauts():\n    return [{'name': 'A'}]\n"
        "@task\n"
        "def greet(greeting, person):\n    print(greeting, person)\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    greet.partial(greeting='Hello! :)').expand(person=get_astronauts())\n"
        "pipeline()\n"
    )
    placeholder = next(t for t in p.tasks if isinstance(t, PlaceholderActivity))
    mapping = placeholder.raw_definition["mapping"]
    assert "greeting='Hello! :)'" in mapping
    assert "expand(person=get_astronauts())" in mapping


def test_taskflow_expand_dict_list_becomes_for_each():
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@task\n"
        "def process(cfg):\n    return cfg\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    process.expand(cfg=[{'a': 1}, {'a': 2}])\n"
        "pipeline()\n"
    )
    task = next(t for t in p.tasks if t.task_key.startswith("process"))
    assert isinstance(task, ForEachActivity)
    # Elements are individually JSON-encoded (each is the JSON text of the dict).
    assert task.items_expression == '["{\\"a\\": 1}", "{\\"a\\": 2}"]'


def test_taskflow_expand_string_elements_round_trip_as_strings():
    # Regression: a list of JSON-looking strings must stay strings, not decode to int/bool/dict.
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@task\n"
        "def process(item):\n    return item\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    process.expand(item=['123', 'true'])\n"
        "pipeline()\n"
    )
    import json

    task = next(t for t in p.tasks if t.task_key.startswith("process"))
    assert isinstance(task, ForEachActivity)
    # Simulate the runtime: each inputs element's content is fed to the notebook's json.loads.
    decoded = [json.loads(element) for element in json.loads(task.items_expression)]
    assert decoded == ["123", "true"]  # strings, not 123 / True


def test_taskflow_expand_nonliteral_iterable_becomes_placeholder():
    # .expand over an upstream task's output isn't statically knowable -> placeholder + gap, never a
    # silent single-run notebook.
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@task\n"
        "def make():\n    return [1, 2, 3]\n"
        "@task\n"
        "def process(item):\n    return item * 2\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    vals = make()\n"
        "    process.expand(item=vals)\n"
        "pipeline()\n"
    )
    process = next(t for t in p.tasks if t.task_key.startswith("process"))
    assert isinstance(process, PlaceholderActivity)
    assert "for_each_task" in process.comment
    assert process.raw_definition is not None
    # The mapped iterable comes from `vals`, so the dependency edge must survive (not be dropped by
    # the mapped-call early return).
    assert [d.task_key for d in process.depends_on] == ["vals"]


def test_taskflow_partial_expand_becomes_placeholder_not_dropped():
    # .partial(...).expand(...) carries fixed args a for_each inner task can't represent, so it must
    # route to a placeholder (not a for_each that silently omits the partial args, nor a silent drop).
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@task\n"
        "def process(a, b):\n    return a + b\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    process.partial(a=1).expand(b=[1, 2, 3])\n"
        "pipeline()\n"
    )
    assert len(p.tasks) == 1
    task = p.tasks[0]
    assert isinstance(task, PlaceholderActivity)


def test_taskflow_partial_expand_preserves_upstream_dependency():
    # A .partial(x=upstream) fixed arg is an upstream data-flow dependency; the edge must survive
    # even though the mapped task routes to a placeholder.
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@task\n"
        "def extract():\n    return 1\n"
        "@task\n"
        "def process(x, z):\n    return x + z\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    raw = extract()\n"
        "    process.partial(x=raw).expand(z=[1, 2, 3])\n"
        "pipeline()\n"
    )
    process = next(t for t in p.tasks if t.task_key.startswith("process"))
    assert isinstance(process, PlaceholderActivity)
    assert [d.task_key for d in process.depends_on] == ["raw"]


def test_taskflow_expand_kwargs_becomes_placeholder():
    # .expand_kwargs([...]) maps whole kwargs dicts (not one param's iterable) -> placeholder, not a
    # single-run notebook.
    p = _load(
        "from airflow.decorators import dag, task\n"
        "@task\n"
        "def process(a, b):\n    return a\n"
        "@dag(dag_id='f')\n"
        "def pipeline():\n"
        "    process.expand_kwargs([{'a': 1, 'b': 2}])\n"
        "pipeline()\n"
    )
    assert len(p.tasks) == 1
    assert isinstance(p.tasks[0], PlaceholderActivity)


def test_task_group_mapped_call_becomes_placeholder_not_dropped():
    # A mapped @task_group is a sub-pipeline flowx can't lower; it must become a placeholder + gap,
    # never a silently empty pipeline.
    p = _load(
        "from airflow.decorators import dag, task, task_group\n"
        "@task\n"
        "def step_a(x):\n    return x + 1\n"
        "@task_group\n"
        "def pair(x):\n    return step_a(x)\n"
        "@dag(dag_id='g')\n"
        "def pipeline():\n"
        "    pair.expand(x=[1, 2, 3])\n"
        "pipeline()\n"
    )
    assert len(p.tasks) == 1
    group = p.tasks[0]
    assert isinstance(group, PlaceholderActivity)
    assert group.original_type == "@task_group"
    assert "maps the group over an iterable" in group.comment
    assert group.raw_definition is not None


def test_task_group_call_preserves_dependency_edges():
    # A @task_group wired with >> must keep its ordering: prep >> grp >> finish.
    p = _load(
        "from airflow.decorators import dag, task, task_group\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "@task\n"
        "def step_a(x):\n    return x + 1\n"
        "@task_group\n"
        "def pair(x):\n    return step_a(x)\n"
        "@dag(dag_id='g')\n"
        "def pipeline():\n"
        "    prep = PythonOperator(task_id='prep', python_callable=w)\n"
        "    grp = pair(5)\n"
        "    finish = PythonOperator(task_id='finish', python_callable=w)\n"
        "    prep >> grp >> finish\n"
        "pipeline()\n"
    )
    tasks = _by_key(p)
    assert isinstance(tasks["grp"], PlaceholderActivity)
    assert [d.task_key for d in tasks["grp"].depends_on] == ["prep"]
    assert [d.task_key for d in tasks["finish"].depends_on] == ["grp"]


def test_multiple_dags_in_one_file_are_loaded_as_separate_pipelines(tmp_path):
    from flowx.sources.airflow.loader import load_pipelines

    source = tmp_path / "multi.py"
    source.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='one') as dag_one:\n"
        "    a = BashOperator(task_id='a', bash_command='echo a')\n"
        "with DAG(dag_id='two') as dag_two:\n"
        "    b = BashOperator(task_id='b', bash_command='echo b')\n",
        encoding="utf-8",
    )

    pipelines = load_pipelines(source)

    assert [(pipeline.name, [task.task_key for task in pipeline.tasks]) for pipeline in pipelines] == [
        ("one", ["a"]),
        ("two", ["b"]),
    ]
