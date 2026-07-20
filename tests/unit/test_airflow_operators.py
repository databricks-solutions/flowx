"""Unit tests for Airflow operator -> flowx IR coverage (Tier 1-4)."""

from __future__ import annotations

import tempfile
from pathlib import Path

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
from flowx.sources.airflow.loader import load_airflow_dag


def _load(dag_source: str):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "dag.py"
        path.write_text(dag_source, encoding="utf-8")
        return load_airflow_dag(path)


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


def test_sql_condition_sensor_without_table_stays_placeholder():
    # A SqlSensor checking an arbitrary condition (no table_name) must NOT vanish;
    # it stays as a placeholder task rather than lifting to a table trigger.
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.common.sql.sensors.sql import SqlSensor\n"
        "with DAG(dag_id='d') as dag:\n"
        "    s = SqlSensor(task_id='chk', sql='SELECT COUNT(*) FROM t WHERE ready')\n"
    )
    task = _by_key(p)["chk"]
    assert isinstance(task, PlaceholderActivity)
    assert p.schedule is None


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


def test_explicit_cron_wins_over_sensor_trigger():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d', schedule_interval='0 6 * * *') as dag:\n"
        "    wait = S3KeySensor(task_id='wait', bucket_key='s3://x/')\n"
        "    go = PythonOperator(task_id='go', python_callable=w)\n"
        "    wait >> go\n"
    )
    assert p.schedule["kind"] == "schedule"
    assert p.schedule["quartz_cron_expression"] == "0 0 6 ? * *"


# --------------------------------------------------------------------------------------
# Tier 4 — fallback
# --------------------------------------------------------------------------------------


def test_unknown_operator_becomes_placeholder():
    p = _load("from airflow import DAG\nwith DAG(dag_id='d') as dag:\n    t = SomeExoticOperator(task_id='mystery')\n")
    task = _by_key(p)["mystery"]
    assert isinstance(task, PlaceholderActivity)
    assert task.original_type == "SomeExoticOperator"


def test_placeholder_carries_operator_source_for_agentic_round():
    # The placeholder must carry the operator's raw source so the agentic-gap round
    # (gaps.json + merge_agentic) can reason from it, like the ADF source's ARM JSON.
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


# --------------------------------------------------------------------------------------
# Cross-cutting: Jinja templating, default_args, trigger_rule
# --------------------------------------------------------------------------------------


def test_jinja_macros_convert_to_dab_refs_and_collect_params():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = PythonOperator(task_id='t', python_callable=w,\n"
        "                       op_kwargs={'date': '{{ ds }}', 'env': '{{ params.env }}'})\n"
    )
    task = _by_key(p)["t"]
    assert task.base_parameters == {"date": "{{job.start_time.iso_date}}", "env": "{{job.parameters.env}}"}
    # The referenced param is declared on the pipeline.
    assert p.parameters == [{"name": "env"}]


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


def test_per_task_retries_override_default_args():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d', default_args={'retries': 3}) as dag:\n"
        "    t = BashOperator(task_id='t', bash_command='echo hi', retries=7)\n"
    )
    assert _by_key(p)["t"].max_retries == 7


def test_trigger_rule_maps_to_dependency_outcome():
    p = _load(
        "from airflow import DAG\n"
        "from airflow.operators.python import PythonOperator\n"
        "def w():\n    pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    a = PythonOperator(task_id='a', python_callable=w)\n"
        "    cleanup = PythonOperator(task_id='cleanup', python_callable=w, trigger_rule='all_done')\n"
        "    fail_only = PythonOperator(task_id='fail_only', python_callable=w, trigger_rule='one_failed')\n"
        "    a >> cleanup\n"
        "    a >> fail_only\n"
    )
    tasks = _by_key(p)
    assert tasks["cleanup"].depends_on[0].outcome == "Completed"  # all_done -> ALL_DONE
    assert tasks["fail_only"].depends_on[0].outcome == "Failed"  # one_failed -> AT_LEAST_ONE_FAILED
    assert tasks["a"].depends_on is None  # default all_success -> no outcome


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


def test_timedelta_schedule_becomes_periodic():
    p = _load(
        "from datetime import timedelta\n"
        "from airflow import DAG\n"
        "with DAG(dag_id='d', schedule_interval=timedelta(days=2)) as dag:\n"
        "    pass\n"
    )
    assert p.schedule == {"kind": "periodic", "interval": 2, "unit": "DAYS", "pause_status": "UNPAUSED"}


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
    assert 'dbutils.widgets.get("target_env")' in task.generated_source
    assert "Variable.get" not in task.generated_source
    assert {"name": "target_env"} in (p.parameters or [])


def test_connection_get_rewritten_to_secrets():
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
    assert "dbutils.secrets.get(" in task.generated_source
    assert "snowflake_default_scope" in task.generated_source
    assert "BaseHook.get_connection" not in task.generated_source


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
