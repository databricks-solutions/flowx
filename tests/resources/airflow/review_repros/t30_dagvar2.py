from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
dag = DAG(
    dag_id="legacy_etl",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024,1,1),
    catchup=True,
    default_args={"retries": 5, "execution_timeout": timedelta(hours=2)},
    params={"env": "prod"},
)
a = BashOperator(task_id="extract", bash_command="run.sh --d {{ ds }}", dag=dag)
b = BashOperator(task_id="load", bash_command="load.sh", dag=dag, trigger_rule="all_done")
a >> b
