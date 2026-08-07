from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(dag_id="legacy_etl", schedule_interval="0 3 * * *", catchup=True,
          default_args={"retries": 5, "execution_timeout": timedelta(hours=2)},
          params={"env": "prod"})
a = BashOperator(task_id="extract", bash_command="run.sh --d {{ ds }}", dag=dag)
b = BashOperator(task_id="load", bash_command="load.sh", dag=dag, trigger_rule="all_done")
a >> b
