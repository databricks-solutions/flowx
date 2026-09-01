from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
with DAG(dag_id="ret", schedule_interval="0 6 * * *", default_args={"retries": 3, "execution_timeout": timedelta(minutes=30), "retry_delay": timedelta(seconds=90)}) as dag:
    a = BashOperator(task_id="a", bash_command="echo a")
    b = BashOperator(task_id="b", bash_command="echo b", retries=0)
    a >> b
