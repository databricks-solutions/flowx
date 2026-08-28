from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id="le", schedule_interval="0 6 * * *") as dag:
    up = BashOperator(task_id="up", bash_command="echo u")
    a = BashOperator(task_id="a", bash_command="echo a")
    b = BashOperator(task_id="b", bash_command="echo b")
    for t in (a, b):
        up >> t
