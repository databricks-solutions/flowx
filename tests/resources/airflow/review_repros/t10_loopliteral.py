from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id="loop2", schedule_interval="0 6 * * *") as dag:
    tasks = []
    for r in ["us", "eu"]:
        tasks.append(BashOperator(task_id="load_" + r, bash_command="echo x"))
