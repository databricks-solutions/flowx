from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id="loop_dag", schedule_interval="0 6 * * *") as dag:
    prev = None
    for region in ["us", "eu", "apac"]:
        t = BashOperator(task_id=f"load_{region}", bash_command=f"echo {region}")
        if prev:
            prev >> t
        prev = t
