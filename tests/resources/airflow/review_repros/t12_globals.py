from airflow import DAG
from airflow.operators.bash import BashOperator
for team in ["alpha", "beta"]:
    with DAG(dag_id=f"etl_{team}", schedule_interval="0 6 * * *") as d:
        BashOperator(task_id="run", bash_command="echo x")
    globals()[f"etl_{team}"] = d
