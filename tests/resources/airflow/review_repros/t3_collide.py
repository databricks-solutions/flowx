from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id="collide_dag", schedule_interval="0 6 * * *") as dag:
    x = BashOperator(task_id="load.data", bash_command="echo 1")
    y = BashOperator(task_id="load_data", bash_command="echo 2")
    z = BashOperator(task_id="final", bash_command="echo 3")
    x >> z
    y >> z
