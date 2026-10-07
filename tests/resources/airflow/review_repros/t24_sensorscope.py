from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
with DAG(dag_id="ss2", schedule_interval=None) as dag:
    s = FileSensor(task_id="wait", filepath="/mnt/in.csv")
    gated = BashOperator(task_id="gated", bash_command="echo g")
    independent = BashOperator(task_id="independent", bash_command="echo i")
    s >> gated
