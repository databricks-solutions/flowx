from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
with DAG(dag_id="sensor_mid", schedule_interval=None) as dag:
    s = FileSensor(task_id="wait", filepath="/mnt/data/in.csv", timeout=3600)
    a = BashOperator(task_id="a", bash_command="echo a")
    b = BashOperator(task_id="b", bash_command="echo b")
    s >> a
    b >> a
