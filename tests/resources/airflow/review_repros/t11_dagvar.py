from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
dag = DAG(dag_id="assigned_dag", schedule_interval="0 3 * * *", start_date=datetime(2024,1,1))
a = BashOperator(task_id="a", bash_command="echo a", dag=dag)
b = BashOperator(task_id="b", bash_command="echo b", dag=dag)
a >> b
