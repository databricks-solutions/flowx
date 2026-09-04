from airflow import DAG
from airflow.operators.python import PythonOperator
CONST = 42
def helper(x):
    return x * CONST
def work():
    return helper(2)
with DAG(dag_id="deps", schedule_interval="0 6 * * *") as dag:
    a = PythonOperator(task_id="a", python_callable=work)
