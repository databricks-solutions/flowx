from airflow import DAG
from airflow.operators.python import PythonOperator
def outer_a():
    def process():
        return "WRONG_BODY_A"
    return process
def process():
    return "CORRECT_BODY"
def outer_b():
    def process():
        return "WRONG_BODY_B"
    return process
with DAG(dag_id="fnc", schedule_interval="0 6 * * *") as dag:
    a = PythonOperator(task_id="a", python_callable=process)
