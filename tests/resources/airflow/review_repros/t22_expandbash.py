from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
def work(region):
    print(region)
with DAG(dag_id="eb", schedule_interval="0 6 * * *") as dag:
    a = BashOperator.partial(task_id="fanb").expand(bash_command=["echo us", "echo eu"])
    b = PythonOperator.partial(task_id="fanp", python_callable=work).expand(op_kwargs=[{"region":"us"},{"region":"eu"}])
