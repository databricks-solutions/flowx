from airflow import DAG
from airflow.operators.bash import BashOperator as Bash
from airflow.operators.python import PythonOperator
def work(): print("hi")
with DAG(dag_id="alias_dag", schedule_interval="0 6 * * *") as dag:
    a = Bash(task_id="aliased", bash_command="echo hi")
    b = PythonOperator(task_id="py", python_callable=work)
    a >> b
