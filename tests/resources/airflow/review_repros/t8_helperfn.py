from airflow import DAG
from airflow.operators.bash import BashOperator
def make(tid):
    return BashOperator(task_id=tid, bash_command="echo x")
with DAG(dag_id="helper_dag", schedule_interval="0 6 * * *") as dag:
    a = make("first")
    b = make("second")
    a >> b
