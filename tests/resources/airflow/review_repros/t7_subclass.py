from airflow import DAG
from airflow.operators.bash import BashOperator
class MyBashOperator(BashOperator):
    pass
with DAG(dag_id="sub_dag", schedule_interval="0 6 * * *") as dag:
    a = MyBashOperator(task_id="custom", bash_command="echo hi")
    b = BashOperator(task_id="plain", bash_command="echo plain")
    a >> b
