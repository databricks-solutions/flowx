from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id="tr_dag", schedule_interval="0 6 * * *") as dag:
    a = BashOperator(task_id="a", bash_command="echo a")
    b = BashOperator(task_id="b", bash_command="echo b")
    cleanup = BashOperator(task_id="cleanup", bash_command="echo c", trigger_rule="all_done")
    normal = BashOperator(task_id="normal", bash_command="echo n")
    a >> cleanup
    b >> cleanup
    a >> normal
