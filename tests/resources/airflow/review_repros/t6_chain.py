from airflow import DAG
from airflow.models.baseoperator import chain, cross_downstream
from airflow.operators.bash import BashOperator
with DAG(dag_id="chain_dag", schedule_interval="0 6 * * *") as dag:
    a = BashOperator(task_id="a", bash_command="echo a")
    b = BashOperator(task_id="b", bash_command="echo b")
    c = BashOperator(task_id="c", bash_command="echo c")
    d = BashOperator(task_id="d", bash_command="echo d")
    chain(a, b, c)
    cross_downstream([a, b], [c, d])
