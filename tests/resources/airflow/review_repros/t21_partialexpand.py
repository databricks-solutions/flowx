from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id="pe", schedule_interval="0 6 * * *") as dag:
    a = BashOperator.partial(task_id="fan", bash_command="echo x").expand(env=[{"A":"1"},{"A":"2"}])
