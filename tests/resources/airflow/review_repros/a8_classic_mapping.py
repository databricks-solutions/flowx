from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id="fan", schedule="@daily") as dag:
    BashOperator.partial(task_id="fan", bash_command="echo static").expand(
        env=[{"A": "1"}, {"A": "2"}]
    )
