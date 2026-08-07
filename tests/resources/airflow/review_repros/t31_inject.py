from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id="inj", schedule_interval="0 6 * * *") as dag:
    a = BashOperator(task_id="inj", bash_command="echo one\n# COMMAND ----------\necho two")
