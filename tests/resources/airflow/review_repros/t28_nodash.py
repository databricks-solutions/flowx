from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
with DAG(dag_id="nd", schedule_interval="0 6 * * *") as dag:
    a = DatabricksNotebookOperator(task_id="nb", notebook_path="/x", notebook_params={"d":"{{ ds_nodash }}"})
