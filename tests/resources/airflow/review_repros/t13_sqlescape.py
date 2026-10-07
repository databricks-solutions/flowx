from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
with DAG(dag_id="sqlesc", schedule_interval="0 6 * * *") as dag:
    a = DatabricksSqlOperator(task_id="q", sql="SELECT * FROM t WHERE name = 'O''Brien' AND d = '{{ ds }}' AND x = '{{ ds_nodash }}'")
