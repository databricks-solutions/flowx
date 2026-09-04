from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
with DAG(dag_id="sqlq", schedule_interval="0 6 * * *") as dag:
    a = DatabricksSqlOperator(task_id="q", sql="SELECT * FROM t WHERE d = '{{ ds }}' AND n = 'O''Brien'")
    b = DatabricksSqlOperator(task_id="q2", sql="SELECT * FROM {{ params.tbl }} WHERE x = 1")
