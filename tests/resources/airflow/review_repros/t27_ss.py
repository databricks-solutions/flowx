from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
with DAG(dag_id="ss3", schedule_interval="0 6 * * *") as dag:
    a = SparkSubmitOperator(task_id="py", application="/jobs/etl.py", conf={"spark.executor.memory":"4g"}, application_args=["--d","2024-01-01"])
