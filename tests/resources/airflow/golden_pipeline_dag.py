"""Golden-bundle fixture DAG for the airflow source.

Exercises the Phase-2 conversion behaviours in one representative DAG so the golden test
pins their end-to-end bundle output:
  - cron schedule AND a root file sensor -> schedule kept + sensor retained as a polling task
  - a mid-DAG table sensor -> polling task (not a trigger)
  - trigger_rule variety -> DAB run_if constants
  - params={...} -> job-parameter defaults; {{ params.x }} -> {{job.parameters.x}}
  - >> and set_upstream dependency forms
Parsed statically by flowx.sources.airflow.loader (no Airflow install required).
"""

from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.databricks.sensors.databricks_partition import DatabricksPartitionSensor


def ingest_orders(target_env=None):
    df = spark.read.json(f"s3://acme-orders/{target_env}/raw/")
    df.write.mode("append").saveAsTable("main.analytics.raw_orders")


def publish_metrics():
    daily = spark.table("main.analytics.raw_orders").groupBy("order_date").count()
    daily.write.mode("overwrite").saveAsTable("main.analytics.daily_order_metrics")


with DAG(
    dag_id="golden_pipeline",
    schedule_interval="0 6 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={"target_env": Param("prod"), "threshold": 100},
) as dag:
    wait_landing = S3KeySensor(
        task_id="wait_landing",
        bucket_key="s3://acme-orders/landing/",
        poke_interval=120,
        timeout=3600,
    )

    ingest = PythonOperator(
        task_id="ingest_orders",
        python_callable=ingest_orders,
        op_kwargs={"target_env": "{{ params.target_env }}"},
    )

    wait_partition = DatabricksPartitionSensor(
        task_id="wait_partition",
        table_name="main.analytics.raw_orders",
        poke_interval=60,
        timeout=1800,
    )

    publish = PythonOperator(
        task_id="publish_metrics",
        python_callable=publish_metrics,
    )

    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=publish_metrics,
        trigger_rule="all_done",
    )

    alert_on_failure = PythonOperator(
        task_id="alert_on_failure",
        python_callable=publish_metrics,
        trigger_rule="one_failed",
    )

    wait_landing >> ingest >> wait_partition >> publish
    cleanup.set_upstream(publish)
    alert_on_failure.set_upstream(publish)
