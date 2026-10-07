"""Sample Airflow DAG used by the airflow-source spike.

Representative of a common field pattern: a Python ingest step, a bash step, and
a Python publish step wired with >> dependencies under a cron schedule. Parsed
statically by flowx.sources.airflow.loader (no Airflow install required).
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def ingest_orders():
    df = spark.read.json("s3://acme-orders/raw/")
    df.write.mode("append").saveAsTable("main.analytics.raw_orders")


def publish_metrics():
    daily = spark.table("main.analytics.raw_orders").groupBy("order_date").count()
    daily.write.mode("overwrite").saveAsTable("main.analytics.daily_order_metrics")


with DAG(
    dag_id="orders_analytics",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    ingest = PythonOperator(
        task_id="ingest_orders",
        python_callable=ingest_orders,
    )

    transform = BashOperator(
        task_id="transform_orders",
        bash_command="python /opt/etl/transform_orders.py --date {{ ds }}",
    )

    publish = PythonOperator(
        task_id="publish_metrics",
        python_callable=publish_metrics,
    )

    ingest >> transform >> publish
