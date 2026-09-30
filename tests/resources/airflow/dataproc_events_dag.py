from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = "customer-analytics"
REGION = "us-central1"
CLUSTER_NAME = "dataproc-events-{{ ds_nodash }}"

CLUSTER_CONFIG = {
    "worker_config": {
        "num_instances": 4,
        "machine_type_uri": "n2-standard-8",
    },
    "software_config": {
        "image_version": "2.2-debian12",
        "properties": {
            "spark:spark.sql.adaptive.enabled": "true",
            "yarn:yarn.nodemanager.resource.memory-mb": "28672",
        },
    },
}

PYSPARK_JOB = {
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": "gs://customer-dataproc-artifacts/jobs/transform_events.py",
        "args": [
            "--input",
            "gs://customer-events/raw/",
            "--output-table",
            "analytics.events_daily",
            "--run-date",
            "{{ ds }}",
        ],
        "properties": {"spark.sql.shuffle.partitions": "200"},
    },
}

with DAG(
    dag_id="dataproc_events",
    schedule_interval="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Los_Angeles"),
    catchup=True,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    },
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    submit_events = DataprocSubmitJobOperator(
        task_id="submit_events",
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB,
        asynchronous=True,
    )

    wait_for_events = DataprocJobSensor(
        task_id="wait_for_events",
        project_id=PROJECT_ID,
        region=REGION,
        dataproc_job_id="{{ ti.xcom_pull(task_ids='submit_events') }}",
        poke_interval=30,
        timeout=7200,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> submit_events >> wait_for_events >> delete_cluster
