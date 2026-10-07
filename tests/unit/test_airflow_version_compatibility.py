"""Version-specific Airflow source compatibility and fail-closed behavior."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from flowx.models.ir import ForEachActivity, NotebookActivity, PlaceholderActivity
from flowx.sources.airflow.loader import load_airflow_dag


def _load(tmp_path: Path, source: str):
    dag = tmp_path / "dag.py"
    dag.write_text(source, encoding="utf-8")
    return load_airflow_dag(dag)


def test_airflow_3_sdk_and_standard_provider_paths_translate_deterministically(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "from airflow.sdk import DAG, task\n"
        "from airflow.providers.standard.operators.bash import BashOperator\n"
        "@task\n"
        "def extract():\n"
        "    return 1\n"
        "with DAG(dag_id='airflow_3', schedule='@daily', catchup=False) as dag:\n"
        "    start = BashOperator(task_id='start', bash_command='echo start')\n"
        "    result = extract()\n"
        "    start >> result\n",
    )

    assert pipeline.reconciliation_status == "verified"
    assert [task.task_key for task in pipeline.tasks] == ["start", "result"]
    assert all(isinstance(task, NotebookActivity) for task in pipeline.tasks)
    assert (pipeline.schedule or {})["quartz_cron_expression"] == "0 0 0 * * ?"


def test_airflow_3_omitted_schedule_preserves_manual_only_default(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "from airflow.sdk import dag\n"
        "from airflow.providers.standard.operators.bash import BashOperator\n"
        "@dag(dag_id='manual_airflow_3')\n"
        "def build():\n"
        "    BashOperator(task_id='work', bash_command='echo work')\n"
        "build()\n",
    )

    assert pipeline.reconciliation_status == "verified"
    assert pipeline.schedule is None
    assert pipeline.tags == {"source": "airflow", "dag_id": "manual_airflow_3"}


@pytest.mark.parametrize(
    ("schedule", "condition"),
    [
        ("[orders, customers]", "ALL_UPDATED"),
        ("orders & customers", "ALL_UPDATED"),
        ("orders | customers", "ANY_UPDATED"),
    ],
)
def test_airflow_3_asset_schedules_with_uc_metadata_become_table_triggers(
    tmp_path: Path,
    schedule: str,
    condition: str,
) -> None:
    pipeline = _load(
        tmp_path,
        "from airflow.sdk import DAG, Asset\n"
        "from airflow.providers.standard.operators.bash import BashOperator\n"
        "orders = Asset('orders', extra={'databricks_table': 'main.raw.orders'})\n"
        "customers = Asset('customers', extra={'databricks_table': 'main.raw.customers'})\n"
        f"with DAG(dag_id='assets', schedule={schedule}) as dag:\n"
        "    BashOperator(task_id='work', bash_command='echo work')\n",
    )

    assert pipeline.reconciliation_status == "verified"
    assert pipeline.schedule == {
        "kind": "table_update",
        "table_names": ["main.raw.orders", "main.raw.customers"],
        "condition": condition,
        "pause_status": "UNPAUSED",
    }
    assert any(item["code"] == "asset_schedule_lowered" for item in pipeline.audit["transformations"])


@pytest.mark.parametrize(
    ("schedule", "finding_code"),
    [
        ("[Asset('s3://landing/orders')]", "unresolved_asset_schedule"),
        (
            "AssetOrTimeSchedule(timetable=CronTriggerTimetable('0 0 * * *'), assets=[Asset('orders')])",
            "unsupported_asset_or_time_schedule",
        ),
    ],
)
def test_airflow_3_unrepresentable_schedules_become_source_semantic_gaps(
    tmp_path: Path,
    schedule: str,
    finding_code: str,
) -> None:
    pipeline = _load(
        tmp_path,
        "from airflow.sdk import DAG, Asset\n"
        "from airflow.timetables.assets import AssetOrTimeSchedule\n"
        "from airflow.timetables.trigger import CronTriggerTimetable\n"
        "from airflow.providers.standard.operators.bash import BashOperator\n"
        f"with DAG(dag_id='asset_gap', schedule={schedule}) as dag:\n"
        "    BashOperator(task_id='work', bash_command='echo work')\n",
    )

    assert pipeline.reconciliation_status == "verified_with_gaps"
    assert pipeline.schedule is None
    assert pipeline.tasks[0].task_key == "__flowx_source_gaps"
    assert any(item["code"] == finding_code for item in pipeline.not_translatable)


def test_airflow_3_async_taskflow_becomes_an_agentic_leaf_gap(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "from airflow.sdk import dag, task\n"
        "@task\n"
        "async def fetch():\n"
        "    return 1\n"
        "@dag(dag_id='async_task', schedule=None)\n"
        "def build():\n"
        "    fetch()\n"
        "build()\n",
    )

    assert pipeline.reconciliation_status == "verified_with_gaps"
    assert len(pipeline.tasks) == 1
    task = pipeline.tasks[0]
    assert isinstance(task, PlaceholderActivity)
    assert task.original_type == "@task.async"
    assert "async def fetch" in (task.raw_definition or {})["source"]
    assert any(item["code"] == "operator_placeholder" for item in pipeline.not_translatable)


def test_airflow_3_mapped_async_taskflow_preserves_the_static_for_each(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "from airflow.sdk import dag, task\n"
        "@task\n"
        "async def fetch(region):\n"
        "    return region\n"
        "@dag(dag_id='mapped_async_task', schedule=None)\n"
        "def build():\n"
        "    fetch.expand(region=['us-west-2', 'eu-west-1'])\n"
        "build()\n",
    )

    assert pipeline.reconciliation_status == "verified_with_gaps"
    assert len(pipeline.tasks) == 1
    mapped = pipeline.tasks[0]
    assert isinstance(mapped, ForEachActivity)
    assert [json.loads(item) for item in json.loads(mapped.items_expression)] == ["us-west-2", "eu-west-1"]
    assert len(mapped.inner_activities) == 1
    task = mapped.inner_activities[0]
    assert isinstance(task, PlaceholderActivity)
    assert task.original_type == "@task.async.expand"
    assert "fetch.expand" in (task.raw_definition or {})["mapping"]


def test_airflow_1_10_assigned_dag_and_legacy_imports_translate_deterministically(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "from airflow import DAG\n"
        "from airflow.operators.bash_operator import BashOperator\n"
        "from airflow.operators.dummy_operator import DummyOperator\n"
        "dag = DAG(dag_id='airflow_1_10', schedule_interval='@daily', catchup=False)\n"
        "start = DummyOperator(task_id='start', dag=dag)\n"
        "work = BashOperator(task_id='work', bash_command='echo work', dag=dag)\n"
        "start >> work\n",
    )

    assert pipeline.reconciliation_status == "verified"
    assert [task.task_key for task in pipeline.tasks] == ["work"]
    assert (pipeline.schedule or {})["quartz_cron_expression"] == "0 0 0 * * ?"


def test_airflow_1_10_implicit_daily_schedule_fails_loudly(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        "from airflow import DAG\n"
        "from airflow.operators.bash_operator import BashOperator\n"
        "dag = DAG(dag_id='implicit_legacy_schedule')\n"
        "work = BashOperator(task_id='work', bash_command='echo work', dag=dag)\n",
    )

    assert pipeline.reconciliation_status == "verified_with_gaps"
    assert pipeline.schedule is None
    assert pipeline.tasks[0].task_key == "__flowx_source_gaps"
    assert any(item["code"] == "ambiguous_airflow_1_10_default_schedule" for item in pipeline.not_translatable)
