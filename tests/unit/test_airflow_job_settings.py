"""Tests for the Databricks job settings an Airflow DAG's schedule carries over: timezone and pause state."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from flowx.bundler.dab_writer import pipeline_dict_to_ir, write_bundle
from flowx.ir_serde import pipeline_to_dict
from flowx.models.ir import Pipeline, PlaceholderActivity
from flowx.preparer.workflow_preparer import prepare_workflow
from flowx.sources.airflow.loader import load_airflow_dag


def _load(
    tmp_path: Path, dag_arguments: str, *, task: str = "BashOperator(task_id='t', bash_command='echo hi')"
) -> Pipeline:
    dag_path = tmp_path / "dag.py"
    dag_path.write_text(
        "import pendulum\n"
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        f"with DAG(dag_id='d', schedule_interval='0 1 * * *'{dag_arguments}) as dag:\n"
        f"    t = {task}\n",
        encoding="utf-8",
    )
    return load_airflow_dag(dag_path)


def _transformation(pipeline: Pipeline, code: str) -> dict | None:
    return next((item for item in pipeline.audit["transformations"] if item["code"] == code), None)


def test_schedule_without_a_timezone_discloses_the_utc_default(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "")

    assert pipeline.schedule["timezone_id"] == "UTC"
    disclosure = _transformation(pipeline, "schedule_timezone_defaulted")
    assert disclosure is not None
    assert "default_timezone" in disclosure["message"]


def test_dag_start_date_timezone_is_not_disclosed(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, ", start_date=pendulum.datetime(2024, 1, 1, tz='Europe/Madrid')")

    assert pipeline.schedule["timezone_id"] == "Europe/Madrid"
    assert _transformation(pipeline, "schedule_timezone_defaulted") is None


def test_default_args_start_date_sets_the_schedule_timezone(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, ", default_args={'start_date': pendulum.datetime(2024, 1, 1, tz='America/New_York')}")

    assert pipeline.schedule["timezone_id"] == "America/New_York"
    assert _transformation(pipeline, "schedule_timezone_defaulted") is None


def test_verified_job_keeps_its_schedule_active(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, ", start_date=pendulum.datetime(2024, 1, 1, tz='UTC')")

    assert pipeline.reconciliation_status == "verified"
    assert pipeline.schedule["pause_status"] == "UNPAUSED"


def test_job_with_a_placeholder_task_is_scheduled_paused(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "", task="BashOperator(task_id='t', bash_command='echo hi', cwd='/opt/work')")

    assert pipeline.reconciliation_status == "verified_with_gaps"
    assert pipeline.schedule["pause_status"] == "PAUSED"
    assert _transformation(pipeline, "schedule_paused_until_migration_completes") is not None


def test_job_with_a_dag_setting_gap_is_scheduled_paused(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, ", default_args=SHARED_DEFAULTS")

    assert pipeline.schedule["pause_status"] == "PAUSED"


@pytest.mark.parametrize(
    "dag_id",
    [
        "os.path.basename(__file__).replace('.py', '')",
        "os.path.splitext(os.path.basename(__file__))[0]",
        "Path(__file__).stem",
    ],
)
def test_dag_id_built_from_the_file_name_names_the_job(tmp_path: Path, dag_id: str) -> None:
    dag_path = tmp_path / "udp_dim_aircraft.py"
    dag_path.write_text(
        "import os\n"
        "from pathlib import Path\n"
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        f"DAG_ID = {dag_id}\n"
        "with DAG(dag_id=DAG_ID) as dag:\n"
        "    t = BashOperator(task_id='t', bash_command='echo hi')\n",
        encoding="utf-8",
    )

    pipeline = load_airflow_dag(dag_path)

    assert pipeline.name == "udp_dim_aircraft"
    assert pipeline.tags["dag_id"] == "udp_dim_aircraft"
    assert _transformation(pipeline, "dag_id_not_static") is None


def test_unresolvable_dag_id_is_disclosed(tmp_path: Path) -> None:
    dag_path = tmp_path / "job.py"
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from settings import PREFIX\n"
        "with DAG(dag_id=f'{PREFIX}_load') as dag:\n"
        "    t = BashOperator(task_id='t', bash_command='echo hi')\n",
        encoding="utf-8",
    )

    pipeline = load_airflow_dag(dag_path)

    assert pipeline.tags["dag_id"] == "job"
    disclosure = _transformation(pipeline, "dag_id_not_static")
    assert disclosure is not None
    assert disclosure["expression"] == "f'{PREFIX}_load'"


def test_max_active_runs_sets_the_job_concurrency(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, ", max_active_runs=3, start_date=pendulum.datetime(2024, 1, 1, tz='UTC')")

    assert pipeline.max_concurrent_runs == 3
    assert pipeline.reconciliation_status == "verified"
    assert _transformation(pipeline, "max_concurrent_runs_defaulted") is None


def test_dynamic_max_active_runs_is_a_gap(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, ", max_active_runs=RUNS")

    assert pipeline.max_concurrent_runs is None
    assert any(item["details"].get("name") == "max_active_runs" for item in pipeline.not_translatable)


def test_unset_max_active_runs_on_a_scheduled_dag_is_disclosed(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "")

    assert pipeline.max_concurrent_runs is None
    disclosure = _transformation(pipeline, "max_concurrent_runs_defaulted")
    assert disclosure is not None
    assert "16 by default" in disclosure["message"]


def test_max_concurrent_runs_reaches_the_bundle(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, ", max_active_runs=2, start_date=pendulum.datetime(2024, 1, 1, tz='UTC')")
    rehydrated, _ = pipeline_dict_to_ir(pipeline_to_dict(pipeline))
    out = tmp_path / "bundle"

    write_bundle(prepare_workflow(rehydrated), out, catalog="main", schema="a")

    job = next(iter(yaml.safe_load(next((out / "resources").glob("*.yml")).read_text())["resources"]["jobs"].values()))
    assert job["max_concurrent_runs"] == 2


def test_path_built_from_dunder_file_is_not_substituted_into_a_task(tmp_path: Path) -> None:
    dag_path = tmp_path / "job.py"
    dag_path.write_text(
        "import os\n"
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "BASE = os.path.dirname(os.path.abspath(__file__))\n"
        "with DAG(dag_id='d') as dag:\n"
        "    t = BashOperator(task_id='t', bash_command=f'python3 {BASE}/scripts/load.py')\n",
        encoding="utf-8",
    )

    task = next(task for task in load_airflow_dag(dag_path).tasks if task.task_key == "t")

    assert isinstance(task, PlaceholderActivity)
    assert str(tmp_path) not in (getattr(task, "generated_source", "") or "")
