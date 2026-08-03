"""Golden-bundle test for the Airflow source.

Converts a single representative DAG (tests/resources/airflow/golden_pipeline_dag.py) all the
way to a DAB bundle on disk and pins the emitted job YAML + notebooks. It guards these
conversion behaviours together, end-to-end:

  - cron schedule + a root file sensor -> schedule kept AND sensor retained as a polling task
    (schedule / file_arrival triggers are mutually exclusive on a Databricks job)
  - a mid-DAG table sensor -> polling task, never a trigger
  - trigger_rule -> DAB run_if constants (ALL_DONE, AT_LEAST_ONE_FAILED, ...)
  - params={...} -> job-parameter defaults; {{ params.x }} -> {{job.parameters.x}}
  - Unix cron day-of-week -> Quartz (Mon: 1 -> 2)
  - >> chains and set_upstream() dependency forms
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
import yaml

from flowx.bundler.dab_writer import write_bundle
from flowx.preparer.workflow_preparer import prepare_workflow
from flowx.sources.airflow.loader import load_airflow_dag
from flowx.validate.bundle_invariants import check_bundle_dir

_DAG = Path(__file__).parent.parent / "resources" / "airflow" / "golden_pipeline_dag.py"


@pytest.fixture(scope="module")
def bundle_dir(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("golden_bundle")
    workflow = prepare_workflow(load_airflow_dag(_DAG))
    write_bundle(workflow, out)
    return out


@pytest.fixture(scope="module")
def job_def(bundle_dir: Path) -> dict:
    doc = yaml.safe_load((bundle_dir / "resources" / "golden_pipeline.yml").read_text())
    return doc["resources"]["jobs"]["golden_pipeline"]


def _task(job_def: dict, key: str) -> dict:
    return next(t for t in job_def["tasks"] if t["task_key"] == key)


def test_all_expected_tasks_present(job_def: dict):
    # Both sensors are retained as tasks (not dropped, not lifted to triggers).
    keys = {t["task_key"] for t in job_def["tasks"]}
    assert keys == {
        "wait_landing",
        "ingest_orders",
        "wait_partition",
        "publish_metrics",
        "cleanup",
        "alert_on_failure",
    }


def test_cron_schedule_kept_with_quartz_weekday_shift(job_def: dict):
    # cron survives the presence of the root sensor; Unix DOW 1 (Mon) -> Quartz 2.
    schedule = job_def["schedule"]
    assert schedule["quartz_cron_expression"] == "0 0 6 ? * 2"
    assert schedule["timezone_id"] == "UTC"
    # No mutually-exclusive job trigger was emitted alongside the schedule.
    assert "trigger" not in job_def


def test_root_file_sensor_is_polling_task(bundle_dir: Path):
    src = (bundle_dir / "src" / "notebooks" / "wait_landing.py").read_text()
    assert "dbutils.fs.ls" in src
    assert "s3://acme-orders/landing/" in src
    assert "POKE_INTERVAL = 120" in src
    assert "TIMEOUT = 3600" in src
    ast.parse(src)


def test_mid_dag_table_sensor_is_polling_task(bundle_dir: Path):
    src = (bundle_dir / "src" / "notebooks" / "wait_partition.py").read_text()
    # The table name is emitted through repr() so a name containing a quote can't break the source.
    assert f"spark.catalog.tableExists({'main.analytics.raw_orders'!r})" in src
    assert "POKE_INTERVAL = 60" in src
    ast.parse(src)


def test_trigger_rules_map_to_run_if(job_def: dict):
    assert _task(job_def, "cleanup")["run_if"] == "ALL_DONE"
    assert _task(job_def, "alert_on_failure")["run_if"] == "AT_LEAST_ONE_FAILED"
    # Default all_success tasks carry no run_if key.
    assert "run_if" not in _task(job_def, "ingest_orders")


def test_dependencies_from_both_shift_and_set_upstream(job_def: dict):
    # >> chain
    assert [d["task_key"] for d in _task(job_def, "ingest_orders")["depends_on"]] == ["wait_landing"]
    assert [d["task_key"] for d in _task(job_def, "wait_partition")["depends_on"]] == ["ingest_orders"]
    # set_upstream() edges
    assert [d["task_key"] for d in _task(job_def, "cleanup")["depends_on"]] == ["publish_metrics"]
    assert [d["task_key"] for d in _task(job_def, "alert_on_failure")["depends_on"]] == ["publish_metrics"]


def test_job_parameters_carry_defaults(job_def: dict):
    params = {p["name"]: p["default"] for p in job_def["parameters"]}
    assert params["target_env"] == "prod"  # from Param("prod")
    assert params["threshold"] == "100"  # Jobs parameter defaults are strings


def test_templated_param_becomes_dab_ref(job_def: dict):
    base = _task(job_def, "ingest_orders")["notebook_task"]["base_parameters"]
    assert base["__flowx_op_kwargs"] == '{"target_env": "{{job.parameters.target_env}}"}'


def test_all_notebooks_are_valid_python(bundle_dir: Path):
    for notebook in (bundle_dir / "src" / "notebooks").glob("*.py"):
        ast.parse(notebook.read_text())


def test_bundle_passes_invariants(bundle_dir: Path):
    result = check_bundle_dir(bundle_dir)
    assert result.ok, "\n".join(f"{f.severity}: {f.message}" for f in result.findings)
