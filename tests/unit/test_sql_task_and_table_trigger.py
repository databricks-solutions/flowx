"""Unit tests for SqlActivity -> sql_task rendering and table_update triggers."""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml

from flowx.bundler.dab_writer import pipeline_dict_to_ir, write_bundle
from flowx.ir_serde import pipeline_to_dict
from flowx.models.ir import NotebookActivity, Pipeline, SqlActivity
from flowx.preparer.workflow_preparer import prepare_workflow


def _bundle(pipeline: Pipeline) -> tuple[dict, Path]:
    wf = prepare_workflow(pipeline)
    out = Path(tempfile.mkdtemp(prefix="sqltest_")).resolve()
    write_bundle(wf, out, catalog="main", schema="a")
    job = yaml.safe_load(next((out / "resources").glob("*.yml")).read_text())
    return job, out


def test_sql_activity_renders_sql_task_with_extracted_file():
    pipeline = Pipeline(
        name="p",
        tasks=[
            SqlActivity(
                name="rep",
                task_key="rep",
                sql="SELECT 1",
                parameters={"run_date": "{{job.parameters.run_date}}"},
            )
        ],
    )
    job, out = _bundle(pipeline)
    task = list(job["resources"]["jobs"].values())[0]["tasks"][0]
    assert task["sql_task"]["warehouse_id"] == "${var.warehouse_id}"
    assert task["sql_task"]["file"]["path"] == "../src/sql/rep.sql"
    assert task["sql_task"]["parameters"] == {"run_date": "{{job.parameters.run_date}}"}
    assert (out / "src" / "sql" / "rep.sql").read_text().strip() == "SELECT 1"


def test_sql_task_declares_warehouse_id_variable():
    pipeline = Pipeline(name="p", tasks=[SqlActivity(name="rep", task_key="rep", sql="SELECT 1")])
    _, out = _bundle(pipeline)
    dby = yaml.safe_load((out / "databricks.yml").read_text())
    assert "warehouse_id" in dby["variables"]


def test_sql_activity_round_trips_through_report():
    pipeline = Pipeline(
        name="p", tasks=[SqlActivity(name="rep", task_key="rep", sql="SELECT 1", parameters={"d": "x"})]
    )
    rehydrated, _ = pipeline_dict_to_ir(pipeline_to_dict(pipeline))
    task = rehydrated.tasks[0]
    assert isinstance(task, SqlActivity)
    assert task.sql == "SELECT 1"
    assert task.parameters == {"d": "x"}


def test_table_update_trigger_renders_on_job():
    pipeline = Pipeline(
        name="p",
        tasks=[NotebookActivity(name="go", task_key="go", notebook_path="notebooks/go.py", generated_source="x")],
        schedule={
            "kind": "table_update",
            "table_names": ["main.silver.events"],
            "condition": "ANY_UPDATED",
            "min_time_between_triggers_seconds": 300,
            "pause_status": "UNPAUSED",
        },
    )
    job, _ = _bundle(pipeline)
    jd = list(job["resources"]["jobs"].values())[0]
    assert jd["trigger"]["table_update"]["table_names"] == ["main.silver.events"]
    assert jd["trigger"]["table_update"]["condition"] == "ANY_UPDATED"
    assert jd["trigger"]["table_update"]["min_time_between_triggers_seconds"] == 300
    assert "schedule" not in jd
