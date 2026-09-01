"""Regression: job parameters must not be duplicated through the CLI report path."""

from __future__ import annotations

import pytest

from flowx.bundler.dab_writer import _build_job_resource, _pipeline_dict_to_workflow
from flowx.ir_serde import pipeline_to_dict
from flowx.models.ir import Pipeline, WaitActivity


def _report(default="us"):
    return {
        "name": "pipeline_simple",
        "parameters": [{"name": "region", "type": "String", "default": default}],
        "tasks": [
            {
                "name": "Ingest Bronze",
                "type": "NotebookActivity",
                "task_key": "ingest_bronze",
                "notebook_path": "/Shared/ETL/01_ingest_bronze",
                "base_parameters": {"region": "@pipeline().parameters.region"},
            },
        ],
    }


def test_report_path_does_not_duplicate_parameters():
    wf = _pipeline_dict_to_workflow(_report())
    names = [p.get("name") for p in wf.parameters]
    assert names == ["region"], f"expected one region parameter, got {names}"


def test_build_job_resource_dedupes_parameters():
    wf = _pipeline_dict_to_workflow(_report())
    # even if a caller double-added, the emitted job declares region once
    wf.parameters = wf.parameters + wf.parameters
    job = _build_job_resource(wf, "pipeline_simple")["resources"]["jobs"]["pipeline_simple"]
    names = [p["name"] for p in job["parameters"]]
    assert names == ["region"]


def test_airflow_job_policy_survives_report_round_trip():
    pipeline = Pipeline(
        name="airflow_policy",
        tasks=[WaitActivity(name="Pause", task_key="pause", wait_time_seconds=1)],
        tags={"source": "airflow"},
        timeout_seconds=900,
        email_notifications={"on_failure": ["alerts@example.com"]},
    )

    workflow = _pipeline_dict_to_workflow(pipeline_to_dict(pipeline))
    job = _build_job_resource(workflow, "airflow_policy")["resources"]["jobs"]["airflow_policy"]

    assert job["timeout_seconds"] == 900
    assert job["email_notifications"] == {"on_failure": ["alerts@example.com"]}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("timeout_seconds", "900"),
        ("email_notifications", {"on_failure": "alerts@example.com"}),
        ("email_notifications", {"task_key": ["alerts@example.com"]}),
    ],
)
def test_airflow_job_policy_report_rejects_malformed_values(field, value):
    report = _report()
    report[field] = value

    with pytest.raises(ValueError):
        _pipeline_dict_to_workflow(report)
