"""Regression: job parameters must not be duplicated through the CLI report path."""

from __future__ import annotations

from flowx.bundler.dab_writer import _build_job_resource, _pipeline_dict_to_workflow


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
