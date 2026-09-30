"""Spark preparers never reach the network for remote artifacts when downloads are off or impossible."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from flowx.adapter.operations import collect_workspace_artifact_paths
from flowx.models.ir import SparkJarActivity, SparkPythonActivity
from flowx.preparer import workspace_downloader
from flowx.preparer.activity_preparers import spark_jar, spark_python


@pytest.fixture
def forbid_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail():
        raise AssertionError("a Spark preparer attempted a workspace download")

    monkeypatch.setattr(workspace_downloader, "_get_workspace_client", fail)


@pytest.mark.parametrize("downloads_enabled", [False, True])
def test_gcs_python_file_becomes_a_placeholder_without_a_download(
    forbid_network: None, monkeypatch: pytest.MonkeyPatch, downloads_enabled: bool
) -> None:
    monkeypatch.setattr(workspace_downloader, "_downloads_enabled", downloads_enabled)
    activity = SparkPythonActivity(name="etl", task_key="etl", python_file="gs://bucket/jobs/transform.py")

    prepared = spark_python.prepare(activity)

    assert prepared.task["spark_python_task"]["python_file"] == "../src/scripts/transform.py"
    script = prepared.notebooks[0].content
    assert "raise NotImplementedError" in script
    assert "gs://bucket/jobs/transform.py" in script


@pytest.mark.parametrize("downloads_enabled", [False, True])
def test_gcs_jar_becomes_a_placeholder_without_a_download(
    forbid_network: None, monkeypatch: pytest.MonkeyPatch, downloads_enabled: bool
) -> None:
    monkeypatch.setattr(workspace_downloader, "_downloads_enabled", downloads_enabled)
    activity = SparkJarActivity(
        name="aggregate",
        task_key="aggregate",
        main_class_name="com.example.Aggregate",
        libraries=[{"jar": "gs://bucket/jars/aggregate.jar"}],
    )

    prepared = spark_jar.prepare(activity)

    assert prepared.task["libraries"] == [{"jar": "../lib/aggregate.jar"}]
    assert not any(notebook.binary_content for notebook in prepared.notebooks)


def test_disabled_downloads_skip_dbfs_artifacts(forbid_network: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(workspace_downloader, "_downloads_enabled", False)
    activity = SparkPythonActivity(name="etl", task_key="etl", python_file="dbfs:/scripts/transform.py")

    prepared = spark_python.prepare(activity)

    assert "raise NotImplementedError" in prepared.notebooks[0].content


def test_multi_pipeline_reports_expose_dbfs_artifacts_for_download(tmp_path: Path) -> None:
    report = {
        "pipelines": [
            {
                "name": "dag_a",
                "tasks": [
                    {"type": "SparkPythonActivity", "python_file": "dbfs:/scripts/a.py"},
                    {"type": "SparkPythonActivity", "python_file": "gs://bucket/b.py"},
                    {"type": "SparkJarActivity", "libraries": [{"jar": "dbfs:/jars/c.jar"}]},
                ],
            }
        ]
    }
    report_path = tmp_path / "translation_report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    assert collect_workspace_artifact_paths(report_path) == ["dbfs:/scripts/a.py", "dbfs:/jars/c.jar"]
