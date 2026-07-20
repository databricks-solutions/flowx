"""Tests that the package phase runs bundle invariants (Tier-0) over its output."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import yaml

from flowx.bundler.dab_writer import main as package_main


def _run_package(report: dict) -> int:
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        work = out / ".work"
        work.mkdir(parents=True)
        (work / "translation_report.json").write_text(json.dumps(report), encoding="utf-8")
        return package_main(["--output-dir", str(out)])


def _notebook_task(name: str, task_key: str) -> dict:
    return {
        "name": name,
        "task_key": task_key,
        "type": "NotebookActivity",
        "notebook_path": f"notebooks/{name}.py",
        "generated_source": "# Databricks notebook source\nprint('x')\n",
    }


def test_package_passes_invariants_for_clean_bundle():
    report = {"name": "clean", "tasks": [_notebook_task("a", "a"), _notebook_task("b", "b")]}
    assert _run_package(report) == 0


def test_package_fails_on_duplicate_task_key():
    # Two tasks sharing a task_key -> duplicate_task_key violation -> non-zero exit.
    report = {"name": "bad", "tasks": [_notebook_task("a", "dup"), _notebook_task("b", "dup")]}
    assert _run_package(report) == 1


def test_package_loads_multi_pipeline_report():
    # A {"pipelines": [...]} report (emitted for multi-DAG conversion) must package all pipelines,
    # not silently produce "no pipelines found". Guards the P0 multi-DAG load crash.
    from flowx.bundler.dab_writer import _load_report

    report = {
        "pipelines": [
            {"name": "first", "tasks": [_notebook_task("x", "x")]},
            {"name": "second", "tasks": [_notebook_task("y", "y")]},
        ]
    }
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        work = out / ".work"
        work.mkdir(parents=True)
        report_path = work / "translation_report.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        workflows = _load_report(report_path)
        assert [w.name for w in workflows] == ["first", "second"]
        assert package_main(["--output-dir", str(out)]) == 0


def test_package_writes_airflow_dags_as_jobs_in_one_shared_bundle():
    report = {
        "pipelines": [
            {
                "name": "parent",
                "tags": {"source": "airflow"},
                "tasks": [
                    _notebook_task("extract", "extract"),
                    {
                        "name": "trigger_child",
                        "task_key": "trigger_child",
                        "type": "RunJobActivity",
                        "job_name": "child",
                    },
                ],
            },
            {
                "name": "child",
                "tags": {"source": "airflow"},
                "tasks": [_notebook_task("extract", "extract")],
            },
        ]
    }
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        work = out / ".work"
        work.mkdir(parents=True)
        (work / "translation_report.json").write_text(json.dumps(report), encoding="utf-8")

        assert package_main(["--output-dir", str(out), "--bundle-name", "airflow-suite"]) == 0
        assert (out / "databricks.yml").exists()
        assert {path.name for path in (out / "resources").glob("*.yml")} == {"parent.yml", "child.yml"}
        assert not (out / "parent" / "databricks.yml").exists()
        assert not (out / "child" / "databricks.yml").exists()
        assert (out / "src" / "parent" / "notebooks" / "extract.py").exists()
        assert (out / "src" / "child" / "notebooks" / "extract.py").exists()

        parent_resource = (out / "resources" / "parent.yml").read_text(encoding="utf-8")
        assert "${resources.jobs.child.id}" in parent_resource


def test_shared_bundle_cross_dag_ref_resolves_for_hyphenated_dag_id():
    # A TriggerDagRunOperator targeting a hyphenated/mixed-case dag_id must reference the target
    # job by its normalized resource key, not a differently-sanitized name, or the ref dangles.
    report = {
        "pipelines": [
            {
                "name": "downstream",
                "tags": {"source": "airflow"},
                "tasks": [
                    {
                        "name": "trig",
                        "task_key": "trig",
                        "type": "RunJobActivity",
                        "job_name": "upstream_dag",  # normalize_task_key("Upstream-DAG")
                    },
                ],
            },
            {
                "name": "Upstream-DAG",
                "tags": {"source": "airflow"},
                "tasks": [_notebook_task("a", "a")],
            },
        ]
    }
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        work = out / ".work"
        work.mkdir(parents=True)
        (work / "translation_report.json").write_text(json.dumps(report), encoding="utf-8")

        assert package_main(["--output-dir", str(out), "--bundle-name", "airflow-suite"]) == 0
        job_files = {path.name for path in (out / "resources").glob("*.yml")}
        assert "upstream_dag.yml" in job_files
        upstream = yaml.safe_load((out / "resources" / "upstream_dag.yml").read_text())
        assert "upstream_dag" in upstream["resources"]["jobs"]
        downstream = (out / "resources" / "downstream.yml").read_text(encoding="utf-8")
        # The ref matches the emitted job resource key (would be ${...Upstream-DAG.id} before the fix).
        assert "${resources.jobs.upstream_dag.id}" in downstream


def test_shared_airflow_bundle_namespaces_pydabs_hooks_and_jobs():
    dbt_task = {
        "name": "dbt",
        "task_key": "dbt",
        "type": "DbtFactoryActivity",
        "project_dir": ".",
        "manifest_path": "target/manifest.json",
        "render_mode": "pydabs",
        "resource_types": ["model"],
    }
    report = {
        "pipelines": [
            {"name": "first", "tags": {"source": "airflow"}, "tasks": [dbt_task]},
            {"name": "second", "tags": {"source": "airflow"}, "tasks": [dbt_task]},
        ]
    }
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        project = out / "dbt-project"
        profiles = out / "dbt-profiles"
        (project / "target").mkdir(parents=True)
        profiles.mkdir()
        (project / "dbt_project.yml").write_text("name: demo\nprofile: demo\n")
        (project / "target" / "manifest.json").write_text(json.dumps({"nodes": {}}))
        (profiles / "profiles.yml").write_text("demo:\n  target: dev\n  outputs: {}\n")
        dbt_task["project_dir"] = str(project)
        dbt_task["profiles_dir"] = str(profiles)
        dbt_task["manifest_path"] = str(project / "target" / "manifest.json")
        work = out / ".work"
        work.mkdir(parents=True)
        (work / "translation_report.json").write_text(json.dumps(report), encoding="utf-8")

        assert package_main(["--output-dir", str(out)]) == 0
        databricks_yml = (out / "databricks.yml").read_text(encoding="utf-8")
        assert "resources.first_dbt_dbt_job:load_resources" in databricks_yml
        assert "resources.second_dbt_dbt_job:load_resources" in databricks_yml
        assert (out / "resources" / "first_dbt_dbt_job.py").exists()
        assert (out / "resources" / "second_dbt_dbt_job.py").exists()
        assert "${resources.jobs.first_dbt_dbt.id}" in (out / "resources" / "first.yml").read_text()
        assert "${resources.jobs.second_dbt_dbt.id}" in (out / "resources" / "second.yml").read_text()
