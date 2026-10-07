"""Tests that the package phase runs bundle invariants (Tier-0) over its output."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import yaml

from flowx.bundler.dab_writer import _report_reconciliation_failures
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


def _adf_pipeline(name: str, tasks: list[dict]) -> dict:
    return {
        "name": name,
        "tags": {"source": "adf"},
        "reconciliation_status": None,
        "tasks": tasks,
    }


def _airflow_pipeline(name: str, tasks: list[dict], *, status: str = "verified") -> dict:
    return {
        "name": name,
        "tags": {"source": "airflow"},
        "reconciliation_status": status,
        "audit": {
            "source_file": f"{name}.py",
            "audited_activity_count": len(tasks),
            "transformations": [],
        },
        "tasks": tasks,
    }


def test_package_passes_invariants_for_clean_bundle():
    report = _adf_pipeline("clean", [_notebook_task("a", "a"), _notebook_task("b", "b")])
    assert _run_package(report) == 0


def test_package_fails_on_duplicate_task_key():
    # Two tasks sharing a task_key -> duplicate_task_key violation -> non-zero exit.
    report = _adf_pipeline("bad", [_notebook_task("a", "dup"), _notebook_task("b", "dup")])
    assert _run_package(report) == 1


def test_package_loads_multi_pipeline_report():
    # A {"pipelines": [...]} report (emitted for multi-DAG conversion) must package all pipelines,
    # not silently produce "no pipelines found".
    from flowx.bundler.dab_writer import _load_report

    report = {
        "pipelines": [
            _adf_pipeline("first", [_notebook_task("x", "x")]),
            _adf_pipeline("second", [_notebook_task("y", "y")]),
        ]
    }
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        work = out / ".work"
        work.mkdir(parents=True)
        report_path = work / "translation_report.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        workflows, _ = _load_report(report_path)
        assert [w.name for w in workflows] == ["first", "second"]
        assert package_main(["--output-dir", str(out)]) == 0


def test_package_writes_airflow_dags_as_jobs_in_one_shared_bundle():
    report = {
        "pipelines": [
            _airflow_pipeline(
                "parent",
                [
                    _notebook_task("extract", "extract"),
                    {
                        "name": "trigger_child",
                        "task_key": "trigger_child",
                        "type": "RunJobActivity",
                        "job_name": "child",
                    },
                ],
            ),
            _airflow_pipeline("child", [_notebook_task("extract", "extract")]),
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
            _airflow_pipeline(
                "downstream",
                [
                    {
                        "name": "trig",
                        "task_key": "trig",
                        "type": "RunJobActivity",
                        "job_name": "upstream_dag",  # normalize_task_key("Upstream-DAG")
                    },
                ],
            ),
            _airflow_pipeline("Upstream-DAG", [_notebook_task("a", "a")]),
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
        # The ref must match the emitted job resource key, which is normalize_task_key(dag_id).
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
            _airflow_pipeline("first", [dbt_task]),
            _airflow_pipeline("second", [dbt_task]),
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


def _preflight_failures(tmp_path: Path, report: object) -> list[str]:
    path = tmp_path / "report.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    return _report_reconciliation_failures(path)


def test_report_preflight_rejects_unknown_reconciliation_status(tmp_path: Path):
    report = _airflow_pipeline("typo", [_notebook_task("a", "a")], status="verifed")

    failures = _preflight_failures(tmp_path, report)

    assert any("unknown reconciliation_status" in failure for failure in failures)


def test_report_preflight_rejects_reviewed_resolution_status_without_replay_evidence(tmp_path: Path):
    report = _airflow_pipeline(
        "premature_resolution",
        [_notebook_task("a", "a")],
        status="verified_with_reviewed_resolutions",
    )

    failures = _preflight_failures(tmp_path, report)

    assert any("agentic resolution evidence" in failure for failure in failures)


def test_report_preflight_rejects_excluded_status_for_included_dag(tmp_path: Path):
    report = _airflow_pipeline("false_exclusion", [_notebook_task("a", "a")], status="excluded")

    failures = _preflight_failures(tmp_path, report)

    assert any("requires migration_status 'excluded'" in failure for failure in failures)


def test_report_preflight_accepts_explicitly_excluded_dag(tmp_path: Path):
    report = _airflow_pipeline("excluded", [_notebook_task("a", "a")], status="excluded")
    report["migration_status"] = "excluded"

    assert _preflight_failures(tmp_path, report) == []


def test_report_preflight_rejects_airflow_without_audit_metadata(tmp_path: Path):
    report = _airflow_pipeline("missing_audit", [_notebook_task("a", "a")])
    report.pop("audit")

    failures = _preflight_failures(tmp_path, report)

    assert any("source-audit metadata" in failure for failure in failures)


def test_report_preflight_rejects_top_level_list(tmp_path: Path):
    failures = _preflight_failures(tmp_path, [_adf_pipeline("p", [_notebook_task("a", "a")])])

    assert any("top-level object" in failure for failure in failures)


def test_package_rejects_malformed_report_before_writing_bundle(tmp_path: Path, capsys):
    report_path = tmp_path / "malformed.json"
    report_path.write_text("[]", encoding="utf-8")
    output_dir = tmp_path / "bundle"

    exit_code = package_main(["--report", str(report_path), "--output-dir", str(output_dir)])

    assert exit_code == 1
    assert "translation report preflight failed" in capsys.readouterr().err
    assert not output_dir.exists()


def test_report_preflight_rejects_unrecognized_dictionary(tmp_path: Path):
    failures = _preflight_failures(tmp_path, {"name": "missing_tasks"})

    assert any("recognized report shape" in failure for failure in failures)


def test_report_preflight_rejects_malformed_pipelines_wrapper(tmp_path: Path):
    failures = _preflight_failures(tmp_path, {"pipelines": "not-a-list"})

    assert any("pipelines must be a list" in failure for failure in failures)


def test_report_preflight_accepts_legacy_adf_translations(tmp_path: Path):
    report = {
        "translations": [
            {
                "pipeline": "legacy_adf",
                "status": "translated",
                "ir": _notebook_task("a", "a"),
            }
        ]
    }

    assert _preflight_failures(tmp_path, report) == []


def test_report_preflight_rejects_airflow_claiming_legacy_adf_shape(tmp_path: Path):
    report = {
        "source": "airflow",
        "translations": [
            {
                "pipeline": "not_airflow_contract",
                "status": "translated",
                "ir": _notebook_task("a", "a"),
            }
        ],
    }

    failures = _preflight_failures(tmp_path, report)

    assert any("legacy ADF" in failure for failure in failures)


def test_report_preflight_rejects_invalid_json(tmp_path: Path):
    path = tmp_path / "report.json"
    path.write_text("{not-json", encoding="utf-8")

    failures = _report_reconciliation_failures(path)

    assert any("invalid JSON" in failure for failure in failures)
