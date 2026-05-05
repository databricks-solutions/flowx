"""Unit tests for the DAB bundle writer."""

from __future__ import annotations

import yaml

from flowx.bundler.dab_writer import write_bundle
from flowx.models.dab import SecretInstruction, SetupTask
from flowx.models.ir import (
    CopyActivity,
    NotebookActivity,
    Pipeline,
    WaitActivity,
)
from flowx.preparer.workflow_preparer import PreparedWorkflow, prepare_workflow

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _simple_workflow(name: str = "test_workflow") -> PreparedWorkflow:
    """Build a minimal PreparedWorkflow with a couple of tasks."""
    pipeline = Pipeline(
        name=name,
        tasks=[
            NotebookActivity(
                name="Run NB",
                task_key="run_nb",
                notebook_path="/Shared/ETL/transform",
                base_parameters={"env": "dev"},
            ),
            WaitActivity(
                name="Pause",
                task_key="pause",
                wait_time_seconds=10,
            ),
            CopyActivity(
                name="Copy Data",
                task_key="copy_data",
                source_type="BlobSource",
                sink_type="DeltaSink",
            ),
        ],
    )
    return prepare_workflow(pipeline)


def _workflow_with_secrets(name: str = "secret_workflow") -> PreparedWorkflow:
    """Build a workflow that includes secret instructions and setup tasks."""
    pipeline = Pipeline(
        name=name,
        tasks=[
            CopyActivity(
                name="Copy SQL",
                task_key="copy_sql",
                source_type="AzureSqlSource",
                sink_type="DeltaSink",
            ),
        ],
    )
    wf = prepare_workflow(pipeline)
    # Ensure the copy preparer added secrets
    return wf


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestWriteBundle:
    def test_databricks_yml_exists(self, tmp_path):
        """write_bundle creates a databricks.yml file."""
        wf = _simple_workflow()
        write_bundle(wf, tmp_path)
        assert (tmp_path / "databricks.yml").exists()

    def test_databricks_yml_structure(self, tmp_path):
        """databricks.yml has the expected top-level keys."""
        wf = _simple_workflow("my_pipeline")
        write_bundle(wf, tmp_path, catalog="my_catalog", schema="my_schema")
        content = yaml.safe_load((tmp_path / "databricks.yml").read_text())
        assert "bundle" in content
        assert content["bundle"]["name"] == "my_pipeline"
        assert "variables" in content
        assert content["variables"]["catalog"]["default"] == "my_catalog"
        assert content["variables"]["schema"]["default"] == "my_schema"
        assert "include" in content
        assert "resources/*.yml" in content["include"]
        assert "targets" in content
        assert "dev" in content["targets"]
        assert "prod" in content["targets"]

    def test_job_resource_yml_exists(self, tmp_path):
        """A job resource YAML is created under resources/."""
        wf = _simple_workflow("my_job")
        write_bundle(wf, tmp_path)
        resource_files = list((tmp_path / "resources").glob("*.yml"))
        assert len(resource_files) >= 1

    def test_job_resource_yml_structure(self, tmp_path):
        """Job resource YAML has correct resources.jobs structure."""
        wf = _simple_workflow("my_job")
        write_bundle(wf, tmp_path)
        resource_file = list((tmp_path / "resources").glob("*.yml"))[0]
        content = yaml.safe_load(resource_file.read_text())
        assert "resources" in content
        assert "jobs" in content["resources"]
        # There should be exactly one job
        jobs = content["resources"]["jobs"]
        assert len(jobs) == 1
        job_key = list(jobs.keys())[0]
        job = jobs[job_key]
        assert "name" in job
        assert "tasks" in job
        assert len(job["tasks"]) == 3  # NB + Wait + Copy

    def test_job_resource_task_keys_unique(self, tmp_path):
        """All task keys within the job resource are unique."""
        wf = _simple_workflow()
        write_bundle(wf, tmp_path)
        resource_file = list((tmp_path / "resources").glob("*.yml"))[0]
        content = yaml.safe_load(resource_file.read_text())
        job = list(content["resources"]["jobs"].values())[0]
        task_keys = [t["task_key"] for t in job["tasks"]]
        assert len(task_keys) == len(set(task_keys))

    def test_notebooks_written(self, tmp_path):
        """Generated notebooks are written to src/notebooks/."""
        wf = _simple_workflow()
        write_bundle(wf, tmp_path)
        notebooks_dir = tmp_path / "src" / "notebooks"
        assert notebooks_dir.exists()
        notebook_files = list(notebooks_dir.glob("*.py"))
        assert len(notebook_files) >= 1

    def test_notebook_content_not_empty(self, tmp_path):
        """Every generated notebook has non-empty content."""
        wf = _simple_workflow()
        write_bundle(wf, tmp_path)
        for nb_file in (tmp_path / "src" / "notebooks").glob("*.py"):
            content = nb_file.read_text()
            assert len(content) > 0, f"Notebook {nb_file.name} is empty"

    def test_setup_notebooks_for_secrets(self, tmp_path):
        """Setup notebooks are created when secrets are present."""
        wf = _workflow_with_secrets()
        write_bundle(wf, tmp_path)
        setup_dir = tmp_path / "src" / "setup"
        if wf.secrets:
            assert setup_dir.exists()
            setup_files = list(setup_dir.glob("*.py"))
            assert len(setup_files) >= 1
            # Check content mentions secret scope
            secrets_nb = setup_dir / "create_secrets.py"
            if secrets_nb.exists():
                content = secrets_nb.read_text()
                assert "createScope" in content

    def test_write_bundle_returns_created_files(self, tmp_path):
        """write_bundle returns a list of all created file paths."""
        wf = _simple_workflow()
        created = write_bundle(wf, tmp_path)
        assert isinstance(created, list)
        assert len(created) > 0
        for path in created:
            assert path.exists(), f"Created file {path} does not exist"

    def test_bundle_name_override(self, tmp_path):
        """Custom bundle name overrides the workflow name."""
        wf = _simple_workflow("original_name")
        write_bundle(wf, tmp_path, bundle_name="custom_bundle")
        content = yaml.safe_load((tmp_path / "databricks.yml").read_text())
        assert content["bundle"]["name"] == "custom_bundle"

    def test_yaml_is_parseable(self, tmp_path):
        """All generated YAML files are valid YAML."""
        wf = _simple_workflow()
        write_bundle(wf, tmp_path)
        for yml_file in tmp_path.rglob("*.yml"):
            content = yaml.safe_load(yml_file.read_text())
            assert content is not None, f"YAML file {yml_file} parsed as None"

    def test_module_state_does_not_leak_across_calls(self, tmp_path):
        """Successive write_bundle calls don't accumulate warnings or cross-bundle vars.

        Regression: ``_bundle_warnings`` and ``_cross_bundle_variables`` used to
        be cleared only by ``main()`` (the CLI), so library callers iterating
        over multiple workflows leaked state from one bundle into the next.
        """
        from flowx.bundler.dab_writer import _bundle_warnings, _cross_bundle_variables

        first_dir = tmp_path / "first"
        second_dir = tmp_path / "second"
        first_dir.mkdir()
        second_dir.mkdir()

        _bundle_warnings.append("- **stale_task**: stale warning from a prior bundle")
        _cross_bundle_variables["stale_var"] = "stale-pipeline"

        write_bundle(_simple_workflow("first"), first_dir)
        write_bundle(_simple_workflow("second"), second_dir)

        first_yml = yaml.safe_load((first_dir / "databricks.yml").read_text())
        second_yml = yaml.safe_load((second_dir / "databricks.yml").read_text())
        assert "stale_var" not in (first_yml.get("variables") or {})
        assert "stale_var" not in (second_yml.get("variables") or {})
        # No WARNINGS.md should appear when the workflow itself produces no warnings.
        assert not (first_dir / "WARNINGS.md").exists()
        assert not (second_dir / "WARNINGS.md").exists()

    def test_load_report_handles_aggregated_translations_format(self, tmp_path):
        """``_load_report`` accepts the multi-pipeline aggregated report.

        Regression: an earlier collapse refactor left this branch calling a
        deleted ``_placeholder_notebook`` helper, so any user passing the
        documented ``translation_report.json`` aggregated format would have
        hit ``NameError`` the first time a notebook task was emitted.
        """
        import json

        from flowx.bundler.dab_writer import _load_report

        report = {
            "translations": [
                {
                    "pipeline": "agg_pipeline",
                    "status": "translated",
                    "ir": {
                        "type": "WaitActivity",
                        "name": "Pause",
                        "task_key": "pause",
                        "wait_time_seconds": 5,
                    },
                },
                {
                    "pipeline": "agg_pipeline",
                    "status": "translated",
                    "ir": {
                        "type": "NotebookActivity",
                        "name": "Run NB",
                        "task_key": "run_nb",
                        "notebook_path": "/Shared/etl/run",
                    },
                },
                {
                    "pipeline": "agg_pipeline",
                    "status": "skipped",
                    "ir": {},
                },
            ]
        }
        report_path = tmp_path / "translation_report.json"
        report_path.write_text(json.dumps(report))

        workflows = _load_report(report_path)
        assert len(workflows) == 1
        assert workflows[0].name == "agg_pipeline"
        task_keys = {task["task_key"] for task in workflows[0].tasks}
        assert task_keys == {"pause", "run_nb"}


class TestSetupGenerator:
    def test_secrets_setup_notebook_content(self):
        from flowx.bundler.setup_generator import generate_setup_tasks

        secrets = [
            SecretInstruction(scope="my-scope", key="jdbc-url", value_source="JDBC URL for source"),
            SecretInstruction(scope="my-scope", key="jdbc-password", value_source="JDBC password for source"),
        ]
        notebooks = generate_setup_tasks(secrets=secrets, setup_tasks=[], catalog="main", schema="default")
        assert len(notebooks) == 1
        nb = notebooks[0]
        assert nb.relative_path == "setup/create_secrets.py"
        assert "createScope" in nb.content
        assert "my-scope" in nb.content
        assert "jdbc-url" in nb.content
        assert "jdbc-password" in nb.content

    def test_volume_setup_notebook(self):
        from flowx.bundler.setup_generator import generate_setup_tasks

        setup_tasks = [
            SetupTask(type="volume", config={"volume_name": "raw_data", "volume_type": "MANAGED"}),
        ]
        notebooks = generate_setup_tasks(secrets=[], setup_tasks=setup_tasks, catalog="prod", schema="ingest")
        assert len(notebooks) == 1
        nb = notebooks[0]
        assert nb.relative_path == "setup/create_volumes.py"
        assert "prod.ingest.raw_data" in nb.content

    def test_connection_setup_notebook(self):
        from flowx.bundler.setup_generator import generate_setup_tasks

        setup_tasks = [
            SetupTask(
                type="connection",
                config={"connection_name": "sql_conn", "connection_type": "SQLSERVER", "host": "sql.example.com"},
            ),
        ]
        notebooks = generate_setup_tasks(secrets=[], setup_tasks=setup_tasks, catalog="main", schema="default")
        assert len(notebooks) == 1
        nb = notebooks[0]
        assert nb.relative_path == "setup/create_connections.py"
        assert "sql_conn" in nb.content

    def test_no_setup_when_empty(self):
        from flowx.bundler.setup_generator import generate_setup_tasks

        notebooks = generate_setup_tasks(secrets=[], setup_tasks=[], catalog="main", schema="default")
        assert len(notebooks) == 0
