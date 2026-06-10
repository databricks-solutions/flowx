"""Unit tests for the DAB bundle writer."""

from __future__ import annotations

import yaml

from orchestra.bundler.dab_writer import write_bundle
from orchestra.models.dab import SecretInstruction, SetupTask
from orchestra.models.ir import (
    CopyActivity,
    NotebookActivity,
    Pipeline,
    WaitActivity,
)
from orchestra.preparer.workflow_preparer import PreparedWorkflow, prepare_workflow

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
                # C-46 (LSC5-002): provision via the SDK WorkspaceClient, not
                # the non-existent dbutils.secrets write API.
                assert "create_scope" in content

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
        from orchestra.bundler.dab_writer import _bundle_warnings, _cross_bundle_variables

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

        from orchestra.bundler.dab_writer import _load_report

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


class TestScheduleEmission:
    """C-10 (SCHED-001): schedule spec on PreparedWorkflow lands in job YAML."""

    def test_schedule_block_emitted(self, tmp_path):
        pipeline = Pipeline(
            name="scheduled_job",
            tasks=[
                WaitActivity(name="Pause", task_key="pause", wait_time_seconds=10),
            ],
            schedule={
                "kind": "schedule",
                "quartz_cron_expression": "0 0 8 * * ?",
                "timezone_id": "Europe/Madrid",
                "pause_status": "UNPAUSED",
            },
        )
        wf = prepare_workflow(pipeline)
        write_bundle(wf, tmp_path)
        resource_file = list((tmp_path / "resources").glob("*.yml"))[0]
        content = yaml.safe_load(resource_file.read_text())
        job_key = list(content["resources"]["jobs"].keys())[0]
        job = content["resources"]["jobs"][job_key]
        assert job["schedule"]["quartz_cron_expression"] == "0 0 8 * * ?"
        assert job["schedule"]["timezone_id"] == "Europe/Madrid"
        assert job["schedule"]["pause_status"] == "UNPAUSED"

    def test_periodic_trigger_emitted(self, tmp_path):
        """SCHED3-002: periodic schedule spec renders as trigger.periodic."""
        pipeline = Pipeline(
            name="periodic_job",
            tasks=[
                WaitActivity(name="Pause", task_key="pause", wait_time_seconds=10),
            ],
            schedule={
                "kind": "periodic",
                "interval": 3,
                "unit": "DAYS",
                "pause_status": "UNPAUSED",
            },
        )
        wf = prepare_workflow(pipeline)
        write_bundle(wf, tmp_path)
        resource_file = list((tmp_path / "resources").glob("*.yml"))[0]
        content = yaml.safe_load(resource_file.read_text())
        job_key = list(content["resources"]["jobs"].keys())[0]
        job = content["resources"]["jobs"][job_key]
        assert job["trigger"]["periodic"]["interval"] == 3
        assert job["trigger"]["periodic"]["unit"] == "DAYS"
        # The cron-style schedule block must NOT appear for periodic specs.
        assert "schedule" not in job

    def test_trigger_parameter_overrides_mutate_job_parameter_defaults(self, tmp_path):
        """SCHED3-003: schedule.parameter_overrides mutates matching
        job.parameters entries' default values."""
        pipeline = Pipeline(
            name="trg_override_job",
            tasks=[
                WaitActivity(name="Pause", task_key="pause", wait_time_seconds=10),
            ],
            schedule={
                "kind": "schedule",
                "quartz_cron_expression": "0 0 2 * * ?",
                "timezone_id": "UTC",
                "pause_status": "UNPAUSED",
                "parameter_overrides": {
                    "negocio": "GLP",
                    "applicationName": "cli0010",
                },
            },
        )
        wf = prepare_workflow(pipeline)
        # Pipeline parameters land on PreparedWorkflow via the report
        # round-trip; emulate that here so the bundler has parameters to
        # mutate.
        wf.parameters = [
            {"name": "negocio", "default": "DEFAULT"},
            {"name": "applicationName", "default": "DEFAULT"},
        ]
        write_bundle(wf, tmp_path)
        resource_file = list((tmp_path / "resources").glob("*.yml"))[0]
        content = yaml.safe_load(resource_file.read_text())
        job_key = list(content["resources"]["jobs"].keys())[0]
        job = content["resources"]["jobs"][job_key]
        params = {p["name"]: p["default"] for p in job["parameters"]}
        assert params["negocio"] == "GLP"
        assert params["applicationName"] == "cli0010"

    def test_file_arrival_trigger_emitted(self, tmp_path):
        pipeline = Pipeline(
            name="blob_triggered_job",
            tasks=[
                WaitActivity(name="Pause", task_key="pause", wait_time_seconds=10),
            ],
            schedule={
                "kind": "file_arrival",
                "url": "/subscriptions/x/y",
                "pause_status": "UNPAUSED",
            },
        )
        wf = prepare_workflow(pipeline)
        write_bundle(wf, tmp_path)
        resource_file = list((tmp_path / "resources").glob("*.yml"))[0]
        content = yaml.safe_load(resource_file.read_text())
        job_key = list(content["resources"]["jobs"].keys())[0]
        job = content["resources"]["jobs"][job_key]
        assert job["trigger"]["file_arrival"]["url"] == "/subscriptions/x/y"


class TestStripDanglingTaskValueRefs:
    """C-12 (VAREX-005): safety net widens to job_parameters and condition operands."""

    def test_strips_dangling_run_job_task_job_parameters(self):
        from orchestra.bundler.dab_writer import _strip_dangling_task_value_refs

        tasks = [
            {
                "task_key": "outer",
                "run_job_task": {
                    "job_id": "${resources.jobs.inner.id}",
                    "job_parameters": {
                        "valid": "{{tasks.outer.values.something}}",
                        "dangling": "{{tasks.gone.values.x}}",
                    },
                },
            },
        ]
        _strip_dangling_task_value_refs(tasks, {"outer"})
        assert tasks[0]["run_job_task"]["job_parameters"]["valid"] == "{{tasks.outer.values.something}}"
        assert tasks[0]["run_job_task"]["job_parameters"]["dangling"] == ""

    def test_strips_dangling_condition_task_operands(self):
        from orchestra.bundler.dab_writer import _strip_dangling_task_value_refs

        tasks = [
            {
                "task_key": "branch",
                "condition_task": {
                    "op": "EQUAL_TO",
                    "left": "{{tasks.missing.values.x}}",
                    "right": "1",
                },
            },
        ]
        neutralized = _strip_dangling_task_value_refs(tasks, {"branch"})
        # Dangling ref blanked; right operand untouched.
        assert tasks[0]["condition_task"]["left"] == ""
        assert tasks[0]["condition_task"]["right"] == "1"
        # C-43 (CF5-001 / CF5-002): the blanked condition operand is
        # recorded so SETUP.md can flag the always-true predicate.
        assert neutralized == [
            {
                "task_key": "branch",
                "field": "left",
                "original_ref": "{{tasks.missing.values.x}}",
            }
        ]

    def test_neutralized_condition_renders_setup_section(self):
        """C-43 (CF5-001 / CF5-002): a blanked condition operand surfaces a
        'Conditions neutralized to always-true' section in SETUP.md so the
        always-true predicate is never silent."""
        from orchestra.bundler.prereqs_writer import build_prereqs, render_setup_md

        prereqs = build_prereqs(
            notebooks=[],
            tasks=[],
            known_bundle_jobs=set(),
            neutralized_conditions=[
                {
                    "task_key": "branch",
                    "field": "left",
                    "original_ref": "{{tasks._init_continue.values.continue}}",
                }
            ],
        )
        assert not prereqs.is_empty()
        md = render_setup_md(prereqs, bundle_name="b")
        assert "Conditions neutralized to always-true" in md
        assert "{{tasks._init_continue.values.continue}}" in md
        assert "`branch`" in md

    def test_recurses_into_for_each_task_body(self):
        from orchestra.bundler.dab_writer import _strip_dangling_task_value_refs

        tasks = [
            {
                "task_key": "loop",
                "for_each_task": {
                    "inputs": "[1, 2, 3]",
                    "task": {
                        "task_key": "loop_body",
                        "run_job_task": {
                            "job_id": "inner",
                            "job_parameters": {"x": "{{tasks.absent.values.x}}"},
                        },
                    },
                },
            },
        ]
        _strip_dangling_task_value_refs(tasks, {"loop", "loop_body"})
        assert tasks[0]["for_each_task"]["task"]["run_job_task"]["job_parameters"]["x"] == ""


class TestAggregatedReportPipelineParameters:
    """Change pipeline-parameters-and-variables-round-trip (P0): VAR-001."""

    def test_load_report_carries_pipeline_parameters(self, tmp_path):
        import json

        from orchestra.bundler.dab_writer import _load_report

        report = {
            "translations": [
                {
                    "pipeline": "p1",
                    "status": "translated",
                    "parameters": [{"name": "env", "default": "dev"}],
                    "ir": {
                        "type": "WaitActivity",
                        "name": "Pause",
                        "task_key": "pause",
                        "wait_time_seconds": 5,
                    },
                },
            ]
        }
        report_path = tmp_path / "translation_report.json"
        report_path.write_text(json.dumps(report))

        workflows = _load_report(report_path)
        assert len(workflows) == 1
        wf = workflows[0]
        # Pipeline-level parameters must survive round-trip.
        assert wf.parameters
        env_param = next(p for p in wf.parameters if p["name"] == "env")
        assert env_param["default"] == "dev"


class TestAggregatedReportSchedule:
    """Change fix-aggregated-report-propagates-schedule (P0): SCHED3-001."""

    def test_load_report_carries_pipeline_schedule(self, tmp_path):
        import json

        from orchestra.bundler.dab_writer import _load_report

        schedule_spec = {
            "kind": "cron",
            "quartz_cron_expression": "0 0 2 ? * * *",
            "timezone_id": "UTC",
            "pause_status": "UNPAUSED",
        }
        report = {
            "translations": [
                {
                    "pipeline": "p_with_schedule",
                    "status": "translated",
                    "schedule": schedule_spec,
                    "ir": {
                        "type": "WaitActivity",
                        "name": "Pause",
                        "task_key": "pause",
                        "wait_time_seconds": 5,
                    },
                },
            ]
        }
        report_path = tmp_path / "translation_report.json"
        report_path.write_text(json.dumps(report))

        workflows = _load_report(report_path)
        assert len(workflows) == 1
        wf = workflows[0]
        assert wf.schedule is not None
        assert wf.schedule["kind"] == "cron"
        assert wf.schedule["quartz_cron_expression"] == "0 0 2 ? * * *"

    def test_load_report_carries_pipeline_schedule_from_ir(self, tmp_path):
        """Older single-pipeline reports nest schedule under ``ir.schedule``."""
        import json

        from orchestra.bundler.dab_writer import _load_report

        schedule_spec = {
            "kind": "cron",
            "quartz_cron_expression": "0 0 4 ? * MON,TUE,WED,THU,FRI *",
            "timezone_id": "Europe/Madrid",
            "pause_status": "UNPAUSED",
        }
        report = {
            "translations": [
                {
                    "pipeline": "p_with_ir_schedule",
                    "status": "translated",
                    "ir": {
                        "type": "WaitActivity",
                        "name": "Pause",
                        "task_key": "pause",
                        "wait_time_seconds": 5,
                        "schedule": schedule_spec,
                    },
                },
            ]
        }
        report_path = tmp_path / "translation_report.json"
        report_path.write_text(json.dumps(report))

        workflows = _load_report(report_path)
        assert len(workflows) == 1
        assert workflows[0].schedule is not None
        assert workflows[0].schedule["quartz_cron_expression"].startswith("0 0 4")


class TestStubBaseParameterCleanup:
    """Change base-parameters-cleanup-and-stub-widgets (P1): NB-5."""

    def test_stub_notebook_strips_unresolvable_adf_expression(self):
        from orchestra.bundler.dab_writer import (
            _extract_manual_parameters_from_existing_notebook_tasks,
        )

        tasks = [
            {
                "task_key": "lakeh_custom_notebook",
                "notebook_task": {
                    "notebook_path": "../src/notebooks/x.py",
                    "base_parameters": {
                        "appName": "@string(coalesce(json(activity('X').output).mail_app_name, ''))",
                        "kept": "literal-value",
                    },
                },
            }
        ]
        manual = _extract_manual_parameters_from_existing_notebook_tasks(tasks)
        assert len(manual) == 1
        assert manual[0].task_key == "lakeh_custom_notebook"
        assert manual[0].widget_name == "appName"
        assert "@string" in manual[0].raw_expression
        # The ADF expression value must be dropped from base_parameters.
        bp = tasks[0]["notebook_task"]["base_parameters"]
        assert "appName" not in bp
        assert bp["kept"] == "literal-value"


class TestStubLibraryBinding:
    """Change library-resolution-and-stub-binding (P0): NB-2."""

    def test_stub_notebook_with_jar_library_binds_to_default_cluster(self):
        from orchestra.bundler.constants import DEFAULT_JOB_CLUSTER_KEY
        from orchestra.bundler.dab_writer import _bind_cluster_to_notebook_tasks

        tasks = [
            {
                "task_key": "lakeh_custom_notebook",
                "notebook_task": {"notebook_path": "../src/notebooks/x.py"},
                "libraries": [{"jar": "/Volumes/x/my.jar"}],
            }
        ]
        _bind_cluster_to_notebook_tasks(tasks)
        # Stub path that ships libraries must bind to the default cluster.
        assert tasks[0]["job_cluster_key"] == DEFAULT_JOB_CLUSTER_KEY

    def test_stub_notebook_no_libraries_stays_unbound(self):
        from orchestra.bundler.dab_writer import _bind_cluster_to_notebook_tasks

        tasks = [
            {
                "task_key": "k",
                "notebook_task": {"notebook_path": "../src/notebooks/x.py"},
            }
        ]
        _bind_cluster_to_notebook_tasks(tasks)
        assert "job_cluster_key" not in tasks[0]

    def test_serverless_compute_mode_with_jar_library_binds_classic(self):
        from orchestra.bundler.constants import DEFAULT_JOB_CLUSTER_KEY
        from orchestra.bundler.dab_writer import _bind_cluster_to_notebook_tasks

        tasks = [
            {
                "task_key": "k",
                "notebook_task": {"notebook_path": "/Shared/nb"},
                "libraries": [{"whl": "/Volumes/x/wheel.whl"}],
                "_compute_mode": "serverless",
            }
        ]
        _bind_cluster_to_notebook_tasks(tasks)
        # Serverless can't host whl libraries -> classic cluster bind.
        assert tasks[0]["job_cluster_key"] == DEFAULT_JOB_CLUSTER_KEY


class TestClusterExtrasPropagation:
    """Change linked-service-cluster-field-coverage (P1): NB-3, LSC-003."""

    def test_extras_merged_into_default_cluster(self, tmp_path):
        from orchestra.bundler.constants import DEFAULT_JOB_CLUSTER_KEY
        from orchestra.bundler.dab_writer import (
            _build_default_cluster,
            _build_default_job_clusters,
            _infer_bundle_cluster_extras,
        )

        # Hint set with consistent extras.
        wf = _simple_workflow()
        wf.cluster_hints = [
            {
                "spark_version": "16.4.x-scala2.12",
                "node_type_id": "Standard_D4s_v3",
                "driver_node_type_id": "Standard_D8s_v3",
                "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
                "custom_tags": {"DigitalCase": "X"},
                "data_security_mode": "SINGLE_USER",
            }
        ]
        extras = _infer_bundle_cluster_extras(wf)
        assert extras["driver_node_type_id"] == "Standard_D8s_v3"
        assert extras["spark_env_vars"]["PYSPARK_PYTHON"] == "/databricks/python3/bin/python3"
        assert extras["custom_tags"]["DigitalCase"] == "X"

        clusters = _build_default_job_clusters({DEFAULT_JOB_CLUSTER_KEY}, extras=extras)
        assert len(clusters) == 1
        new_cluster = clusters[0]["new_cluster"]
        assert new_cluster["driver_node_type_id"] == "Standard_D8s_v3"
        assert new_cluster["spark_env_vars"]["PYSPARK_PYTHON"] == "/databricks/python3/bin/python3"
        assert new_cluster["custom_tags"]["DigitalCase"] == "X"

        # _build_default_cluster() with no extras keeps the legacy shape.
        baseline = _build_default_cluster()
        assert "driver_node_type_id" not in baseline["new_cluster"]

    def test_num_workers_mined_into_default_cluster(self):
        """C-40 (NB-ITER5-001): cluster_hints carrying num_workers!=1 must
        flow into the default job_cluster instead of the hardcoded 1."""
        from orchestra.bundler.constants import DEFAULT_JOB_CLUSTER_KEY
        from orchestra.bundler.dab_writer import (
            _build_default_cluster,
            _build_default_job_clusters,
            _infer_bundle_cluster_extras,
        )

        wf = _simple_workflow()
        wf.cluster_hints = [
            {
                "spark_version": "16.4.x-scala2.12",
                "node_type_id": "Standard_D4s_v3",
                "num_workers": 2,
            }
        ]
        extras = _infer_bundle_cluster_extras(wf)
        assert extras["num_workers"] == 2

        clusters = _build_default_job_clusters({DEFAULT_JOB_CLUSTER_KEY}, extras=extras)
        assert clusters[0]["new_cluster"]["num_workers"] == 2

        # No hint -> legacy single-worker default preserved.
        baseline = _build_default_cluster()
        assert baseline["new_cluster"]["num_workers"] == 1


class TestManualCredentialFromMsiLinkedService:
    """C-39 (LSC4-004): when an ADF linked service authenticates via MSI
    (or a CredentialReference), the bundle's default_cluster silently
    uses ``single_user_name: ${workspace.current_user.userName}``.  The
    workflow_preparer must surface a manual_credential SetupTask so
    SETUP.md flags the substitution."""

    def test_msi_authentication_emits_manual_credential_setup_task(self):
        from orchestra.models.ir import NotebookActivity, Pipeline
        from orchestra.preparer.workflow_preparer import prepare_workflow

        activity = NotebookActivity(
            name="Notebook1",
            task_key="notebook1",
            notebook_path="/Shared/x",
            cluster={
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "Standard_D4s_v3",
                "data_security_mode": "SINGLE_USER",
                "_adf_authentication": "MSI",
            },
        )
        pipeline = Pipeline(name="msi_pipe", tasks=[activity])
        wf = prepare_workflow(pipeline)
        manual = [st for st in wf.setup_tasks if st.type == "manual_credential"]
        assert len(manual) == 1
        config = manual[0].config
        assert config["authentication"] == "MSI"
        assert "service principal" in config["note"].lower()


class TestUnparseableClusterHintsFiltered:
    """C-29 (NB-ITER4-002): unparseable spark_version / node_type_id values
    are filtered before Counter so the bundle default stays deployable."""

    def test_unparseable_spark_version_falls_back_to_default(self):
        from orchestra.bundler.dab_writer import (
            _DEFAULT_SPARK_VERSION,
            _infer_bundle_cluster_defaults,
        )

        wf = _simple_workflow()
        wf.cluster_hints = [
            {
                "spark_version": "@if(equals(item()?.photon,true),'15.4.x-photon-scala2.12','15.4.x-scala2.12')",
                "node_type_id": "Standard_DS3_v2",
            },
        ]
        spark_version, node_type_id = _infer_bundle_cluster_defaults(wf)
        assert spark_version == _DEFAULT_SPARK_VERSION
        assert node_type_id == "Standard_DS3_v2"

    def test_unparseable_node_type_falls_back_to_default(self):
        from orchestra.bundler.dab_writer import (
            _DEFAULT_NODE_TYPE_ID,
            _infer_bundle_cluster_defaults,
        )

        wf = _simple_workflow()
        wf.cluster_hints = [
            {
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "@pipeline().parameters.unresolved",
            },
        ]
        spark_version, node_type_id = _infer_bundle_cluster_defaults(wf)
        assert spark_version == "15.4.x-scala2.12"
        assert node_type_id == _DEFAULT_NODE_TYPE_ID

    def test_real_spark_version_still_wins(self):
        from orchestra.bundler.dab_writer import _infer_bundle_cluster_defaults

        wf = _simple_workflow()
        wf.cluster_hints = [
            {"spark_version": "15.4.x-photon-scala2.12", "node_type_id": "Standard_D4s_v3"},
            {"spark_version": "15.4.x-photon-scala2.12", "node_type_id": "Standard_D4s_v3"},
            {"spark_version": "@if(equals(item()?.photon,true),X,Y)", "node_type_id": "Standard_D4s_v3"},
        ]
        spark_version, _ = _infer_bundle_cluster_defaults(wf)
        assert spark_version == "15.4.x-photon-scala2.12"


class TestSingleUserNameOnSingleUserClusters:
    """Change fix-single-user-cluster-requires-single-user-name (P0): NB-ITER3-003."""

    def test_default_cluster_includes_single_user_name(self):
        from orchestra.bundler.dab_writer import _build_default_cluster

        cluster = _build_default_cluster()["new_cluster"]
        assert cluster["data_security_mode"] == "SINGLE_USER"
        assert cluster["single_user_name"] == "${workspace.current_user.userName}"

    def test_single_node_cluster_includes_single_user_name(self):
        from orchestra.bundler.dab_writer import _build_single_node_cluster

        cluster = _build_single_node_cluster()["new_cluster"]
        assert cluster["data_security_mode"] == "SINGLE_USER"
        assert cluster["single_user_name"] == "${workspace.current_user.userName}"

    def test_multi_node_cluster_includes_single_user_name(self):
        from orchestra.bundler.dab_writer import _build_multi_node_cluster

        cluster = _build_multi_node_cluster()["new_cluster"]
        assert cluster["data_security_mode"] == "SINGLE_USER"
        assert cluster["single_user_name"] == "${workspace.current_user.userName}"


class TestSetupMd:
    def test_parameter_approximations_render_to_setup_md(self, tmp_path):
        pipeline = Pipeline(
            name="approx_pipeline",
            tasks=[
                NotebookActivity(
                    name="Score",
                    task_key="score",
                    notebook_path="/Shared/score",
                    base_parameters={"scoring_date": "{{job.start_time.iso_date}}"},
                    parameter_approximations=[
                        {
                            "widget_name": "scoring_date",
                            "raw_expression": "@formatDateTime(utcnow(), 'yyyy-MM-dd')",
                            "replacement": "{{job.start_time.iso_date}}",
                            "note": "Mapped ADF `utcnow()` to the Databricks job start time.",
                        }
                    ],
                ),
            ],
        )
        wf = prepare_workflow(pipeline)
        write_bundle(wf, tmp_path)
        setup_md = (tmp_path / "SETUP.md").read_text()
        assert "## Parameter substitutions" in setup_md
        assert "`score`" in setup_md
        assert "`scoring_date`" in setup_md
        assert "@formatDateTime(utcnow(), 'yyyy-MM-dd')" in setup_md
        assert "{{job.start_time.iso_date}}" in setup_md
        assert "Mapped ADF `utcnow()`" in setup_md


class TestManualVariableRollupSetupMd:
    """Change fix-cross-foreach-variable-read-warning (P1): VAREX3-003."""

    def test_setup_md_surfaces_manual_variable_rollup(self, tmp_path):
        from orchestra.models.ir import ForEachActivity, IfConditionActivity, SetVariableActivity

        # Mirror the preparer-side detection by building the same pipeline.
        inner_set = SetVariableActivity(
            name="MarkStop",
            task_key="mark_stop",
            variable_name="continue",
            variable_value="false",
        )
        loop = ForEachActivity(
            name="Loop",
            task_key="loop",
            items_expression="@output.value",
            inner_activities=[inner_set],
            concurrency=2,
        )
        sibling = IfConditionActivity(
            name="CheckCont",
            task_key="check_cont",
            op="EQUAL_TO",
            left="@variables('continue')",
            right="true",
            if_true_activities=[],
            if_false_activities=[],
        )
        pipeline = Pipeline(
            name="rollup_pipeline",
            tasks=[loop, sibling],
        )
        wf = prepare_workflow(pipeline)
        write_bundle(wf, tmp_path)
        setup_md = (tmp_path / "SETUP.md").read_text()
        assert "## Manual variable roll-ups" in setup_md
        assert "`continue`" in setup_md
        assert "`loop`" in setup_md


class TestSetupGenerator:
    def test_secrets_setup_notebook_content(self):
        from orchestra.bundler.setup_generator import generate_setup_tasks

        secrets = [
            SecretInstruction(scope="my-scope", key="jdbc-url", value_source="JDBC URL for source"),
            SecretInstruction(scope="my-scope", key="jdbc-password", value_source="JDBC password for source"),
        ]
        notebooks = generate_setup_tasks(secrets=secrets, setup_tasks=[], catalog="main", schema="default")
        assert len(notebooks) == 1
        nb = notebooks[0]
        assert nb.relative_path == "setup/create_secrets.py"
        # C-46 (LSC5-002): generated against the SDK WorkspaceClient, since
        # dbutils.secrets is read-only (no createScope / put).
        assert "w.secrets.create_scope" in nb.content
        assert "w.secrets.put_secret" in nb.content
        assert "WorkspaceClient" in nb.content
        assert "dbutils.secrets.createScope" not in nb.content
        assert "dbutils.secrets.put" not in nb.content
        assert "my-scope" in nb.content
        assert "jdbc-url" in nb.content
        assert "jdbc-password" in nb.content

    def test_volume_setup_notebook(self):
        from orchestra.bundler.setup_generator import generate_setup_tasks

        setup_tasks = [
            SetupTask(type="volume", config={"volume_name": "raw_data", "volume_type": "MANAGED"}),
        ]
        notebooks = generate_setup_tasks(secrets=[], setup_tasks=setup_tasks, catalog="prod", schema="ingest")
        assert len(notebooks) == 1
        nb = notebooks[0]
        assert nb.relative_path == "setup/create_volumes.py"
        assert "prod.ingest.raw_data" in nb.content

    def test_connection_setup_notebook(self):
        from orchestra.bundler.setup_generator import generate_setup_tasks

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
        from orchestra.bundler.setup_generator import generate_setup_tasks

        notebooks = generate_setup_tasks(secrets=[], setup_tasks=[], catalog="main", schema="default")
        assert len(notebooks) == 0


class TestPipelineDictToIrBridgeFields:
    """C-14 (CF3-001 / VAREX3-001): bridge fields survive JSON roundtrip."""

    def test_if_condition_bridge_fields_preserved(self):
        from orchestra.bundler.dab_writer import pipeline_dict_to_ir
        from orchestra.models.ir import IfConditionActivity

        pipeline_dict = {
            "name": "p",
            "parameters": [],
            "tasks": [
                {
                    "type": "IfConditionActivity",
                    "name": "Branch",
                    "task_key": "branch",
                    "op": "EQUAL_TO",
                    "left": "__BRIDGE__::result",
                    "right": "True",
                    "if_true_activities": [],
                    "if_false_activities": [],
                    "bridge_notebook_code": "result = not bool(some_param)",
                    "bridge_notebook_imports": ["import os"],
                    "bridge_required_parameters": {"some_param": "{{job.parameters.x}}"},
                }
            ],
        }
        pipeline, _ = pipeline_dict_to_ir(pipeline_dict)
        task = pipeline.tasks[0]
        assert isinstance(task, IfConditionActivity)
        assert task.bridge_notebook_code == "result = not bool(some_param)"
        assert task.bridge_notebook_imports == ["import os"]
        assert task.bridge_required_parameters == {"some_param": "{{job.parameters.x}}"}

    def test_switch_bridge_fields_preserved(self):
        from orchestra.bundler.dab_writer import pipeline_dict_to_ir
        from orchestra.models.ir import SwitchActivity

        pipeline_dict = {
            "name": "p",
            "parameters": [],
            "tasks": [
                {
                    "type": "SwitchActivity",
                    "name": "Sw",
                    "task_key": "sw",
                    "on_expression": "__BRIDGE__::result",
                    "cases": [],
                    "default_activities": [],
                    "bridge_notebook_code": "result = item.get('type', 'default').upper()",
                    "bridge_notebook_imports": [],
                    "bridge_required_parameters": {"item": "{{tasks.upstream.values.row}}"},
                }
            ],
        }
        pipeline, _ = pipeline_dict_to_ir(pipeline_dict)
        task = pipeline.tasks[0]
        assert isinstance(task, SwitchActivity)
        assert task.bridge_notebook_code == "result = item.get('type', 'default').upper()"
        assert task.bridge_required_parameters == {"item": "{{tasks.upstream.values.row}}"}
