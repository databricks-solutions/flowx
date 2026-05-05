"""Unit tests for activity preparers and the workflow_preparer module."""

from __future__ import annotations

import ast
from typing import Any

import pytest

from flowx.models.ir import (
    AppendVariableActivity,
    CopyActivity,
    DeleteActivity,
    Dependency,
    ExecutePipelineActivity,
    FilterActivity,
    ForEachActivity,
    LookupActivity,
    NotebookActivity,
    Pipeline,
    PlaceholderActivity,
    RunJobActivity,
    SetVariableActivity,
    SparkJarActivity,
    SparkPythonActivity,
    SwitchActivity,
    SwitchCase,
    WaitActivity,
    WebActivity,
)
from flowx.preparer.workflow_preparer import (
    PreparedActivity,
    PreparedWorkflow,
    prepare_activity,
    prepare_workflow,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_base(name: str = "test", task_key: str = "test") -> dict[str, Any]:
    return {
        "name": name,
        "task_key": task_key,
        "description": None,
        "timeout_seconds": None,
        "max_retries": None,
        "min_retry_interval_millis": None,
        "depends_on": None,
        "cluster": None,
    }


# ---------------------------------------------------------------------------
# Individual activity preparer tests
# ---------------------------------------------------------------------------


class TestNotebookPreparer:
    def test_prepare_notebook_task_structure(self):
        activity = NotebookActivity(
            **_make_base("Run NB", "run_nb"),
            notebook_path="/Shared/ETL/transform",
            base_parameters={"env": "dev"},
        )
        prepared = prepare_activity(activity)
        assert isinstance(prepared, PreparedActivity)
        assert prepared.task["task_key"] == "run_nb"
        assert "notebook_task" in prepared.task
        # Existing notebook at an absolute workspace path is referenced
        # in place -- no placeholder is synthesized into the bundle src
        # tree because the notebook already exists in the workspace.
        assert prepared.task["notebook_task"]["notebook_path"] == "/Shared/ETL/transform"
        assert prepared.task["notebook_task"]["base_parameters"] == {"env": "dev"}
        assert prepared.notebooks == []

    def test_prepare_notebook_no_params(self):
        activity = NotebookActivity(
            **_make_base("NB", "nb"),
            notebook_path="/Shared/simple",
            base_parameters=None,
        )
        prepared = prepare_activity(activity)
        assert "base_parameters" not in prepared.task.get("notebook_task", {})
        # No placeholder for an absolute workspace path.
        assert prepared.notebooks == []

    def test_prepare_notebook_resolves_expression_params(self):
        """ADF expression params are mapped to DAB dynamic value references."""
        activity = NotebookActivity(
            **_make_base("Expr NB", "expr_nb"),
            notebook_path="/Shared/nb",
            base_parameters={
                "run_id": "@pipeline().RunId",
                "env": "dev",
                "trigger_time": {"type": "Expression", "value": "@pipeline().TriggerTime"},
            },
        )
        prepared = prepare_activity(activity)
        params = prepared.task["notebook_task"]["base_parameters"]
        assert params["run_id"] == "{{job.run_id}}"
        assert params["env"] == "dev"
        assert params["trigger_time"] == "{{job.start_time.iso_datetime}}"


class TestCopyPreparer:
    def test_prepare_copy_generates_notebook(self):
        activity = CopyActivity(
            **_make_base("Copy Data", "copy_data"),
            source_type="BlobSource",
            sink_type="DeltaSink",
        )
        prepared = prepare_activity(activity)
        assert "notebook_task" in prepared.task
        assert len(prepared.notebooks) == 1
        assert prepared.notebooks[0].relative_path == "notebooks/copy_data.py"
        # Verify generated content is valid Python
        content = prepared.notebooks[0].content
        assert "Databricks notebook source" in content

    def test_prepare_copy_db_source_creates_secrets(self):
        activity = CopyActivity(
            **_make_base("Copy SQL", "copy_sql"),
            source_type="AzureSqlSource",
            sink_type="DeltaSink",
        )
        prepared = prepare_activity(activity)
        assert len(prepared.secrets) >= 2
        scopes = {s.key for s in prepared.secrets}
        assert "jdbc-url" in scopes
        assert "jdbc-password" in scopes


class TestSparkJarPreparer:
    def test_prepare_spark_jar_task(self):
        activity = SparkJarActivity(
            **_make_base("Jar Task", "jar_task"),
            main_class_name="com.example.Main",
            parameters=["--arg1"],
            libraries=[{"jar": "dbfs:/libs/my.jar"}],
        )
        prepared = prepare_activity(activity)
        assert "spark_jar_task" in prepared.task
        assert prepared.task["spark_jar_task"]["main_class_name"] == "com.example.Main"
        # Libraries are rewritten to bundle-relative paths
        assert prepared.task["libraries"] == [{"jar": "../lib/my.jar"}]
        # Placeholder readme is generated
        assert len(prepared.notebooks) == 1
        assert "jar_task_README.txt" in prepared.notebooks[0].relative_path


class TestSparkPythonPreparer:
    def test_prepare_spark_python_task(self):
        activity = SparkPythonActivity(
            **_make_base("Py Task", "py_task"),
            python_file="dbfs:/scripts/etl.py",
            parameters=["--mode", "batch"],
        )
        prepared = prepare_activity(activity)
        assert "spark_python_task" in prepared.task
        # Path is rewritten to bundle-relative
        assert prepared.task["spark_python_task"]["python_file"] == "../src/scripts/etl.py"
        # Placeholder script is generated
        assert len(prepared.notebooks) == 1
        assert "scripts/etl.py" in prepared.notebooks[0].relative_path
        assert "dbfs:/scripts/etl.py" in prepared.notebooks[0].content


class TestLookupPreparer:
    def test_prepare_lookup_generates_notebook(self):
        activity = LookupActivity(
            **_make_base("Lookup", "lookup"),
            source_type="AzureSqlSource",
            first_row_only=True,
            source_query="SELECT 1",
        )
        prepared = prepare_activity(activity)
        assert len(prepared.notebooks) == 1
        assert "notebook_task" in prepared.task
        assert prepared.task["notebook_task"]["base_parameters"]["first_row_only"] == "true"


class TestWebActivityPreparer:
    def test_prepare_web_activity_generates_notebook(self):
        activity = WebActivity(
            **_make_base("Call API", "call_api"),
            url="https://api.example.com",
            method="GET",
        )
        prepared = prepare_activity(activity)
        assert len(prepared.notebooks) == 1
        assert "notebook_task" in prepared.task
        assert prepared.task["notebook_task"]["base_parameters"]["url"] == "https://api.example.com"
        assert prepared.task["notebook_task"]["base_parameters"]["method"] == "GET"

    def test_prepare_web_activity_with_auth_creates_secrets(self):
        activity = WebActivity(
            **_make_base("Auth API", "auth_api"),
            url="https://api.example.com",
            method="POST",
            authentication={"type": "ServicePrincipal"},
        )
        prepared = prepare_activity(activity)
        assert len(prepared.secrets) >= 1
        assert any(s.key == "auth-credential" for s in prepared.secrets)


class TestDeletePreparer:
    def test_prepare_delete_generates_notebook(self):
        activity = DeleteActivity(
            **_make_base("Delete Files", "delete_files"),
            dataset_name="ds_staging",
            recursive=True,
        )
        prepared = prepare_activity(activity)
        assert len(prepared.notebooks) == 1
        assert "notebook_task" in prepared.task


class TestSetVariablePreparer:
    def test_prepare_set_variable_generates_notebook(self):
        activity = SetVariableActivity(
            **_make_base("Set Var", "set_var"),
            variable_name="status",
            variable_value="completed",
            value_kind="literal",
        )
        prepared = prepare_activity(activity)
        assert "notebook_task" in prepared.task
        assert len(prepared.notebooks) == 1
        # Literal value should be in base_parameters
        params = prepared.task["notebook_task"]["base_parameters"]
        assert params["value"] == "completed"

    def test_prepare_set_variable_dab_ref(self):
        activity = SetVariableActivity(
            **_make_base("Set Env", "set_env"),
            variable_name="env",
            variable_value="{{job.parameters.environment}}",
            value_kind="dab_ref",
        )
        prepared = prepare_activity(activity)
        params = prepared.task["notebook_task"]["base_parameters"]
        assert params["value"] == "{{job.parameters.environment}}"

    def test_prepare_set_variable_notebook_code_not_in_params(self):
        """notebook_code values must NOT appear in base_parameters."""
        activity = SetVariableActivity(
            **_make_base("Set Date", "set_date"),
            variable_name="runDate",
            variable_value="datetime.now(timezone.utc).strftime('%Y-%m-%d')",
            value_kind="notebook_code",
            notebook_code="datetime.now(timezone.utc).strftime('%Y-%m-%d')",
            notebook_imports=["from datetime import datetime, timezone"],
        )
        prepared = prepare_activity(activity)
        params = prepared.task["notebook_task"]["base_parameters"]
        # Should NOT have 'value' key with Python code
        assert "value" not in params
        # But should have variable_name
        assert params["variable_name"] == "runDate"
        # Notebook should contain the code
        content = prepared.notebooks[0].content
        assert "strftime" in content
        assert "datetime" in content


class TestAppendVariablePreparer:
    def test_prepare_append_variable_generates_notebook(self):
        activity = AppendVariableActivity(
            **_make_base("Append Var", "append_var"),
            variable_name="logEntries",
            append_value="step1 done",
            value_kind="literal",
        )
        prepared = prepare_activity(activity)
        assert "notebook_task" in prepared.task
        assert len(prepared.notebooks) == 1
        params = prepared.task["notebook_task"]["base_parameters"]
        assert params["value"] == "step1 done"

    def test_prepare_append_variable_notebook_code_not_in_params(self):
        """notebook_code values must NOT appear in base_parameters."""
        activity = AppendVariableActivity(
            **_make_base("Append TS", "append_ts"),
            variable_name="timestamps",
            append_value="datetime.now(timezone.utc).isoformat()",
            value_kind="notebook_code",
            notebook_code="datetime.now(timezone.utc).isoformat()",
            notebook_imports=["from datetime import datetime, timezone"],
        )
        prepared = prepare_activity(activity)
        params = prepared.task["notebook_task"]["base_parameters"]
        assert "value" not in params


class TestFilterPreparer:
    def test_prepare_filter_generates_notebook(self):
        activity = FilterActivity(
            **_make_base("Filter Items", "filter_items"),
            items_expression="@variables('myList')",
            condition_expression="@not(empty(item()))",
        )
        prepared = prepare_activity(activity)
        assert "notebook_task" in prepared.task
        assert len(prepared.notebooks) == 1


class TestWaitPreparer:
    def test_prepare_wait_generates_notebook(self):
        activity = WaitActivity(
            **_make_base("Wait 30s", "wait_30s"),
            wait_time_seconds=30,
        )
        prepared = prepare_activity(activity)
        assert "notebook_task" in prepared.task
        assert len(prepared.notebooks) == 1
        assert prepared.task["notebook_task"]["base_parameters"]["wait_seconds"] == "30"


class TestForEachPreparer:
    def test_prepare_for_each_wraps_inner(self):
        inner = WaitActivity(**_make_base("Inner", "inner"), wait_time_seconds=1)
        activity = ForEachActivity(
            **_make_base("Loop", "loop"),
            items_expression="@output.value",
            inner_activities=[inner],
            concurrency=10,
        )
        prepared = prepare_activity(activity)
        assert "for_each_task" in prepared.task
        assert prepared.task["for_each_task"]["concurrency"] == 10
        assert prepared.task["for_each_task"]["inputs"] == "@output.value"


class TestExecutePipelinePreparer:
    def test_prepare_execute_pipeline_task(self):
        activity = ExecutePipelineActivity(
            **_make_base("Run Child", "run_child"),
            pipeline_name="child_pipeline",
            parameters={"date": "2024-01-01"},
            wait_on_completion=True,
        )
        prepared = prepare_activity(activity)
        assert "run_job_task" in prepared.task


class TestRunJobPreparer:
    def test_prepare_databricks_job_task(self):
        activity = RunJobActivity(
            **_make_base("Run Job", "run_job"),
            job_name="nightly-agg",
            existing_job_id="12345",
        )
        prepared = prepare_activity(activity)
        assert "run_job_task" in prepared.task

    def test_run_job_round_trips_through_translation_report_json(self, tmp_path):
        """RunJobActivity.job_parameters survive the JSON serialise/reload cycle.

        Regression: the engine used to omit ``job_parameters`` from the
        serialised IR, and dab_writer's reload path read ``parameters`` (which
        only ExecutePipelineActivity emits), silently dropping every
        RunJobActivity's parameters when bundles were produced via the CLI
        ``--report`` flow.
        """
        import json

        import yaml

        from flowx.bundler.dab_writer import _load_report, write_bundle
        from flowx.translator.engine import _activity_to_dict, _pipeline_to_dict

        run_job = RunJobActivity(
            **_make_base("Nightly Aggregator", "nightly_aggregator"),
            job_name="nightly-agg",
            existing_job_id="12345",
            job_parameters={"window_start": "2024-01-01", "table": "orders"},
        )
        pipeline = Pipeline(name="rj_pipeline", tasks=[run_job])
        pipeline_dict = _pipeline_to_dict(pipeline)
        # Sanity: serialiser must include job_parameters.
        run_job_dict = next(t for t in pipeline_dict["tasks"] if t["task_key"] == "nightly_aggregator")
        assert run_job_dict["job_parameters"] == {"window_start": "2024-01-01", "table": "orders"}
        assert _activity_to_dict(run_job)["job_parameters"] == run_job.job_parameters

        report_path = tmp_path / "rj.json"
        report_path.write_text(json.dumps(pipeline_dict))
        workflows = _load_report(report_path)
        bundle_dir = tmp_path / "bundle"
        bundle_dir.mkdir()
        write_bundle(workflows[0], bundle_dir)

        job_yml = next((bundle_dir / "resources").iterdir())
        job = yaml.safe_load(job_yml.read_text())
        task = job["resources"]["jobs"]["rj_pipeline"]["tasks"][0]
        assert task["run_job_task"]["job_parameters"] == {
            "window_start": "2024-01-01",
            "table": "orders",
        }


class TestMotifPreparer:
    def test_motif_preparer_registered(self):
        """prepare_activity dispatches MotifActivity to the motif preparer.

        Regression: the motif system was added without a registered preparer,
        so any pipeline whose translation matched a motif raised
        ``ValueError("No preparer registered for activity type MotifActivity")``
        when prepare_workflow ran.
        """
        from flowx.models.ir import MotifActivity

        activity = MotifActivity(
            **_make_base("Bulk Ingest", "bulk_ingest"),
            motif_id="metadata_driven_bulk_copy",
            display_name="Metadata-driven bulk copy",
            databricks_replacement="for_each_ingestion",
            matched_activity_names=["GetTableList", "ForEachTable", "CopyTable"],
            source_type_hint="database",
            confidence_notes=["Lookup feeds ForEach feeds Copy"],
            original_activities=[],
            motif_config={"sink_table": "raw.{schema_name}_{table_name}"},
        )
        prepared = prepare_activity(activity)
        assert "notebook_task" in prepared.task
        assert prepared.task["notebook_task"]["notebook_path"].endswith("bulk_ingest.py")
        assert len(prepared.notebooks) == 1
        assert "metadata_driven_bulk_copy" in prepared.notebooks[0].content


class TestSwitchPreparer:
    def test_prepare_switch_generates_condition_task(self):
        inner = WaitActivity(**_make_base("CaseWait", "case_wait"), wait_time_seconds=1)
        default_inner = WaitActivity(**_make_base("DefaultWait", "default_wait"), wait_time_seconds=2)
        activity = SwitchActivity(
            **_make_base("Route", "route"),
            on_expression="@item().type",
            cases=[SwitchCase(value="full", activities=[inner])],
            default_activities=[default_inner],
        )
        prepared = prepare_activity(activity)
        # The main task is the first case condition: ``<switch>_case_<value>``.
        # ``task_key_remap`` rewires upstream depends_on edges from the
        # original switch key to this new key.
        assert prepared.task["task_key"] == "route_case_full"
        assert prepared.task_key_remap == {"route": "route_case_full"}
        cond = prepared.task["condition_task"]
        assert cond["op"] == "EQUAL_TO"
        assert "left" in cond
        assert cond["right"] == "full"
        assert "if_true" not in cond
        assert "if_false" not in cond
        # Case body is a sibling task gated on outcome="true".
        case_task_keys = {task["task_key"] for task in prepared.extra_tasks}
        assert "case_wait" in case_task_keys
        case_task = next(task for task in prepared.extra_tasks if task["task_key"] == "case_wait")
        assert case_task["depends_on"] == [{"task_key": "route_case_full", "outcome": "true"}]
        # Default body is gated on outcome="false" of the last case.
        default_task = next(task for task in prepared.extra_tasks if task["task_key"] == "default_wait")
        assert default_task["depends_on"] == [{"task_key": "route_case_full", "outcome": "false"}]

    def test_prepare_switch_multi_case_chains_conditions(self):
        """Each case becomes a chained condition task linked by outcome=false."""
        inner1 = WaitActivity(**_make_base("Wait1", "wait1"), wait_time_seconds=1)
        inner2 = WaitActivity(**_make_base("Wait2", "wait2"), wait_time_seconds=2)
        inner3 = WaitActivity(**_make_base("Wait3", "wait3"), wait_time_seconds=3)
        default = WaitActivity(**_make_base("Default", "default_wait"), wait_time_seconds=5)
        activity = SwitchActivity(
            **_make_base("Route", "route"),
            on_expression="@pipeline().parameters.env",
            cases=[
                SwitchCase(value="dev", activities=[inner1]),
                SwitchCase(value="staging", activities=[inner2]),
                SwitchCase(value="prod", activities=[inner3]),
            ],
            default_activities=[default],
        )
        prepared = prepare_activity(activity)
        # First case condition is the main task: ``route_case_dev``.  No
        # if_true/if_false nesting -- branches hang off as siblings.
        assert prepared.task["task_key"] == "route_case_dev"
        cond = prepared.task["condition_task"]
        assert cond["right"] == "dev"
        assert "if_true" not in cond and "if_false" not in cond
        # Subsequent case conditions live as siblings, chained via outcome=false.
        extra_by_key = {task["task_key"]: task for task in prepared.extra_tasks}
        assert "route_case_staging" in extra_by_key
        assert "route_case_prod" in extra_by_key
        assert extra_by_key["route_case_staging"]["depends_on"] == [{"task_key": "route_case_dev", "outcome": "false"}]
        assert extra_by_key["route_case_prod"]["depends_on"] == [{"task_key": "route_case_staging", "outcome": "false"}]
        # Case bodies hang off their own condition with outcome=true.
        assert extra_by_key["wait1"]["depends_on"] == [{"task_key": "route_case_dev", "outcome": "true"}]
        assert extra_by_key["wait2"]["depends_on"] == [{"task_key": "route_case_staging", "outcome": "true"}]
        assert extra_by_key["wait3"]["depends_on"] == [{"task_key": "route_case_prod", "outcome": "true"}]
        # Default hangs off the last case's outcome=false.
        assert extra_by_key["default_wait"]["depends_on"] == [{"task_key": "route_case_prod", "outcome": "false"}]

    def test_prepare_switch_resolves_variables_expression(self):
        """Switch on @variables('x') resolves to a DAB task value ref."""
        inner = WaitActivity(**_make_base("CaseWait", "case_wait"), wait_time_seconds=1)
        activity = SwitchActivity(
            **_make_base("Route", "route"),
            on_expression="@variables('sourceType')",
            cases=[SwitchCase(value="SQL", activities=[inner])],
            default_activities=[],
        )
        prepared = prepare_activity(activity)
        cond = prepared.task["condition_task"]
        # Should be resolved to a DAB ref (fallback: variable name used as task key)
        assert "tasks." in cond["left"]
        assert "sourceType" in cond["left"]

    def test_prepare_switch_resolves_pipeline_param(self):
        """Switch on @pipeline().parameters.X resolves to a DAB job parameter ref."""
        inner = WaitActivity(**_make_base("CaseWait", "case_wait"), wait_time_seconds=1)
        activity = SwitchActivity(
            **_make_base("Route", "route"),
            on_expression="@pipeline().parameters.env",
            cases=[SwitchCase(value="dev", activities=[inner])],
            default_activities=[],
        )
        prepared = prepare_activity(activity)
        cond = prepared.task["condition_task"]
        assert cond["left"] == "{{job.parameters.env}}"

    def test_reload_path_resolves_unresolved_on_expression(self, tmp_path):
        """dab_writer's reload path resolves a leftover @variables() before emitting YAML.

        Regression: ``_handle_switch`` used to read ``on_expression`` straight
        from the IR JSON, so a hand-edited or future-translator-produced IR
        carrying an unresolved ``@variables(...)`` leaked the raw ADF syntax
        into ``condition_task.left``, which ``databricks bundle validate``
        would reject.
        """
        import json

        import yaml

        from flowx.bundler.dab_writer import _load_report, write_bundle

        pipeline_dict = {
            "name": "switch_pipeline",
            "tasks": [
                {
                    "type": "SwitchActivity",
                    "name": "Route",
                    "task_key": "route",
                    "on_expression": "@pipeline().parameters.env",
                    "cases": [
                        {
                            "value": "dev",
                            "activities": [
                                {
                                    "type": "WaitActivity",
                                    "name": "Wait",
                                    "task_key": "wait",
                                    "wait_time_seconds": 1,
                                }
                            ],
                        }
                    ],
                    "default_activities": [],
                }
            ],
        }
        report_path = tmp_path / "switch.json"
        report_path.write_text(json.dumps(pipeline_dict))
        workflows = _load_report(report_path)
        bundle_dir = tmp_path / "bundle"
        bundle_dir.mkdir()
        write_bundle(workflows[0], bundle_dir)

        job_yml = next((bundle_dir / "resources").iterdir())
        job = yaml.safe_load(job_yml.read_text())
        switch_task = next(t for t in job["resources"]["jobs"]["switch_pipeline"]["tasks"] if "condition_task" in t)
        assert switch_task["condition_task"]["left"] == "{{job.parameters.env}}"
        assert switch_task["task_key"].endswith("_case_dev")


class TestInjectOutcomeDependency:
    def test_preserves_external_dependencies(self):
        """Regression: branch tasks with external deps keep them after gating.

        Previously ``inject_outcome_dependency`` clobbered ``depends_on`` with
        a single outcome edge whenever the task didn't depend on a sibling,
        silently dropping any dependency on a task outside the branch (e.g.
        a global setup task).
        """
        from flowx.preparer.activity_preparers.if_condition import inject_outcome_dependency

        external_dep = {"task_key": "global_setup"}
        branch_tasks = [
            {"task_key": "branch_root", "depends_on": [external_dep]},
            {"task_key": "branch_child", "depends_on": [{"task_key": "branch_root"}]},
        ]
        inject_outcome_dependency(branch_tasks, "if_check", "true")

        root_deps = branch_tasks[0]["depends_on"]
        assert {"task_key": "if_check", "outcome": "true"} in root_deps
        assert external_dep in root_deps, "external dep lost when gating branch root"
        # Internal child task still depends only on its sibling.
        assert branch_tasks[1]["depends_on"] == [{"task_key": "branch_root"}]

    def test_idempotent(self):
        """Calling twice with the same outcome doesn't duplicate the edge."""
        from flowx.preparer.activity_preparers.if_condition import inject_outcome_dependency

        branch_tasks = [{"task_key": "branch_root"}]
        inject_outcome_dependency(branch_tasks, "if_check", "true")
        inject_outcome_dependency(branch_tasks, "if_check", "true")
        assert branch_tasks[0]["depends_on"] == [{"task_key": "if_check", "outcome": "true"}]


class TestPlaceholderPreparer:
    def test_prepare_placeholder_generates_stub(self):
        activity = PlaceholderActivity(
            **_make_base("Unknown Act", "unknown_act"),
            original_type="SomeFutureType",
            comment="This activity requires manual implementation.",
        )
        prepared = prepare_activity(activity)
        assert "notebook_task" in prepared.task
        assert len(prepared.notebooks) == 1
        assert "NotImplementedError" in prepared.notebooks[0].content


# ---------------------------------------------------------------------------
# Notebook content validity
# ---------------------------------------------------------------------------


class TestNotebookContentValidity:
    """Verify that generated notebooks are syntactically valid Python."""

    def _get_all_notebooks(self) -> list[tuple[str, str]]:
        """Build all activity types and collect generated notebooks."""
        activities = [
            CopyActivity(**_make_base("c", "c"), source_type="BlobSource", sink_type="DeltaSink"),
            LookupActivity(**_make_base("l", "l"), source_type="AzureSqlSource", first_row_only=True),
            WebActivity(**_make_base("w", "w"), url="https://example.com", method="GET"),
            DeleteActivity(**_make_base("d", "d"), dataset_name="ds", recursive=True),
            SetVariableActivity(
                **_make_base("sv", "sv"), variable_name="x", variable_value="completed", value_kind="literal"
            ),
            AppendVariableActivity(
                **_make_base("av", "av"), variable_name="arr", append_value="step1_done", value_kind="literal"
            ),
            FilterActivity(
                **_make_base("f", "f"), items_expression="@vars('list')", condition_expression="@not(empty(item()))"
            ),
            WaitActivity(**_make_base("wait", "wait"), wait_time_seconds=5),
            PlaceholderActivity(**_make_base("ph", "ph"), original_type="FutureType", comment="TODO"),
        ]
        results = []
        for act in activities:
            prepared = prepare_activity(act)
            for nb in prepared.notebooks:
                results.append((nb.relative_path, nb.content))
        return results

    def test_all_generated_notebooks_are_valid_python(self):
        """Every generated notebook passes ast.parse without SyntaxError."""
        notebooks = self._get_all_notebooks()
        assert len(notebooks) > 0, "Expected at least some generated notebooks"
        for path, content in notebooks:
            # Strip Databricks magic comments for Python parsing
            lines = []
            for line in content.split("\n"):
                stripped = line.lstrip()
                if stripped.startswith("# MAGIC") or stripped.startswith("# COMMAND"):
                    continue
                if stripped == "# Databricks notebook source":
                    continue
                lines.append(line)
            python_code = "\n".join(lines)
            try:
                ast.parse(python_code)
            except SyntaxError as exc:
                pytest.fail(f"Notebook {path} has invalid Python syntax: {exc}")


# ---------------------------------------------------------------------------
# prepare_workflow (full pipeline)
# ---------------------------------------------------------------------------


class TestPrepareWorkflow:
    def test_prepare_workflow_aggregates(self):
        pipeline = Pipeline(
            name="test_pipeline",
            tasks=[
                CopyActivity(**_make_base("Copy", "copy"), source_type="BlobSource", sink_type="DeltaSink"),
                WaitActivity(**_make_base("Wait", "wait"), wait_time_seconds=10),
                NotebookActivity(**_make_base("NB", "nb"), notebook_path="/Shared/nb"),
            ],
        )
        wf = prepare_workflow(pipeline)
        assert isinstance(wf, PreparedWorkflow)
        assert wf.name == "test_pipeline"
        assert len(wf.tasks) == 3
        # Copy and Wait synthesize bundled notebooks; the NotebookActivity
        # references an existing absolute workspace path so it does not
        # contribute a bundle artifact.
        assert len(wf.notebooks) == 2
        relative_paths = {nb.relative_path for nb in wf.notebooks}
        assert relative_paths == {"notebooks/copy.py", "notebooks/wait.py"}

    def test_prepare_workflow_deduplicates_secrets(self):
        """Duplicate secrets across tasks are deduplicated."""
        pipeline = Pipeline(
            name="dup_secrets",
            tasks=[
                CopyActivity(**_make_base("C1", "c1"), source_type="AzureSqlSource", sink_type="DeltaSink"),
                CopyActivity(**_make_base("C2", "c2"), source_type="AzureSqlSource", sink_type="DeltaSink"),
            ],
        )
        wf = prepare_workflow(pipeline)
        # Each copy generates its own scope, but within each scope secrets are deduped
        scope_keys = [(s.scope, s.key) for s in wf.secrets]
        assert len(scope_keys) == len(set(scope_keys)), "Secret (scope, key) pairs should be unique"

    def test_prepare_workflow_task_keys_unique(self):
        """Every task has a unique task_key."""
        pipeline = Pipeline(
            name="unique_keys",
            tasks=[
                WaitActivity(**_make_base("A", "a"), wait_time_seconds=1),
                WaitActivity(**_make_base("B", "b"), wait_time_seconds=2),
                WaitActivity(**_make_base("C", "c"), wait_time_seconds=3),
            ],
        )
        wf = prepare_workflow(pipeline)
        task_keys = [t["task_key"] for t in wf.tasks]
        assert len(task_keys) == len(set(task_keys))

    def test_prepare_workflow_with_dependencies(self):
        """Dependencies are preserved in prepared tasks."""
        pipeline = Pipeline(
            name="deps",
            tasks=[
                WaitActivity(**_make_base("First", "first"), wait_time_seconds=1),
                WaitActivity(
                    **{**_make_base("Second", "second"), "depends_on": [Dependency(task_key="first")]},
                    wait_time_seconds=2,
                ),
            ],
        )
        wf = prepare_workflow(pipeline)
        second_task = next(t for t in wf.tasks if t["task_key"] == "second")
        assert "depends_on" in second_task
        assert second_task["depends_on"][0]["task_key"] == "first"

    def test_prepare_workflow_with_retries(self):
        """Retry settings are carried through."""
        pipeline = Pipeline(
            name="retries",
            tasks=[
                WaitActivity(
                    name="Retry Me",
                    task_key="retry_me",
                    timeout_seconds=3600,
                    max_retries=3,
                    min_retry_interval_millis=60000,
                    wait_time_seconds=10,
                ),
            ],
        )
        wf = prepare_workflow(pipeline)
        task = wf.tasks[0]
        assert task["timeout_seconds"] == 3600
        assert task["max_retries"] == 3
        assert task["min_retry_interval_millis"] == 60000
        assert task["retry_on_timeout"] is True
