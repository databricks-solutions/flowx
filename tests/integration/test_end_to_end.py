"""End-to-end integration tests for the flowx translation pipeline.

These tests exercise the full profile -> translate -> prepare -> bundle pipeline
against realistic ADF fixture files, simulating what happens when a user
invokes the flowx skills.
"""

from __future__ import annotations

import ast

import pytest
import yaml

from flowx.bundler.dab_writer import write_bundle
from flowx.models.adf_ast import AdfDefinitions, TranslationStrategy
from flowx.models.ir import (
    CopyActivity,
    ForEachActivity,
    PlaceholderActivity,
    SwitchActivity,
)
from flowx.parser.adf_loader import build_inventory
from flowx.preparer.workflow_preparer import PreparedWorkflow, prepare_workflow
from flowx.translator.engine import translate_pipeline

# ---------------------------------------------------------------------------
# TestTranslateAllPipelines — simulates "translate all pipelines"
# ---------------------------------------------------------------------------


class TestTranslateAllPipelines:
    """Tests simulating 'translate all pipelines' prompt."""

    def test_ingest_all_pipelines(self, adf_definitions):
        """All pipelines load successfully from the fixture directory."""
        assert isinstance(adf_definitions, AdfDefinitions)
        assert len(adf_definitions.pipelines) >= 4

    def test_translate_all_pipelines(self, adf_definitions):
        """All pipelines translate without errors."""
        for pipeline in adf_definitions.pipelines:
            report = translate_pipeline(pipeline, adf_definitions)
            assert report.pipeline is not None
            assert report.pipeline.name == pipeline.name
            total = report.deterministic_count + report.agentic_count + report.unsupported_count
            assert total > 0, f"Pipeline {pipeline.name} has no translated activities"

    def test_prepare_all_pipelines(self, adf_definitions):
        """All translated pipelines produce valid PreparedWorkflows."""
        for pipeline in adf_definitions.pipelines:
            report = translate_pipeline(pipeline, adf_definitions)
            wf = prepare_workflow(report.pipeline)
            assert isinstance(wf, PreparedWorkflow)
            assert len(wf.tasks) > 0, f"Pipeline {pipeline.name} produced no tasks"
            # Every task must have a task_key
            for task in wf.tasks:
                assert "task_key" in task, f"Task missing task_key in pipeline {pipeline.name}"
                assert task["task_key"], f"Empty task_key in pipeline {pipeline.name}"

    def test_bundle_all_pipelines(self, adf_definitions, tmp_path):
        """All pipelines produce valid DAB output files."""
        for i, pipeline in enumerate(adf_definitions.pipelines):
            report = translate_pipeline(pipeline, adf_definitions)
            wf = prepare_workflow(report.pipeline)
            output_dir = tmp_path / f"bundle_{i}"
            created = write_bundle(wf, output_dir)
            assert len(created) > 0

            # Verify databricks.yml exists and is valid YAML
            dby = output_dir / "databricks.yml"
            assert dby.exists(), f"databricks.yml missing for {pipeline.name}"
            parsed = yaml.safe_load(dby.read_text())
            assert "bundle" in parsed

            # Verify resources/*.yml exist and are valid
            resource_files = list((output_dir / "resources").glob("*.yml"))
            assert len(resource_files) >= 1, f"No resource YAML for {pipeline.name}"
            for rf in resource_files:
                content = yaml.safe_load(rf.read_text())
                assert content is not None


# ---------------------------------------------------------------------------
# TestTranslateSpecificPipeline — simulates "translate a specific pipeline"
# ---------------------------------------------------------------------------


class TestTranslateSpecificPipeline:
    """Tests simulating 'translate a specific pipeline' prompt."""

    def test_translate_copy_csv_pipeline(self, adf_definitions, pipeline_by_name):
        """Copy CSV to Delta pipeline translates correctly."""
        pipeline = pipeline_by_name("pipeline_copy_csv_to_delta")
        report = translate_pipeline(pipeline, adf_definitions)
        assert report.deterministic_count == 1
        assert report.agentic_count == 0
        assert isinstance(report.pipeline.tasks[0], CopyActivity)
        copy_task = report.pipeline.tasks[0]
        assert copy_task.source_type == "DelimitedTextSource"
        assert copy_task.sink_type == "DeltaSink"
        assert copy_task.column_mapping is not None
        assert len(copy_task.column_mapping) == 5

    def test_translate_notebook_basic_pipeline(self, adf_definitions, pipeline_by_name):
        """Basic notebook pipeline translates with notebook path preserved."""
        pipeline = pipeline_by_name("pipeline_notebook_basic")
        report = translate_pipeline(pipeline, adf_definitions)
        assert report.deterministic_count == 1
        from flowx.models.ir import NotebookActivity

        nb = report.pipeline.tasks[0]
        assert isinstance(nb, NotebookActivity)
        assert nb.notebook_path == "/Shared/ETL/transform_customers"

    def test_translate_copy_sql_pipeline(self, adf_definitions, pipeline_by_name):
        """SQL to Delta pipeline preserves source query and column mappings."""
        pipeline = pipeline_by_name("pipeline_copy_sql_to_delta")
        report = translate_pipeline(pipeline, adf_definitions)
        assert report.deterministic_count == 1
        copy_task = report.pipeline.tasks[0]
        assert isinstance(copy_task, CopyActivity)
        assert copy_task.source_type == "AzureSqlSource"
        assert copy_task.column_mapping is not None
        assert len(copy_task.column_mapping) == 6

    def test_translate_complex_etl_pipeline(self, adf_definitions, pipeline_by_name):
        """Complex multi-activity ETL pipeline translates all activities."""
        pipeline = pipeline_by_name("pipeline_complex_etl")
        report = translate_pipeline(pipeline, adf_definitions)
        # Should have at least 5 deterministic activities
        assert report.deterministic_count >= 5
        task_types = {type(t).__name__ for t in report.pipeline.tasks}
        assert len(task_types) > 1

    def test_translate_foreach_switch_pipeline(self, adf_definitions, pipeline_by_name):
        """ForEach + Switch pipeline preserves control flow structure."""
        pipeline = pipeline_by_name("pipeline_foreach_switch")
        report = translate_pipeline(pipeline, adf_definitions)
        # Should have Lookup + ForEach + SetVariable at top level
        task_names = {t.name for t in report.pipeline.tasks}
        assert "Get Table List" in task_names
        assert "Process Each Table" in task_names
        assert "Set Completion Status" in task_names

    def test_translate_all_activity_types_pipeline(self, adf_definitions, pipeline_by_name):
        """Pipeline with all 16 deterministic types translates fully."""
        pipeline = pipeline_by_name("pipeline_all_activity_types")
        report = translate_pipeline(pipeline, adf_definitions)
        # All 13 top-level activities should be deterministic
        assert report.deterministic_count == 13
        assert report.agentic_count == 0
        assert report.unsupported_count == 0

    def test_translate_mixed_agentic_pipeline(self, adf_definitions, pipeline_by_name):
        """Mixed pipeline has both deterministic and agentic/unsupported items."""
        pipeline = pipeline_by_name("pipeline_mixed_agentic")
        report = translate_pipeline(pipeline, adf_definitions)
        assert report.deterministic_count >= 1  # Copy
        assert (report.agentic_count + report.unsupported_count) >= 1  # ExecuteDataFlow, etc.
        # Should have placeholders for agentic types
        placeholders = [t for t in report.pipeline.tasks if isinstance(t, PlaceholderActivity)]
        assert len(placeholders) >= 1


# ---------------------------------------------------------------------------
# TestActivityTypeTranslation — specific activity translation accuracy
# ---------------------------------------------------------------------------


class TestActivityTypeTranslation:
    """Tests for specific activity type translation accuracy."""

    def test_copy_activity_source_sink(self, adf_definitions, pipeline_by_name):
        """Copy activity preserves source/sink properties."""
        pipeline = pipeline_by_name("pipeline_copy_csv_to_delta")
        report = translate_pipeline(pipeline, adf_definitions)
        copy = report.pipeline.tasks[0]
        assert isinstance(copy, CopyActivity)
        assert copy.source_type is not None
        assert copy.sink_type is not None
        # Source properties should contain storeSettings, formatSettings, etc.
        assert copy.source_properties is not None

    def test_foreach_items_expression(self, adf_definitions, pipeline_by_name):
        """ForEach items expression is parsed correctly."""
        pipeline = pipeline_by_name("pipeline_foreach_switch")
        report = translate_pipeline(pipeline, adf_definitions)
        foreach_tasks = [t for t in report.pipeline.tasks if isinstance(t, ForEachActivity)]
        assert len(foreach_tasks) == 1
        fe = foreach_tasks[0]
        assert "{{tasks.Get_Table_List.values.result}}" == fe.items_expression

    def test_switch_cases_translation(self, adf_definitions, pipeline_by_name):
        """Switch cases produce correct case branches."""
        pipeline = pipeline_by_name("pipeline_foreach_switch")
        report = translate_pipeline(pipeline, adf_definitions)
        foreach_tasks = [t for t in report.pipeline.tasks if isinstance(t, ForEachActivity)]
        assert len(foreach_tasks) == 1
        switch_children = [a for a in foreach_tasks[0].inner_activities if isinstance(a, SwitchActivity)]
        assert len(switch_children) == 1
        child = switch_children[0]
        assert len(child.cases) == 2
        assert child.cases[0].value == "full"
        assert child.cases[1].value == "incremental"

    def test_dependency_conditions_preserved(self, adf_definitions, pipeline_by_name):
        """All dependency conditions (Succeeded, Failed, Completed) are preserved."""
        pipeline = pipeline_by_name("pipeline_all_activity_types")
        report = translate_pipeline(pipeline, adf_definitions)
        # Most tasks depend on the previous one via Succeeded
        for task in report.pipeline.tasks:
            if task.depends_on:
                for dep in task.depends_on:
                    assert dep.outcome in ("Succeeded", "Failed", "Completed", "Skipped", None)


# ---------------------------------------------------------------------------
# TestBundleOutput — DAB bundle output validity
# ---------------------------------------------------------------------------


class TestBundleOutput:
    """Tests for DAB bundle output validity."""

    def test_databricks_yml_structure(self, adf_definitions, tmp_path):
        """Generated databricks.yml has correct structure for every pipeline."""
        for i, pipeline in enumerate(adf_definitions.pipelines):
            report = translate_pipeline(pipeline, adf_definitions)
            wf = prepare_workflow(report.pipeline)
            out = tmp_path / f"bundle_{i}"
            write_bundle(wf, out, catalog="prod", schema="analytics")
            content = yaml.safe_load((out / "databricks.yml").read_text())
            assert content["variables"]["catalog"]["default"] == "prod"
            assert content["variables"]["schema"]["default"] == "analytics"
            assert "targets" in content
            assert set(content["targets"].keys()) == {"dev", "staging", "prod"}

    def test_job_yaml_task_keys_match_activities(self, adf_definitions, pipeline_by_name, tmp_path):
        """Job YAML has unique task keys matching the pipeline activities."""
        pipeline = pipeline_by_name("pipeline_all_activity_types")
        report = translate_pipeline(pipeline, adf_definitions)
        wf = prepare_workflow(report.pipeline)
        write_bundle(wf, tmp_path)

        resource_file = list((tmp_path / "resources").glob("*.yml"))[0]
        content = yaml.safe_load(resource_file.read_text())
        job = list(content["resources"]["jobs"].values())[0]
        task_keys = [t["task_key"] for t in job["tasks"]]

        # All task keys should be unique
        assert len(task_keys) == len(set(task_keys))
        # Should have as many tasks as activities
        assert len(task_keys) == len(pipeline.activities)

    def test_notebooks_are_valid_python(self, adf_definitions, tmp_path):
        """All generated notebooks are syntactically valid Python."""
        for i, pipeline in enumerate(adf_definitions.pipelines):
            report = translate_pipeline(pipeline, adf_definitions)
            wf = prepare_workflow(report.pipeline)
            out = tmp_path / f"bundle_{i}"
            write_bundle(wf, out)

            src_dir = out / "src"
            if not src_dir.exists():
                continue
            for nb_file in src_dir.rglob("*.py"):
                content = nb_file.read_text()
                # Strip Databricks magic comments
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
                    pytest.fail(
                        f"Notebook {nb_file.relative_to(out)} in pipeline '{pipeline.name}' has invalid Python: {exc}"
                    )

    def test_setup_notebooks_generated(self, adf_definitions, pipeline_by_name, tmp_path):
        """Setup notebooks are created for activities requiring secrets/volumes."""
        # pipeline_copy_sql_to_delta uses AzureSqlSource which needs JDBC secrets
        pipeline = pipeline_by_name("pipeline_copy_sql_to_delta")
        report = translate_pipeline(pipeline, adf_definitions)
        wf = prepare_workflow(report.pipeline)
        write_bundle(wf, tmp_path)

        if wf.secrets:
            setup_dir = tmp_path / "src" / "setup"
            assert setup_dir.exists()
            assert any(setup_dir.glob("*.py"))


# ---------------------------------------------------------------------------
# TestInventoryAccuracy — activity classification accuracy
# ---------------------------------------------------------------------------


class TestInventoryAccuracy:
    """Tests for activity classification accuracy."""

    def test_deterministic_count(self, adf_definitions):
        """Inventory correctly counts deterministic activities."""
        inv = build_inventory(adf_definitions)
        assert inv.deterministic_count > 0
        det_items = [i for i in inv.items if i.strategy is TranslationStrategy.DETERMINISTIC]
        assert len(det_items) == inv.deterministic_count

    def test_agentic_activities_identified(self, adf_definitions):
        """Agentic activities are identified and counted."""
        inv = build_inventory(adf_definitions)
        agentic_items = [i for i in inv.items if i.strategy is TranslationStrategy.AGENTIC]
        assert len(agentic_items) == inv.agentic_count

    def test_mixed_pipeline_classification(self, adf_definitions):
        """Mixed pipeline has both deterministic and agentic items."""
        inv = build_inventory(adf_definitions)
        # pipeline_mixed_agentic has Copy (deterministic) + ExecuteDataFlow (agentic) + more
        mixed_items = [i for i in inv.items if i.pipeline_name == "pipeline_mixed_agentic"]
        strategies = {i.strategy for i in mixed_items}
        assert TranslationStrategy.DETERMINISTIC in strategies
        assert TranslationStrategy.AGENTIC in strategies or TranslationStrategy.UNSUPPORTED in strategies

    def test_unsupported_type_counted(self, adf_definitions):
        """Unknown activity types are counted as unsupported."""
        inv = build_inventory(adf_definitions)
        # pipeline_mixed_agentic has SomeFutureActivity
        unsupported = [i for i in inv.items if i.strategy is TranslationStrategy.UNSUPPORTED]
        assert len(unsupported) == inv.unsupported_count
        future_items = [i for i in unsupported if i.activity_type == "SomeFutureActivity"]
        assert len(future_items) >= 1

    def test_inventory_pipeline_count_matches(self, adf_definitions):
        """Inventory pipeline count equals the number of loaded pipelines."""
        inv = build_inventory(adf_definitions)
        assert inv.pipeline_count == len(adf_definitions.pipelines)

    def test_recursive_classification_includes_children(self, adf_definitions):
        """Activities nested inside ForEach/IfCondition are also classified.

        Note: The inventory classifies activities recursively through
        if_true_activities, if_false_activities, and activities (ForEach children),
        but Switch case activities remain in typeProperties and are not recursively
        classified at the inventory level (they are translated by the switch translator).
        """
        inv = build_inventory(adf_definitions)
        # pipeline_foreach_switch has activities inside ForEach > Switch
        foreach_pipeline_items = [i for i in inv.items if i.pipeline_name == "pipeline_foreach_switch"]
        activity_types = {i.activity_type for i in foreach_pipeline_items}
        assert "ForEach" in activity_types
        assert "Switch" in activity_types
        # ForEach child activities are classified recursively
        assert "Lookup" in activity_types or "SetVariable" in activity_types
