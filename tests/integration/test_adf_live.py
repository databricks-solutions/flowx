"""Integration tests against a live Azure Data Factory.

These tests export pipeline definitions from a real ADF factory, run
them through the flowx translation pipeline, and validate that
the output is correct.

Requires:
- Azure CLI authenticated (``az login``)
- Access to subscription edd4cc45-85c7-4aec-8bf5-648062d519bf
"""

from __future__ import annotations

import ast
import json
import re
import subprocess

import pytest
import yaml

from flowx.bundler.dab_writer import write_bundle
from flowx.preparer.workflow_preparer import PreparedWorkflow, prepare_workflow
from flowx.translator.engine import translate_pipeline

SUBSCRIPTION = "edd4cc45-85c7-4aec-8bf5-648062d519bf"
RESOURCE_GROUP = "ghansen-flowx-rg"
FACTORY_NAME = "ghansen-flowx-adf"
API_VERSION = "2018-06-01"


def _az_authenticated() -> bool:
    """Check if Azure CLI is authenticated."""
    try:
        result = subprocess.run(
            ["az", "account", "show", "--query", "id", "-o", "tsv"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _az_authenticated(),
        reason="Azure CLI not authenticated -- run 'az login' first",
    ),
]


# ---------------------------------------------------------------------------
# ADF REST helpers
# ---------------------------------------------------------------------------


def _export_resource(resource_type: str, name: str) -> dict:
    """Export a single ADF resource via Azure REST API."""
    url = (
        f"https://management.azure.com/subscriptions/{SUBSCRIPTION}"
        f"/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.DataFactory"
        f"/factories/{FACTORY_NAME}/{resource_type}/{name}"
        f"?api-version={API_VERSION}"
    )
    result = subprocess.run(
        ["az", "rest", "--method", "get", "--url", url, "-o", "json"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"Failed to export {resource_type}/{name}: {result.stderr}"
    return json.loads(result.stdout)


def _list_resources(resource_type: str) -> list[str]:
    """List all ADF resources of a given type."""
    url = (
        f"https://management.azure.com/subscriptions/{SUBSCRIPTION}"
        f"/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.DataFactory"
        f"/factories/{FACTORY_NAME}/{resource_type}"
        f"?api-version={API_VERSION}"
    )
    result = subprocess.run(
        ["az", "rest", "--method", "get", "--url", url, "-o", "json"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"Failed to list {resource_type}: {result.stderr}"
    data = json.loads(result.stdout)
    return [item["name"] for item in data.get("value", [])]


# ---------------------------------------------------------------------------
# Module-scoped fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def adf_export_dir(tmp_path_factory):
    """Export all ADF definitions to a temp directory."""
    export_dir = tmp_path_factory.mktemp("adf_live_export")

    # Export pipelines
    pipelines_dir = export_dir / "pipelines"
    pipelines_dir.mkdir()
    for name in _list_resources("pipelines"):
        data = _export_resource("pipelines", name)
        (pipelines_dir / f"{name}.json").write_text(json.dumps(data, indent=2))

    # Export datasets
    datasets_dir = export_dir / "datasets"
    datasets_dir.mkdir()
    for name in _list_resources("datasets"):
        data = _export_resource("datasets", name)
        (datasets_dir / f"{name}.json").write_text(json.dumps(data, indent=2))

    # Export linked services
    ls_dir = export_dir / "linked_services"
    ls_dir.mkdir()
    for name in _list_resources("linkedservices"):
        data = _export_resource("linkedservices", name)
        (ls_dir / f"{name}.json").write_text(json.dumps(data, indent=2))

    # Export triggers
    triggers_dir = export_dir / "triggers"
    triggers_dir.mkdir()
    for name in _list_resources("triggers"):
        data = _export_resource("triggers", name)
        (triggers_dir / f"{name}.json").write_text(json.dumps(data, indent=2))

    return export_dir


@pytest.fixture(scope="module")
def live_definitions(adf_export_dir):
    """Load all exported ADF definitions."""
    from flowx.parser.adf_loader import load_adf_definitions

    return load_adf_definitions(adf_export_dir)


# ---------------------------------------------------------------------------
# TestAdfExport -- validate that we can export and parse all ADF definitions
# ---------------------------------------------------------------------------


class TestAdfExport:
    """Validate that we can export and parse all ADF definitions."""

    def test_export_loads_all_pipelines(self, live_definitions):
        """All 18 pipelines are loaded."""
        assert len(live_definitions.pipelines) >= 18

    def test_export_loads_datasets(self, live_definitions):
        """All 9 datasets are loaded."""
        assert len(live_definitions.datasets) >= 9

    def test_export_loads_linked_services(self, live_definitions):
        """All 5 linked services are loaded."""
        assert len(live_definitions.linked_services) >= 5

    def test_export_loads_triggers(self, live_definitions):
        """All 2 triggers are loaded."""
        assert len(live_definitions.triggers) >= 2


# ---------------------------------------------------------------------------
# TestAdfTranslation -- validate translation of all live ADF pipelines
# ---------------------------------------------------------------------------


class TestAdfTranslation:
    """Validate translation of all live ADF pipelines."""

    def test_translate_all_pipelines(self, live_definitions):
        """All pipelines translate without errors."""
        for pl in live_definitions.pipelines:
            report = translate_pipeline(pl, live_definitions)
            assert report.pipeline is not None, f"Pipeline {pl.name} produced no IR"
            total = report.deterministic_count + report.agentic_count + report.unsupported_count
            assert total > 0, f"Pipeline {pl.name} has no translated activities"

    def test_prepare_all_pipelines(self, live_definitions):
        """All translated pipelines produce valid PreparedWorkflows."""
        for pl in live_definitions.pipelines:
            report = translate_pipeline(pl, live_definitions)
            wf = prepare_workflow(report.pipeline)
            assert isinstance(wf, PreparedWorkflow), f"Pipeline {pl.name} did not produce PreparedWorkflow"
            assert len(wf.tasks) > 0, f"Pipeline {pl.name} produced no tasks"
            for task in wf.tasks:
                assert "task_key" in task, f"Task missing task_key in pipeline {pl.name}"
                assert task["task_key"], f"Empty task_key in pipeline {pl.name}"

    def test_no_python_code_in_parameters(self, live_definitions):
        """No base_parameters contain executable Python code."""
        forbidden = ["__import__", "dbutils.jobs.taskValues.get", "eval(", "exec("]

        for pl in live_definitions.pipelines:
            report = translate_pipeline(pl, live_definitions)
            wf = prepare_workflow(report.pipeline)
            for task in wf.tasks:
                params = task.get("notebook_task", {}).get("base_parameters", {})
                for key, val in params.items():
                    val_str = str(val)
                    for pattern in forbidden:
                        assert pattern not in val_str, (
                            f"Pipeline {pl.name}, task {task.get('task_key')}, "
                            f"param {key} contains forbidden pattern '{pattern}': {val_str}"
                        )

    def test_dab_refs_are_well_formed(self, live_definitions):
        """All DAB dynamic value references use valid syntax."""
        dab_ref_re = re.compile(r"\{\{[a-zA-Z0-9_.]+\}\}")

        for pl in live_definitions.pipelines:
            report = translate_pipeline(pl, live_definitions)
            wf = prepare_workflow(report.pipeline)
            for task in wf.tasks:
                params = task.get("notebook_task", {}).get("base_parameters", {})
                for key, val in params.items():
                    val_str = str(val)
                    if "{{" in val_str:
                        assert dab_ref_re.match(val_str), (
                            f"Malformed DAB ref in {pl.name}/{task.get('task_key')}/{key}: {val_str}"
                        )


# ---------------------------------------------------------------------------
# TestAdfBundleOutput -- validate DAB bundle output from live ADF pipelines
# ---------------------------------------------------------------------------


class TestAdfBundleOutput:
    """Validate DAB bundle output from live ADF pipelines."""

    def test_generate_bundles_for_all_pipelines(self, live_definitions, tmp_path):
        """All pipelines generate valid DAB bundles."""
        for pl in live_definitions.pipelines:
            report = translate_pipeline(pl, live_definitions)
            wf = prepare_workflow(report.pipeline)
            bundle_dir = tmp_path / pl.name
            write_bundle(wf, bundle_dir, catalog="main", schema="bronze")

            # databricks.yml must exist
            assert (bundle_dir / "databricks.yml").exists(), f"Missing databricks.yml for {pl.name}"

            # At least one resource YAML
            resource_files = list((bundle_dir / "resources").glob("*.yml"))
            assert len(resource_files) > 0, f"No resource YAML for {pl.name}"

    def test_generated_notebooks_are_valid_python(self, live_definitions, tmp_path):
        """All generated notebooks parse as valid Python."""
        for pl in live_definitions.pipelines:
            report = translate_pipeline(pl, live_definitions)
            wf = prepare_workflow(report.pipeline)
            bundle_dir = tmp_path / f"{pl.name}_nb"
            write_bundle(wf, bundle_dir, catalog="main", schema="bronze")

            for nb_file in bundle_dir.rglob("*.py"):
                content = nb_file.read_text()
                lines = []
                for line in content.split("\n"):
                    stripped = line.lstrip()
                    if stripped.startswith("# MAGIC"):
                        continue
                    if stripped.startswith("# COMMAND"):
                        continue
                    if stripped == "# Databricks notebook source":
                        continue
                    lines.append(line)
                try:
                    ast.parse("\n".join(lines))
                except SyntaxError as e:
                    pytest.fail(f"Notebook {nb_file} in pipeline '{pl.name}' has syntax error: {e}")

    def test_yaml_is_parseable(self, live_definitions, tmp_path):
        """All generated YAML files are valid."""
        for pl in live_definitions.pipelines:
            report = translate_pipeline(pl, live_definitions)
            wf = prepare_workflow(report.pipeline)
            bundle_dir = tmp_path / f"{pl.name}_yaml"
            write_bundle(wf, bundle_dir, catalog="main", schema="bronze")

            for yml_file in bundle_dir.rglob("*.yml"):
                content = yml_file.read_text()
                try:
                    yaml.safe_load(content)
                except yaml.YAMLError as e:
                    pytest.fail(f"YAML parse error in {yml_file} for pipeline '{pl.name}': {e}")


# ---------------------------------------------------------------------------
# TestSpecificPipelines -- targeted tests for specific ADF pipeline patterns
# ---------------------------------------------------------------------------


class TestSpecificPipelines:
    """Targeted tests for specific ADF pipeline patterns."""

    def test_copy_csv_resolves_source_path(self, live_definitions):
        """Copy CSV pipeline resolves source path to a volume path."""
        pl = next((p for p in live_definitions.pipelines if p.name == "pl_copy_csv_to_delta"), None)
        if pl is None:
            pytest.skip("pl_copy_csv_to_delta not found")
        report = translate_pipeline(pl, live_definitions)
        wf = prepare_workflow(report.pipeline)

        params = wf.tasks[0].get("notebook_task", {}).get("base_parameters", {})
        source_path = params.get("source_path", "")
        assert "/Volumes/" in source_path, f"Expected volume path, got: {source_path}"

    def test_foreach_uses_task_value_inputs(self, live_definitions):
        """ForEach pipeline uses task value references for inputs."""
        pl = next((p for p in live_definitions.pipelines if p.name == "pl_foreach_copy_tables"), None)
        if pl is None:
            pytest.skip("pl_foreach_copy_tables not found")
        report = translate_pipeline(pl, live_definitions)
        wf = prepare_workflow(report.pipeline)

        foreach_task = next((t for t in wf.tasks if "for_each_task" in t), None)
        assert foreach_task is not None, "No for_each_task found in workflow"
        inputs = foreach_task["for_each_task"]["inputs"]
        assert "{{tasks." in inputs and ".values." in inputs, f"Expected task value ref in inputs: {inputs}"

    def test_if_condition_uses_structured_op(self, live_definitions):
        """IfCondition pipeline uses structured op/left/right."""
        pl = next((p for p in live_definitions.pipelines if p.name == "pl_if_condition_branch"), None)
        if pl is None:
            pytest.skip("pl_if_condition_branch not found")
        report = translate_pipeline(pl, live_definitions)
        wf = prepare_workflow(report.pipeline)

        cond_task = next((t for t in wf.tasks if "condition_task" in t), None)
        assert cond_task is not None, "No condition_task found in workflow"
        ct = cond_task["condition_task"]
        assert "op" in ct, f"condition_task missing 'op': {ct}"
        assert "left" in ct, f"condition_task missing 'left': {ct}"
        assert "right" in ct, f"condition_task missing 'right': {ct}"
        valid_ops = (
            "EQUAL_TO",
            "NOT_EQUAL",
            "GREATER_THAN",
            "GREATER_THAN_OR_EQUAL",
            "LESS_THAN",
            "LESS_THAN_OR_EQUAL",
        )
        assert ct["op"] in valid_ops, f"Unexpected op '{ct['op']}', expected one of {valid_ops}"

    def test_switch_generates_condition_chain(self, live_definitions):
        """Switch pipeline generates condition task chain."""
        pl = next((p for p in live_definitions.pipelines if p.name == "pl_switch_environment"), None)
        if pl is None:
            pytest.skip("pl_switch_environment not found")
        report = translate_pipeline(pl, live_definitions)
        wf = prepare_workflow(report.pipeline)
        assert len(wf.tasks) >= 1, "Switch pipeline produced no tasks"

    def test_set_variable_no_code_injection(self, live_definitions):
        """SetVariable pipeline has no Python code in parameters."""
        pl = next((p for p in live_definitions.pipelines if p.name == "pl_set_variable_chain"), None)
        if pl is None:
            pytest.skip("pl_set_variable_chain not found")
        report = translate_pipeline(pl, live_definitions)
        wf = prepare_workflow(report.pipeline)

        for task in wf.tasks:
            params = task.get("notebook_task", {}).get("base_parameters", {})
            for key, val in params.items():
                val_str = str(val)
                assert "__import__" not in val_str, f"Code injection in {task['task_key']}/{key}: {val_str}"
                assert "dbutils.jobs.taskValues.get" not in val_str, (
                    f"Code in param {task['task_key']}/{key}: {val_str}"
                )
