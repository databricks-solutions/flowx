"""Golden-file integration tests for the 15 pl_test_* coverage pipelines.

These tests exercise the full load -> translate -> prepare -> write pipeline
against the 15 coverage test pipelines exported from ADF, asserting structural
properties of the output: correct task counts, unique task keys, clean
parameters, valid Python notebooks, and pipeline-specific patterns.
"""

from __future__ import annotations

import ast
import re

import pytest
import yaml

from flowx.bundler.dab_writer import write_bundle
from flowx.models.ir import (
    CopyActivity,
    SetVariableActivity,
)
from flowx.parser.adf_loader import load_adf_definitions
from flowx.preparer.workflow_preparer import PreparedWorkflow, prepare_workflow
from flowx.translator.engine import translate_pipeline

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXTURES_DIR = pytest.importorskip("pathlib").Path(__file__).parent.parent / "resources" / "json"

_PL_TEST_NAMES = [
    "pl_test_copy_coverage",
    "pl_test_notebook_coverage",
    "pl_test_sparkjar_coverage",
    "pl_test_sparkpython_coverage",
    "pl_test_foreach_coverage",
    "pl_test_ifcondition_coverage",
    "pl_test_setvariable_coverage",
    "pl_test_appendvariable_coverage",
    "pl_test_switch_coverage",
    "pl_test_lookup_coverage",
    "pl_test_webactivity_coverage",
    "pl_test_delete_coverage",
    "pl_test_executepipeline_coverage",
    "pl_test_wait_coverage",
    "pl_test_filter_coverage",
]

# Forbidden patterns that must NEVER appear in base_parameters values
FORBIDDEN_PATTERNS = [
    "__import__",
    "dbutils.jobs.taskValues.get",
    "eval(",
    "exec(",
]

# ADF expression patterns that should be resolved before reaching parameters
ADF_EXPRESSION_RE = re.compile(r"@(?:activity|pipeline|variables|item|concat|equals|greater|utcNow)\(")
DAB_REF_RE = re.compile(r"\{\{[a-zA-Z0-9_.]+\}\}")


@pytest.fixture(scope="module")
def all_definitions():
    """Load all ADF definitions from the test fixtures."""
    return load_adf_definitions(FIXTURES_DIR)


@pytest.fixture(scope="module")
def test_pipelines(all_definitions):
    """Return only the pl_test_* coverage pipelines, keyed by name."""
    result = {}
    for pl in all_definitions.pipelines:
        if pl.name in _PL_TEST_NAMES:
            result[pl.name] = pl
    return result


@pytest.fixture(scope="module")
def translated_pipelines(test_pipelines, all_definitions):
    """Translate all test pipelines and return (name -> TranslationReport)."""
    return {name: translate_pipeline(pl, all_definitions) for name, pl in test_pipelines.items()}


@pytest.fixture(scope="module")
def prepared_workflows(translated_pipelines):
    """Prepare all translated pipelines and return (name -> PreparedWorkflow)."""
    return {name: prepare_workflow(rpt.pipeline) for name, rpt in translated_pipelines.items()}


@pytest.fixture(scope="module")
def bundle_dirs(prepared_workflows, tmp_path_factory):
    """Write all bundles and return (name -> bundle_path)."""
    result = {}
    for name, wf in prepared_workflows.items():
        out = tmp_path_factory.mktemp(name)
        write_bundle(wf, out, catalog="test_catalog", schema="test_schema")
        result[name] = out
    return result


def _strip_magic(content: str) -> str:
    """Strip Databricks magic comments for Python syntax checking."""
    lines = []
    for line in content.split("\n"):
        stripped = line.lstrip()
        if stripped.startswith("# MAGIC") or stripped.startswith("# COMMAND"):
            continue
        if stripped == "# Databricks notebook source":
            continue
        lines.append(line)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Structural tests across all 15 pipelines
# ---------------------------------------------------------------------------


class TestAllPipelinesLoad:
    """Verify that all 15 test pipelines load from fixtures."""

    def test_all_15_pipelines_found(self, test_pipelines):
        missing = set(_PL_TEST_NAMES) - set(test_pipelines.keys())
        assert not missing, f"Missing test pipelines: {missing}"

    def test_all_15_pipelines_translate(self, translated_pipelines):
        for name, report in translated_pipelines.items():
            assert report.pipeline is not None, f"{name}: translation produced no pipeline"
            total = report.deterministic_count + report.agentic_count + report.unsupported_count
            assert total > 0, f"{name}: no translated activities"

    def test_all_15_pipelines_prepare(self, prepared_workflows):
        for name, wf in prepared_workflows.items():
            assert isinstance(wf, PreparedWorkflow), f"{name}: not a PreparedWorkflow"
            assert len(wf.tasks) > 0, f"{name}: no tasks"


class TestBundleStructure:
    """Verify bundle output structure for every test pipeline."""

    def test_databricks_yml_exists(self, bundle_dirs):
        for name, path in bundle_dirs.items():
            assert (path / "databricks.yml").exists(), f"{name}: missing databricks.yml"

    def test_databricks_yml_has_correct_bundle_name(self, bundle_dirs):
        for name, path in bundle_dirs.items():
            content = yaml.safe_load((path / "databricks.yml").read_text())
            assert "bundle" in content
            assert content["bundle"]["name"] is not None

    def test_job_resource_yml_exists(self, bundle_dirs):
        for name, path in bundle_dirs.items():
            resources = list((path / "resources").glob("*.yml"))
            assert len(resources) >= 1, f"{name}: no resource YAMLs"

    def test_task_keys_unique_within_each_job(self, bundle_dirs):
        for name, path in bundle_dirs.items():
            for rf in (path / "resources").glob("*.yml"):
                content = yaml.safe_load(rf.read_text())
                if not content or "resources" not in content:
                    continue
                for job in content["resources"].get("jobs", {}).values():
                    task_keys = _collect_all_task_keys(job.get("tasks", []))
                    assert len(task_keys) == len(set(task_keys)), (
                        f"{name}: duplicate task keys in {rf.name}: {task_keys}"
                    )


class TestCleanParameters:
    """Verify no forbidden patterns in base_parameters across all pipelines."""

    def test_no_code_injection_in_parameters(self, prepared_workflows):
        for name, wf in prepared_workflows.items():
            for task in wf.tasks:
                params = task.get("notebook_task", {}).get("base_parameters", {})
                for key, val in params.items():
                    val_str = str(val)
                    for pattern in FORBIDDEN_PATTERNS:
                        assert pattern not in val_str, (
                            f"{name}/{task.get('task_key')}/{key}: forbidden pattern '{pattern}' found: {val_str}"
                        )

    def test_no_raw_adf_expressions_in_parameters(self, prepared_workflows):
        """No unresolved ADF @expression() calls in parameter values."""
        for name, wf in prepared_workflows.items():
            for task in wf.tasks:
                params = task.get("notebook_task", {}).get("base_parameters", {})
                for key, val in params.items():
                    val_str = str(val)
                    # Skip values that are deliberately ADF expressions for
                    # notebook interpretation (items_expression, condition_expression)
                    if key in ("items_expression", "condition_expression"):
                        continue
                    assert not ADF_EXPRESSION_RE.search(val_str), (
                        f"{name}/{task.get('task_key')}/{key}: unresolved ADF expression found: {val_str}"
                    )

    def test_dab_refs_well_formed(self, prepared_workflows):
        """All {{...}} references use valid DAB syntax."""
        for name, wf in prepared_workflows.items():
            for task in wf.tasks:
                params = task.get("notebook_task", {}).get("base_parameters", {})
                for key, val in params.items():
                    val_str = str(val)
                    if "{{" in val_str:
                        assert DAB_REF_RE.search(val_str), (
                            f"{name}/{task.get('task_key')}/{key}: malformed DAB ref: {val_str}"
                        )


class TestNotebookValidity:
    """Verify all generated notebooks pass ast.parse()."""

    def test_all_notebooks_valid_python(self, bundle_dirs):
        for name, path in bundle_dirs.items():
            src_dir = path / "src"
            if not src_dir.exists():
                continue
            for nb_file in src_dir.rglob("*.py"):
                content = nb_file.read_text()
                python_code = _strip_magic(content)
                try:
                    ast.parse(python_code)
                except SyntaxError as exc:
                    pytest.fail(f"{name}/{nb_file.relative_to(path)}: syntax error: {exc}")


# ---------------------------------------------------------------------------
# Pipeline-specific structural assertions
# ---------------------------------------------------------------------------


class TestCopyCoverage:
    """pl_test_copy_coverage: file source + SQL source copies."""

    def test_has_two_copy_tasks(self, translated_pipelines):
        report = translated_pipelines.get("pl_test_copy_coverage")
        if report is None:
            pytest.skip("pl_test_copy_coverage not in fixtures")
        copies = [t for t in report.pipeline.tasks if isinstance(t, CopyActivity)]
        assert len(copies) == 2

    def test_notebook_has_cloudfiles_or_jdbc(self, bundle_dirs):
        path = bundle_dirs.get("pl_test_copy_coverage")
        if path is None:
            pytest.skip("pl_test_copy_coverage not in bundle_dirs")
        notebooks = list((path / "src" / "notebooks").glob("*.py"))
        assert len(notebooks) >= 2
        all_content = " ".join(nb.read_text() for nb in notebooks)
        # At least one should use cloudFiles (Auto Loader) or JDBC
        has_auto_loader = "cloudFiles" in all_content
        has_jdbc = "jdbc" in all_content
        assert has_auto_loader or has_jdbc, "Expected at least one cloudFiles or JDBC notebook"


class TestForEachCoverage:
    """pl_test_foreach_coverage: ForEach with task value inputs."""

    def test_has_foreach_task(self, prepared_workflows):
        wf = prepared_workflows.get("pl_test_foreach_coverage")
        if wf is None:
            pytest.skip("pl_test_foreach_coverage not in prepared_workflows")
        foreach_tasks = [t for t in wf.tasks if "for_each_task" in t]
        assert len(foreach_tasks) >= 1, "Expected at least one for_each_task"

    def test_foreach_has_inputs(self, prepared_workflows):
        wf = prepared_workflows.get("pl_test_foreach_coverage")
        if wf is None:
            pytest.skip("pl_test_foreach_coverage not in prepared_workflows")
        foreach_task = next((t for t in wf.tasks if "for_each_task" in t), None)
        assert foreach_task is not None
        inputs = foreach_task["for_each_task"].get("inputs", "")
        # Inputs should reference a parameter or task value
        assert inputs, "for_each_task inputs should not be empty"


class TestIfConditionCoverage:
    """pl_test_ifcondition_coverage: structured op/left/right conditions."""

    def test_has_condition_tasks(self, prepared_workflows):
        wf = prepared_workflows.get("pl_test_ifcondition_coverage")
        if wf is None:
            pytest.skip("pl_test_ifcondition_coverage not in prepared_workflows")
        cond_tasks = [t for t in wf.tasks if "condition_task" in t]
        assert len(cond_tasks) >= 1, "Expected at least one condition_task"

    def test_condition_uses_op_left_right(self, prepared_workflows):
        wf = prepared_workflows.get("pl_test_ifcondition_coverage")
        if wf is None:
            pytest.skip("pl_test_ifcondition_coverage not in prepared_workflows")
        for task in wf.tasks:
            if "condition_task" not in task:
                continue
            ct = task["condition_task"]
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
            assert ct["op"] in valid_ops, f"Unexpected op '{ct['op']}'"


class TestSwitchCoverage:
    """pl_test_switch_coverage: chained condition_task with unique keys."""

    def test_has_condition_task(self, prepared_workflows):
        wf = prepared_workflows.get("pl_test_switch_coverage")
        if wf is None:
            pytest.skip("pl_test_switch_coverage not in prepared_workflows")
        cond_tasks = [t for t in wf.tasks if "condition_task" in t]
        assert len(cond_tasks) >= 1, "Expected at least one condition_task from Switch"

    def test_condition_chain_unique_keys(self, bundle_dirs):
        """All task keys in the switch output are unique."""
        path = bundle_dirs.get("pl_test_switch_coverage")
        if path is None:
            pytest.skip("pl_test_switch_coverage not in bundle_dirs")
        for rf in (path / "resources").glob("*.yml"):
            content = yaml.safe_load(rf.read_text())
            if not content or "resources" not in content:
                continue
            for job in content["resources"].get("jobs", {}).values():
                all_keys = _collect_all_task_keys(job.get("tasks", []))
                assert len(all_keys) == len(set(all_keys)), f"Duplicate keys in switch: {all_keys}"


class TestSetVariableCoverage:
    """pl_test_setvariable_coverage: literal, notebook_code, dab_ref kinds."""

    def test_translates_all_five(self, translated_pipelines):
        report = translated_pipelines.get("pl_test_setvariable_coverage")
        if report is None:
            pytest.skip("pl_test_setvariable_coverage not in translated_pipelines")
        svs = [t for t in report.pipeline.tasks if isinstance(t, SetVariableActivity)]
        assert len(svs) == 5

    def test_utcnow_uses_notebook_code(self, translated_pipelines):
        report = translated_pipelines.get("pl_test_setvariable_coverage")
        if report is None:
            pytest.skip("pl_test_setvariable_coverage not in translated_pipelines")
        utcnow_sv = next(
            (
                t
                for t in report.pipeline.tasks
                if isinstance(t, SetVariableActivity) and t.variable_name == "runTimestamp"
            ),
            None,
        )
        assert utcnow_sv is not None, "Expected SetVariable for runTimestamp"
        assert utcnow_sv.value_kind == "notebook_code"

    def test_pipeline_param_uses_dab_ref(self, translated_pipelines):
        report = translated_pipelines.get("pl_test_setvariable_coverage")
        if report is None:
            pytest.skip("pl_test_setvariable_coverage not in translated_pipelines")
        param_sv = next(
            (
                t
                for t in report.pipeline.tasks
                if isinstance(t, SetVariableActivity) and t.variable_name == "environment"
            ),
            None,
        )
        assert param_sv is not None, "Expected SetVariable for environment"
        assert param_sv.value_kind == "dab_ref"
        assert "job.parameters" in param_sv.variable_value


class TestWebActivityCoverage:
    """pl_test_webactivity_coverage: resolved headers and auth."""

    def test_no_expression_dicts_in_params(self, prepared_workflows):
        wf = prepared_workflows.get("pl_test_webactivity_coverage")
        if wf is None:
            pytest.skip("pl_test_webactivity_coverage not in prepared_workflows")
        for task in wf.tasks:
            params = task.get("notebook_task", {}).get("base_parameters", {})
            for key, val in params.items():
                assert not isinstance(val, dict), f"Expression dict leaked into params: {key}={val}"


class TestNotebookCoverage:
    """pl_test_notebook_coverage: base_parameters resolved to DAB refs."""

    def test_params_are_resolved(self, prepared_workflows):
        wf = prepared_workflows.get("pl_test_notebook_coverage")
        if wf is None:
            pytest.skip("pl_test_notebook_coverage not in prepared_workflows")
        for task in wf.tasks:
            params = task.get("notebook_task", {}).get("base_parameters", {})
            for key, val in params.items():
                assert not isinstance(val, dict), f"Expression dict in params: {key}={val}"


class TestLookupCoverage:
    """pl_test_lookup_coverage: lookup generates notebooks with task values."""

    def test_has_two_lookup_tasks(self, translated_pipelines):
        report = translated_pipelines.get("pl_test_lookup_coverage")
        if report is None:
            pytest.skip("pl_test_lookup_coverage not in translated_pipelines")
        from flowx.models.ir import LookupActivity

        lookups = [t for t in report.pipeline.tasks if isinstance(t, LookupActivity)]
        assert len(lookups) == 2


class TestDeleteCoverage:
    """pl_test_delete_coverage: delete generates notebook."""

    def test_has_delete_task(self, translated_pipelines):
        report = translated_pipelines.get("pl_test_delete_coverage")
        if report is None:
            pytest.skip("pl_test_delete_coverage not in translated_pipelines")
        from flowx.models.ir import DeleteActivity

        deletes = [t for t in report.pipeline.tasks if isinstance(t, DeleteActivity)]
        assert len(deletes) == 1


class TestWaitCoverage:
    """pl_test_wait_coverage: wait generates notebooks."""

    def test_has_two_wait_tasks(self, translated_pipelines):
        report = translated_pipelines.get("pl_test_wait_coverage")
        if report is None:
            pytest.skip("pl_test_wait_coverage not in translated_pipelines")
        from flowx.models.ir import WaitActivity

        waits = [t for t in report.pipeline.tasks if isinstance(t, WaitActivity)]
        assert len(waits) == 2


class TestFilterCoverage:
    """pl_test_filter_coverage: filter generates notebooks."""

    def test_has_filter_tasks(self, translated_pipelines):
        report = translated_pipelines.get("pl_test_filter_coverage")
        if report is None:
            pytest.skip("pl_test_filter_coverage not in translated_pipelines")
        from flowx.models.ir import FilterActivity

        filters = [t for t in report.pipeline.tasks if isinstance(t, FilterActivity)]
        assert len(filters) >= 1


class TestAppendVariableCoverage:
    """pl_test_appendvariable_coverage: append variable generates notebooks."""

    def test_has_append_tasks(self, translated_pipelines):
        report = translated_pipelines.get("pl_test_appendvariable_coverage")
        if report is None:
            pytest.skip("pl_test_appendvariable_coverage not in translated_pipelines")
        from flowx.models.ir import AppendVariableActivity

        appends = [t for t in report.pipeline.tasks if isinstance(t, AppendVariableActivity)]
        assert len(appends) == 3


class TestExecutePipelineCoverage:
    """pl_test_executepipeline_coverage: generates run_job_task."""

    def test_has_run_job_tasks(self, prepared_workflows):
        wf = prepared_workflows.get("pl_test_executepipeline_coverage")
        if wf is None:
            pytest.skip("pl_test_executepipeline_coverage not in prepared_workflows")
        run_jobs = [t for t in wf.tasks if "run_job_task" in t]
        assert len(run_jobs) >= 1


class TestSparkJarCoverage:
    """pl_test_sparkjar_coverage: generates spark_jar_task."""

    def test_has_spark_jar_task(self, prepared_workflows):
        wf = prepared_workflows.get("pl_test_sparkjar_coverage")
        if wf is None:
            pytest.skip("pl_test_sparkjar_coverage not in prepared_workflows")
        jar_tasks = [t for t in wf.tasks if "spark_jar_task" in t]
        assert len(jar_tasks) >= 1


class TestSparkPythonCoverage:
    """pl_test_sparkpython_coverage: generates spark_python_task."""

    def test_has_spark_python_task(self, prepared_workflows):
        wf = prepared_workflows.get("pl_test_sparkpython_coverage")
        if wf is None:
            pytest.skip("pl_test_sparkpython_coverage not in prepared_workflows")
        py_tasks = [t for t in wf.tasks if "spark_python_task" in t]
        assert len(py_tasks) >= 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _collect_all_task_keys(tasks: list[dict]) -> list[str]:
    """Recursively collect all task_keys from tasks including nested condition chains."""
    keys = []
    for t in tasks:
        if "task_key" in t:
            keys.append(t["task_key"])
        # Recurse into condition_task branches
        ct = t.get("condition_task", {})
        if ct:
            for branch_name in ("if_true", "if_false"):
                branch = ct.get(branch_name, [])
                if isinstance(branch, list):
                    keys.extend(_collect_all_task_keys(branch))
        # Recurse into for_each_task
        fe = t.get("for_each_task", {})
        if fe:
            inner_tasks = fe.get("tasks", [])
            if isinstance(inner_tasks, list):
                keys.extend(_collect_all_task_keys(inner_tasks))
    return keys
