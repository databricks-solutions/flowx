"""Regression tests for three ADF -> Databricks translation fixes.

1. ForEach inputs-bridge notebooks must carry the ``# Databricks notebook
   source`` marker or ``bundle validate`` rejects the notebook_task.
2. A join after an ``IfCondition`` must use ``run_if: NONE_FAILED`` so a
   failure in the *taken* branch propagates (``AT_LEAST_ONE_SUCCESS`` masks it).
3. Synthesised ``_init_<var>`` tasks that nothing references must be pruned at
   package time, after the agentic merge and after preparation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from flowx import ir_serde
from flowx.bundler.dab_writer import pipeline_dict_to_ir, write_bundle
from flowx.models.adf_ast import AdfActivity, AdfDefinitions, AdfDependency, AdfPipeline, AdfVariable
from flowx.models.dab import DabNotebook
from flowx.preparer.activity_preparers.naming import notebook_filename
from flowx.preparer.code_generator import render_bridge_notebook
from flowx.preparer.workflow_preparer import (
    PreparedActivity,
    PreparedArtifacts,
    PreparedWorkflow,
    find_dead_variable_inits,
    prepare_workflow,
)
from flowx.sources.adf.translate import translate_pipeline

_DEFS = AdfDefinitions(pipelines=[], datasets={}, linked_services={}, triggers=[])

# ---------------------------------------------------------------------------
# Bug 1 — inputs-bridge notebook header
# ---------------------------------------------------------------------------


def test_inputs_bridge_starts_with_notebook_source_marker() -> None:
    source = render_bridge_notebook(
        "['a']",
        [],
        [],
        "items",
        title="ForEach inputs bridge: Loop",
    )
    assert source.splitlines()[0] == "# Databricks notebook source"


def test_inputs_bridge_marker_precedes_imports() -> None:
    source = render_bridge_notebook(
        "[1, 2]",
        ["import json"],
        ["item"],
        "items",
        title="ForEach inputs bridge: Loop",
    )
    lines = source.splitlines()
    assert lines[0] == "# Databricks notebook source"
    assert "dbutils.jobs.taskValues.set(key='items', value=_bridge_value)" in source


# ---------------------------------------------------------------------------
# Bug 2 — IfCondition join run_if
# ---------------------------------------------------------------------------


def _condition_join_tasks() -> list[dict]:
    """A condition task, a true-branch terminal, and a join that depended on the
    condition plus an always-on sibling (mirrors a ForEach inputs-bridge)."""
    return [
        {"task_key": "CheckRunMode", "condition_task": {"op": "EQUAL_TO"}},
        {
            "task_key": "NotifyFull",
            "depends_on": [{"task_key": "CheckRunMode", "outcome": "true"}],
        },
        {"task_key": "items_bridge"},
        {
            "task_key": "ProcessItems",
            "depends_on": [
                {"task_key": "CheckRunMode"},
                {"task_key": "items_bridge"},
            ],
        },
    ]


def _bundled_tasks(tasks: list[dict[str, Any]], output_dir: Path) -> dict[str, dict[str, Any]]:
    workflow = PreparedWorkflow(name="job", tasks=tasks, notebooks=[], secrets=[], setup_tasks=[])
    write_bundle(workflow, output_dir)
    job = yaml.safe_load((output_dir / "resources" / "job.yml").read_text())["resources"]["jobs"]["job"]
    return {task["task_key"]: task for task in job["tasks"]}


def _dependency_keys(task: dict[str, Any]) -> set[str]:
    return {dependency["task_key"] for dependency in task["depends_on"]}


def test_condition_join_uses_none_failed_not_at_least_one_success(tmp_path: Path) -> None:
    join = _bundled_tasks(_condition_join_tasks(), tmp_path)["ProcessItems"]

    assert join["run_if"] == "NONE_FAILED"
    assert _dependency_keys(join) == {"NotifyFull", "items_bridge"}


def test_switch_case_join_uses_none_failed(tmp_path: Path) -> None:
    tasks: list[dict[str, Any]] = [
        {"task_key": "RouteMode", "condition_task": {"op": "EQUAL_TO"}},
        {
            "task_key": "RouteMode_case_full",
            "condition_task": {"op": "EQUAL_TO"},
            "depends_on": [{"task_key": "RouteMode", "outcome": "false"}],
        },
        {"task_key": "LoadDelta", "depends_on": [{"task_key": "RouteMode", "outcome": "true"}]},
        {"task_key": "LoadFull", "depends_on": [{"task_key": "RouteMode_case_full", "outcome": "true"}]},
        {"task_key": "Publish", "depends_on": [{"task_key": "RouteMode"}]},
    ]
    join = _bundled_tasks(tasks, tmp_path)["Publish"]

    assert join["run_if"] == "NONE_FAILED"
    assert _dependency_keys(join) == {"LoadDelta", "LoadFull"}


def test_non_join_task_keeps_its_run_if(tmp_path: Path) -> None:
    tasks = _condition_join_tasks()
    tasks.append({"task_key": "Cleanup", "depends_on": [{"task_key": "items_bridge"}], "run_if": "ALL_DONE"})
    tasks.append({"task_key": "Audit", "depends_on": [{"task_key": "items_bridge"}]})
    by_key = _bundled_tasks(tasks, tmp_path)

    assert by_key["Cleanup"]["run_if"] == "ALL_DONE"
    assert "run_if" not in by_key["Audit"]
    assert "run_if" not in by_key["NotifyFull"]


def test_condition_join_keeps_explicit_run_if(tmp_path: Path) -> None:
    tasks = _condition_join_tasks()
    next(task for task in tasks if task["task_key"] == "ProcessItems")["run_if"] = "ALL_DONE"

    assert _bundled_tasks(tasks, tmp_path)["ProcessItems"]["run_if"] == "ALL_DONE"


# ---------------------------------------------------------------------------
# Bug 3 — dead _init_<var> pruning: reference matching
# ---------------------------------------------------------------------------


def _find_dead(
    tasks: list[dict[str, Any]],
    init_keys: set[str],
    *,
    notebooks: tuple[DabNotebook, ...] = (),
    inner_workflows: tuple[PreparedWorkflow, ...] = (),
) -> frozenset[str]:
    prepared_inits = {key: PreparedActivity(task={"task_key": key}) for key in init_keys}
    artifacts = PreparedArtifacts(notebooks=notebooks, inner_workflows=inner_workflows)
    return find_dead_variable_inits(tasks, artifacts, prepared_inits)


def test_find_dead_reports_init_when_explicit_setter_dominates() -> None:
    tasks = [
        {"task_key": "_init_runMode"},
        {"task_key": "SetRunMode"},
        {
            "task_key": "CheckRunMode",
            "condition_task": {"left": "{{tasks.SetRunMode.values.runMode}}", "right": "full"},
        },
    ]
    assert _find_dead(tasks, {"_init_runMode"}) == {"_init_runMode"}


def test_find_dead_keeps_init_when_value_is_read() -> None:
    tasks = [
        {"task_key": "_init_runMode"},
        {
            "task_key": "CheckRunMode",
            "condition_task": {"left": "{{tasks._init_runMode.values.runMode}}", "right": "full"},
        },
    ]
    assert _find_dead(tasks, {"_init_runMode"}) == frozenset()


def test_find_dead_keeps_init_when_referenced_by_depends_on() -> None:
    tasks = [
        {"task_key": "_init_runMode"},
        {"task_key": "Next", "depends_on": [{"task_key": "_init_runMode"}]},
    ]
    assert _find_dead(tasks, {"_init_runMode"}) == frozenset()


def test_find_dead_keeps_init_named_as_a_whole_parameter_value() -> None:
    tasks = [
        {"task_key": "_init_items"},
        {"task_key": "AddItem", "notebook_task": {"base_parameters": {"source_task_key": "_init_items"}}},
    ]
    assert _find_dead(tasks, {"_init_items"}) == frozenset()


def test_find_dead_keeps_init_read_from_notebook_code() -> None:
    tasks = [{"task_key": "_init_runMode"}, {"task_key": "Web"}]
    notebook = DabNotebook(
        relative_path="src/notebooks/web.py",
        content="body = f\"{dbutils.jobs.taskValues.get(taskKey='_init_runMode', key='runMode')}\"",
    )
    assert _find_dead(tasks, {"_init_runMode"}, notebooks=(notebook,)) == frozenset()


def test_find_dead_keeps_init_read_from_inner_workflow() -> None:
    tasks = [{"task_key": "_init_runMode"}, {"task_key": "Loop"}]
    inner = PreparedWorkflow(
        name="Loop_inner_tasks",
        tasks=[{"task_key": "Body", "notebook_task": {"base_parameters": {"v": "{{tasks._init_runMode.values.v}}"}}}],
        notebooks=[],
        secrets=[],
        setup_tasks=[],
    )
    assert _find_dead(tasks, {"_init_runMode"}, inner_workflows=(inner,)) == frozenset()


def test_find_dead_keeps_init_read_by_another_init() -> None:
    tasks = [
        {"task_key": "_init_base"},
        {"task_key": "_init_derived", "notebook_task": {"base_parameters": {"v": "{{tasks._init_base.values.base}}"}}},
        {"task_key": "Reader", "notebook_task": {"base_parameters": {"v": "{{tasks._init_derived.values.derived}}"}}},
    ]
    assert _find_dead(tasks, {"_init_base", "_init_derived"}) == frozenset()


def test_find_dead_reports_whole_unused_init_chain() -> None:
    tasks = [
        {"task_key": "_init_a"},
        {"task_key": "_init_b", "notebook_task": {"base_parameters": {"v": "{{tasks._init_a.values.a}}"}}},
        {"task_key": "Reader"},
    ]
    assert _find_dead(tasks, {"_init_a", "_init_b"}) == {"_init_a", "_init_b"}


def test_find_dead_keeps_init_read_from_notebook_code_with_double_quotes() -> None:
    tasks = [{"task_key": "_init_runMode"}, {"task_key": "Web"}]
    notebook = DabNotebook(
        relative_path="src/notebooks/web.py",
        content='mode = dbutils.jobs.taskValues.get(taskKey="_init_runMode", key="runMode")',
    )
    assert _find_dead(tasks, {"_init_runMode"}, notebooks=(notebook,)) == frozenset()


def test_find_dead_keeps_init_named_in_foreach_inner_job_parameters() -> None:
    tasks = [
        {"task_key": "_init_items"},
        {
            "task_key": "Loop",
            "for_each_task": {
                "inputs": "[1, 2]",
                "task": {
                    "task_key": "Loop_iteration",
                    "run_job_task": {"job_id": "1", "job_parameters": {"source_task_key": "_init_items"}},
                },
            },
        },
    ]
    assert _find_dead(tasks, {"_init_items"}) == frozenset()


def test_find_dead_ignores_unrelated_field_equal_to_key() -> None:
    tasks = [
        {"task_key": "_init_runMode"},
        {
            "task_key": "Reader",
            "description": "_init_runMode",
            "tags": {"origin": "_init_runMode"},
            "sql_task": {"parameters": {"config": {"origin": "_init_runMode"}}},
        },
    ]
    assert _find_dead(tasks, {"_init_runMode"}) == {"_init_runMode"}


def test_find_dead_only_touches_synthesized_keys() -> None:
    """A task the translator did not synthesise is never reported, even when unreferenced."""
    tasks = [{"task_key": "_init_report"}, {"task_key": "Downstream"}]
    assert _find_dead(tasks, set()) == frozenset()


def test_find_dead_reports_init_despite_prefix_collision() -> None:
    """A dead ``_init_x`` is reported even when its key text appears inside an
    unrelated key or path; a bare substring match must not keep it alive."""
    tasks = [
        {"task_key": "_init_x"},
        {"task_key": "_init_x_2"},
        {
            "task_key": "Reader",
            "notebook_task": {"notebook_path": "/Shared/_init_x_report"},
            "condition_task": {"left": "{{tasks._init_x_2.values.x2}}", "right": "2"},
        },
    ]
    assert _find_dead(tasks, {"_init_x", "_init_x_2"}) == {"_init_x"}


# ---------------------------------------------------------------------------
# Bug 3 — dead _init_<var> pruning: translate -> report -> package
# ---------------------------------------------------------------------------


def _notebook_reading(name: str, variable: str, depends_on: str | None = None) -> AdfActivity:
    return AdfActivity(
        name=name,
        type="DatabricksNotebook",
        depends_on=[AdfDependency(activity=depends_on)] if depends_on else None,
        type_properties={
            "notebookPath": "/Shared/reader",
            "baseParameters": {"v": {"value": f"@variables('{variable}')", "type": "Expression"}},
        },
    )


def _report_dict(pipeline: AdfPipeline) -> dict[str, Any]:
    return ir_serde.pipeline_to_dict(translate_pipeline(pipeline, _DEFS).pipeline)


def _package(report: dict[str, Any]) -> PreparedWorkflow:
    pipeline, _parameters = pipeline_dict_to_ir(report)
    return prepare_workflow(pipeline)


def _task_keys(workflow: PreparedWorkflow) -> set[str]:
    return {task["task_key"] for task in workflow.tasks}


def _has_init_notebook(workflow: PreparedWorkflow, init_key: str) -> bool:
    filename = notebook_filename(init_key, init_key)
    return any(notebook.relative_path.endswith(f"/{filename}") for notebook in workflow.notebooks)


def test_dominated_init_is_pruned_with_its_notebook() -> None:
    pipeline = AdfPipeline(
        name="p",
        variables={"runMode": AdfVariable(type="String", default_value="full")},
        activities=[
            AdfActivity(
                name="SetRunMode",
                type="SetVariable",
                type_properties={"variableName": "runMode", "value": "delta"},
            ),
            _notebook_reading("Reader", "runMode", depends_on="SetRunMode"),
        ],
    )
    report = _report_dict(pipeline)
    assert "_init_runMode" in {task["task_key"] for task in report["tasks"]}
    assert report["synthesized_variable_init_keys"] == ["_init_runMode"]

    workflow = _package(report)
    assert "_init_runMode" not in _task_keys(workflow)
    assert "SetRunMode" in _task_keys(workflow)
    assert not _has_init_notebook(workflow, "_init_runMode")
    assert _has_init_notebook(workflow, "SetRunMode")


def _unresolved_init_report(*activities: AdfActivity) -> dict[str, Any]:
    report = _report_dict(
        AdfPipeline(
            name="p",
            variables={"runMode": AdfVariable(type="String", default_value="full")},
            activities=list(activities),
        )
    )
    init_task = next(task for task in report["tasks"] if task["task_key"] == "_init_runMode")
    init_task.update(value_kind="unresolved", raw_expression="@pipeline().globalParameters.mode")
    return report


def _setup_task_keys(workflow: PreparedWorkflow) -> set[str]:
    return {setup_task.config.get("task_key") for setup_task in workflow.setup_tasks}


def test_dominated_init_is_pruned_with_its_setup_tasks() -> None:
    workflow = _package(
        _unresolved_init_report(
            AdfActivity(
                name="SetRunMode", type="SetVariable", type_properties={"variableName": "runMode", "value": "x"}
            ),
            _notebook_reading("Reader", "runMode", depends_on="SetRunMode"),
        )
    )
    assert "_init_runMode" not in _task_keys(workflow)
    assert "_init_runMode" not in _setup_task_keys(workflow)


def test_read_init_keeps_its_setup_tasks() -> None:
    workflow = _package(_unresolved_init_report(_notebook_reading("Reader", "runMode")))
    assert "_init_runMode" in _task_keys(workflow)
    assert "_init_runMode" in _setup_task_keys(workflow)


def test_read_init_is_kept() -> None:
    pipeline = AdfPipeline(
        name="p",
        variables={"runMode": AdfVariable(type="String", default_value="full")},
        activities=[_notebook_reading("Reader", "runMode")],
    )
    workflow = _package(_report_dict(pipeline))
    assert "_init_runMode" in _task_keys(workflow)
    assert _has_init_notebook(workflow, "_init_runMode")


def test_init_feeding_append_variable_is_kept() -> None:
    """AppendVariable binds its prior writer only at prepare time, so IR-level pruning would drop this init."""
    pipeline = AdfPipeline(
        name="p",
        variables={"items": AdfVariable(type="Array", default_value=[])},
        activities=[
            AdfActivity(name="AddItem", type="AppendVariable", type_properties={"variableName": "items", "value": "a"}),
        ],
    )
    workflow = _package(_report_dict(pipeline))
    assert "_init_items" in _task_keys(workflow)


def test_init_read_by_merged_agentic_task_is_kept() -> None:
    pipeline = AdfPipeline(
        name="p",
        variables={"runMode": AdfVariable(type="String", default_value="full")},
        activities=[
            AdfActivity(
                name="SetRunMode",
                type="SetVariable",
                type_properties={"variableName": "runMode", "value": "delta"},
            ),
        ],
    )
    report = _report_dict(pipeline)
    report["tasks"].append(
        {
            "name": "AgentTask",
            "task_key": "AgentTask",
            "type": "NotebookActivity",
            "notebook_path": "/Shared/agent",
            "base_parameters": {"v": "{{tasks._init_runMode.values.runMode}}"},
        }
    )
    workflow = _package(report)
    assert "_init_runMode" in _task_keys(workflow)


def test_report_omits_init_keys_without_default_valued_variables() -> None:
    pipeline = AdfPipeline(
        name="p",
        activities=[AdfActivity(name="W", type="Wait", type_properties={"waitTimeInSeconds": 1})],
    )
    assert "synthesized_variable_init_keys" not in _report_dict(pipeline)
