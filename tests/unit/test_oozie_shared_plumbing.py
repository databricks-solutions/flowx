"""Shared IR/preparer behaviour the Oozie source relies on (explicit run_if, condition-outcome edges)."""

from __future__ import annotations

from flowx.bundler.dab_writer import pipeline_dict_to_ir
from flowx.ir_serde import pipeline_to_dict
from flowx.models.ir import Dependency, NotebookActivity, Pipeline
from flowx.preparer.workflow_preparer import build_common_task_fields


def _notebook(key: str, **kwargs) -> NotebookActivity:
    return NotebookActivity(name=key, task_key=key, notebook_path=f"/x/{key}", **kwargs)


def test_explicit_run_if_overrides_derived_value():
    task = build_common_task_fields(
        _notebook(
            "merge", depends_on=[Dependency(task_key="a"), Dependency(task_key="b")], run_if="AT_LEAST_ONE_SUCCESS"
        )
    )
    assert task["run_if"] == "AT_LEAST_ONE_SUCCESS"


def test_condition_outcome_is_emitted_on_dependency():
    task = build_common_task_fields(_notebook("branch", depends_on=[Dependency(task_key="decide", outcome="true")]))
    assert task["depends_on"] == [{"task_key": "decide", "outcome": "true"}]
    assert "run_if" not in task


def test_non_condition_outcomes_are_not_emitted():
    task = build_common_task_fields(_notebook("handler", depends_on=[Dependency(task_key="a", outcome="Failed")]))
    assert task["depends_on"] == [{"task_key": "a"}]
    assert task["run_if"] == "AT_LEAST_ONE_FAILED"


def test_run_if_round_trips_through_report_serde():
    pipeline = Pipeline(name="p", tasks=[_notebook("merge", run_if="AT_LEAST_ONE_SUCCESS")])
    restored, _ = pipeline_dict_to_ir(pipeline_to_dict(pipeline))
    assert restored.tasks[0].run_if == "AT_LEAST_ONE_SUCCESS"
