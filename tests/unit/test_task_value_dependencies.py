"""Tests for the convert pass that gives task-value readers a dependency on the task they read."""

from __future__ import annotations

import copy

from flowx.models.ir import (
    Activity,
    Dependency,
    ExecutePipelineActivity,
    ForEachActivity,
    IfConditionActivity,
    MotifActivity,
    NotebookActivity,
    Pipeline,
    RunJobActivity,
    SetVariableActivity,
    SwitchActivity,
    SwitchCase,
    WaitActivity,
)
from flowx.sources.adf.task_value_dependencies import backfill_task_value_dependencies


def test_reader_with_no_path_to_its_producer_gains_the_edge() -> None:
    pipeline = _pipeline(_setter("set_x"), _reader("reader", "{{tasks.set_x.values.x}}"))

    backfilled, warnings = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "reader")) == ["set_x"]
    assert warnings == []


def test_reader_already_downstream_through_another_task_gains_no_edge() -> None:
    pipeline = _pipeline(
        _setter("producer"),
        _wait("middle", depends_on=["producer"]),
        _reader("reader", "{{tasks.producer.values.x}}", depends_on=["middle"]),
    )

    backfilled, warnings = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "reader")) == ["middle"]
    assert warnings == []


def test_existing_direct_edge_is_not_duplicated() -> None:
    pipeline = _pipeline(_setter("set_x"), _reader("reader", "{{tasks.set_x.values.x}}", depends_on=["set_x"]))

    backfilled, _ = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "reader")) == ["set_x"]


def test_for_each_items_reference_gains_the_edge() -> None:
    loop = ForEachActivity(name="loop", task_key="loop", items_expression="{{tasks.lookup.values.value}}")
    pipeline = _pipeline(_setter("lookup"), loop)

    backfilled, _ = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "loop")) == ["lookup"]


def test_execute_pipeline_and_run_job_parameters_gain_the_edge() -> None:
    execute = ExecutePipelineActivity(
        name="call_child",
        task_key="call_child",
        pipeline_name="child",
        parameters={"run_date": "{{tasks.set_date.values.run_date}}"},
    )
    run_job = RunJobActivity(
        name="run_job",
        task_key="run_job",
        job_name="other",
        job_parameters={"run_date": "{{tasks.set_date.values.run_date}}"},
    )
    pipeline = _pipeline(_setter("set_date"), execute, run_job)

    backfilled, _ = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "call_child")) == ["set_date"]
    assert _dependency_keys(_task(backfilled, "run_job")) == ["set_date"]


def test_for_each_body_reading_an_outer_task_puts_the_edge_on_the_loop() -> None:
    inner = _reader("inner", "{{tasks.set_x.values.x}}")
    loop = ForEachActivity(name="loop", task_key="loop", items_expression="[1, 2]", inner_activities=[inner])
    pipeline = _pipeline(_setter("set_x"), loop)

    backfilled, warnings = _backfill(pipeline)

    backfilled_loop = _task(backfilled, "loop")
    assert isinstance(backfilled_loop, ForEachActivity)
    assert _dependency_keys(backfilled_loop) == ["set_x"]
    assert _dependency_keys(backfilled_loop.inner_activities[0]) == []
    assert warnings == []


def test_siblings_inside_an_if_branch_get_the_edge_within_the_branch() -> None:
    branch = IfConditionActivity(
        name="check",
        task_key="check",
        op="EQUAL_TO",
        left="a",
        right="a",
        if_true_activities=[_setter("set_y"), _reader("use_y", "{{tasks.set_y.values.y}}")],
    )
    pipeline = _pipeline(branch)

    backfilled, _ = _backfill(pipeline)

    backfilled_branch = _task(backfilled, "check")
    assert isinstance(backfilled_branch, IfConditionActivity)
    assert _dependency_keys(backfilled_branch.if_true_activities[1]) == ["set_y"]
    assert _dependency_keys(backfilled_branch) == []


def test_switch_case_task_reading_an_outer_task_gains_the_edge_itself() -> None:
    case_reader = _reader("case_reader", "{{tasks.set_mode.values.mode}}")
    switch = SwitchActivity(
        name="route",
        task_key="route",
        on_expression="{{tasks.set_status.values.status}}",
        cases=[SwitchCase(value="running", activities=[case_reader])],
    )
    pipeline = _pipeline(_setter("set_status"), _setter("set_mode"), switch)

    backfilled, _ = _backfill(pipeline)

    backfilled_switch = _task(backfilled, "route")
    assert isinstance(backfilled_switch, SwitchActivity)
    assert _dependency_keys(backfilled_switch) == ["set_status"]
    assert _dependency_keys(backfilled_switch.cases[0].activities[0]) == ["set_mode"]


def test_if_branch_task_reading_an_outer_task_gains_the_edge_and_the_condition_does_not() -> None:
    branch = IfConditionActivity(
        name="check",
        task_key="check",
        op="EQUAL_TO",
        left="a",
        right="a",
        if_true_activities=[_reader("on_true", "{{tasks.set_x.values.x}}")],
        if_false_activities=[_wait("on_false")],
    )
    pipeline = _pipeline(_setter("set_x"), branch)

    backfilled, _ = _backfill(pipeline)

    backfilled_branch = _task(backfilled, "check")
    assert isinstance(backfilled_branch, IfConditionActivity)
    assert _dependency_keys(backfilled_branch) == []
    assert _dependency_keys(backfilled_branch.if_true_activities[0]) == ["set_x"]


def test_branch_task_already_after_the_outer_producer_through_its_condition_gains_no_edge() -> None:
    branch = IfConditionActivity(
        name="check",
        task_key="check",
        op="EQUAL_TO",
        left="a",
        right="a",
        depends_on=[Dependency(task_key="set_x")],
        if_true_activities=[_reader("on_true", "{{tasks.set_x.values.x}}")],
    )
    pipeline = _pipeline(_setter("set_x"), branch)

    backfilled, _ = _backfill(pipeline)

    backfilled_branch = _task(backfilled, "check")
    assert isinstance(backfilled_branch, IfConditionActivity)
    assert _dependency_keys(backfilled_branch.if_true_activities[0]) == []


def test_reading_a_task_in_the_other_branch_of_the_same_condition_gets_no_edge_and_a_warning() -> None:
    branch = IfConditionActivity(
        name="check",
        task_key="check",
        op="EQUAL_TO",
        left="a",
        right="a",
        if_true_activities=[_reader("on_true", "{{tasks.set_on_false.values.x}}")],
        if_false_activities=[_setter("set_on_false")],
    )
    pipeline = _pipeline(branch)

    backfilled, warnings = _backfill(pipeline)

    backfilled_branch = _task(backfilled, "check")
    assert isinstance(backfilled_branch, IfConditionActivity)
    assert _dependency_keys(backfilled_branch.if_true_activities[0]) == []
    assert len(warnings) == 1
    assert "another branch" in warnings[0]


def test_producer_that_runs_after_the_reader_gets_no_edge_and_a_warning() -> None:
    pipeline = _pipeline(
        _reader("reader", "{{tasks.set_x.values.x}}"),
        _setter("set_x", depends_on=["reader"]),
    )

    backfilled, warnings = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "reader")) == []
    assert len(warnings) == 1
    assert "'reader'" in warnings[0] and "'set_x'" in warnings[0] and "cycle" in warnings[0]


def test_reader_that_runs_on_failure_gains_the_edge_and_a_run_condition_warning() -> None:
    reader = _reader("on_failure", "{{tasks.set_x.values.x}}")
    reader.depends_on = [Dependency(task_key="work", outcome="Failed")]
    pipeline = _pipeline(_setter("set_x"), _wait("work"), reader)

    backfilled, warnings = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "on_failure")) == ["work", "set_x"]
    assert len(warnings) == 1
    assert "run condition" in warnings[0]


def test_reader_after_a_condition_also_waits_for_the_branch_task_it_reads() -> None:
    pipeline = _pipeline(
        _branch_setting("set_in_branch", before=[_wait("first_in_branch")]),
        _reader("after", "{{tasks.set_in_branch.values.x}}", depends_on=["check"]),
    )

    backfilled, warnings = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "after")) == ["check", "set_in_branch"]
    assert warnings == []


def test_loop_body_reading_a_branch_task_puts_the_edge_on_the_loop() -> None:
    loop = ForEachActivity(
        name="loop",
        task_key="loop",
        items_expression="[1]",
        inner_activities=[_reader("inner", "{{tasks.set_in_branch.values.x}}")],
    )
    pipeline = _pipeline(_branch_setting("set_in_branch"), loop)

    backfilled, _ = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "loop")) == ["set_in_branch"]


def test_reading_a_value_set_inside_a_loop_body_warns_even_when_the_reader_waits_for_the_loop() -> None:
    loop = ForEachActivity(name="loop", task_key="loop", items_expression="[1]", inner_activities=[_setter("set_x")])
    pipeline = _pipeline(loop, _reader("after", "{{tasks.set_x.values.x}}", depends_on=["loop"]))

    backfilled, warnings = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "after")) == ["loop"]
    assert len(warnings) == 1
    assert "'set_x' (inside 'loop')" in warnings[0] and "do not leave a ForEach body" in warnings[0]


def test_cycle_warning_names_the_task_inside_the_loop_that_reads_the_value() -> None:
    loop = ForEachActivity(
        name="loop",
        task_key="loop",
        items_expression="[1]",
        inner_activities=[_reader("inner", "{{tasks.set_x.values.x}}")],
    )
    pipeline = _pipeline(loop, _setter("set_x", depends_on=["loop"]))

    backfilled, warnings = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "loop")) == []
    assert len(warnings) == 1
    assert warnings[0].startswith("'inner' (inside 'loop') reads a task value from 'set_x'")
    assert "cycle" in warnings[0]


def test_reference_in_a_description_is_not_read_as_a_dependency() -> None:
    reader = _wait("documented")
    reader.description = "Reads {{tasks.set_x.values.x}} in a later version."
    pipeline = _pipeline(_setter("set_x"), reader)

    backfilled, _ = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "documented")) == []


def test_reference_to_a_task_not_in_the_pipeline_gets_no_edge() -> None:
    pipeline = _pipeline(_reader("reader", "{{tasks.folded_into_motif.values.x}}"))

    backfilled, warnings = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "reader")) == []
    assert warnings == []


def test_motif_original_activities_are_not_read_as_references() -> None:
    motif = MotifActivity(
        name="motif",
        task_key="motif_copy",
        motif_id="copy",
        display_name="Copy",
        databricks_replacement="auto_loader",
        matched_activity_names=["old"],
        original_activities=[_reader("old", "{{tasks.set_x.values.x}}")],
    )
    pipeline = _pipeline(_setter("set_x"), motif)

    backfilled, _ = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "motif_copy")) == []


def test_self_reference_gets_no_edge() -> None:
    pipeline = _pipeline(_reader("reader", "{{tasks.reader.values.x}}"))

    backfilled, _ = _backfill(pipeline)

    assert _dependency_keys(_task(backfilled, "reader")) == []


def test_input_pipeline_is_left_unchanged() -> None:
    inner = _reader("inner", "{{tasks.set_x.values.x}}")
    loop = ForEachActivity(name="loop", task_key="loop", items_expression="[1]", inner_activities=[inner])
    pipeline = _pipeline(_setter("set_x"), _reader("reader", "{{tasks.set_x.values.x}}"), loop)
    before = copy.deepcopy(pipeline)

    _backfill(pipeline)

    assert pipeline == before


def _backfill(pipeline: Pipeline) -> tuple[Pipeline, list[str]]:
    warnings: list[str] = []
    return backfill_task_value_dependencies(pipeline, warnings), warnings


def _pipeline(*tasks: Activity) -> Pipeline:
    return Pipeline(name="pipeline", tasks=list(tasks))


def _setter(task_key: str, *, depends_on: list[str] | None = None) -> SetVariableActivity:
    return SetVariableActivity(
        name=task_key,
        task_key=task_key,
        variable_name="x",
        variable_value="1",
        depends_on=_dependencies(depends_on),
    )


def _reader(task_key: str, reference: str, *, depends_on: list[str] | None = None) -> NotebookActivity:
    return NotebookActivity(
        name=task_key,
        task_key=task_key,
        notebook_path="/Shared/reader",
        base_parameters={"value": reference},
        depends_on=_dependencies(depends_on),
    )


def _branch_setting(setter_key: str, *, before: list[Activity] | None = None) -> IfConditionActivity:
    earlier = before or []
    setter = _setter(setter_key, depends_on=[activity.task_key for activity in earlier] or None)
    return IfConditionActivity(
        name="check",
        task_key="check",
        op="EQUAL_TO",
        left="a",
        right="a",
        if_true_activities=[*earlier, setter],
    )


def _wait(task_key: str, *, depends_on: list[str] | None = None) -> WaitActivity:
    return WaitActivity(name=task_key, task_key=task_key, wait_time_seconds=1, depends_on=_dependencies(depends_on))


def _dependencies(task_keys: list[str] | None) -> list[Dependency] | None:
    return [Dependency(task_key=task_key) for task_key in task_keys] if task_keys else None


def _task(pipeline: Pipeline, task_key: str) -> Activity:
    return next(activity for activity in pipeline.tasks if activity.task_key == task_key)


def _dependency_keys(activity: Activity) -> list[str]:
    return [dependency.task_key for dependency in activity.depends_on or []]
