"""Lowering Oozie transition graphs into Lakeflow dependencies, run_if rules, and condition chains."""

from __future__ import annotations

import pytest

from flowx.models.ir import IfConditionActivity, NotebookActivity
from flowx.sources.oozie.loader.ast import ActionNode
from flowx.sources.oozie.loader.el_render import AgenticCondition, BridgedCondition, NativeCondition
from flowx.sources.oozie.loader.graph import (
    OozieGraphError,
    TranslatedAction,
    assign_task_keys,
    lower_workflow,
)
from flowx.sources.oozie.loader.parse import parse_workflow


def _action(name: str, ok: str = "end", error: str = "fail", extra: str = "") -> str:
    return f'<action name="{name}" {extra}><pig><script>x</script></pig><ok to="{ok}"/><error to="{error}"/></action>'


def _workflow(body: str, start: str) -> str:
    return (
        f'<workflow-app name="w" xmlns="uri:oozie:workflow:1.0"><start to="{start}"/>{body}'
        '<kill name="fail"><message>boom</message></kill><end name="end"/></workflow-app>'
    )


def _lower(xml_text: str, predicates: dict[str, object] | None = None):
    workflow = parse_workflow(xml_text)
    keys = assign_task_keys(workflow)

    def translate_action(action: ActionNode, task_key: str) -> TranslatedAction:
        return TranslatedAction(
            main=NotebookActivity(name=action.name, task_key=task_key, notebook_path=f"/n/{task_key}")
        )

    def translate_predicate(text: str):
        return (predicates or {}).get(text, NativeCondition(op="EQUAL_TO", left=text, right="true"))

    return lower_workflow(
        workflow, task_keys=keys, translate_action=translate_action, translate_predicate=translate_predicate
    )


def _deps(result, key: str) -> list[tuple[str, str | None]]:
    task = next(task for task in result.tasks if task.task_key == key)
    return [(dependency.task_key, dependency.outcome) for dependency in task.depends_on or []]


def _task(result, key: str):
    return next(task for task in result.tasks if task.task_key == key)


def test_linear_ok_edges_and_error_to_kill_drops_the_edge():
    result = _lower(_workflow(_action("a", ok="b") + _action("b"), "a"))
    assert [task.task_key for task in result.tasks] == ["a", "b"]
    assert _deps(result, "a") == [] and _deps(result, "b") == [("a", None)]
    assert {entry["rule"] for entry in result.transformations} >= {"CF-OK-EDGE", "CF-ERROR-KILL"}
    assert result.kill_messages == {"fail": "boom"}


def test_error_to_action_goes_through_outcome_gates():
    result = _lower(_workflow(_action("a", error="cleanup") + _action("cleanup", ok="fail"), "a"))
    assert _deps(result, "cleanup") == [("a__failed", "true")]
    failed_gate, ok_gate = _task(result, "a__failed"), _task(result, "a__ok")
    assert (failed_gate.left, failed_gate.right, failed_gate.run_if) == (
        "{{tasks.a.result_state}}",
        "failed",
        "ALL_DONE",
    )
    assert (ok_gate.left, ok_gate.right, ok_gate.run_if) == ("{{tasks.a.result_state}}", "success", "ALL_DONE")
    assert _deps(result, "a__failed") == [("a", None)] and _deps(result, "a__ok") == [("a", None)]
    assert "a__reached" not in {task.task_key for task in result.tasks}  # a has no upstream
    fail_task = _task(result, result.task_keys.kills["fail"])
    assert isinstance(fail_task, NotebookActivity) and "raise RuntimeError" in (fail_task.generated_source or "")
    assert _deps(result, fail_task.task_key) == [("cleanup", None)]


def test_error_to_end_keeps_the_action_off_the_leaves():
    result = _lower(_workflow(_action("a", error="end"), "a"))
    assert {task.task_key for task in result.tasks} == {"a", "a__ok"}
    assert _deps(result, "a__ok") == [("a", None)]
    assert any(entry["rule"] == "CF-ERROR-END" for entry in result.transformations)


def test_recoverable_action_after_an_upstream_gets_a_reach_sentinel():
    result = _lower(
        _workflow(_action("first", ok="a") + _action("a", ok="next", error="end") + _action("next"), "first")
    )
    assert _deps(result, "a__reached") == [("first", None)]
    sentinel = _task(result, "a__reached")
    assert (sentinel.left, sentinel.right) == ("true", "true")
    assert _deps(result, "next") == [("a__ok", "true")]


def test_fork_join_emits_no_tasks_and_joins_with_and():
    body = (
        '<fork name="f"><path start="p1"/><path start="p2a"/></fork>'
        + _action("p1", ok="j")
        + _action("p2a", ok="p2b")
        + _action("p2b", ok="j")
        + '<join name="j" to="after"/>'
        + _action("after")
    )
    result = _lower(_workflow(_action("first", ok="f") + body, "first"))
    assert _deps(result, "p1") == [("first", None)] and _deps(result, "p2a") == [("first", None)]
    assert sorted(_deps(result, "after")) == [("p1", None), ("p2b", None)]
    assert _task(result, "after").run_if is None


def test_decision_chain_and_or_merge():
    body = (
        '<decision name="d"><switch><case to="x">${c1}</case><case to="y">${c2}</case>'
        '<default to="z"/></switch></decision>'
        + _action("x", ok="merge")
        + _action("y", ok="merge")
        + _action("z", ok="merge")
        + _action("merge")
    )
    result = _lower(_workflow(_action("a", ok="d") + body, "a"))
    first, second = result.task_keys.decisions["d"]
    assert _deps(result, first) == [("a", None)]
    assert _deps(result, second) == [(first, "false")]
    assert _deps(result, "x") == [(first, "true")]
    assert _deps(result, "y") == [(second, "true")]
    assert _deps(result, "z") == [(second, "false")]
    assert sorted(_deps(result, "merge")) == [("x", None), ("y", None), ("z", None)]
    assert _task(result, "merge").run_if == "AT_LEAST_ONE_SUCCESS"


def test_empty_branch_merge_runs():
    body = (
        '<decision name="d"><switch><case to="extra">${c1}</case><default to="merge"/></switch></decision>'
        + _action("extra", ok="merge")
        + _action("merge")
    )
    result = _lower(_workflow(body, "d"))
    (condition,) = result.task_keys.decisions["d"]
    assert sorted(_deps(result, "merge")) == [(condition, "false"), ("extra", None)]
    assert _task(result, "merge").run_if == "AT_LEAST_ONE_SUCCESS"


def test_or_merge_with_an_error_edge_uses_the_failure_gate():
    body = (
        '<decision name="d"><switch><case to="a">${c1}</case><default to="handler"/></switch></decision>'
        + _action("a", ok="end", error="handler")
        + _action("handler")
    )
    result = _lower(_workflow(body, "d"))
    (condition,) = result.task_keys.decisions["d"]
    assert sorted(_deps(result, "handler")) == sorted([(condition, "false"), ("a__failed", "true")])
    assert _task(result, "handler").run_if == "AT_LEAST_ONE_SUCCESS"


def test_no_dependency_on_a_condition_task_lacks_an_outcome():
    body = (
        '<decision name="d"><switch><case to="x">${c1}</case><default to="y"/></switch></decision>'
        + _action("x", ok="end")
        + _action("y", ok="end", error="end")
    )
    result = _lower(_workflow(body, "d"))
    condition_keys = {task.task_key for task in result.tasks if isinstance(task, IfConditionActivity)}
    for task in result.tasks:
        for dependency in task.depends_on or []:
            if dependency.task_key in condition_keys:
                assert dependency.outcome in ("true", "false"), (task.task_key, dependency)


def test_bridged_and_agentic_predicates():
    body = (
        '<decision name="d"><switch><case to="x">${p_bridge}</case><case to="y">${p_agentic}</case>'
        '<default to="end"/></switch></decision>' + _action("x") + _action("y")
    )
    predicates = {
        "${p_bridge}": BridgedCondition(code='"true"', imports=["import os"], widget_bindings={"w": "{{job.run_id}}"}),
        "${p_agentic}": AgenticCondition(reason="counters"),
    }
    result = _lower(_workflow(body, "d"), predicates)
    first, second = result.task_keys.decisions["d"]
    bridged, agentic = _task(result, first), _task(result, second)
    assert bridged.left == f"{{{{tasks.{first}_bridge.values.result}}}}" and bridged.right == "true"
    assert _deps(result, first) == [(f"{first}_bridge", None)]
    assert agentic.left == f"{{{{tasks.{second}_bridge.values.result}}}}" and agentic.right == "true"
    assert _deps(result, f"{second}_bridge") == [(first, "false")]
    bridge = _task(result, f"{first}_bridge")
    assert isinstance(bridge, NotebookActivity) and bridge.base_parameters == {"w": "{{job.run_id}}"}
    assert '"true"' in (bridge.generated_source or "")
    assert "counters" in (_task(result, f"{second}_bridge").generated_source or "")
    assert result.agentic_task_keys == {second}
    assert any(finding["code"] == "agentic_decision" for finding in result.findings)


def test_bridge_task_waits_for_condition_upstreams():
    body = (
        '<decision name="d"><switch><case to="x">${p_bridge}</case><default to="end"/></switch></decision>'
        + _action("x")
    )
    predicates = {"${p_bridge}": BridgedCondition(code='"true"', imports=[], widget_bindings={})}
    result = _lower(_workflow(_action("produce", ok="d") + body, "produce"), predicates)
    (condition,) = result.task_keys.decisions["d"]
    assert _deps(result, f"{condition}_bridge") == [("produce", None)]
    assert _deps(result, condition) == [(f"{condition}_bridge", None)]


def test_retries_carry_over():
    result = _lower(_workflow(_action("a", extra='retry-max="3" retry-interval="2"'), "a"))
    assert (_task(result, "a").max_retries, _task(result, "a").min_retry_interval_millis) == (3, 120000)


def test_zero_retry_interval_retries_immediately():
    result = _lower(_workflow(_action("a", extra='retry-max="3" retry-interval="0"'), "a"))
    assert _task(result, "a").min_retry_interval_millis == 0


def test_missing_retry_interval_uses_the_oozie_default():
    result = _lower(_workflow(_action("a", extra='retry-max="3"'), "a"))
    assert _task(result, "a").min_retry_interval_millis == 600000


def test_task_keys_unique_after_normalisation():
    body = _action("load-data", ok="load_data") + _action("load_data", ok="Load_Data") + _action("Load_Data")
    keys = assign_task_keys(parse_workflow(_workflow(body, "load-data"))).actions
    assert len(set(keys.values())) == 3
    assert keys["load-data"] == "load_data"
    assert all(key.replace("_", "").isalnum() for key in keys.values())


def test_before_tasks_take_the_entry_and_main_waits_for_them():
    workflow = parse_workflow(_workflow(_action("a", ok="b") + _action("b"), "a"))
    keys = assign_task_keys(workflow)

    def translate_action(action: ActionNode, task_key: str) -> TranslatedAction:
        main = NotebookActivity(name=action.name, task_key=task_key, notebook_path="/n")
        before = [NotebookActivity(name=f"{action.name} prep", task_key=f"{task_key}_prep", notebook_path="/p")]
        return TranslatedAction(main=main, before=before if action.name == "b" else [])

    result = lower_workflow(
        workflow, task_keys=keys, translate_action=translate_action, translate_predicate=lambda text: None
    )
    assert _deps(result, "b_prep") == [("a", None)] and _deps(result, "b") == [("b_prep", None)]


@pytest.mark.parametrize(
    "body, start, message",
    [
        (_action("a", ok="nowhere"), "a", "unknown node"),
        (_action("a", ok="b") + _action("b", ok="a"), "a", "cycle"),
        ('<join name="j" to="end"/>' + _action("a", ok="j"), "a", "join"),
    ],
)
def test_invalid_graphs_are_rejected(body: str, start: str, message: str):
    with pytest.raises(OozieGraphError, match=message):
        _lower(_workflow(body, start))


def test_two_forks_sharing_one_join_are_rejected():
    body = (
        '<fork name="f1"><path start="a"/><path start="f2"/></fork>'
        + '<fork name="f2"><path start="b"/><path start="c"/></fork>'
        + _action("a", ok="j")
        + _action("b", ok="j")
        + _action("c", ok="j")
        + '<join name="j" to="end"/>'
    )
    with pytest.raises(OozieGraphError, match="join 'j' is the join of both fork"):
        _lower(_workflow(body, "f1"))


def test_join_arrival_on_two_paths_of_its_fork_is_rejected():
    body = (
        '<fork name="f"><path start="a"/><path start="b"/></fork>'
        + _action("a", ok="shared")
        + _action("b", ok="shared")
        + _action("shared", ok="j")
        + '<join name="j" to="end"/>'
    )
    with pytest.raises(OozieGraphError, match="'shared' isn't on exactly one path of fork 'f'"):
        _lower(_workflow(body, "f"))


def test_sequential_forks_each_pair_with_their_own_join():
    body = (
        '<fork name="f1"><path start="a"/><path start="b"/></fork>'
        + _action("a", ok="j1")
        + _action("b", ok="j1")
        + '<join name="j1" to="f2"/>'
        + '<fork name="f2"><path start="c"/><path start="d"/></fork>'
        + _action("c", ok="j2")
        + _action("d", ok="j2")
        + '<join name="j2" to="end"/>'
    )
    result = _lower(_workflow(body, "f1"))
    assert sorted(_deps(result, "c")) == [("a", None), ("b", None)]


def test_unreachable_nodes_are_warned():
    result = _lower(_workflow(_action("a") + _action("orphan"), "a"))
    assert any("orphan" in warning for warning in result.warnings)
    assert "orphan" not in {task.name for task in result.tasks}


def test_nested_fork_join_is_accepted_and_joins_each_level():
    body = (
        '<fork name="outer"><path start="left"/><path start="inner"/></fork>'
        + _action("left", ok="outer_join")
        + '<fork name="inner"><path start="b"/><path start="c"/></fork>'
        + _action("b", ok="inner_join")
        + _action("c", ok="inner_join")
        + '<join name="inner_join" to="d"/>'
        + _action("d", ok="outer_join")
        + '<join name="outer_join" to="after"/>'
        + _action("after")
    )
    result = _lower(_workflow(body, "outer"))
    assert sorted(_deps(result, "d")) == [("b", None), ("c", None)]
    assert sorted(_deps(result, "after")) == [("d", None), ("left", None)]


def test_kill_reached_straight_from_start_still_fails_the_run():
    body = '<decision name="d"><switch><default to="fail"/></switch></decision>'
    result = _lower(_workflow(body, "d"))
    kill = _task(result, result.task_keys.kills["fail"])
    assert isinstance(kill, NotebookActivity) and kill.depends_on is None


def _gate_deps(result, key: str) -> list[tuple[str, str | None]]:
    gate = _task(result, key)
    assert isinstance(gate, IfConditionActivity) and (gate.left, gate.right) == ("true", "true")
    return _deps(result, key)


def test_join_ors_each_paths_recovery_routes_then_ands_across_paths():
    body = '<fork name="f"><path start="p1"/><path start="p2"/></fork>'
    for index in (1, 2):
        body += _action(f"p{index}", ok="j", error=f"c{index}") + _action(f"c{index}", ok="j")
    body += '<join name="j" to="after"/>' + _action("after")
    result = _lower(_workflow(body, "f"))
    after = _deps(result, "after")
    assert len(after) == 2 and all(outcome == "true" for _, outcome in after)
    assert _task(result, "after").run_if is None
    per_path = sorted(sorted(_gate_deps(result, key)) for key, _ in after)
    assert per_path == [[("c1", None), ("p1__ok", "true")], [("c2", None), ("p2__ok", "true")]]
    assert all(_task(result, key).run_if == "AT_LEAST_ONE_SUCCESS" for key, _ in after)


def test_join_ors_a_decision_inside_one_path():
    body = (
        '<fork name="f"><path start="d"/><path start="other"/></fork>'
        '<decision name="d"><switch><case to="x">${c1}</case><default to="y"/></switch></decision>'
        + _action("x", ok="j")
        + _action("y", ok="j")
        + _action("other", ok="j")
        + '<join name="j" to="after"/>'
        + _action("after")
    )
    result = _lower(_workflow(body, "f"))
    after = _deps(result, "after")
    assert ("other", None) in after and len(after) == 2
    (gate_key,) = [key for key, _ in after if key != "other"]
    assert sorted(_gate_deps(result, gate_key)) == [("x", None), ("y", None)]
    assert _task(result, gate_key).run_if == "AT_LEAST_ONE_SUCCESS"


def _covered_choices(result, key: str) -> set[tuple[str, str | None]]:
    """Every upstream edge the merge can be satisfied by, looking through no-compute gates."""
    covered: set[tuple[str, str | None]] = set()
    for upstream, outcome in _deps(result, key):
        task = _task(result, upstream)
        if isinstance(task, IfConditionActivity) and (task.left, task.right) == ("true", "true"):
            covered |= set(_deps(result, upstream))
        else:
            covered.add((upstream, outcome))
    return covered


def test_or_merge_keeps_both_outcomes_of_the_last_case():
    body = (
        '<decision name="d"><switch><case to="x">${c1}</case><case to="merge">${c2}</case>'
        '<default to="merge"/></switch></decision>' + _action("x", ok="merge") + _action("merge")
    )
    result = _lower(_workflow(body, "d"))
    _, second = result.task_keys.decisions["d"]
    merge_keys = [key for key, _ in _deps(result, "merge")]
    assert len(merge_keys) == len(set(merge_keys))
    assert _covered_choices(result, "merge") == {("x", None), (second, "true"), (second, "false")}
    assert _task(result, "merge").run_if == "AT_LEAST_ONE_SUCCESS"


def test_or_merge_when_single_case_and_default_share_a_target():
    body = '<decision name="d"><switch><case to="n">${c1}</case><default to="n"/></switch></decision>' + _action("n")
    result = _lower(_workflow(_action("a", ok="d") + body, "a"))
    (condition,) = result.task_keys.decisions["d"]
    merge_keys = [key for key, _ in _deps(result, "n")]
    assert len(merge_keys) == len(set(merge_keys)) == 2
    assert _covered_choices(result, "n") == {(condition, "true"), (condition, "false")}
    assert _task(result, "n").run_if == "AT_LEAST_ONE_SUCCESS"
