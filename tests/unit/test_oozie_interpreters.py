"""The reference interpreters themselves: hand-checked runs on both sides."""

from __future__ import annotations

import pytest

from flowx.sources.oozie.equivalence import (
    Scenario,
    check_equivalence,
    enumerate_scenarios,
    run_lakeflow,
    run_oozie,
)
from flowx.sources.oozie.loader.graph import TaskKeys
from flowx.sources.oozie.loader.parse import parse_workflow

_WORKFLOW = parse_workflow(
    '<workflow-app name="w"><start to="a"/>'
    '<action name="a"><pig/><ok to="d"/><error to="h"/></action>'
    '<decision name="d"><switch><case to="b">${x}</case><default to="end"/></switch></decision>'
    '<action name="b"><pig/><ok to="end"/><error to="fail"/></action>'
    '<action name="h"><pig/><ok to="fail"/><error to="fail"/></action>'
    '<kill name="fail"/><end name="end"/></workflow-app>'
)


def test_oozie_interpreter_follows_transitions():
    assert run_oozie(_WORKFLOW, Scenario(action_succeeds={}, decision_choice={"d": 0})).executed_actions == {"a", "b"}
    outcome = run_oozie(_WORKFLOW, Scenario(action_succeeds={"a": False}, decision_choice={}))
    assert outcome.executed_actions == {"a", "h"} and not outcome.succeeded
    assert run_oozie(_WORKFLOW, Scenario(action_succeeds={}, decision_choice={"d": -1})).executed_actions == {"a"}


def test_lakeflow_interpreter_applies_outcomes_and_run_if():
    keys = TaskKeys(actions={"a": "a", "b": "b", "h": "h"}, decisions={"d": ["d"]}, kills={"fail": "fail_kill"})
    tasks = [
        {"task_key": "a"},
        {
            "task_key": "d",
            "depends_on": [{"task_key": "a"}],
            "condition_task": {"op": "EQUAL_TO", "left": "x", "right": "true"},
        },
        {"task_key": "b", "depends_on": [{"task_key": "d", "outcome": "true"}]},
        {"task_key": "h", "depends_on": [{"task_key": "a"}], "run_if": "AT_LEAST_ONE_FAILED"},
        {"task_key": "fail_kill", "depends_on": [{"task_key": "h"}]},
    ]
    chosen = run_lakeflow(
        tasks, Scenario(action_succeeds={}, decision_choice={"d": 0}), keys, fail_task_keys={"fail_kill"}
    )
    assert chosen.executed_actions == {"a", "b"} and chosen.succeeded
    failed = run_lakeflow(
        tasks, Scenario(action_succeeds={"a": False}, decision_choice={}), keys, fail_task_keys={"fail_kill"}
    )
    assert failed.executed_actions == {"a", "h"} and not failed.succeeded


def test_result_state_gates_tell_own_failure_from_upstream_failure():
    keys = TaskKeys(actions={"x": "x", "a": "a", "h": "h"}, decisions={}, kills={})
    tasks = [
        {"task_key": "x"},
        {"task_key": "a", "depends_on": [{"task_key": "x"}]},
        {
            "task_key": "a__failed",
            "depends_on": [{"task_key": "a"}],
            "run_if": "ALL_DONE",
            "condition_task": {"op": "EQUAL_TO", "left": "{{tasks.a.result_state}}", "right": "failed"},
        },
        {
            "task_key": "a__reached",
            "depends_on": [{"task_key": "x"}],
            "condition_task": {"op": "EQUAL_TO", "left": "true", "right": "true"},
        },
        {"task_key": "h", "depends_on": [{"task_key": "a__failed", "outcome": "true"}]},
    ]
    own = run_lakeflow(tasks, Scenario(action_succeeds={"a": False}, decision_choice={}), keys, fail_task_keys=set())
    assert own.executed_actions == {"x", "a", "h"} and own.succeeded  # handler recovered; a__reached succeeded
    upstream = run_lakeflow(
        tasks, Scenario(action_succeeds={"x": False}, decision_choice={}), keys, fail_task_keys=set()
    )
    assert "h" not in upstream.executed_actions and not upstream.succeeded  # a__reached is an UPSTREAM_FAILED leaf


def test_scenarios_cover_every_combination_for_small_workflows():
    scenarios = enumerate_scenarios(_WORKFLOW)
    assert len(scenarios) == 2**3 * 2  # three actions ok/fail x two decision choices


def _action(name: str, ok: str, error: str) -> str:
    return f'<action name="{name}"><pig/><ok to="{ok}"/><error to="{error}"/></action>'


def _gate(key: str, source: str) -> dict:
    return {
        "task_key": key,
        "depends_on": [{"task_key": source}],
        "run_if": "ALL_DONE",
        "condition_task": {"op": "EQUAL_TO", "left": f"{{{{tasks.{source}.result_state}}}}", "right": "failed"},
    }


def _handler_job(*, spurious_dependency: bool) -> tuple[list[dict], TaskKeys]:
    error_handler = {"task_key": "e", "depends_on": [{"task_key": "b__failed", "outcome": "true"}]}
    if spurious_dependency:
        error_handler["depends_on"].append({"task_key": "a__failed", "outcome": "true"})
        error_handler["run_if"] = "AT_LEAST_ONE_SUCCESS"
    tasks = [
        {"task_key": "a"},
        _gate("a__failed", "a"),
        {"task_key": "h", "depends_on": [{"task_key": "a__failed", "outcome": "true"}]},
        {"task_key": "h_kill", "depends_on": [{"task_key": "h"}]},
        {"task_key": "b", "depends_on": [{"task_key": "a"}]},
        _gate("b__failed", "b"),
        error_handler,
        {"task_key": "e_kill", "depends_on": [{"task_key": "e"}]},
    ]
    keys = TaskKeys(actions={name: name for name in "abhe"}, decisions={}, kills={"fail": "h_kill"})
    return tasks, keys


_HANDLER_WORKFLOW = parse_workflow(
    '<workflow-app name="w"><start to="a"/>'
    + _action("a", "b", "h")
    + _action("b", "end", "e")
    + _action("h", "fail", "fail")
    + _action("e", "fail", "fail")
    + '<kill name="fail"/><end name="end"/></workflow-app>'
)


def test_check_equivalence_accepts_a_correct_handler_job():
    tasks, keys = _handler_job(spurious_dependency=False)
    assert check_equivalence(_HANDLER_WORKFLOW, tasks, keys, fail_task_keys={"h_kill", "e_kill"}) == []


def test_check_equivalence_catches_a_handler_that_runs_on_a_killed_run():
    tasks, keys = _handler_job(spurious_dependency=True)
    mismatches = check_equivalence(_HANDLER_WORKFLOW, tasks, keys, fail_task_keys={"h_kill", "e_kill"})
    assert mismatches and any("'e'" in mismatch for mismatch in mismatches)


_FORK_WORKFLOW = parse_workflow(
    '<workflow-app name="w"><start to="f"/><fork name="f"><path start="p"/><path start="q"/></fork>'
    + _action("p", "j", "fail")
    + _action("q", "j", "fail")
    + '<join name="j" to="z"/>'
    + _action("z", "end", "fail")
    + '<kill name="fail"/><end name="end"/></workflow-app>'
)
_FORK_TASKS = [
    {"task_key": "p"},
    {"task_key": "q"},
    {"task_key": "z", "depends_on": [{"task_key": "p"}, {"task_key": "q"}]},
]
_FORK_KEYS = TaskKeys(actions={"p": "p", "q": "q", "z": "z"}, decisions={}, kills={})


def test_fork_join_runs_every_path_on_both_sides():
    all_ok = Scenario(action_succeeds={}, decision_choice={})
    assert run_oozie(_FORK_WORKFLOW, all_ok).executed_actions == {"p", "q", "z"}
    assert run_lakeflow(_FORK_TASKS, all_ok, _FORK_KEYS, fail_task_keys=set()).executed_actions == {"p", "q", "z"}
    one_fails = Scenario(action_succeeds={"p": False}, decision_choice={})
    oozie = run_oozie(_FORK_WORKFLOW, one_fails)
    lakeflow = run_lakeflow(_FORK_TASKS, one_fails, _FORK_KEYS, fail_task_keys=set())
    assert oozie.executed_actions == lakeflow.executed_actions == {"p", "q"}
    assert not oozie.succeeded and not lakeflow.succeeded
    assert check_equivalence(_FORK_WORKFLOW, _FORK_TASKS, _FORK_KEYS, fail_task_keys=set()) == []


def test_fork_path_reaching_end_without_join_counts_as_killed():
    workflow = parse_workflow(
        '<workflow-app name="w"><start to="f"/><fork name="f"><path start="p"/><path start="q"/></fork>'
        + _action("p", "end", "end")
        + _action("q", "j", "j")
        + '<join name="j" to="end"/><end name="end"/></workflow-app>'
    )
    assert not run_oozie(workflow, Scenario(action_succeeds={}, decision_choice={})).succeeded


def test_all_done_after_an_excluded_dependency_is_excluded():
    tasks = [
        {"task_key": "c", "condition_task": {"op": "EQUAL_TO", "left": "true", "right": "false"}},
        {"task_key": "x", "depends_on": [{"task_key": "c", "outcome": "true"}]},
        {"task_key": "y", "depends_on": [{"task_key": "x"}], "run_if": "ALL_DONE"},
    ]
    keys = TaskKeys(actions={"x": "x", "y": "y"}, decisions={}, kills={})
    outcome = run_lakeflow(tasks, Scenario(action_succeeds={}, decision_choice={}), keys, fail_task_keys=set())
    assert outcome.executed_actions == frozenset() and outcome.succeeded


def test_unsupported_condition_operator_raises():
    tasks = [{"task_key": "c", "condition_task": {"op": "GREATER_THAN", "left": "1", "right": "0"}}]
    keys = TaskKeys(actions={}, decisions={}, kills={})
    with pytest.raises(ValueError):
        run_lakeflow(tasks, Scenario(action_succeeds={}, decision_choice={}), keys, fail_task_keys=set())


def test_large_workflows_are_sampled_with_distinct_scenarios_including_edge_cases():
    body = "".join(_action(f"a{index}", f"a{index + 1}", "fail") for index in range(12))
    workflow = parse_workflow(
        f'<workflow-app name="w"><start to="a0"/>{body}'
        '<action name="a12"><pig/><ok to="end"/><error to="fail"/></action><kill name="fail"/><end name="end"/>'
        "</workflow-app>"
    )
    scenarios = enumerate_scenarios(workflow, limit=50)
    assert len(scenarios) == 50
    assert len({tuple(sorted(s.action_succeeds.items())) for s in scenarios}) == 50
    assert all(scenarios[0].action_succeeds.values())
    assert sum(not all(s.action_succeeds.values()) for s in scenarios[1:14]) == 13
    assert all(list(s.action_succeeds.values()).count(False) == 1 for s in scenarios[1:14])


def test_sampling_never_exceeds_the_limit_and_survives_huge_workflows():
    def chain(length: int) -> str:
        body = "".join(_action(f"a{index}", f"a{index + 1}", "fail") for index in range(length))
        return (
            f'<workflow-app name="w"><start to="a0"/>{body}'
            f'<action name="a{length}"><pig/><ok to="end"/><error to="fail"/></action><kill name="fail"/>'
            '<end name="end"/></workflow-app>'
        )

    few = enumerate_scenarios(parse_workflow(chain(12)), limit=5)
    assert len(few) == 5
    huge = enumerate_scenarios(parse_workflow(chain(70)), limit=100)
    assert len(huge) == 100
    assert len({tuple(sorted(s.action_succeeds.items())) for s in huge}) == 100
