"""Regression tests for three ADF -> Databricks translation fixes.

1. ForEach inputs-bridge notebooks must carry the ``# Databricks notebook
   source`` marker or ``bundle validate`` rejects the notebook_task.
2. A join after an ``IfCondition`` must use ``run_if: NONE_FAILED`` so a
   failure in the *taken* branch propagates (``AT_LEAST_ONE_SUCCESS`` masks it).
3. Synthesised ``_init_<var>`` tasks that nothing references must be pruned.
"""

from __future__ import annotations

from flowx.bundler.dab_writer import _rewrite_post_branch_dependencies
from flowx.preparer.code_generator import render_bridge_notebook
from flowx.translator.engine import _prune_dead_variable_inits

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
    # The bridge still publishes the computed value.
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
                {"task_key": "CheckRunMode"},  # outcome-less -> rewritten to terminals
                {"task_key": "items_bridge"},
            ],
        },
    ]


def test_condition_join_uses_none_failed_not_at_least_one_success() -> None:
    tasks = _condition_join_tasks()
    _rewrite_post_branch_dependencies(tasks)
    join = next(t for t in tasks if t["task_key"] == "ProcessItems")

    assert join["run_if"] == "NONE_FAILED"
    # The outcome-less condition edge is replaced by the branch terminal,
    # the always-on sibling is preserved.
    dep_keys = {d["task_key"] for d in join["depends_on"]}
    assert dep_keys == {"NotifyFull", "items_bridge"}


# ---------------------------------------------------------------------------
# Bug 3 — dead _init_<var> pruning
# ---------------------------------------------------------------------------


def test_prune_drops_init_when_explicit_setter_dominates() -> None:
    tasks = [
        {"task_key": "_init_runMode", "variable_name": "runMode", "variable_value": "full"},
        {"task_key": "SetRunMode", "variable_name": "runMode", "variable_value": "full"},
        {
            "task_key": "CheckRunMode",
            "condition_task": {"left": "{{tasks.SetRunMode.values.runMode}}", "right": "full"},
        },
    ]
    pruned = _prune_dead_variable_inits(tasks, frozenset({"_init_runMode"}))
    keys = [t["task_key"] for t in pruned]
    assert "_init_runMode" not in keys
    assert keys == ["SetRunMode", "CheckRunMode"]


def test_prune_keeps_init_when_value_is_read() -> None:
    tasks = [
        {"task_key": "_init_runMode", "variable_name": "runMode", "variable_value": "full"},
        {
            "task_key": "CheckRunMode",
            "condition_task": {"left": "{{tasks._init_runMode.values.runMode}}", "right": "full"},
        },
    ]
    pruned = _prune_dead_variable_inits(tasks, frozenset({"_init_runMode"}))
    assert any(t["task_key"] == "_init_runMode" for t in pruned)


def test_prune_keeps_init_when_referenced_by_depends_on() -> None:
    tasks = [
        {"task_key": "_init_runMode", "variable_name": "runMode", "variable_value": "full"},
        {"task_key": "Next", "depends_on": [{"task_key": "_init_runMode"}]},
    ]
    pruned = _prune_dead_variable_inits(tasks, frozenset({"_init_runMode"}))
    assert any(t["task_key"] == "_init_runMode" for t in pruned)


def test_prune_noop_without_init_tasks() -> None:
    tasks = [{"task_key": "SetRunMode"}, {"task_key": "Next"}]
    assert _prune_dead_variable_inits(tasks, frozenset()) == tasks


def test_prune_only_touches_synthesized_keys() -> None:
    """Only keys the translator actually synthesised are eligible for pruning;
    a task the translator did not synthesise is left alone even when it is
    otherwise unreferenced."""
    tasks = [
        {"task_key": "_init_report", "variable_name": "report", "variable_value": "x"},
        {"task_key": "Downstream", "depends_on": [{"task_key": "Upstream"}]},
    ]
    # "_init_report" is not in the synthesised-key set, so it stays.
    pruned = _prune_dead_variable_inits(tasks, frozenset())
    assert [t["task_key"] for t in pruned] == ["_init_report", "Downstream"]


def test_prune_drops_init_despite_prefix_collision() -> None:
    """A dead ``_init_x`` must be dropped even when its key text appears inside
    an unrelated key or path. A bare substring match against the serialised IR
    wrongly kept it alive; matching the anchored ``tasks.<key>.values`` token
    and structured ``depends_on`` edges instead makes pruning robust."""
    tasks = [
        {"task_key": "_init_x", "variable_name": "x", "variable_value": "1"},
        {"task_key": "_init_x_2", "variable_name": "x2", "variable_value": "2"},
        {
            "task_key": "Reader",
            # Notebook path merely contains the text "_init_x"; the live read
            # is of the *sibling* init "_init_x_2", not of "_init_x".
            "notebook_task": {"notebook_path": "/Shared/_init_x_report"},
            "condition_task": {"left": "{{tasks._init_x_2.values.x2}}", "right": "2"},
        },
    ]
    pruned = _prune_dead_variable_inits(tasks, frozenset({"_init_x", "_init_x_2"}))
    keys = [t["task_key"] for t in pruned]
    assert "_init_x" not in keys  # genuinely unreferenced -> dropped
    assert "_init_x_2" in keys  # referenced via task-value ref -> kept
