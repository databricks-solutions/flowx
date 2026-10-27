"""Reference interpreters that check an Oozie workflow and its generated Lakeflow job behave the same.

For every combination of action outcomes and decision choices, both sides are "run" on paper: Oozie by
following transitions, Lakeflow by evaluating ``depends_on`` / ``run_if`` / condition outcomes over the
generated job's tasks. The Lakeflow rules come from measurements on a real workspace
(``lakeflow_run_semantics.json``, recorded by ``scripts/oozie/run_if_probe``), not from assumptions.
"""

from __future__ import annotations

import itertools
import json
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flowx.sources.oozie.loader.ast import (
    ActionNode,
    DecisionNode,
    EndNode,
    ForkNode,
    JoinNode,
    KillNode,
    OozieWorkflow,
)
from flowx.sources.oozie.loader.graph import TaskKeys

_SEMANTICS_FILE = Path(__file__).with_name("lakeflow_run_semantics.json")
# Measured on a workspace (Task 1); used only if the JSON file is missing.
_FALLBACK_SEMANTICS = {
    "upstream_failed_counts_as_failed": True,
    "excluded_counts_as_done_for_all_done": False,
    "condition_outcome_dependency_counts_as_success": True,
    "leaf_upstream_failed_fails_run": True,
    "result_state_literals": {"SUCCESS": "success", "FAILED": "failed", "UPSTREAM_FAILED": "upstream_failed"},
}
_RESULT_STATE_REFERENCE = re.compile(r"\{\{tasks\.([A-Za-z0-9_]+)\.result_state\}\}")
RUN_SEMANTICS: dict[str, Any] = {
    **_FALLBACK_SEMANTICS,
    **(json.loads(_SEMANTICS_FILE.read_text(encoding="utf-8")) if _SEMANTICS_FILE.is_file() else {}),
}

_SUCCESS, _FAILED, _EXCLUDED, _UPSTREAM_FAILED = "SUCCESS", "FAILED", "EXCLUDED", "UPSTREAM_FAILED"


@dataclass(frozen=True, slots=True, kw_only=True)
class Scenario:
    action_succeeds: dict[str, bool]
    decision_choice: dict[str, int]


@dataclass(frozen=True, slots=True, kw_only=True)
class RunOutcome:
    executed_actions: frozenset[str]
    succeeded: bool


def run_oozie(workflow: OozieWorkflow, scenario: Scenario) -> RunOutcome:
    """Follows Oozie's transitions for *scenario* and reports which actions ran and whether it succeeded."""
    executed: set[str] = set()
    steps = 0
    step_limit = 10_000

    def walk(name: str) -> str:
        """Runs from *name* until end ("end"), kill ("killed"), or a join ("join:<name>")."""
        nonlocal steps
        while True:
            node = workflow.nodes[name]
            steps += 1
            if steps > step_limit:
                raise RuntimeError(f"Oozie workflow {workflow.name!r} did not finish within {step_limit} steps")
            if isinstance(node, ActionNode):
                executed.add(name)
                name = node.ok_to if scenario.action_succeeds.get(name, True) else node.error_to
            elif isinstance(node, DecisionNode):
                choice = scenario.decision_choice.get(name, -1)
                name = node.cases[choice].to if 0 <= choice < len(node.cases) else node.default_to
            elif isinstance(node, ForkNode):
                arrivals = [walk(path) for path in node.paths]
                if "killed" in arrivals:
                    return "killed"
                joins = {arrival for arrival in arrivals if arrival.startswith("join:")}
                if len(joins) != 1 or len(joins) != len(set(arrivals)):
                    return "killed"
                name = workflow.nodes[next(iter(joins)).removeprefix("join:")].to  # type: ignore[union-attr]
            elif isinstance(node, JoinNode):
                return f"join:{name}"
            elif isinstance(node, KillNode):
                return "killed"
            elif isinstance(node, EndNode):
                return "end"
            else:
                raise RuntimeError(f"Unknown Oozie node type {type(node).__name__} at {name!r}")

    reached_end = walk(workflow.start.to) == "end"
    return RunOutcome(executed_actions=frozenset(executed), succeeded=reached_end)


def _topological(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key = {task["task_key"]: task for task in tasks}
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()

    def visit(key: str) -> None:
        if key in seen:
            return
        seen.add(key)
        for dependency in by_key[key].get("depends_on") or []:
            visit(dependency["task_key"])
        ordered.append(by_key[key])

    for task in tasks:
        visit(task["task_key"])
    return ordered


def _dependency_state(dependency: dict[str, str], states: dict[str, str], outcomes: dict[str, str]) -> str:
    upstream = states[dependency["task_key"]]
    if "outcome" in dependency:
        if upstream == _SUCCESS:
            matched = outcomes.get(dependency["task_key"]) == dependency["outcome"]
            return (
                "success" if matched and RUN_SEMANTICS["condition_outcome_dependency_counts_as_success"] else "excluded"
            )
        if upstream == _FAILED:
            return "failed"
        if upstream == _UPSTREAM_FAILED:
            return "failed" if RUN_SEMANTICS["upstream_failed_counts_as_failed"] else "excluded"
        return "excluded"
    if upstream == _SUCCESS:
        return "success"
    if upstream == _FAILED:
        return "failed"
    if upstream == _UPSTREAM_FAILED:
        return "failed" if RUN_SEMANTICS["upstream_failed_counts_as_failed"] else "excluded"
    return "excluded"


# Measured on a workspace: ALL_SUCCESS, AT_LEAST_ONE_SUCCESS, ALL_DONE and AT_LEAST_ONE_FAILED, each with a single
# dependency. Assumed (not measured): NONE_FAILED, ALL_FAILED, and every rule with mixed dependency states.
# TIMEDOUT and CANCELED task states are not modelled.
def _runs(rule: str, kinds: list[str]) -> bool:
    success, failed, excluded = kinds.count("success"), kinds.count("failed"), kinds.count("excluded")
    total = len(kinds)
    return {
        "ALL_SUCCESS": success == total,
        "AT_LEAST_ONE_SUCCESS": success > 0,
        "NONE_FAILED": failed == 0 and success > 0,
        "AT_LEAST_ONE_FAILED": failed > 0,
        "ALL_FAILED": failed == total,
        "ALL_DONE": excluded == 0 or RUN_SEMANTICS["excluded_counts_as_done_for_all_done"],
    }[rule]


def _evaluate_condition(condition: dict[str, str], states: dict[str, str]) -> str:
    """Evaluates a gate condition, resolving ``{{tasks.X.result_state}}`` to the literal Lakeflow would substitute."""
    literals = RUN_SEMANTICS.get("result_state_literals", _FALLBACK_SEMANTICS["result_state_literals"])

    def resolve(operand: str) -> str:
        return _RESULT_STATE_REFERENCE.sub(
            lambda match: literals.get(states.get(match.group(1), "")) or "excluded", operand
        )

    left, right = resolve(str(condition.get("left", ""))), resolve(str(condition.get("right", "")))
    operator = condition.get("op", "EQUAL_TO")
    if operator == "EQUAL_TO":
        return "true" if left == right else "false"
    if operator == "NOT_EQUAL":
        return "true" if left != right else "false"
    raise ValueError(f"Unsupported condition operator {operator!r}; only EQUAL_TO and NOT_EQUAL are modelled")


def run_lakeflow(
    tasks: list[dict[str, Any]], scenario: Scenario, task_keys: TaskKeys, *, fail_task_keys: set[str]
) -> RunOutcome:
    """Evaluates the generated job's tasks for *scenario* using the recorded Lakeflow semantics."""
    action_by_key = {key: name for name, key in task_keys.actions.items()}
    condition_truth: dict[str, str] = {}
    for decision, keys in task_keys.decisions.items():
        choice = scenario.decision_choice.get(decision, -1)
        for index, key in enumerate(keys):
            condition_truth[key] = "true" if index == choice else "false"
    states: dict[str, str] = {}
    outcomes: dict[str, str] = {}
    dependents: set[str] = set()
    for task in _topological(tasks):
        key = task["task_key"]
        dependencies = task.get("depends_on") or []
        dependents.update(dependency["task_key"] for dependency in dependencies)
        kinds = [_dependency_state(dependency, states, outcomes) for dependency in dependencies]
        if dependencies and not _runs(task.get("run_if", "ALL_SUCCESS"), kinds):
            states[key] = _UPSTREAM_FAILED if "failed" in kinds else _EXCLUDED
            continue
        if key in fail_task_keys or (
            key in action_by_key and not scenario.action_succeeds.get(action_by_key[key], True)
        ):
            states[key] = _FAILED
            continue
        states[key] = _SUCCESS
        if "condition_task" in task:
            outcomes[key] = condition_truth.get(key) or _evaluate_condition(task["condition_task"], states)
    leaves = [task["task_key"] for task in tasks if task["task_key"] not in dependents]
    bad_leaf_states = {_FAILED, _UPSTREAM_FAILED} if RUN_SEMANTICS["leaf_upstream_failed_fails_run"] else {_FAILED}
    executed = frozenset(
        action_by_key[key] for key, state in states.items() if key in action_by_key and state in (_SUCCESS, _FAILED)
    )
    return RunOutcome(executed_actions=executed, succeeded=not any(states[key] in bad_leaf_states for key in leaves))


def _reachable_names(workflow: OozieWorkflow) -> set[str]:
    """Names of nodes reachable from start; unreachable ones can't change a run, only inflate the count."""
    seen: set[str] = set()
    pending = [workflow.start.to]
    while pending:
        name = pending.pop()
        node = workflow.nodes.get(name)
        if name in seen or node is None:
            continue
        seen.add(name)
        if isinstance(node, ActionNode):
            pending += [node.ok_to, node.error_to]
        elif isinstance(node, DecisionNode):
            pending += [case.to for case in node.cases] + [node.default_to]
        elif isinstance(node, ForkNode):
            pending += node.paths
        elif isinstance(node, JoinNode):
            pending.append(node.to)
    return seen


def enumerate_scenarios(workflow: OozieWorkflow, limit: int = 4096, seed: int = 0) -> list[Scenario]:
    """Every ok/fail × decision-choice combination, or a seeded sample of distinct ones when there are more.

    A sample always includes the all-success scenario and every single-failure scenario.
    """
    reachable = _reachable_names(workflow)
    actions = sorted(name for name in reachable if isinstance(workflow.nodes[name], ActionNode))
    decisions = sorted(name for name in reachable if isinstance(workflow.nodes[name], DecisionNode))
    choices = [list(range(len(workflow.nodes[name].cases))) + [-1] for name in decisions]  # type: ignore[union-attr]
    total = 2 ** len(actions)
    for options in choices:
        total *= len(options)

    def build(outcome_bits: tuple[bool, ...], picks: tuple[int, ...]) -> Scenario:
        return Scenario(
            action_succeeds=dict(zip(actions, outcome_bits, strict=True)),
            decision_choice=dict(zip(decisions, picks, strict=True)),
        )

    if total <= limit:
        return [
            build(bits, picks)
            for bits in itertools.product((True, False), repeat=len(actions))
            for picks in itertools.product(*choices)
        ]
    radices = [2] * len(actions) + [len(options) for options in choices]

    def decode(index: int) -> Scenario:
        digits: list[int] = []
        for radix in reversed(radices):
            index, digit = divmod(index, radix)
            digits.append(digit)
        digits.reverse()
        bits = tuple(digit == 0 for digit in digits[: len(actions)])
        picks = tuple(choices[position][digit] for position, digit in enumerate(digits[len(actions) :]))
        return build(bits, picks)

    default_picks = tuple(-1 for _ in decisions)
    scenarios = [build(tuple(True for _ in actions), default_picks)]
    scenarios += [
        build(tuple(position != failing for position in range(len(actions))), default_picks)
        for failing in range(len(actions))
    ]
    scenarios = scenarios[:limit]
    seen = {_scenario_key(scenario) for scenario in scenarios}
    generator = random.Random(seed)
    attempts = 0
    while len(scenarios) < limit and attempts < 20 * limit:
        attempts += 1
        scenario = decode(generator.randrange(total))
        if _scenario_key(scenario) not in seen:
            seen.add(_scenario_key(scenario))
            scenarios.append(scenario)
    return scenarios


def _scenario_key(scenario: Scenario) -> tuple[tuple[tuple[str, bool], ...], tuple[tuple[str, int], ...]]:
    return tuple(sorted(scenario.action_succeeds.items())), tuple(sorted(scenario.decision_choice.items()))


def check_equivalence(
    workflow: OozieWorkflow,
    tasks: list[dict[str, Any]],
    task_keys: TaskKeys,
    *,
    fail_task_keys: set[str],
    limit: int = 4096,
) -> list[str]:
    """Returns one description per scenario where Oozie and the generated job disagree.

    Both the final state and the exact set of executed actions are compared on every run, killed ones included:
    ``run_oozie`` walks every fork path, so its set is the maximal one Lakeflow should also reach.
    """
    mismatches: list[str] = []
    for scenario in enumerate_scenarios(workflow, limit=limit):
        expected = run_oozie(workflow, scenario)
        actual = run_lakeflow(tasks, scenario, task_keys, fail_task_keys=fail_task_keys)
        if expected.succeeded != actual.succeeded or expected.executed_actions != actual.executed_actions:
            mismatches.append(
                f"{scenario}: Oozie {'succeeded' if expected.succeeded else 'killed'} "
                f"running {sorted(expected.executed_actions)}, "
                f"Lakeflow {'succeeded' if actual.succeeded else 'failed'} running {sorted(actual.executed_actions)}"
            )
    return mismatches
