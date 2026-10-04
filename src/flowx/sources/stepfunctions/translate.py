"""Translate a parsed ASL state machine into the shared flowx Pipeline IR.

A state machine is a flat graph of guarded transitions; the flowx IR mixes a
task DAG (``depends_on`` edges) with nested control-flow containers
(:class:`~flowx.models.ir.IfConditionActivity`,
:class:`~flowx.models.ir.ForEachActivity`). The translator reconstructs the
nested structure by walking each ``Next`` chain and, at a ``Choice``, finding
the join state where its branches reconverge so the branch bodies nest inside
the condition and the continuation nests after it.

Deterministic states (``Choice`` -> condition, ``Map`` -> for-each, ``Parallel``
-> concurrent branches, ``Wait``, ``Pass``, and ``Task`` states that start a
nested state machine -> run-job) translate without an LLM. ``Task`` states that
run a Lambda handler, a Glue job, or a service integration carry arbitrary code
and become agentic placeholders for the convert phase's gap list. Irreducible or
cyclic graphs fall back to a placeholder rather than mistranslating.

See ``design/stepfunctions-ir-mapping.md`` for the mapping rationale and the
constructs this first increment defers.
"""

from __future__ import annotations

import dataclasses
import json
import re
from dataclasses import dataclass, field
from typing import Any

from flowx.models.ir import (
    Activity,
    Dependency,
    ForEachActivity,
    IfConditionActivity,
    Pipeline,
    PlaceholderActivity,
    RunJobActivity,
    SetVariableActivity,
    WaitActivity,
)
from flowx.sources.stepfunctions.asl import State, StateMachine, build_states, iter_substates
from flowx.sources.stepfunctions.jsonpath import resolve_parameters, result_field
from flowx.utils import normalize_task_key

# ASL comparison operators that reduce to a flowx condition operator. Compound
# rules (And/Or/Not), the ``*Path`` variants, and the ``Is*`` type checks are not
# in this map; they keep the raw rule in the condition's left operand for review.
_COMPARATORS: dict[str, str] = {
    "StringEquals": "==",
    "StringLessThan": "<",
    "StringGreaterThan": ">",
    "StringLessThanEquals": "<=",
    "StringGreaterThanEquals": ">=",
    "NumericEquals": "==",
    "NumericLessThan": "<",
    "NumericGreaterThan": ">",
    "NumericLessThanEquals": "<=",
    "NumericGreaterThanEquals": ">=",
    "BooleanEquals": "==",
    "TimestampEquals": "==",
    "TimestampLessThan": "<",
    "TimestampGreaterThan": ">",
    "TimestampLessThanEquals": "<=",
    "TimestampGreaterThanEquals": ">=",
}

_SLUG = re.compile(r"[^0-9A-Za-z]+")


@dataclass(slots=True)
class _KeyAllocator:
    """Hands out task keys that are unique within one pipeline."""

    _used: set[str] = field(default_factory=set)

    def allocate(self, name: str) -> str:
        """Returns a slugified, deduplicated task key derived from *name*."""
        base = _SLUG.sub("_", name).strip("_").lower() or "task"
        key = base
        suffix = 2
        while key in self._used:
            key = f"{base}_{suffix}"
            suffix += 1
        self._used.add(key)
        return key


@dataclass(slots=True)
class _Flow:
    """Dataflow accumulated across states as they are translated in execution order.

    Attributes:
        producers: Field name -> task key of the state that wrote it (via ``ResultPath``),
            so a later state's ``Parameters`` reference resolves to that task's value.
        job_parameters: Declared job parameters (name -> default) for references that
            trace to the state-machine input rather than an upstream state.
        visited_states: States processed in the main walk; used by the Catch second pass to
            know which handler regions still need translation.
        state_task_keys: State name -> last allocated task key; used to wire rejoin edges when
            a catch handler eventually reaches a state already on the main path.
        catch_edges: Handler state name -> failure-outcome deps to prepend; populated while
            translating Task states that carry a ``Catch`` block.
        in_catch_handler: True while translating catch handler regions; changes the visited-
            state check from "make a cycle placeholder" to "stop at rejoin, add a note."
    """

    producers: dict[str, str] = field(default_factory=dict)
    job_parameters: dict[str, str] = field(default_factory=dict)
    visited_states: set[str] = field(default_factory=set)
    state_task_keys: dict[str, str] = field(default_factory=dict)
    catch_edges: dict[str, list[Dependency]] = field(default_factory=dict)
    in_catch_handler: bool = False


def translate_state_machine(machine: StateMachine) -> Pipeline:
    """Translates a parsed state machine into a flowx :class:`Pipeline`.

    Args:
        machine: The parsed Amazon States Language state machine.

    Returns:
        A pipeline whose tasks are the translated states. Approximations and
        deferred constructs are recorded in ``not_translatable``.
    """
    allocator = _KeyAllocator()
    notes: list[dict[str, Any]] = []
    flow = _Flow()
    tasks, _ = _translate_region(
        states=machine.states,
        start=machine.start_at,
        stop=None,
        incoming=[],
        allocator=allocator,
        notes=notes,
        flow=flow,
        visited=frozenset(),
    )
    tasks = _apply_catch_edges(tasks, flow, machine.states, allocator, notes)
    parameters = [{"name": name, "default": default} for name, default in sorted(flow.job_parameters.items())]
    return Pipeline(
        name=machine.name,
        description=machine.comment,
        parameters=parameters or None,
        tasks=tasks,
        not_translatable=notes,
        tags={"source": "stepfunctions", "state_machine": machine.name},
    )


def _translate_region(
    *,
    states: dict[str, State],
    start: str,
    stop: str | None,
    incoming: list[str],
    allocator: _KeyAllocator,
    notes: list[dict[str, Any]],
    flow: _Flow,
    visited: frozenset[str],
) -> tuple[list[Activity], list[str]]:
    """Translates the chain of states from *start* up to (but excluding) *stop*.

    Args:
        states: The state map this chain lives in (a machine or a Map/Parallel sub-machine).
        start: Name of the first state in the region.
        stop: Name of the join state that ends the region, or ``None`` to run to the path's end.
        incoming: Task keys the region's first task depends on.
        allocator: Shared task-key allocator.
        notes: Accumulator for approximation / deferral notes.
        visited: States already on this translation path, used to break cycles.

    Returns:
        ``(activities, exit_keys)`` -- the translated activities and the task keys
        a successor region should depend on.
    """
    activities: list[Activity] = []
    exits = list(incoming)
    current: str | None = start
    while current is not None and current != stop:
        if current not in states:
            notes.append({"state": current, "issue": "transition targets an unknown state"})
            break
        if current in visited:
            if flow.in_catch_handler and current in flow.visited_states:
                notes.append(
                    {"state": current, "issue": "catch handler rejoins main path; downstream deps are partially wired"}
                )
                if key := flow.state_task_keys.get(current):
                    exits = [key]
                break
            placeholder = _placeholder(states[current], allocator, "cyclic transition is not supported", exits)
            activities.append(placeholder)
            notes.append({"state": current, "issue": "cyclic transition routed to an agentic placeholder"})
            return activities, [placeholder.task_key]
        visited = visited | {current}
        flow.visited_states.add(current)
        state = states[current]

        if state.type in ("Succeed", "Fail"):
            return activities, exits
        activity: Activity
        if state.type == "Pass":
            pass_activity = _translate_pass(state, allocator, exits)
            if pass_activity is None:
                if state.is_terminal:
                    return activities, exits
                current = state.next_state
                continue
            activity = pass_activity
            activities.append(activity)
            exits = [activity.task_key]
            flow.state_task_keys[current] = activity.task_key
        elif state.type == "Wait":
            activity = _translate_wait(state, allocator, notes, exits)
            activities.append(activity)
            exits = [activity.task_key]
            flow.state_task_keys[current] = activity.task_key
        elif state.type == "Task":
            activity = _translate_task(state, allocator, notes, flow, exits)
            activity = _apply_retry(state, activity, notes)
            activities.append(activity)
            exits = [activity.task_key]
            flow.state_task_keys[current] = activity.task_key
            _register_producer(state, activity.task_key, flow)
            _register_catch(state, activity.task_key, flow)
        elif state.type == "Map":
            activity = _translate_map(state, allocator, notes, flow, exits)
            activities.append(activity)
            exits = [activity.task_key]
            flow.state_task_keys[current] = activity.task_key
        elif state.type == "Parallel":
            branch_tasks, exits = _translate_parallel(state, allocator, notes, flow, exits)
            activities.extend(branch_tasks)
        elif state.type == "Choice":
            condition, join = _translate_choice(
                states=states,
                state=state,
                incoming=exits,
                allocator=allocator,
                notes=notes,
                flow=flow,
                visited=visited,
            )
            activities.append(condition)
            exits = [condition.task_key]
            current = join
            continue
        else:
            placeholder = _placeholder(state, allocator, f"unsupported state type {state.type!r}", exits)
            activities.append(placeholder)
            exits = [placeholder.task_key]

        if state.is_terminal:
            return activities, exits
        current = state.next_state
    return activities, exits


def _translate_task(
    state: State, allocator: _KeyAllocator, notes: list[dict[str, Any]], flow: _Flow, incoming: list[str]
) -> Activity:
    """Translates a Task state.

    A nested state machine (``states:startExecution``) or a Glue workflow
    (``glue:startWorkflowRun``) becomes a run-job task targeting the job the
    peer source converts that workload into -- the ``job_name`` is normalised to
    the bundle job-resource key so ``${resources.jobs.<key>.id}`` resolves when
    both jobs are packaged in one bundle. Everything else (Lambda, a single Glue
    job, a service integration) becomes an agentic placeholder whose ``Parameters``
    (JSONPath I/O) are rewritten into Databricks task parameters.
    """
    key = allocator.allocate(state.name)
    resource = str(state.definition.get("Resource", "") or "")
    raw_parameters = state.definition.get("Parameters")
    parameters = raw_parameters if isinstance(raw_parameters, dict) else {}
    if "states:startExecution" in resource:
        job_name = normalize_task_key(_arn_name(parameters.get("StateMachineArn")) or state.name)
        return RunJobActivity(name=state.name, task_key=key, job_name=job_name, depends_on=_deps(incoming))
    if "glue:startWorkflowRun" in resource:
        workflow_name = parameters.get("Name")
        if isinstance(workflow_name, str) and workflow_name:
            if parameters.get("RunProperties"):
                notes.append(
                    {"state": state.name, "issue": "Glue workflow RunProperties are not mapped to job parameters"}
                )
            return RunJobActivity(
                name=state.name, task_key=key, job_name=normalize_task_key(workflow_name), depends_on=_deps(incoming)
            )
        notes.append(
            {"state": state.name, "issue": "Glue startWorkflowRun has no literal Name; routed to a placeholder"}
        )
    service = _service_label(resource)
    base_parameters = (
        resolve_parameters(parameters, flow.producers, flow.job_parameters, notes, state.name) if parameters else None
    )
    return PlaceholderActivity(
        name=state.name,
        task_key=key,
        original_type=f"Task:{service}",
        comment=f"Step Functions Task {state.name!r} invokes {service}; port the handler or job body.",
        raw_definition=state.definition,
        base_parameters=base_parameters or None,
        depends_on=_deps(incoming),
    )


def _apply_retry(state: State, activity: Activity, notes: list[dict[str, Any]]) -> Activity:
    """Returns *activity* with retry fields set from the state's ``Retry`` block.

    Takes the entry with the highest ``MaxAttempts`` as the governing policy.
    ``BackoffRate`` has no IR equivalent and is noted.
    """
    retry_list = [entry for entry in state.definition.get("Retry", []) if isinstance(entry, dict)]
    if not retry_list:
        return activity
    if any(entry.get("BackoffRate") for entry in retry_list):
        notes.append({"state": state.name, "issue": "Retry BackoffRate is not mapped; retries use a fixed interval"})
    best = max(retry_list, key=lambda entry: int(entry.get("MaxAttempts", 3)))
    max_retries = int(best.get("MaxAttempts", 3))
    interval = best.get("IntervalSeconds", 1)
    min_retry_interval_millis = int(float(interval) * 1000) if isinstance(interval, (int, float)) else 1000
    return dataclasses.replace(activity, max_retries=max_retries, min_retry_interval_millis=min_retry_interval_millis)


def _register_catch(state: State, task_key: str, flow: _Flow) -> None:
    """Records each ``Catch`` entry's handler as a pending failure-dep edge."""
    for entry in state.definition.get("Catch", []):
        if not isinstance(entry, dict):
            continue
        handler_name = entry.get("Next")
        if isinstance(handler_name, str) and handler_name:
            flow.catch_edges.setdefault(handler_name, []).append(Dependency(task_key=task_key, outcome="ALL_FAILED"))


def _apply_catch_edges(
    tasks: list[Activity],
    flow: _Flow,
    states: dict[str, Any],
    allocator: _KeyAllocator,
    notes: list[dict[str, Any]],
) -> list[Activity]:
    """Translates catch handler regions and wires failure deps onto their first tasks.

    Handlers already on the main path get the failure dep patched in place.
    Handlers not yet translated are translated as top-level sibling tasks with
    the failure dep on their first activity. This runs once after the main
    ``_translate_region`` walk in ``translate_state_machine``.
    """
    if not flow.catch_edges:
        return tasks
    tasks = list(tasks)
    flow.in_catch_handler = True
    for handler_name, failure_deps in sorted(flow.catch_edges.items()):
        if handler_name in flow.visited_states:
            for index, task in enumerate(tasks):
                if task.name == handler_name:
                    existing = list(task.depends_on or [])
                    tasks[index] = dataclasses.replace(task, depends_on=existing + failure_deps)
                    break
        else:
            handler_tasks, _ = _translate_region(
                states=states,
                start=handler_name,
                stop=None,
                incoming=[],
                allocator=allocator,
                notes=notes,
                flow=flow,
                visited=frozenset(flow.visited_states),
            )
            if handler_tasks:
                existing = list(handler_tasks[0].depends_on or [])
                handler_tasks[0] = dataclasses.replace(handler_tasks[0], depends_on=existing + failure_deps)
            tasks.extend(handler_tasks)
    flow.in_catch_handler = False
    return tasks


def _register_producer(state: State, task_key: str, flow: _Flow) -> None:
    """Records the field a Task writes via ``ResultPath`` so later states resolve it to this task's value."""
    field = result_field(state.definition)
    if field:
        flow.producers[field] = task_key


def _translate_wait(
    state: State, allocator: _KeyAllocator, notes: list[dict[str, Any]], incoming: list[str]
) -> WaitActivity:
    """Translates a Wait state; dynamic/timestamp durations default to 0s with a note."""
    key = allocator.allocate(state.name)
    seconds = state.definition.get("Seconds")
    if not isinstance(seconds, int):
        notes.append({"state": state.name, "issue": "Wait uses a path or timestamp duration; defaulted to 0 seconds"})
        seconds = 0
    return WaitActivity(name=state.name, task_key=key, wait_time_seconds=seconds, depends_on=_deps(incoming))


def _translate_pass(state: State, allocator: _KeyAllocator, incoming: list[str]) -> SetVariableActivity | None:
    """Translates a Pass state; a Result-less (pure routing) Pass emits no task."""
    result = state.definition.get("Result")
    if result is None:
        return None
    key = allocator.allocate(state.name)
    value = result if isinstance(result, str) else json.dumps(result)
    return SetVariableActivity(
        name=state.name,
        task_key=key,
        variable_name=state.name,
        variable_value=value,
        value_kind="literal",
        depends_on=_deps(incoming),
    )


def _translate_map(
    state: State, allocator: _KeyAllocator, notes: list[dict[str, Any]], flow: _Flow, incoming: list[str]
) -> ForEachActivity:
    """Translates a Map state into a for-each over its item processor's states."""
    key = allocator.allocate(state.name)
    items = _strip_path(str(state.definition.get("ItemsPath", "$"))) or "$"
    concurrency = state.definition.get("MaxConcurrency")
    inner: list[Activity] = []
    substates = iter_substates(state.definition)
    if substates:
        start, states_raw = substates[0]
        inner, _ = _translate_region(
            states=build_states(states_raw),
            start=start,
            stop=None,
            incoming=[],
            allocator=allocator,
            notes=notes,
            flow=flow,
            visited=frozenset(),
        )
    return ForEachActivity(
        name=state.name,
        task_key=key,
        items_expression=items,
        inner_activities=inner,
        concurrency=concurrency if isinstance(concurrency, int) and concurrency > 0 else None,
        depends_on=_deps(incoming),
    )


def _translate_parallel(
    state: State, allocator: _KeyAllocator, notes: list[dict[str, Any]], flow: _Flow, incoming: list[str]
) -> tuple[list[Activity], list[str]]:
    """Translates a Parallel state: each branch's states run concurrently off the same upstream."""
    substates = iter_substates(state.definition)
    if not substates:
        placeholder = _placeholder(state, allocator, "Parallel state has no branches", incoming)
        return [placeholder], [placeholder.task_key]
    tasks: list[Activity] = []
    exits: list[str] = []
    for start, states_raw in substates:
        branch_tasks, branch_exits = _translate_region(
            states=build_states(states_raw),
            start=start,
            stop=None,
            incoming=incoming,
            allocator=allocator,
            notes=notes,
            flow=flow,
            visited=frozenset(),
        )
        tasks.extend(branch_tasks)
        exits.extend(branch_exits)
    return tasks, exits


def _translate_choice(
    *,
    states: dict[str, State],
    state: State,
    incoming: list[str],
    allocator: _KeyAllocator,
    notes: list[dict[str, Any]],
    flow: _Flow,
    visited: frozenset[str],
) -> tuple[IfConditionActivity, str | None]:
    """Translates a Choice state into a (possibly cascaded) condition and returns its join state."""
    rules = [rule for rule in state.definition.get("Choices", []) if isinstance(rule, dict) and rule.get("Next")]
    default = state.definition.get("Default")
    targets = [rule["Next"] for rule in rules]
    if isinstance(default, str):
        targets.append(default)
    join = _find_join(states, targets) if len(targets) >= 2 else None

    def region(start: str | None) -> list[Activity]:
        if start is None:
            return []
        activities, _ = _translate_region(
            states=states,
            start=start,
            stop=join,
            incoming=[],
            allocator=allocator,
            notes=notes,
            flow=flow,
            visited=visited,
        )
        return activities

    def cascade(remaining: list[dict[str, Any]], *, top: bool) -> IfConditionActivity:
        rule = remaining[0]
        operator, left, right = _comparison(rule, state.name, notes)
        if len(remaining) == 1:
            if_false = region(default if isinstance(default, str) else None)
        else:
            if_false = [cascade(remaining[1:], top=False)]
        key = allocator.allocate(state.name if top else f"{state.name}_else")
        return IfConditionActivity(
            name=state.name if top else f"{state.name} (else)",
            task_key=key,
            op=operator,
            left=left,
            right=right,
            if_true_activities=region(rule["Next"]),
            if_false_activities=if_false,
            depends_on=_deps(incoming) if top else None,
        )

    if not rules:
        placeholder_key = allocator.allocate(state.name)
        condition = IfConditionActivity(
            name=state.name,
            task_key=placeholder_key,
            op="expr",
            left="",
            right="",
            if_false_activities=region(default if isinstance(default, str) else None),
            depends_on=_deps(incoming),
        )
        notes.append({"state": state.name, "issue": "Choice state has no comparison rules"})
        return condition, join
    return cascade(rules, top=True), join


def _placeholder(state: State, allocator: _KeyAllocator, reason: str, incoming: list[str]) -> PlaceholderActivity:
    """Builds an agentic placeholder for a state the first increment does not translate."""
    return PlaceholderActivity(
        name=state.name,
        task_key=allocator.allocate(state.name),
        original_type=f"State:{state.type or 'Unknown'}",
        comment=reason,
        raw_definition=state.definition,
        depends_on=_deps(incoming),
    )


def _comparison(rule: dict[str, Any], state_name: str, notes: list[dict[str, Any]]) -> tuple[str, str, str]:
    """Reduces one ASL choice rule to ``(op, left, right)`` operands for a condition.

    Compound and unmapped rules keep the raw rule JSON in *left* and use the
    ``expr`` operator so the convert reviewer can see the predicate verbatim.
    """
    for comparator, operator in _COMPARATORS.items():
        if comparator in rule:
            return operator, _strip_path(str(rule.get("Variable", ""))), _literal(rule[comparator])
    notes.append({"state": state_name, "issue": "choice predicate not reduced; left operand carries the raw rule"})
    raw = {key: value for key, value in rule.items() if key != "Next"}
    return "expr", json.dumps(raw, sort_keys=True), ""


def _find_join(states: dict[str, State], targets: list[str]) -> str | None:
    """Returns the nearest state reachable from every branch target, or ``None``.

    Approximates the branches' post-dominator: among the states reachable from
    all targets, it picks the one closest to the targets (smallest worst-case
    hop count). Good enough for reducible if/else shapes; irreducible graphs
    simply get ``None`` and their branches run to their own ends.
    """
    reachable = [_forward_reach(states, target) for target in targets]
    common = set(reachable[0])
    for nodes in reachable[1:]:
        common &= nodes
    if not common:
        return None
    depths = [_bfs_depths(states, target) for target in targets]

    def worst_case(node: str) -> int:
        return max(depth.get(node, 1_000_000) for depth in depths)

    return min(common, key=worst_case)


def _successors(states: dict[str, State], name: str) -> list[str]:
    """Returns the next-state names a state transitions to, for graph traversal."""
    state = states.get(name)
    if state is None or state.type in ("Succeed", "Fail"):
        return []
    if state.type == "Choice":
        out = [
            rule["Next"] for rule in state.definition.get("Choices", []) if isinstance(rule, dict) and rule.get("Next")
        ]
        default = state.definition.get("Default")
        if isinstance(default, str):
            out.append(default)
        return out
    return [state.next_state] if state.next_state else []


def _forward_reach(states: dict[str, State], start: str) -> set[str]:
    """Returns every state reachable from *start* (inclusive), following transitions."""
    seen: set[str] = set()
    stack = [start]
    while stack:
        name = stack.pop()
        if name in seen:
            continue
        seen.add(name)
        stack.extend(_successors(states, name))
    return seen


def _bfs_depths(states: dict[str, State], start: str) -> dict[str, int]:
    """Returns the hop count from *start* to each reachable state."""
    depths = {start: 0}
    queue = [start]
    while queue:
        name = queue.pop(0)
        for successor in _successors(states, name):
            if successor not in depths:
                depths[successor] = depths[name] + 1
                queue.append(successor)
    return depths


def _deps(keys: list[str]) -> list[Dependency] | None:
    """Builds a ``depends_on`` list from upstream task keys, or ``None`` when there are none."""
    return [Dependency(task_key=key) for key in keys] or None


def _strip_path(path: str) -> str:
    """Strips the leading ``$.`` / ``$`` from a JSONPath reference variable."""
    if path.startswith("$."):
        return path[2:]
    if path == "$":
        return ""
    return path


def _literal(value: Any) -> str:
    """Renders a choice comparison value as a condition operand string."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _service_label(resource: str) -> str:
    """Returns the AWS service a Task's ``Resource`` ARN targets (``glue``, ``lambda``, ...)."""
    if ":::" in resource:
        return resource.split(":::", 1)[1].split(":", 1)[0] or "unknown"
    if resource.startswith("arn:aws:"):
        parts = resource.split(":")
        return parts[2] if len(parts) > 2 and parts[2] else "unknown"
    return "unknown"


def _arn_name(arn: Any) -> str | None:
    """Returns the trailing name segment of an ARN, or ``None``."""
    if not isinstance(arn, str) or not arn:
        return None
    return arn.rsplit(":", 1)[-1].rsplit("/", 1)[-1] or None
