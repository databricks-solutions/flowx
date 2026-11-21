"""Adds the dependency edges that task-value references need.

When convert turns ``@activity('P').output`` or ``@variables('X')`` into ``{{tasks.P.values.Y}}``, the
reading task must have ``P`` upstream of it, or Databricks rejects the job at deploy time with
"Task 'P' must be a dependency of task 'T'". A path through other tasks is enough (checked against a
live workspace), so an edge is added only when there is no path at all.

The pass follows the job layout package writes. IfCondition branches and Switch cases become tasks of
the same job, so a reference from or to a branch task gets its edge on that task. A ForEach body runs
inside its loop task, so the loop stands for its whole body in the enclosing job, and the body is
handled as a job of its own.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, fields, is_dataclass, replace
from typing import Any

from flowx.models.ir import (
    Activity,
    Dependency,
    ForEachActivity,
    IfConditionActivity,
    Pipeline,
    SwitchActivity,
)

TASK_VALUE_REFERENCE = re.compile(r"\{\{tasks\.([^.}]+)\.values\.[^}]+\}\}")

# Nested activities are walked on their own, a motif's original_activities are kept only for reference,
# and the rest are notes for people rather than values a task reads at run time.
_IGNORED_FIELDS = frozenset(
    {
        "inner_activities",
        "if_true_activities",
        "if_false_activities",
        "cases",
        "default_activities",
        "original_activities",
        "depends_on",
        "description",
        "raw_definition",
        "parameter_approximations",
    }
)

_NON_SUCCESS_OUTCOMES = frozenset({"Failed", "Skipped", "Completed"})

# One step on the way down to a branch task: the IfCondition or Switch key and which branch or case.
_BranchStep = tuple[str, int]


@dataclass(slots=True, kw_only=True)
class _JobTask:
    """An activity that becomes one task of a job, with the branches it sits in."""

    activity: Activity
    branch_path: tuple[_BranchStep, ...]


def backfill_task_value_dependencies(pipeline: Pipeline, warnings: list[str]) -> Pipeline:
    """Returns a copy of the pipeline where every task-value reader waits for the task it reads.

    The input pipeline is left as it was. A reader that cannot be given a safe edge gets none and is
    described in ``warnings`` instead.

    Args:
        pipeline: The translated pipeline, after motif collapse.
        warnings: Translation warnings list; one entry is appended for each reader to review.

    Returns:
        A new :class:`Pipeline` with the missing ``depends_on`` edges added.
    """
    added_upstream: dict[int, list[str]] = {}
    _plan_job(pipeline.tasks, added_upstream, warnings)
    return replace(pipeline, tasks=_with_added_upstream(pipeline.tasks, added_upstream))


def _plan_job(activities: list[Activity], added_upstream: dict[int, list[str]], warnings: list[str]) -> None:
    """Works out the edges one job needs, keyed by ``id()`` of the reading activity, then does each loop body."""
    job_tasks = _job_tasks(activities, branch_path=())
    index_of = {job_task.activity.task_key: index for index, job_task in enumerate(job_tasks)}
    holder_of = dict(index_of)
    for index, job_task in enumerate(job_tasks):
        if isinstance(job_task.activity, ForEachActivity):
            for task_key in _task_keys_in_tree(job_task.activity.inner_activities):
                holder_of.setdefault(task_key, index)
    upstream = [_direct_upstream(job_task, job_tasks, index_of) for job_task in job_tasks]

    for index, job_task in enumerate(job_tasks):
        reader = job_task.activity
        new_upstream: list[str] = []
        for producer in sorted(_references_read_by(reader)):
            holder = holder_of.get(producer)
            if holder is None or holder == index:
                continue
            holder_task = job_tasks[holder]
            described = _describe(producer, holder_task.activity.task_key)
            reading = _describe(_reader_of(reader, producer), reader.task_key)
            if _in_other_branch(job_task.branch_path, holder_task.branch_path):
                warnings.append(
                    f"{reading} reads a task value from {described}, which is in another branch of the "
                    "same IfCondition or Switch and never runs alongside it; no dependency was added. Review this "
                    "task before deploying the job."
                )
                continue
            if holder_task.activity.task_key != producer:
                warnings.append(
                    f"{reading} reads a task value from {described}; task values do not leave a ForEach body, so "
                    "the value needs another route. Review this task before deploying the job."
                )
            if _is_upstream(holder, of=index, upstream=upstream):
                continue
            if _is_upstream(index, of=holder, upstream=upstream):
                warnings.append(
                    f"{reading} reads a task value from {described}, which runs after it; no dependency "
                    "was added because it would make a cycle. Review this task before deploying the job."
                )
                continue
            new_upstream.append(holder_task.activity.task_key)
            upstream[index].add(holder)

        if new_upstream:
            if any(dependency.outcome in _NON_SUCCESS_OUTCOMES for dependency in reader.depends_on or []):
                warnings.append(
                    f"'{reader.task_key}' now also waits for {_quoted_list(new_upstream)} because it reads a task "
                    "value from there, and it already runs on another task's failure, skip or completion. Check its "
                    "run condition still matches the ADF pipeline."
                )
            added_upstream[id(reader)] = new_upstream

    for job_task in job_tasks:
        if isinstance(job_task.activity, ForEachActivity):
            _plan_job(job_task.activity.inner_activities, added_upstream, warnings)


def _job_tasks(activities: list[Activity], *, branch_path: tuple[_BranchStep, ...]) -> list[_JobTask]:
    """Lists the activities that become tasks of one job, opening IfCondition and Switch branches."""
    job_tasks: list[_JobTask] = []
    for activity in activities:
        job_tasks.append(_JobTask(activity=activity, branch_path=branch_path))
        for branch_index, branch in enumerate(_branches(activity)):
            job_tasks.extend(_job_tasks(branch, branch_path=(*branch_path, (activity.task_key, branch_index))))
    return job_tasks


def _branches(activity: Activity) -> list[list[Activity]]:
    """Returns an IfCondition's two branches or a Switch's cases and default; nothing for other activities."""
    if isinstance(activity, IfConditionActivity):
        return [activity.if_true_activities, activity.if_false_activities]
    if isinstance(activity, SwitchActivity):
        return [case.activities for case in activity.cases] + [activity.default_activities]
    return []


def _nested_scopes(activity: Activity) -> list[list[Activity]]:
    """Returns every list of activities a container holds: branches, cases or a loop body."""
    if isinstance(activity, ForEachActivity):
        return [activity.inner_activities]
    return _branches(activity)


def _direct_upstream(job_task: _JobTask, job_tasks: list[_JobTask], index_of: dict[str, int]) -> set[int]:
    """Returns the tasks of the same job one task waits for directly once package lays the job out.

    A branch task that waits for nothing else in its own branch is gated on its IfCondition or Switch.
    """
    upstream = {
        index_of[dependency.task_key]
        for dependency in job_task.activity.depends_on or []
        if dependency.task_key in index_of
    }
    if job_task.branch_path:
        container_key, _ = job_task.branch_path[-1]
        same_branch = {
            index_of[other.activity.task_key] for other in job_tasks if other.branch_path == job_task.branch_path
        }
        if not upstream & same_branch:
            upstream.add(index_of[container_key])
    return upstream


def _in_other_branch(reader_path: tuple[_BranchStep, ...], producer_path: tuple[_BranchStep, ...]) -> bool:
    """Reports whether two tasks sit in different branches of the same IfCondition or Switch."""
    producer_branches = dict(producer_path)
    return any(
        container_key in producer_branches and producer_branches[container_key] != branch_index
        for container_key, branch_index in reader_path
    )


def _references_read_by(activity: Activity) -> set[str]:
    """Names the producers a task reads: its own fields, plus its whole body for a ForEach."""
    producers = _own_references(activity)
    if isinstance(activity, ForEachActivity):
        producers |= _references_in_tree(activity.inner_activities)
    return producers


def _references_in_tree(activities: list[Activity]) -> set[str]:
    """Names the producers referenced by any of these activities, at any depth."""
    producers: set[str] = set()
    for activity in activities:
        producers |= _own_references(activity)
        for scope in _nested_scopes(activity):
            producers |= _references_in_tree(scope)
    return producers


def _with_added_upstream(activities: list[Activity], added_upstream: dict[int, list[str]]) -> list[Activity]:
    """Rebuilds an activity list with the planned edges, leaving the original objects untouched."""
    rebuilt: list[Activity] = []
    for activity in activities:
        updated = _with_rebuilt_children(activity, added_upstream)
        new_upstream = added_upstream.get(id(activity))
        if new_upstream:
            added = [Dependency(task_key=task_key) for task_key in new_upstream]
            updated = replace(updated, depends_on=[*(activity.depends_on or []), *added])
        rebuilt.append(updated)
    return rebuilt


def _with_rebuilt_children(activity: Activity, added_upstream: dict[int, list[str]]) -> Activity:
    """Returns a container with its branches, cases or body rebuilt; other activities come back as they are."""
    if isinstance(activity, ForEachActivity):
        return replace(activity, inner_activities=_with_added_upstream(activity.inner_activities, added_upstream))
    if isinstance(activity, IfConditionActivity):
        return replace(
            activity,
            if_true_activities=_with_added_upstream(activity.if_true_activities, added_upstream),
            if_false_activities=_with_added_upstream(activity.if_false_activities, added_upstream),
        )
    if isinstance(activity, SwitchActivity):
        return replace(
            activity,
            cases=[
                replace(case, activities=_with_added_upstream(case.activities, added_upstream))
                for case in activity.cases
            ],
            default_activities=_with_added_upstream(activity.default_activities, added_upstream),
        )
    return activity


def _task_keys_in_tree(activities: list[Activity]) -> frozenset[str]:
    """Collects the task key of every activity, at any depth."""
    task_keys: set[str] = set()
    for activity in activities:
        task_keys.add(activity.task_key)
        for scope in _nested_scopes(activity):
            task_keys |= _task_keys_in_tree(scope)
    return frozenset(task_keys)


def _own_references(activity: Activity) -> set[str]:
    """Names the producers referenced in an activity's own fields, leaving out the activities it holds."""
    producers: set[str] = set()
    for activity_field in fields(activity):
        if activity_field.name not in _IGNORED_FIELDS:
            _collect_references(getattr(activity, activity_field.name), producers)
    return producers


def _collect_references(value: Any, producers: set[str]) -> None:
    """Adds the producer of every ``{{tasks.P.values.Y}}`` found in a value, however deeply nested."""
    if isinstance(value, str):
        producers.update(TASK_VALUE_REFERENCE.findall(value))
    elif isinstance(value, dict):
        for item in value.values():
            _collect_references(item, producers)
    elif isinstance(value, list | tuple | set | frozenset):
        for item in value:
            _collect_references(item, producers)
    elif is_dataclass(value) and not isinstance(value, type):
        for value_field in fields(value):
            _collect_references(getattr(value, value_field.name), producers)


def _is_upstream(candidate: int, *, of: int, upstream: list[set[int]]) -> bool:
    """Reports whether task ``candidate`` runs before task ``of`` through any chain of dependencies."""
    seen: set[int] = set()
    pending = list(upstream[of])
    while pending:
        index = pending.pop()
        if index == candidate:
            return True
        if index not in seen:
            seen.add(index)
            pending.extend(upstream[index])
    return False


def _describe(task_key: str, job_task_key: str) -> str:
    """Names a task for a warning, with the loop that holds it when it sits in a ForEach body."""
    return f"'{task_key}'" if task_key == job_task_key else f"'{task_key}' (inside '{job_task_key}')"


def _reader_of(activity: Activity, producer: str) -> str:
    """Names the task that holds a reference: the activity itself, or the task inside its loop body."""
    if producer in _own_references(activity):
        return activity.task_key
    for scope in _nested_scopes(activity):
        for nested_activity in scope:
            if producer in _own_references(nested_activity):
                return nested_activity.task_key
            nested_reader = _reader_of(nested_activity, producer)
            if nested_reader != nested_activity.task_key:
                return nested_reader
    return activity.task_key


def _quoted_list(task_keys: list[str]) -> str:
    """Formats task keys as a quoted, comma-separated list for a warning."""
    return ", ".join(f"'{task_key}'" for task_key in task_keys)
