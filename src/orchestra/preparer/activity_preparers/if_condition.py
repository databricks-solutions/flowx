"""Preparer for IfConditionActivity -> condition_task + branch sibling tasks.

References:
- https://docs.databricks.com/aws/en/jobs/if-else
- https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from flowx.preparer.workflow_preparer import (
    PreparedActivity,
    PreparedArtifacts,
    build_common_task_fields,
    merge_prepared_artifacts,
    prepare_activity,
)

if TYPE_CHECKING:
    from flowx.models.ir import IfConditionActivity


def inject_outcome_dependency(tasks: list[dict[str, Any]], condition_key: str, outcome: str) -> None:
    """Gates branch-root tasks on the condition's outcome.

    A "branch root" is any task in the branch that does not depend on a
    sibling within the same branch.  External dependencies (on a task
    outside the branch) are preserved so a branch task can still wait
    on a global setup task or another upstream activity; the outcome
    edge is appended to ``depends_on`` rather than replacing it.

    Args:
        tasks: Tasks in one branch (mutated in place).
        condition_key: Task key of the enclosing condition task.
        outcome: ``"true"`` or ``"false"``.
    """
    branch_keys = {task.get("task_key") for task in tasks}
    outcome_dep = {"task_key": condition_key, "outcome": outcome}
    for task in tasks:
        deps = list(task.get("depends_on") or [])
        refers_to_branch_sibling = any(dep.get("task_key") in branch_keys for dep in deps)
        if refers_to_branch_sibling:
            continue
        if any(dep.get("task_key") == condition_key and dep.get("outcome") == outcome for dep in deps):
            continue
        task["depends_on"] = [outcome_dep, *deps]


def prepare(activity: IfConditionActivity, *, scope: str = "") -> PreparedActivity:
    """Converts an IfConditionActivity into a condition_task + flattened branches.

    Args:
        activity: The translated if-condition activity from the IR.
        scope: Secret scope name passed through to child preparers.

    Returns:
        A PreparedActivity with the condition_task, sibling branch tasks,
        and aggregated artifacts from both branches.
    """
    task = build_common_task_fields(activity)
    task["condition_task"] = {
        "op": activity.op,
        "left": activity.left,
        "right": activity.right,
    }

    artifacts = PreparedArtifacts()

    if_true_tasks: list[dict[str, Any]] = []
    for child in activity.if_true_activities:
        prepared = prepare_activity(child, scope=scope)
        if_true_tasks.append(prepared.task)
        if_true_tasks.extend(prepared.extra_tasks)
        artifacts = merge_prepared_artifacts(artifacts, prepared)
    inject_outcome_dependency(if_true_tasks, activity.task_key, "true")

    if_false_tasks: list[dict[str, Any]] = []
    for child in activity.if_false_activities:
        prepared = prepare_activity(child, scope=scope)
        if_false_tasks.append(prepared.task)
        if_false_tasks.extend(prepared.extra_tasks)
        artifacts = merge_prepared_artifacts(artifacts, prepared)
    inject_outcome_dependency(if_false_tasks, activity.task_key, "false")

    return PreparedActivity(
        task=task,
        extra_tasks=if_true_tasks + if_false_tasks,
        notebooks=list(artifacts.notebooks),
        secrets=list(artifacts.secrets),
        setup_tasks=list(artifacts.setup_tasks),
        inner_workflows=list(artifacts.inner_workflows),
    )
