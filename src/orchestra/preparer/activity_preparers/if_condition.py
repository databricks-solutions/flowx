"""Preparer for IfConditionActivity -> condition_task + branch sibling tasks.

References:
- https://docs.databricks.com/aws/en/jobs/if-else
- https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orchestra.models.dab import DabNotebook
from orchestra.preparer.workflow_preparer import (
    PreparedActivity,
    PreparedArtifacts,
    build_common_task_fields,
    merge_prepared_artifacts,
    prepare_activity,
)

if TYPE_CHECKING:
    from orchestra.models.ir import IfConditionActivity

# Placeholder emitted by the translator when an operand requires a bridge.
_BRIDGE_PLACEHOLDER_PREFIX = "__BRIDGE__::"


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

    bridge_task, bridge_value_ref, bridge_notebooks = _build_bridge_task(activity)
    left = _rewrite_bridge_placeholder(activity.left, bridge_value_ref)
    right = _rewrite_bridge_placeholder(activity.right, bridge_value_ref)

    task["condition_task"] = {
        "op": activity.op,
        "left": left,
        "right": right,
    }

    # Bridge task runs ahead of the condition task -- the condition must
    # depend on the bridge succeeding.
    if bridge_task is not None:
        bridge_dep = {"task_key": bridge_task["task_key"]}
        existing_deps = list(task.get("depends_on") or [])
        task["depends_on"] = [*existing_deps, bridge_dep]

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

    extras: list[dict[str, Any]] = []
    if bridge_task is not None:
        extras.append(bridge_task)
    extras.extend(if_true_tasks + if_false_tasks)

    notebooks = list(artifacts.notebooks)
    notebooks.extend(bridge_notebooks)

    return PreparedActivity(
        task=task,
        extra_tasks=extras,
        notebooks=notebooks,
        secrets=list(artifacts.secrets),
        setup_tasks=list(artifacts.setup_tasks),
        inner_workflows=list(artifacts.inner_workflows),
    )


def _build_bridge_task(
    activity: IfConditionActivity,
) -> tuple[dict[str, Any] | None, str | None, list[DabNotebook]]:
    """Synthesises a hidden SetVariable-like task that evaluates a bridged
    notebook_code expression for an IfCondition operand.

    C-07 (CF-iter2-001 / CF-iter2-003 / VAREX-003): when the translator
    surfaces ``bridge_notebook_code``, the preparer wires it into the job
    graph as a Python notebook task that writes a single task value the
    condition operand can reference.
    """
    if not activity.bridge_notebook_code:
        return None, None, []

    bridge_key = f"{activity.task_key}_bridge"
    value_key = "result"
    notebook_relative_path = f"notebooks/{bridge_key}.py"

    # Build the bridge notebook source.  base_parameters can include widget
    # bindings the bridge expression depends on.
    base_parameters: dict[str, str] = dict(activity.bridge_required_parameters)
    notebook_source = _render_bridge_notebook(
        activity.bridge_notebook_code,
        activity.bridge_notebook_imports,
        list(base_parameters.keys()),
        value_key,
    )

    bridge_task: dict[str, Any] = {
        "task_key": bridge_key,
        "notebook_task": {
            "notebook_path": f"../src/{notebook_relative_path}",
            "base_parameters": base_parameters,
        },
    }
    bridge_value_ref = f"{{{{tasks.{bridge_key}.values.{value_key}}}}}"
    notebooks = [DabNotebook(relative_path=notebook_relative_path, content=notebook_source)]
    return bridge_task, bridge_value_ref, notebooks


def _rewrite_bridge_placeholder(operand: str, bridge_value_ref: str | None) -> str:
    """Rewrites a translator-side bridge placeholder to the real task value."""
    if not isinstance(operand, str):
        return operand
    if not operand.startswith(_BRIDGE_PLACEHOLDER_PREFIX):
        return operand
    if bridge_value_ref is None:
        # Defensive: translator surfaced a placeholder but no bridge code.
        return operand
    return bridge_value_ref


def _render_bridge_notebook(
    notebook_code: str,
    imports: list[str],
    widget_names: list[str],
    value_key: str,
) -> str:
    """Generates the Python source for a condition bridge notebook."""
    lines: list[str] = []
    seen_imports: set[str] = set()
    for imp in imports:
        if imp in seen_imports:
            continue
        seen_imports.add(imp)
        lines.append(imp)
    if seen_imports:
        lines.append("")
    for widget in widget_names:
        lines.append(f"dbutils.widgets.text('{widget}', '')")
    if widget_names:
        lines.append("")
    lines.append(f"_bridge_value = {notebook_code}")
    lines.append(f"dbutils.jobs.taskValues.set(key='{value_key}', value=_bridge_value)")
    return "\n".join(lines) + "\n"
