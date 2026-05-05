"""Preparer for SwitchActivity -> chained condition_tasks + flattened branches.

References:
- https://docs.databricks.com/aws/en/jobs/if-else
- https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from flowx.models.ir import TranslationContext
from flowx.parser.expression_parser import resolve_expression, resolve_interpolated_string
from flowx.preparer.activity_preparers.if_condition import inject_outcome_dependency
from flowx.preparer.workflow_preparer import (
    PreparedActivity,
    PreparedArtifacts,
    build_common_task_fields,
    merge_prepared_artifacts,
    prepare_activity,
)

if TYPE_CHECKING:
    from flowx.models.ir import SwitchActivity


def sanitize_case_key(value: str) -> str:
    """Returns a task-key-safe form of a switch case value.

    Used by both the in-process Switch preparer and the JSON-reload path in
    ``dab_writer`` so the rendered task graph is identical regardless of
    which path produced it.
    """
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", value)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    return sanitized or "empty"


def resolve_switch_on_expression(on_expression: str) -> str:
    """Resolves the switch ``on`` expression to a DAB dynamic value ref.

    Idempotent: an already-resolved DAB ref (``{{job.parameters.X}}``) or
    plain literal passes through unchanged.  Both the in-process preparer
    and the JSON-reload path call this so a hand-edited IR with a raw
    ``@variables(...)`` is still resolved before being written to YAML.
    """
    context = TranslationContext()
    if "@{" in on_expression:
        return resolve_interpolated_string(on_expression, context)
    if on_expression.startswith("@"):
        result = resolve_expression(on_expression, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value
    return on_expression


def prepare(activity: SwitchActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a SwitchActivity into a chain of flattened condition tasks.

    Args:
        activity: The translated switch activity from the IR.
        scope: Secret scope name passed through to child preparers.

    Returns:
        A PreparedActivity with the first condition as ``task`` and all
        subsequent conditions + case bodies as ``extra_tasks``.
    """
    artifacts = PreparedArtifacts()
    extra_tasks: list[dict[str, Any]] = []

    resolved_expr = resolve_switch_on_expression(activity.on_expression)

    if not activity.cases:
        task = build_common_task_fields(activity)
        task["condition_task"] = {"op": "EQUAL_TO", "left": "true", "right": "true"}

        default_tasks: list[dict[str, Any]] = []
        for child in activity.default_activities:
            prepared = prepare_activity(child, scope=scope)
            default_tasks.append(prepared.task)
            default_tasks.extend(prepared.extra_tasks)
            artifacts = merge_prepared_artifacts(artifacts, prepared)
        inject_outcome_dependency(default_tasks, activity.task_key, "true")

        return PreparedActivity(
            task=task,
            extra_tasks=default_tasks,
            notebooks=list(artifacts.notebooks),
            secrets=list(artifacts.secrets),
            setup_tasks=list(artifacts.setup_tasks),
            inner_workflows=list(artifacts.inner_workflows),
        )

    # Build one condition task per case, chained via outcome="false" deps.
    # Every case (including the first) is named ``<activity>_case_<value>``
    # for clarity in the rendered job graph.  The first case carries the
    # original Switch's depends_on edges; ``prepare_workflow`` rewrites any
    # downstream task that referenced the bare ``<activity>`` key to point
    # at the renamed first case.
    case_keys: list[str] = []
    for index, case in enumerate(activity.cases):
        is_first = index == 0
        case_key = f"{activity.task_key}_case_{sanitize_case_key(case.value)}"
        case_keys.append(case_key)

        condition_task: dict[str, Any] = {
            "task_key": case_key,
            "condition_task": {"op": "EQUAL_TO", "left": resolved_expr, "right": case.value},
        }
        if is_first:
            # First condition takes the original activity's depends_on, timeouts,
            # retries, etc. — same baseline as before, just under the new key.
            base = build_common_task_fields(activity)
            base.pop("task_key", None)
            condition_task.update(base)
        else:
            condition_task["depends_on"] = [{"task_key": case_keys[index - 1], "outcome": "false"}]

        case_branch_tasks: list[dict[str, Any]] = []
        for child in case.activities:
            prepared = prepare_activity(child, scope=scope)
            case_branch_tasks.append(prepared.task)
            case_branch_tasks.extend(prepared.extra_tasks)
            artifacts = merge_prepared_artifacts(artifacts, prepared)
        inject_outcome_dependency(case_branch_tasks, case_key, "true")

        if is_first:
            first_condition_task = condition_task
        else:
            extra_tasks.append(condition_task)
        extra_tasks.extend(case_branch_tasks)

    branch_default_tasks: list[dict[str, Any]] = []
    for child in activity.default_activities:
        prepared = prepare_activity(child, scope=scope)
        branch_default_tasks.append(prepared.task)
        branch_default_tasks.extend(prepared.extra_tasks)
        artifacts = merge_prepared_artifacts(artifacts, prepared)
    if branch_default_tasks:
        inject_outcome_dependency(branch_default_tasks, case_keys[-1], "false")
        extra_tasks.extend(branch_default_tasks)

    # Tell prepare_workflow to remap any depends_on that referenced the
    # original Switch task_key onto the renamed first case.
    remap = {activity.task_key: case_keys[0]}

    return PreparedActivity(
        task=first_condition_task,
        extra_tasks=extra_tasks,
        notebooks=list(artifacts.notebooks),
        secrets=list(artifacts.secrets),
        setup_tasks=list(artifacts.setup_tasks),
        inner_workflows=list(artifacts.inner_workflows),
        task_key_remap=remap,
    )
