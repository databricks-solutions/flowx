"""Preparer for SwitchActivity -> chained condition_tasks + flattened branches.

References:
- https://docs.databricks.com/aws/en/jobs/if-else
- https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from orchestra.models.dab import DabNotebook
from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import resolve_expression, resolve_interpolated_string
from orchestra.preparer.activity_preparers.if_condition import inject_outcome_dependency
from orchestra.preparer.workflow_preparer import (
    PreparedActivity,
    PreparedArtifacts,
    build_common_task_fields,
    merge_prepared_artifacts,
    prepare_activity,
)

if TYPE_CHECKING:
    from orchestra.models.ir import SwitchActivity

_BRIDGE_PLACEHOLDER_PREFIX = "__BRIDGE__::"


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

    C-13 (CF-iter2-004): when the input already contains a ``{{...}}``
    DAB dynamic value reference or is not an ``@``-prefixed ADF
    expression, return it unchanged.  Constructing a bare
    :class:`TranslationContext` from this side strips global parameters
    and the variable_cache, so re-resolving a previously-lowered ref
    would discard the data the translator already populated.  The
    translator-side bridge placeholder (``__BRIDGE__::``) is likewise
    preserved so the bridge rewrite step downstream can fill it.
    """
    if not isinstance(on_expression, str):
        return on_expression
    if not on_expression:
        return on_expression
    # Already a DAB ref / translator placeholder: pass through unchanged.
    if "{{" in on_expression or on_expression.startswith(_BRIDGE_PLACEHOLDER_PREFIX):
        return on_expression
    # Only attempt resolution for bare ADF expressions.  Other strings
    # (raw literals) pass through.
    if not on_expression.startswith("@"):
        return on_expression
    context = TranslationContext()
    if "@{" in on_expression:
        return resolve_interpolated_string(on_expression, context)
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
    # C-07: if the translator produced a bridge_notebook_code, synthesise
    # the bridge task and rewrite the on-expression to its task value.
    bridge_task, bridge_value_ref, bridge_notebooks = _build_switch_bridge_task(activity)
    if bridge_value_ref is not None and resolved_expr.startswith(_BRIDGE_PLACEHOLDER_PREFIX):
        resolved_expr = bridge_value_ref

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

        extras_no_cases: list[dict[str, Any]] = []
        notebooks_no_cases = list(artifacts.notebooks)
        if bridge_task is not None:
            extras_no_cases.append(bridge_task)
            notebooks_no_cases.extend(bridge_notebooks)
            existing_deps = list(task.get("depends_on") or [])
            task["depends_on"] = [*existing_deps, {"task_key": bridge_task["task_key"]}]
        extras_no_cases.extend(default_tasks)

        return PreparedActivity(
            task=task,
            extra_tasks=extras_no_cases,
            notebooks=notebooks_no_cases,
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

    notebooks_out = list(artifacts.notebooks)
    extras_out = extra_tasks
    if bridge_task is not None:
        # Bridge task runs first; the first condition task depends on it.
        extras_out = [bridge_task, *extra_tasks]
        notebooks_out.extend(bridge_notebooks)
        existing_deps = list(first_condition_task.get("depends_on") or [])
        first_condition_task["depends_on"] = [
            *existing_deps,
            {"task_key": bridge_task["task_key"]},
        ]

    return PreparedActivity(
        task=first_condition_task,
        extra_tasks=extras_out,
        notebooks=notebooks_out,
        secrets=list(artifacts.secrets),
        setup_tasks=list(artifacts.setup_tasks),
        inner_workflows=list(artifacts.inner_workflows),
        task_key_remap=remap,
    )


def _build_switch_bridge_task(
    activity: SwitchActivity,
) -> tuple[dict[str, Any] | None, str | None, list[DabNotebook]]:
    """Synthesises a bridge SetVariable-style task for a Switch on-expression.

    C-07 (CF-iter2-001 / CF-iter2-003): when ``on_expression`` contains an
    ADF function call (e.g. ``@toUpper(coalesce(item()?.type, 'default'))``)
    we route the value through a hidden notebook task so the
    ``condition_task.left`` operand is a real task-value reference and not
    a raw ADF expression string.
    """
    if not activity.bridge_notebook_code:
        return None, None, []

    bridge_key = f"{activity.task_key}_bridge"
    value_key = "result"
    notebook_relative_path = f"notebooks/{bridge_key}.py"

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


def _render_bridge_notebook(
    notebook_code: str,
    imports: list[str],
    widget_names: list[str],
    value_key: str,
) -> str:
    """Generates the Python source for a Switch on-expression bridge notebook."""
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
