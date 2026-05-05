"""Preparer for ForEachActivity -> for_each_task dict.

References:
- https://docs.databricks.com/aws/en/jobs/for-each
- https://docs.databricks.com/aws/en/jobs/task-values
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from flowx.bundler.inner_job_params import (
    collect_inner_job_params,
    normalize_inner_task_params,
)
from flowx.models.dab import DabNotebook, SecretInstruction, SetupTask
from flowx.models.ir import TranslationContext
from flowx.parser.expression_parser import resolve_expression
from flowx.preparer.workflow_preparer import (
    PreparedActivity,
    PreparedWorkflow,
    build_common_task_fields,
    prepare_activity,
)
from flowx.utils import normalize_task_key

if TYPE_CHECKING:
    from flowx.models.ir import ForEachActivity


def _resolve_for_each_inputs(items_expression: str) -> str:
    """Converts an ADF items expression to a DAB dynamic value reference.

    Args:
        items_expression: The raw ADF expression for ForEach items.

    Returns:
        A DAB dynamic value reference string, or the original expression if
        it cannot be resolved.
    """
    if items_expression.startswith("{{"):
        return items_expression

    context = TranslationContext()
    result = resolve_expression(items_expression, context)
    if result is not None and result.kind in ("dab_ref", "literal"):
        return result.value

    # Also try with @ prefix if not present
    if not items_expression.startswith("@"):
        result = resolve_expression("@" + items_expression, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value

    return items_expression


def _inject_input_parameter(inner_task: dict) -> dict:
    """Adds ``{{input}}`` as a base_parameter on the inner task.

    Args:
        inner_task: The prepared inner task dict.

    Returns:
        The task dict with ``item`` parameter injected.
    """
    if "notebook_task" in inner_task:
        params = inner_task["notebook_task"].setdefault("base_parameters", {})
        params["item"] = "{{input}}"
    elif "run_job_task" in inner_task:
        params = inner_task["run_job_task"].setdefault("job_parameters", {})
        params["item"] = "{{input}}"
    return inner_task


def prepare(activity: ForEachActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a ForEachActivity into a DAB for_each_task definition.

    Args:
        activity: The translated for-each activity from the IR.
        scope: Secret scope name (typically the pipeline/job name).

    Returns:
        A PreparedActivity with the for_each_task, plus any notebooks, secrets,
        and inner_workflows from the child activities.
    """
    task = build_common_task_fields(activity)
    concurrency = activity.concurrency if activity.concurrency is not None else 20
    inputs = _resolve_for_each_inputs(activity.items_expression)

    inner_activities = activity.inner_activities
    all_notebooks: list[DabNotebook] = []
    all_secrets: list[SecretInstruction] = []
    all_setup_tasks: list[SetupTask] = []
    inner_workflows: list[PreparedWorkflow] = []

    if len(inner_activities) == 1:
        inner_prepared = prepare_activity(inner_activities[0], scope=scope)
        inner_task = _inject_input_parameter(inner_prepared.task)
        all_notebooks.extend(inner_prepared.notebooks)
        all_secrets.extend(inner_prepared.secrets)
        all_setup_tasks.extend(inner_prepared.setup_tasks)
        inner_workflows.extend(inner_prepared.inner_workflows)

        task["for_each_task"] = {
            "inputs": inputs,
            "task": inner_task,
            "concurrency": concurrency,
        }

    elif len(inner_activities) > 1:
        inner_job_name = f"{activity.task_key}_inner_tasks"
        inner_tasks: list[dict[str, Any]] = []

        for child in inner_activities:
            child_prepared = prepare_activity(child, scope=scope)
            inner_tasks.append(child_prepared.task)
            all_notebooks.extend(child_prepared.notebooks)
            all_secrets.extend(child_prepared.secrets)
            all_setup_tasks.extend(child_prepared.setup_tasks)
            inner_workflows.extend(child_prepared.inner_workflows)

        normalize_inner_task_params(inner_tasks)

        parameters, job_parameters = collect_inner_job_params(inner_tasks)

        inner_workflow = PreparedWorkflow(
            name=inner_job_name,
            tasks=inner_tasks,
            notebooks=[],  # notebooks already collected in all_notebooks
            secrets=[],
            setup_tasks=[],
            parameters=parameters,
        )
        inner_workflows.append(inner_workflow)

        inner_job_key = normalize_task_key(inner_job_name)
        body_task: dict[str, Any] = {
            "task_key": f"{activity.task_key}_iteration",
            "run_job_task": {
                "job_id": f"${{resources.jobs.{inner_job_key}.id}}",
                "job_parameters": job_parameters,
            },
        }

        task["for_each_task"] = {
            "inputs": inputs,
            "task": body_task,
            "concurrency": concurrency,
        }

    else:
        task["for_each_task"] = {
            "inputs": inputs,
            "task": {"task_key": f"{activity.task_key}_noop"},
            "concurrency": concurrency,
        }

    return PreparedActivity(
        task=task,
        notebooks=all_notebooks,
        secrets=all_secrets,
        setup_tasks=all_setup_tasks,
        inner_workflows=inner_workflows,
    )
