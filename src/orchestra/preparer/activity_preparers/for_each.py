"""Preparer for ForEachActivity -> for_each_task dict.

References:
- https://docs.databricks.com/aws/en/jobs/for-each
- https://docs.databricks.com/aws/en/jobs/task-values
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orchestra.bundler.inner_job_params import (
    collect_inner_job_params,
    normalize_inner_task_params,
)
from orchestra.models.dab import DabNotebook, SecretInstruction, SetupTask
from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import resolve_expression
from orchestra.preparer.workflow_preparer import (
    PreparedActivity,
    PreparedWorkflow,
    _iter_activity_with_descendants,
    build_common_task_fields,
    prepare_activity,
)
from orchestra.utils import normalize_task_key

if TYPE_CHECKING:
    from orchestra.models.ir import ForEachActivity


def _resolve_for_each_inputs_with_bridge(
    activity: ForEachActivity,
) -> tuple[str, dict[str, Any] | None, list[DabNotebook]]:
    """Resolves the ForEach items expression and emits a bridge task when needed.

    C-08 (CF-iter2-002): per Databricks docs, ``for_each_task.inputs``
    accepts a literal JSON array, ``{{tasks.X.values.Y}}``, or
    ``{{job.parameters.X}}``.  Function calls like ``@split(<param>, ',')``
    are rejected.  When the expression resolves to ``notebook_code`` we
    synthesise a hidden seed task that computes the array and publishes
    it as a task value the ForEach inputs reference.

    C-31 (CF4-001): the translator now stashes the resolved bridge code
    on the IR (``inputs_bridge_notebook_code`` and friends) while the
    full TranslationContext is available.  The preparer reads those
    fields rather than re-resolving against an empty TranslationContext
    — the latter silently failed for any expression that needed
    variable_cache lookups (e.g. ``@split(variables('fecha'),',')``).
    """
    items_expression = activity.items_expression
    task_key = activity.task_key

    # IR-supplied bridge wins (C-31).  Falls through to the legacy
    # re-resolution path only when no bridge code was captured.
    if activity.inputs_bridge_notebook_code:
        bridge_key = f"{task_key}_inputs_bridge"
        value_key = "items"
        notebook_relative_path = f"notebooks/{bridge_key}.py"
        base_parameters: dict[str, str] = dict(activity.inputs_bridge_required_parameters)
        notebook_source = _render_for_each_inputs_bridge(
            activity.inputs_bridge_notebook_code,
            list(activity.inputs_bridge_notebook_imports),
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
        return bridge_value_ref, bridge_task, notebooks

    if items_expression.startswith("{{"):
        return items_expression, None, []

    context = TranslationContext()
    result = resolve_expression(items_expression, context)
    if result is None and not items_expression.startswith("@"):
        result = resolve_expression("@" + items_expression, context)

    if result is not None and result.kind in ("dab_ref", "literal"):
        return result.value, None, []

    if result is not None and result.kind == "notebook_code":
        bridge_key = f"{task_key}_inputs_bridge"
        value_key = "items"
        notebook_relative_path = f"notebooks/{bridge_key}.py"
        base_parameters = dict(result.required_parameters)
        notebook_source = _render_for_each_inputs_bridge(
            result.value,
            result.imports,
            list(base_parameters.keys()),
            value_key,
        )
        bridge_task = {
            "task_key": bridge_key,
            "notebook_task": {
                "notebook_path": f"../src/{notebook_relative_path}",
                "base_parameters": base_parameters,
            },
        }
        bridge_value_ref = f"{{{{tasks.{bridge_key}.values.{value_key}}}}}"
        notebooks = [DabNotebook(relative_path=notebook_relative_path, content=notebook_source)]
        return bridge_value_ref, bridge_task, notebooks

    return items_expression, None, []


def _render_for_each_inputs_bridge(
    notebook_code: str,
    imports: list[str],
    widget_names: list[str],
    value_key: str,
) -> str:
    """Generates the Python source for a ForEach-inputs bridge notebook.

    The notebook computes the array value and publishes it via
    ``dbutils.jobs.taskValues.set`` so the parent ForEach task can
    reference it via ``{{tasks.<bridge>.values.items}}``.
    """
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


def prepare(
    activity: ForEachActivity,
    *,
    scope: str = "",
    variable_task_keys: dict[str, str] | None = None,
) -> PreparedActivity:
    """Converts a ForEachActivity into a DAB for_each_task definition.

    Args:
        activity: The translated for-each activity from the IR.
        scope: Secret scope name (typically the pipeline/job name).
        variable_task_keys: C-06 (VAREX-004): parent-job variable->setter
            mapping threaded into ``collect_inner_job_params`` so
            ``@variables('X')`` references in the inner-job body route
            through the variable's task-value rather than fabricating an
            undeclared inner-job parameter.

    Returns:
        A PreparedActivity with the for_each_task, plus any notebooks, secrets,
        and inner_workflows from the child activities.
    """
    task = build_common_task_fields(activity)
    concurrency = activity.concurrency if activity.concurrency is not None else 20
    inputs, inputs_bridge_task, inputs_bridge_notebooks = _resolve_for_each_inputs_with_bridge(activity)
    if inputs_bridge_task is not None:
        existing_deps = list(task.get("depends_on") or [])
        task["depends_on"] = [*existing_deps, {"task_key": inputs_bridge_task["task_key"]}]

    inner_activities = activity.inner_activities
    all_notebooks: list[DabNotebook] = list(inputs_bridge_notebooks)
    all_secrets: list[SecretInstruction] = []
    all_setup_tasks: list[SetupTask] = []
    inner_workflows: list[PreparedWorkflow] = []
    extra_tasks: list[dict[str, Any]] = []
    if inputs_bridge_task is not None:
        extra_tasks.append(inputs_bridge_task)

    if len(inner_activities) == 1:
        inner_prepared = prepare_activity(inner_activities[0], scope=scope)
        all_notebooks.extend(inner_prepared.notebooks)
        all_secrets.extend(inner_prepared.secrets)
        all_setup_tasks.extend(inner_prepared.setup_tasks)
        inner_workflows.extend(inner_prepared.inner_workflows)

        # If the single child contributed extra_tasks (e.g. IfCondition or
        # Switch branch bodies) we cannot inline as for_each_task.task —
        # for_each only accepts a single task.  Escalate to the sub-job
        # path so the entire branch body survives (CF-001).
        if inner_prepared.extra_tasks:
            inner_job_name = f"{activity.task_key}_inner_tasks"
            inner_tasks: list[dict[str, Any]] = [
                inner_prepared.task,
                *inner_prepared.extra_tasks,
            ]
            normalize_inner_task_params(inner_tasks)
            parameters, job_parameters = collect_inner_job_params(inner_tasks, variable_task_keys=variable_task_keys)

            # LSC3-001: gather cluster hints from inner activities so the
            # inner-job default cluster lifts spark_env_vars / custom_tags /
            # driver_node_type_id etc. from the LS-derived cluster spec.
            inner_cluster_hints: list[dict[str, Any]] = []
            for nested_activity in _iter_activity_with_descendants(inner_activities[0]):
                if nested_activity.cluster:
                    inner_cluster_hints.append(dict(nested_activity.cluster))

            inner_workflow = PreparedWorkflow(
                name=inner_job_name,
                tasks=inner_tasks,
                notebooks=[],
                secrets=[],
                setup_tasks=[],
                parameters=parameters,
                cluster_hints=inner_cluster_hints,
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
            inner_task = _inject_input_parameter(inner_prepared.task)
            task["for_each_task"] = {
                "inputs": inputs,
                "task": inner_task,
                "concurrency": concurrency,
            }

    elif len(inner_activities) > 1:
        inner_job_name = f"{activity.task_key}_inner_tasks"
        inner_tasks = []

        for child in inner_activities:
            child_prepared = prepare_activity(child, scope=scope)
            inner_tasks.append(child_prepared.task)
            # Carry IfCondition / Switch branch bodies through so the
            # nested control flow survives the ForEach wrap (CF-001).
            inner_tasks.extend(child_prepared.extra_tasks)
            all_notebooks.extend(child_prepared.notebooks)
            all_secrets.extend(child_prepared.secrets)
            all_setup_tasks.extend(child_prepared.setup_tasks)
            inner_workflows.extend(child_prepared.inner_workflows)

        normalize_inner_task_params(inner_tasks)

        parameters, job_parameters = collect_inner_job_params(inner_tasks, variable_task_keys=variable_task_keys)

        # LSC3-001: gather cluster hints from every nested inner activity
        # so the inner-job default cluster picks up LS-derived
        # spark_env_vars / custom_tags / driver_node_type_id.
        inner_cluster_hints = []
        for child in inner_activities:
            for nested_activity in _iter_activity_with_descendants(child):
                if nested_activity.cluster:
                    inner_cluster_hints.append(dict(nested_activity.cluster))

        inner_workflow = PreparedWorkflow(
            name=inner_job_name,
            tasks=inner_tasks,
            notebooks=[],  # notebooks already collected in all_notebooks
            secrets=[],
            setup_tasks=[],
            parameters=parameters,
            cluster_hints=inner_cluster_hints,
        )
        inner_workflows.append(inner_workflow)

        inner_job_key = normalize_task_key(inner_job_name)
        body_task = {
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
        extra_tasks=extra_tasks,
        notebooks=all_notebooks,
        secrets=all_secrets,
        setup_tasks=all_setup_tasks,
        inner_workflows=inner_workflows,
    )
