"""for_each wrapping and TaskFlow task construction."""

from __future__ import annotations

import ast
import json

from flowx.models.ir import (
    Activity,
    Dependency,
    ForEachActivity,
    NotebookActivity,
    PlaceholderActivity,
)
from flowx.sources.airflow import callable_notebook
from flowx.sources.airflow import operators as ops
from flowx.sources.airflow.loader.captures import _TaskFlowTask

# TaskFlow decorators that gate downstream tasks at runtime -- can't lower to a notebook (same
# reason BranchPythonOperator/ShortCircuitOperator route to the agentic round).
_TASKFLOW_BRANCHING = frozenset({"task.branch", "task.short_circuit"})


def _wrap_in_for_each(
    activity: Activity,
    task_id: str,
    task_key: str,
    depends_on: list[Dependency] | None,
    kwargs: dict[str, ast.expr],
    expand_kwargs: list[str],
) -> ForEachActivity:
    """Wraps a dynamically-mapped operator in a ForEachActivity (-> for_each_task).

    Airflow ``.expand(x=[...])`` fans a task out over an iterable. The for_each's ``inputs`` is the
    first list-valued kwarg passed to ``.expand()`` -- restricted to *expand* kwargs because a
    list-valued ``.partial()`` arg is a fixed value, and taking it would fan the task out over the
    wrong list. The mapped operator becomes the single inner activity, re-keyed so it doesn't collide
    with the for_each task key.
    """
    items = "[]"
    candidates = expand_kwargs or [key for key in kwargs if key not in ("task_id", "group_id")]
    for key in candidates:
        node = kwargs.get(key)
        if node is None or key in ("task_id", "group_id"):
            continue
        value = ops.literal_value(node)
        if isinstance(value, list):
            items = json.dumps(value)
            break
    inner = activity
    inner.task_key = f"{task_key}_iteration"
    inner.name = f"{task_id}_iteration"
    inner.depends_on = None
    return ForEachActivity(
        name=task_id,
        task_key=task_key,
        depends_on=depends_on,
        items_expression=items,
        inner_activities=[inner],
    )


def _wrap_taskflow_in_for_each(
    activity: Activity,
    tf: _TaskFlowTask,
    task_key: str,
    depends_on: list[Dependency] | None,
) -> ForEachActivity:
    """Wraps a mapped ``@task.expand(param=[...])`` notebook in a ForEachActivity (-> for_each_task).

    The literal iterable becomes the for_each ``inputs`` array; the preparer injects each element as
    the inner task's ``item`` widget, which the callable notebook reads for the mapped parameter.
    """
    activity.task_key = f"{task_key}_iteration"
    activity.name = f"{tf.task_id}_iteration"
    activity.depends_on = None
    return ForEachActivity(
        name=tf.task_id,
        task_key=task_key,
        depends_on=depends_on,
        items_expression=tf.expand_items_json or "[]",
        inner_activities=[activity],
    )


def _build_taskflow_task(
    tf: _TaskFlowTask,
    var_to_task_key: dict[str, str],
    functions: dict[str, ast.FunctionDef],
    source: str,
    task_key: str,
) -> Activity:
    """Builds an Activity for one TaskFlow ``@task`` instance.

    The callable is rendered as a notebook that reads each upstream task's return value via
    ``dbutils.jobs.taskValues.get`` (TaskFlow's implicit XCom data flow), invokes the function with
    those bound arguments, and publishes its own return value. Callables that read Airflow task
    context/XCom, or use a branching decorator, route to a placeholder for the agentic round.
    """

    func = functions.get(tf.def_name)
    if func is None:
        return PlaceholderActivity(
            name=tf.task_id,
            task_key=task_key,
            original_type=f"@{tf.decorator}",
            comment=f"TaskFlow @{tf.decorator} '{tf.def_name}' could not be resolved; translate manually.",
        )
    if tf.unresolved_arguments:
        arguments = ", ".join(tf.unresolved_arguments)
        return PlaceholderActivity(
            name=tf.task_id,
            task_key=task_key,
            original_type=f"@{tf.decorator}",
            comment=f"TaskFlow call uses nonliteral argument(s) {arguments}; bind them manually.",
            raw_definition={"operator": f"@{tf.decorator}", "source": ast.get_source_segment(source, func) or ""},
        )
    if tf.decorator in _TASKFLOW_BRANCHING:
        return PlaceholderActivity(
            name=tf.task_id,
            task_key=task_key,
            original_type=f"@{tf.decorator}",
            comment=(
                f"TaskFlow @{tf.decorator} '{tf.def_name}' selects downstream tasks at runtime. "
                "Translate to a Databricks condition_task and gate each downstream branch with a "
                "true/false outcome dependency; do NOT run all branches."
            ),
            raw_definition={"operator": f"@{tf.decorator}", "source": ast.get_source_segment(source, func) or ""},
        )
    reason = callable_notebook.airflow_runtime_reason(func, source)
    if reason is not None:
        return PlaceholderActivity(
            name=tf.task_id,
            task_key=task_key,
            original_type=f"@{tf.decorator}",
            comment=(
                f"TaskFlow @{tf.decorator} '{tf.def_name}' {reason}. flowx has no Airflow runtime to "
                "supply it; pass upstream data via job parameters or map XCom to dbutils.jobs.taskValues."
            ),
            raw_definition={"operator": f"@{tf.decorator}", "source": ast.get_source_segment(source, func) or ""},
        )

    prelude = callable_notebook.render_definitions(func, source, note=f"TaskFlow @{tf.decorator}")
    body = _taskflow_invocation(func, tf, var_to_task_key)
    return NotebookActivity(
        name=tf.task_id,
        task_key=task_key,
        notebook_path=f"notebooks/{task_key}.py",
        generated_source=prelude + body,
    )


def _taskflow_invocation(func: ast.FunctionDef, tf: _TaskFlowTask, var_to_task_key: dict[str, str]) -> str:
    """The invocation cell for a TaskFlow task: read upstream taskValues, call, publish return.

    Each bound upstream task's ``return_value`` is fetched with ``dbutils.jobs.taskValues.get`` and
    passed in the argument position/keyword it was wired to. Unbound parameters fall back to the
    callable's own defaults.
    """
    lines: list[str] = []
    call_positional: list[str] = []
    call_keywords: list[str] = []

    def _reader(dep_var: str) -> str:
        dep_key = var_to_task_key.get(dep_var, dep_var)
        return f"dbutils.jobs.taskValues.get(taskKey='{dep_key}', key='return_value', debugValue=None)"

    for position in sorted(set(tf.positional_deps) | set(tf.positional_values)):
        if position in tf.positional_deps:
            variable = f"_upstream_{position}"
            lines.append(f"{variable} = {_reader(tf.positional_deps[position])}")
            call_positional.append(variable)
        else:
            call_positional.append(tf.positional_values[position])
    for name, dep_var in tf.keyword_deps.items():
        variable = f"_upstream_{name}"
        lines.append(f"{variable} = {_reader(dep_var)}")
        call_keywords.append(f"{name}={variable}")
    call_keywords.extend(f"{name}={value}" for name, value in tf.keyword_values.items())
    if tf.expand_kwarg is not None:
        # .expand(param=[...]) fan-out: each for_each `inputs` element is the JSON text of the
        # original value (see _capture_expand), so json.loads on the injected `item` widget recovers
        # it exactly -- ints stay ints and JSON-looking strings stay strings. The except is a defensive
        # fallback for an unexpected raw value.
        lines.append("_raw_item = dbutils.widgets.get('item')")
        lines.append("try:")
        lines.append("    _expand_item = json.loads(_raw_item)")
        lines.append("except (ValueError, TypeError):")
        lines.append("    _expand_item = _raw_item")
        call_keywords.append(f"{tf.expand_kwarg}=_expand_item")

    call_args = ", ".join(call_positional + call_keywords)
    returns = any(isinstance(n, ast.Return) and n.value is not None for n in ast.walk(func))
    prefix = "result = " if returns else ""
    lines.append(f"{prefix}{func.name}({call_args})")
    if returns:
        lines.append("dbutils.jobs.taskValues.set(key='return_value', value=result)")
    return "\n".join(lines) + "\n"
