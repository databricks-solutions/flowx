"""Translate a parsed Glue Workflow into the shared flowx Pipeline IR.

A Glue Workflow is a graph of job, crawler, and trigger nodes. Jobs and crawlers
become tasks; this first increment emits each as an agentic placeholder, since
the workflow export names a job but does not carry its script -- the convert
phase fills the body. Triggers carry the orchestration and translate
deterministically:

- a scheduled start trigger sets the Lakeflow Job schedule from its cron,
- an on-demand start trigger leaves its actions as manual roots,
- a conditional trigger adds ``depends_on`` edges from each predicate condition's
  node to each action node, with the predicate reduced to a Databricks ``run_if``
  stamped as the edge outcome (``run_if_from_adf_outcomes`` passes it through).

See ``design/glue-ir-mapping.md`` for the mapping rationale and deferrals.
"""

from __future__ import annotations

import re
from typing import Any

from flowx.models.ir import Activity, Dependency, Pipeline, PlaceholderActivity
from flowx.sources.glue.workflow import Condition, GlueWorkflow, Trigger

_SLUG = re.compile(r"[^0-9A-Za-z]+")
_CRON = re.compile(r"^cron\((.*)\)$")


def translate_workflow(workflow: GlueWorkflow) -> Pipeline:
    """Translates a parsed Glue Workflow into a flowx :class:`Pipeline`.

    Args:
        workflow: The parsed workflow graph.

    Returns:
        A pipeline whose tasks are the job and crawler nodes, wired by the
        workflow's triggers. Schedule approximations and deferred constructs are
        recorded in ``not_translatable``.
    """
    notes: list[dict[str, Any]] = []
    job_keys = {name: _task_key(name) for name in workflow.jobs}
    crawler_keys = {name: _task_key(name) for name in workflow.crawlers}
    dependencies: dict[str, list[Dependency]] = {}

    schedule: dict[str, Any] | None = None
    for trigger in workflow.triggers:
        if trigger.type == "CONDITIONAL":
            _apply_conditional(trigger, job_keys, crawler_keys, dependencies, notes)
        elif trigger.type == "SCHEDULED":
            schedule = _schedule_from_cron(trigger.schedule, notes) or schedule
        elif trigger.type not in ("ON_DEMAND", ""):
            notes.append({"trigger": trigger.name, "issue": f"unsupported trigger type {trigger.type!r}"})

    tasks: list[Activity] = [
        _placeholder(name, key, "GlueJob", f"Glue job {name!r}; port the job script.", dependencies)
        for name, key in job_keys.items()
    ]
    tasks += [
        _placeholder(
            name,
            key,
            "GlueCrawler",
            f"Glue crawler {name!r}; map to Auto Loader or a Unity Catalog setup step.",
            dependencies,
        )
        for name, key in crawler_keys.items()
    ]
    return Pipeline(
        name=workflow.name,
        schedule=schedule,
        tasks=tasks,
        not_translatable=notes,
        tags={"source": "glue", "workflow": workflow.name},
    )


def _apply_conditional(
    trigger: Trigger,
    job_keys: dict[str, str],
    crawler_keys: dict[str, str],
    dependencies: dict[str, list[Dependency]],
    notes: list[dict[str, Any]],
) -> None:
    """Records dependency edges from a conditional trigger's predicate nodes to its action nodes."""
    outcome = _run_if(trigger)
    upstream = [key for condition in trigger.conditions if (key := _condition_key(condition, job_keys, crawler_keys))]
    if not upstream:
        notes.append({"trigger": trigger.name, "issue": "conditional trigger has no resolvable predicate nodes"})
        return
    for action_name in trigger.action_jobs:
        _record_edges(action_name, job_keys, upstream, outcome, trigger, dependencies, notes)
    for action_name in trigger.action_crawlers:
        _record_edges(action_name, crawler_keys, upstream, outcome, trigger, dependencies, notes)


def _record_edges(
    action_name: str,
    keys: dict[str, str],
    upstream: list[str],
    outcome: str | None,
    trigger: Trigger,
    dependencies: dict[str, list[Dependency]],
    notes: list[dict[str, Any]],
) -> None:
    """Records edges onto an action node, noting an action that names an unknown node."""
    key = keys.get(action_name)
    if key is None:
        notes.append({"trigger": trigger.name, "issue": f"action targets unknown node {action_name!r}"})
        return
    dependencies.setdefault(key, []).extend(
        Dependency(task_key=upstream_key, outcome=outcome) for upstream_key in upstream
    )


def _condition_key(condition: Condition, job_keys: dict[str, str], crawler_keys: dict[str, str]) -> str | None:
    """Returns the task key of the node a predicate condition watches, or ``None`` when unknown."""
    if condition.job_name and condition.job_name in job_keys:
        return job_keys[condition.job_name]
    if condition.crawler_name and condition.crawler_name in crawler_keys:
        return crawler_keys[condition.crawler_name]
    return None


def _run_if(trigger: Trigger) -> str | None:
    """Reduces a conditional trigger's predicate to a Databricks ``run_if`` constant.

    Returns ``None`` for the all-success / AND case (Databricks defaults to
    ``ALL_SUCCESS`` when no ``run_if`` is set). Mixed or non-success/failure
    states (``TIMEOUT`` / ``STOPPED`` / ``CANCELLED``) map to ``ALL_DONE``, since
    Databricks gates only on success or failure.
    """
    states = {condition.state for condition in trigger.conditions if condition.state}
    any_logical = trigger.logical.upper() == "ANY"
    if states <= {"SUCCEEDED"}:
        return "AT_LEAST_ONE_SUCCESS" if any_logical else None
    if states <= {"FAILED"}:
        return "AT_LEAST_ONE_FAILED" if any_logical else "ALL_FAILED"
    return "ALL_DONE"


def _schedule_from_cron(expression: str | None, notes: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Converts a Glue ``cron(...)`` expression into a Quartz schedule spec.

    AWS Glue cron is 6-field (minute hour day-of-month month day-of-week year)
    and shares Quartz's ``?`` day convention, so a valid Quartz expression is the
    AWS fields prefixed with a ``0`` seconds field. Glue schedules are always UTC.
    """
    if not expression:
        return None
    match = _CRON.match(expression.strip())
    if not match:
        notes.append({"schedule": expression, "issue": "schedule is not a cron(...) expression; left unset"})
        return None
    return {
        "kind": "schedule",
        "quartz_cron_expression": f"0 {match.group(1).strip()}",
        "timezone_id": "UTC",
    }


def _placeholder(
    name: str,
    key: str,
    original_type: str,
    comment: str,
    dependencies: dict[str, list[Dependency]],
) -> PlaceholderActivity:
    """Builds a placeholder activity for a job or crawler node with its collected edges."""
    return PlaceholderActivity(
        name=name,
        task_key=key,
        original_type=original_type,
        comment=comment,
        depends_on=dependencies.get(key) or None,
    )


def _task_key(name: str) -> str:
    """Returns a slugified task key for a job or crawler name."""
    return _SLUG.sub("_", name).strip("_").lower() or "node"
