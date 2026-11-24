"""Airflow DAGs that ``package --skip-failed-dags`` leaves out of a bundle because they failed reconciliation.

The other DAGs are packaged. A task that triggers a left-out DAG becomes a placeholder, recorded the way
``--exclude-dag`` records one, since the job it would run is not in the bundle.
"""

from __future__ import annotations

from typing import Any

from flowx.utils import to_lowercase_key

# Findings of a file that builds DAGs flowx cannot name: a generator, or a declaration it could not capture.
UNNAMED_DAG_FINDINGS = frozenset({"unsupported_dag_generator", "unsupported_dag_factory"})
# Report task fields that hold nested task lists.
_NESTED_TASK_LISTS = ("inner_activities", "if_true_activities", "if_false_activities", "default_activities")


def _report_pipelines(report: Any) -> list[dict[str, Any]]:
    if not isinstance(report, dict):
        return []
    entries = report["pipelines"] if isinstance(report.get("pipelines"), list) else [report]
    return [entry for entry in entries if isinstance(entry, dict)]


def failed_airflow_pipelines(report: Any) -> list[str]:
    """Returns the names of the included Airflow pipelines in a parsed report whose reconciliation failed.

    Each name appears once, even when a generator file has several report entries. A report that is not a dict
    returns nothing, so the package preflight reports the problem itself.
    """
    names: list[str] = []
    for entry in _report_pipelines(report):
        name = entry.get("name")
        if (
            isinstance(name, str)
            and name not in names
            and isinstance(entry.get("tags"), dict)
            and entry["tags"].get("source") == "airflow"
            and entry.get("migration_status", "included") == "included"
            and entry.get("reconciliation_status") == "failed"
        ):
            names.append(name)
    return names


def excluded_failed_pipelines(report: Any) -> list[str]:
    """Returns the names of the excluded Airflow pipelines in a parsed report whose reconciliation failed.

    MCP ``migrate`` with ``skip_failed_dags`` leaves failed DAGs out by excluding them, so a report it wrote
    names them this way.
    """
    names: list[str] = []
    for entry in _report_pipelines(report):
        name = entry.get("name")
        if (
            isinstance(name, str)
            and name not in names
            and entry.get("migration_status") == "excluded"
            and entry.get("reconciliation_status") == "failed"
        ):
            names.append(name)
    return names


def unpackaged_trigger_targets(report: Any, skipped_dags: frozenset[str]) -> frozenset[str] | None:
    """Returns the pipelines that will be packaged when a left-out file is a DAG generator, or None otherwise.

    A generator file builds DAGs flowx cannot name, so a trigger of any DAG outside this set may target one of
    them and cannot be wired to a job in this bundle.
    """
    pipelines = _report_pipelines(report)
    if not any(
        entry.get("name") in skipped_dags
        and any(
            isinstance(finding, dict) and finding.get("code") in UNNAMED_DAG_FINDINGS
            for finding in entry.get("not_translatable") or []
        )
        for entry in pipelines
    ):
        return None
    return frozenset(
        entry["name"]
        for entry in pipelines
        if isinstance(entry.get("name"), str)
        and entry["name"] not in skipped_dags
        and entry.get("migration_status", "included") != "excluded"
    )


def without_skipped_dag_references(
    pipeline_dict: dict[str, Any],
    skipped_dags: frozenset[str],
    *,
    unpackaged_targets: frozenset[str] | None = None,
) -> dict[str, Any]:
    """Returns a copy of a report pipeline whose tasks that trigger a left-out DAG are placeholders.

    Only a task that triggers a DAG by name counts; one that runs an existing Databricks job by id, such as a
    ``DatabricksRunNowOperator``, does not. With *unpackaged_targets* (see :func:`unpackaged_trigger_targets`), a
    trigger of any DAG outside that set counts too. As with ``--exclude-dag``, the placeholder keeps the task's
    key and dependencies, the pipeline gains a gap finding per replaced task and becomes ``verified_with_gaps``,
    its task counts move the task from deterministic to agentic, and its schedule is paused and recorded so.
    """
    skipped_by_key = {to_lowercase_key(name): name for name in skipped_dags}
    packaged_keys = None if unpackaged_targets is None else {to_lowercase_key(name) for name in unpackaged_targets}
    replaced: list[tuple[str, str]] = []

    def left_out_target(task: dict[str, Any]) -> str | None:
        if task.get("type") != "RunJobActivity" or task.get("existing_job_id"):
            return None
        job_name = task.get("job_name")
        if job_name in skipped_by_key:
            return skipped_by_key[job_name]
        if packaged_keys is not None and isinstance(job_name, str) and job_name not in packaged_keys:
            return job_name
        return None

    def rewrite(tasks: list[Any]) -> list[Any]:
        rewritten: list[Any] = []
        for task in tasks:
            if not isinstance(task, dict):
                rewritten.append(task)
                continue
            skipped = left_out_target(task)
            if skipped is not None:
                replaced.append((str(task.get("task_key", "")), skipped))
                placeholder = {
                    key: task[key] for key in ("name", "task_key", "depends_on", "description") if key in task
                }
                reason = (
                    f"Airflow DAG {skipped!r} failed reconciliation and was left out of this bundle"
                    if skipped in skipped_dags
                    else f"Airflow DAG {skipped!r} is not in this bundle and may be built by a generator file that "
                    "failed reconciliation and was left out"
                )
                placeholder.update(
                    {
                        "type": "PlaceholderActivity",
                        "original_type": "SkippedDagReference",
                        "comment": (
                            f"{reason} (--skip-failed-dags). Fix it and package it, then point this task at its job."
                        ),
                        "raw_definition": {"skipped_dag": skipped},
                    }
                )
                rewritten.append(placeholder)
                continue
            task = dict(task)
            for field in _NESTED_TASK_LISTS:
                if isinstance(task.get(field), list):
                    task[field] = rewrite(task[field])
            if isinstance(task.get("cases"), list):
                task["cases"] = [
                    {**case, "activities": rewrite(case["activities"])}
                    if isinstance(case, dict) and isinstance(case.get("activities"), list)
                    else case
                    for case in task["cases"]
                ]
            rewritten.append(task)
        return rewritten

    updated = {**pipeline_dict, "tasks": rewrite(list(pipeline_dict.get("tasks") or []))}
    if not replaced:
        return pipeline_dict
    updated["not_translatable"] = [
        *(pipeline_dict.get("not_translatable") or []),
        *(
            {
                "code": "skipped_dag_reference",
                "severity": "gap",
                "message": f"Task {task_key!r} triggers Airflow DAG {skipped!r}, which was left out of this bundle.",
                "details": {"task_key": task_key, "skipped_dag": skipped},
            }
            for task_key, skipped in replaced
        ),
    ]
    if updated.get("reconciliation_status") != "failed":
        updated["reconciliation_status"] = "verified_with_gaps"
    audit = dict(updated.get("audit") or {})
    audit["agentic_count"] = int(audit.get("agentic_count", 0)) + len(replaced)
    audit["deterministic_count"] = max(0, int(audit.get("deterministic_count", 0)) - len(replaced))
    schedule = updated.get("schedule")
    if isinstance(schedule, dict) and schedule.get("pause_status") != "PAUSED":
        updated["schedule"] = {**schedule, "pause_status": "PAUSED"}
        audit["transformations"] = [
            *(audit.get("transformations") or []),
            {
                "code": "schedule_paused_until_migration_completes",
                "schedule_kind": schedule.get("kind"),
                "rationale": f"reconciliation_status_{updated['reconciliation_status']}",
                "message": (
                    "The job's schedule is emitted paused because the migration has open gaps or placeholder "
                    "tasks. Unpause it once every task is converted and checked."
                ),
            },
        ]
    updated["audit"] = audit
    return updated
