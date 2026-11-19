"""Airflow DAGs that ``package --skip-failed-dags`` leaves out of a bundle because they failed reconciliation.

The other DAGs are packaged. A task that triggers a left-out DAG becomes a placeholder, since the job it
would run is not in the bundle.
"""

from __future__ import annotations

from typing import Any

from flowx.utils import to_lowercase_key

# The exit code package uses when it wrote a bundle but left out DAGs that failed reconciliation.
DAGS_LEFT_OUT_EXIT_CODE = 4

# Report task fields that hold nested task lists.
_NESTED_TASK_LISTS = ("inner_activities", "if_true_activities", "if_false_activities", "default_activities")


def failed_airflow_pipelines(report: Any) -> list[str]:
    """Returns the names of the included Airflow pipelines in a parsed report whose reconciliation failed.

    A report that is not a dict returns nothing, so the package preflight reports the problem itself.
    """
    if not isinstance(report, dict):
        return []
    entries = report["pipelines"] if isinstance(report.get("pipelines"), list) else [report]
    return [
        entry["name"]
        for entry in entries
        if isinstance(entry, dict)
        and isinstance(entry.get("name"), str)
        and isinstance(entry.get("tags"), dict)
        and entry["tags"].get("source") == "airflow"
        and entry.get("migration_status", "included") == "included"
        and entry.get("reconciliation_status") == "failed"
    ]


def without_skipped_dag_references(pipeline_dict: dict[str, Any], skipped_dags: frozenset[str]) -> dict[str, Any]:
    """Returns a copy of a report pipeline whose tasks that trigger a left-out DAG are placeholders.

    Only a task that triggers a DAG by name counts; one that runs an existing Databricks job by id, such as a
    ``DatabricksRunNowOperator``, does not. The placeholder keeps the task's key and dependencies. The pipeline
    gains a gap finding per replaced task, a ``verified`` status becomes ``verified_with_gaps``, and its schedule
    is paused, since the job can no longer run as migrated.
    """
    skipped_by_key = {to_lowercase_key(name): name for name in skipped_dags}
    replaced: list[tuple[str, str]] = []

    def rewrite(tasks: list[Any]) -> list[Any]:
        rewritten: list[Any] = []
        for task in tasks:
            if not isinstance(task, dict):
                rewritten.append(task)
                continue
            if (
                task.get("type") == "RunJobActivity"
                and not task.get("existing_job_id")
                and task.get("job_name") in skipped_by_key
            ):
                skipped = skipped_by_key[task["job_name"]]
                replaced.append((str(task.get("task_key", "")), skipped))
                placeholder = {
                    key: task[key] for key in ("name", "task_key", "depends_on", "description") if key in task
                }
                placeholder.update(
                    {
                        "type": "PlaceholderActivity",
                        "original_type": "SkippedDagReference",
                        "comment": (
                            f"Airflow DAG {skipped!r} failed reconciliation and was left out of this bundle "
                            "(--skip-failed-dags). Fix it and package it, then point this task at its job."
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
    if updated.get("reconciliation_status") == "verified":
        updated["reconciliation_status"] = "verified_with_gaps"
    if isinstance(updated.get("schedule"), dict):
        updated["schedule"] = {**updated["schedule"], "pause_status": "PAUSED"}
    return updated
