"""Preparer for DbtCommandActivity -> a native Databricks dbt_task.

The task runs its dbt commands against the bundle's SQL warehouse, catalog, and schema, so Databricks
generates the dbt profile. The dbt project is copied into the bundle from where the source task ran it,
when packaging can read it there.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

from flowx.bundler.constants import DEFAULT_JOB_CLUSTER_KEY
from flowx.dbt.bundle_files import (
    DBT_PACKAGES,
    MAX_TASK_COMMANDS,
    local_packages_outside,
    project_artifacts,
    task_commands,
)
from flowx.models.dab import DabNotebook
from flowx.preparer.workflow_preparer import PreparedActivity, build_common_task_fields
from flowx.utils import to_lowercase_key

if TYPE_CHECKING:
    from flowx.models.ir import DbtCommandActivity

_DBT_PROJECTS_RELATIVE_PATH = "dbt_projects"


def _project_folder(activity: DbtCommandActivity) -> str:
    """Returns the project's folder under ``src/``: the project folder's name and a digest of its path.

    The path is the one the source task names, so the folder is the same wherever flowx runs, and the digest
    keeps two projects with the same folder name apart.
    """
    source = activity.source_project_dir or activity.project_dir
    name = to_lowercase_key(Path(source).name) or "project"
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()[:8]
    return f"{_DBT_PROJECTS_RELATIVE_PATH}/{name}_{digest}"


def _failing_task(activity: DbtCommandActivity, *, suffix: str, title: str, message: str) -> PreparedActivity:
    """Returns a task that fails with *message*, saying what to change before the dbt task can run."""
    relative_path = f"notebooks/{activity.task_key}_{suffix}.py"
    content = (
        "# Databricks notebook source\n"
        f"# {title} for migrated task {activity.task_key}.\n\n"
        f"raise RuntimeError({message!r})\n"
    )
    task = build_common_task_fields(activity)
    task["notebook_task"] = {"notebook_path": f"../src/{relative_path}"}
    return PreparedActivity(task=task, notebooks=[DabNotebook(relative_path=relative_path, content=content)])


def _missing_project(activity: DbtCommandActivity) -> PreparedActivity:
    """Returns a failing task that says which dbt project to add before the job can run."""
    return _failing_task(
        activity,
        suffix="dbt_project_required",
        title="dbt project required",
        message=(
            f"dbt project {activity.project_dir} was not found where flowx package ran. Make it available at that "
            f"path, re-run flowx package so it is copied to src/{_project_folder(activity)}, and redeploy."
        ),
    )


def prepare(activity: DbtCommandActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a DbtCommandActivity into a DAB dbt_task, with its project copied into the bundle."""
    project = Path(activity.project_dir).expanduser()
    if not (project / "dbt_project.yml").is_file():
        return _missing_project(activity)
    outside = local_packages_outside(project)
    if outside:
        return _failing_task(
            activity,
            suffix="dbt_local_packages",
            title="dbt local packages outside the project",
            message=(
                f"dbt project {activity.project_dir} installs local package(s) {', '.join(outside)} from outside "
                "the project folder, which the bundle cannot carry. Move them into the project or publish them as "
                "packages, re-run flowx package, and redeploy."
            ),
        )
    commands = task_commands(activity.commands, project)
    if len(commands) > MAX_TASK_COMMANDS:
        return _failing_task(
            activity,
            suffix="dbt_too_many_commands",
            title="Too many dbt commands",
            message=(
                f"This dbt task runs {len(commands)} commands, and a Databricks dbt task runs at most "
                f"{MAX_TASK_COMMANDS}. Split them across dbt tasks and redeploy."
            ),
        )
    folder = _project_folder(activity)

    task = build_common_task_fields(activity)
    task["dbt_task"] = {
        "project_directory": f"../src/{folder}",
        "commands": commands,
        "warehouse_id": activity.warehouse_ref,
        "catalog": "${var.catalog}",
        "schema": "${var.schema}",
        "source": "WORKSPACE",
    }
    task["job_cluster_key"] = DEFAULT_JOB_CLUSTER_KEY
    task["libraries"] = [{"pypi": {"package": package}} for package in DBT_PACKAGES]
    return PreparedActivity(task=task, notebooks=project_artifacts(project, folder))
