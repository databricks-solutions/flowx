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
from flowx.models.dab import DabNotebook
from flowx.preparer.activity_preparers.dbt_factory import DBT_PACKAGES, tree_artifacts
from flowx.preparer.workflow_preparer import PreparedActivity, build_common_task_fields
from flowx.utils import to_lowercase_key

if TYPE_CHECKING:
    from flowx.models.ir import DbtCommandActivity

_DBT_PROJECTS_RELATIVE_PATH = "dbt_projects"
# Installed packages and connection profiles are not part of the project: the task runs ``dbt deps`` and
# Databricks generates the profile from the task's warehouse.
_EXCLUDED_PROJECT_PARTS = frozenset({"dbt_packages", "profiles.yml"})
# Files that declare dbt packages, which a fresh task environment installs with ``dbt deps``.
_PACKAGE_FILES = ("packages.yml", "dependencies.yml")


def _project_folder(activity: DbtCommandActivity) -> str:
    """Returns the project's folder under ``src/``: the project folder's name and a digest of its path.

    The digest keeps two projects with the same folder name apart.
    """
    name = to_lowercase_key(Path(activity.project_dir).name) or "project"
    digest = hashlib.sha256(activity.project_dir.encode("utf-8")).hexdigest()[:8]
    return f"{_DBT_PROJECTS_RELATIVE_PATH}/{name}_{digest}"


def _missing_project(activity: DbtCommandActivity) -> PreparedActivity:
    """Returns a failing task that says which dbt project to add before the job can run."""
    relative_path = f"notebooks/{activity.task_key}_dbt_project_required.py"
    folder = _project_folder(activity)
    message = (
        f"dbt project {activity.project_dir} was not found where flowx package ran. Make it available at that "
        f"path, re-run flowx package so it is copied to src/{folder}, and redeploy."
    )
    content = (
        "# Databricks notebook source\n"
        f"# dbt project required for migrated task {activity.task_key}.\n\n"
        f"raise RuntimeError({message!r})\n"
    )
    task = build_common_task_fields(activity)
    task["notebook_task"] = {"notebook_path": f"../src/{relative_path}"}
    return PreparedActivity(task=task, notebooks=[DabNotebook(relative_path=relative_path, content=content)])


def prepare(activity: DbtCommandActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a DbtCommandActivity into a DAB dbt_task, with its project copied into the bundle."""
    project = Path(activity.project_dir).expanduser()
    if not (project / "dbt_project.yml").is_file():
        return _missing_project(activity)
    folder = _project_folder(activity)
    commands = list(activity.commands)
    installs_packages = any(command.split()[:2] == ["dbt", "deps"] for command in commands)
    if not installs_packages and any((project / name).is_file() for name in _PACKAGE_FILES):
        commands.insert(0, "dbt deps")

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
    return PreparedActivity(
        task=task, notebooks=tree_artifacts(project, folder, excluded_parts=_EXCLUDED_PROJECT_PARTS)
    )
