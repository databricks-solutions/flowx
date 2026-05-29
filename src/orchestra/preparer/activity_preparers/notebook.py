"""Preparer for NotebookActivity -> notebook_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orchestra.models.dab import DabNotebook
from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import resolve_expression, resolve_interpolated_string
from orchestra.preparer.activity_preparers.naming import notebook_filename, workspace_notebook_filename
from orchestra.preparer.workflow_preparer import PreparedActivity, build_common_task_fields
from orchestra.preparer.workspace_downloader import download_notebook, workspace_downloads_enabled

if TYPE_CHECKING:
    from orchestra.models.ir import NotebookActivity


def _notebook_placeholder(original_path: str, activity_name: str, filename: str) -> str:
    """Return placeholder notebook content with manual-export instructions."""
    return (
        "# Databricks notebook source\n"
        "# MAGIC %md\n"
        f"# MAGIC # {activity_name}\n"
        "# MAGIC\n"
        f"# MAGIC **Source workspace path**: `{original_path}`\n"
        "# MAGIC\n"
        f"# MAGIC This notebook was referenced by ADF pipeline activity `{activity_name}`.\n"
        "# MAGIC Replace this placeholder with the actual notebook content.\n"
        "# MAGIC\n"
        "# MAGIC Export from workspace:\n"
        "# MAGIC ```\n"
        f'# MAGIC databricks workspace export "{original_path}" --format SOURCE -o src/notebooks/{filename}\n'
        "# MAGIC ```\n"
        "\n# COMMAND ----------\n\n"
        "# TODO: Replace this placeholder with the exported notebook content\n"
        "raise NotImplementedError(\n"
        f'    f"Export notebook from workspace: {original_path}"\n'
        ")\n"
    )


def _resolve_base_parameters(
    params: dict[str, str],
    *,
    variable_task_keys: dict[str, str] | None = None,
    existing_notebook: bool = False,
) -> dict[str, str]:
    """Resolves ADF expressions in ``base_parameters`` to DAB-compatible values.

    For *existing* notebooks (absolute workspace paths) we keep ``notebook_code``
    parameters as their raw original expression so the downstream
    ``_extract_manual_parameters_from_existing_notebook_tasks`` scanner can
    pick them up and surface them in SETUP.md -- orchestra cannot patch the
    notebook body, so the user has to compute the value in-line themselves.

    For bundle-generated notebooks the embedded notebook body owns the
    runtime computation (via ``required_parameters``), so ``notebook_code``
    parameters are dropped here.
    """
    context = TranslationContext()
    resolved: dict[str, str] = {}
    for key, value in params.items():
        result = resolve_expression(value, context, variable_task_keys=variable_task_keys)
        if result is not None and result.kind in ("literal", "dab_ref"):
            resolved[key] = result.value
            continue
        if result is not None and result.kind == "notebook_code":
            if existing_notebook:
                resolved[key] = _raw_expression(value)
            continue
        if isinstance(value, dict) and value.get("type") == "Expression" and "value" in value:
            resolved[key] = str(value["value"])
        else:
            resolved[key] = str(value)
    return resolved


def _raw_expression(value: object) -> str:
    """Returns the original ADF expression text for an unresolved parameter."""
    if isinstance(value, dict) and value.get("type") == "Expression" and "value" in value:
        return str(value["value"])
    return str(value)


def _resolve_notebook_path(path: str) -> str:
    """Resolves any ADF expression embedded in a notebook workspace path."""
    context = TranslationContext()
    if "@{" in path:
        return resolve_interpolated_string(path, context)
    if path.startswith("@"):
        result = resolve_expression(path, context)
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value
    return path


def prepare(
    activity: NotebookActivity,
    *,
    scope: str = "",
    variable_task_keys: dict[str, str] | None = None,
) -> PreparedActivity:
    """Converts a NotebookActivity into a DAB notebook_task definition."""
    resolved_path = _resolve_notebook_path(activity.notebook_path)
    task = build_common_task_fields(activity)
    is_existing_notebook = resolved_path.startswith("/")

    base_parameters: dict[str, str] | None = None
    if activity.base_parameters:
        base_parameters = _resolve_base_parameters(
            dict(activity.base_parameters),
            variable_task_keys=variable_task_keys,
            existing_notebook=is_existing_notebook,
        )

    if is_existing_notebook:
        downloaded = download_notebook(resolved_path) if workspace_downloads_enabled() else None
        if downloaded is not None:
            filename = workspace_notebook_filename(resolved_path) or notebook_filename(activity.task_key, activity.name)
            notebook_relative_path = f"notebooks/{filename}"
            task["notebook_task"] = {"notebook_path": f"../src/{notebook_relative_path}"}
            if base_parameters is not None:
                task["notebook_task"]["base_parameters"] = base_parameters
            if activity.compute_mode != "serverless":
                task["job_cluster_key"] = "default_cluster"
            notebooks = [DabNotebook(relative_path=notebook_relative_path, content=downloaded)]
            return PreparedActivity(task=task, notebooks=notebooks)

        task["notebook_task"] = {"notebook_path": resolved_path}
        if base_parameters is not None:
            task["notebook_task"]["base_parameters"] = base_parameters
        return PreparedActivity(task=task)

    placeholder_filename = notebook_filename(activity.task_key, activity.name)
    notebook_relative_path = f"notebooks/{placeholder_filename}"
    content = download_notebook(resolved_path) or _notebook_placeholder(
        resolved_path, activity.name, placeholder_filename
    )

    task["notebook_task"] = {"notebook_path": f"../src/{notebook_relative_path}"}
    if base_parameters is not None:
        task["notebook_task"]["base_parameters"] = base_parameters

    notebooks = [DabNotebook(relative_path=notebook_relative_path, content=content)]
    return PreparedActivity(task=task, notebooks=notebooks)
