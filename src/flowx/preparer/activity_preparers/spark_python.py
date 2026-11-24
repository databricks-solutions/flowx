"""Preparer for SparkPythonActivity -> spark_python_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from flowx.models.dab import DabNotebook
from flowx.preparer.activity_preparers.helpers import (
    bind_requested_job_cluster,
    is_remote_artifact_uri,
    resolve_param_value,
)
from flowx.preparer.workflow_preparer import PreparedActivity, build_common_task_fields
from flowx.preparer.workspace_downloader import download_dbfs_file

if TYPE_CHECKING:
    from flowx.models.ir import SparkPythonActivity

SERVERLESS_ENVIRONMENT_KEY = "serverless"
_COMPUTE_FIELDS = ("environment_key", "job_cluster_key", "existing_cluster_id", "new_cluster")


def serverless_environment(task: dict[str, Any]) -> list[dict[str, Any]]:
    """Runs a Python task that names no compute on serverless, returning the job environment it needs.

    Unlike a notebook task, a Python script task must name its compute: a cluster or a job environment.
    A task whose source asked for no cluster gets the serverless environment; a task with compute is
    left alone and needs none.

    Args:
        task: The task dict being prepared; ``environment_key`` is set on it when it names no compute.

    Returns:
        The ``{environment_key, spec}`` entries the job must declare, empty when the task has compute.
    """
    if any(field in task for field in _COMPUTE_FIELDS):
        return []
    task["environment_key"] = SERVERLESS_ENVIRONMENT_KEY
    return [{"environment_key": SERVERLESS_ENVIRONMENT_KEY, "spec": {"environment_version": "5"}}]


def _python_placeholder(original_path: str, activity_name: str) -> str:
    """Generates placeholder Python script with download instructions.

    Args:
        original_path: The original DBFS/workspace path from ADF.
        activity_name: The ADF activity name.

    Returns:
        Placeholder Python content as a string.
    """
    return (
        f"# Placeholder for Python script referenced by activity: {activity_name}\n"
        f"# Original path: {original_path}\n"
        "#\n"
        "# Download and replace this file with the actual script:\n"
        f'#   databricks fs cp "{original_path}" src/scripts/{original_path.rsplit("/", 1)[-1]}\n'
        "#\n"
        f'raise NotImplementedError("Download script from: {original_path}")\n'
    )


def prepare(activity: SparkPythonActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a SparkPythonActivity into a DAB spark_python_task definition.

    Args:
        activity: The translated Spark Python activity from the IR.

    Returns:
        A PreparedActivity containing the spark_python_task dict and placeholder file.
    """
    task = build_common_task_fields(activity)

    original_path = resolve_param_value(activity.python_file) if activity.python_file else ""
    if activity.keep_remote_artifacts and is_remote_artifact_uri(original_path):
        task["spark_python_task"] = {"python_file": original_path}
        if activity.parameters:
            task["spark_python_task"]["parameters"] = list(activity.parameters)
        if activity.libraries:
            task["libraries"] = activity.libraries
        bind_requested_job_cluster(task, activity)
        return PreparedActivity(task=task, notebooks=[], environments=serverless_environment(task))
    if original_path and ("dbfs:" in original_path or "/" in original_path):
        filename = original_path.rsplit("/", 1)[-1] if "/" in original_path else original_path
    else:
        filename = f"{activity.task_key}.py"
    script_rel_path = f"scripts/{filename}"

    downloaded = None if activity.generated_source is not None else download_dbfs_file(original_path)
    content = (
        activity.generated_source
        if activity.generated_source is not None
        else downloaded.decode("utf-8")
        if downloaded is not None
        else _python_placeholder(original_path, activity.name)
    )
    notebooks = [
        DabNotebook(
            relative_path=script_rel_path,
            content=content,
            language="python",
        )
    ]

    task["spark_python_task"] = {
        "python_file": f"../src/{script_rel_path}",
    }
    if activity.parameters:
        task["spark_python_task"]["parameters"] = list(activity.parameters)
    if activity.libraries:
        task["libraries"] = activity.libraries
    bind_requested_job_cluster(task, activity)
    return PreparedActivity(task=task, notebooks=notebooks, environments=serverless_environment(task))
