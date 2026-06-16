"""Preparer for SparkJarActivity -> spark_jar_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowx.models.dab import DabNotebook
from flowx.preparer.activity_preparers.helpers import resolve_param_value
from flowx.preparer.workflow_preparer import PreparedActivity, build_common_task_fields
from flowx.preparer.workspace_downloader import download_dbfs_file

if TYPE_CHECKING:
    from flowx.models.ir import SparkJarActivity


def _jar_placeholder(libraries: list[dict] | None, activity_name: str) -> str:
    """Generates a placeholder file with download instructions for JAR libraries.

    Args:
        libraries: Library descriptors from the SparkJarActivity.
        activity_name: The ADF activity name.

    Returns:
        Placeholder content as a string.
    """
    lib_lines = ""
    if libraries:
        for lib in libraries:
            for key, path in lib.items():
                lib_lines += f"#   {key}: {path}\n"
    return (
        f"# Placeholder for JARs referenced by activity: {activity_name}\n"
        "#\n"
        "# Download the following libraries and place them in this directory:\n"
        f"{lib_lines}"
        "#\n"
        "# Use the Databricks CLI to upload JARs:\n"
        "#   databricks fs cp <local_jar> dbfs:/FileStore/jars/<jar_name>\n"
    )


def prepare(activity: SparkJarActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a SparkJarActivity into a DAB spark_jar_task definition.

    Args:
        activity: The translated Spark JAR activity from the IR.

    Returns:
        A PreparedActivity containing the spark_jar_task dict and placeholder files.
    """
    task = build_common_task_fields(activity)

    rewritten_libraries: list[dict] = []
    notebooks: list[DabNotebook] = []
    downloaded_any = False
    if activity.libraries:
        for lib in activity.libraries:
            rewritten_lib = {}
            for key, path in lib.items():
                if isinstance(path, str) and ("dbfs:" in path or "/" in path):
                    filename = path.rsplit("/", 1)[-1] if "/" in path else path
                    rewritten_lib[key] = f"../lib/{filename}"
                    if key == "jar":
                        jar_content = download_dbfs_file(path)
                        if jar_content is not None:
                            downloaded_any = True
                            notebooks.append(
                                DabNotebook(
                                    relative_path=f"lib/{filename}",
                                    binary_content=jar_content,
                                )
                            )
                else:
                    rewritten_lib[key] = path
            rewritten_libraries.append(rewritten_lib)

        if not downloaded_any:
            placeholder_content = _jar_placeholder(activity.libraries, activity.name)
            notebooks.append(
                DabNotebook(
                    relative_path=f"lib/{activity.task_key}_README.txt",
                    content=placeholder_content,
                    language="python",
                )
            )

    resolved_main_class = resolve_param_value(activity.main_class_name) if activity.main_class_name else ""
    task["spark_jar_task"] = {
        "main_class_name": resolved_main_class,
    }
    if activity.parameters:
        task["spark_jar_task"]["parameters"] = list(activity.parameters)
    if rewritten_libraries:
        task["libraries"] = rewritten_libraries
    elif activity.libraries:
        task["libraries"] = list(activity.libraries)

    return PreparedActivity(task=task, notebooks=notebooks)
