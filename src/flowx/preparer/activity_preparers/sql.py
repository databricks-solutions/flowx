"""Preparer for SqlActivity -> sql_task dict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowx.models.dab import DabNotebook
from flowx.preparer.workflow_preparer import PreparedActivity, build_common_task_fields

if TYPE_CHECKING:
    from flowx.models.ir import SqlActivity


def prepare(activity: SqlActivity, *, scope: str = "") -> PreparedActivity:
    """Converts a SqlActivity into a DAB sql_task with an extracted .sql file.

    The SQL text is written to ``src/sql/<task_key>.sql`` and the task references it
    via ``sql_task.file.path`` on the warehouse given by ``warehouse_ref``. Named
    ``parameters`` become ``sql_task.parameters`` (referenced as ``:name`` in the SQL).
    """
    task = build_common_task_fields(activity)

    sql_rel_path = f"sql/{activity.task_key}.sql"
    content = activity.sql if activity.sql.endswith("\n") else activity.sql + "\n"
    notebooks = [DabNotebook(relative_path=sql_rel_path, content=content, language="sql")]

    sql_task: dict[str, object] = {
        "warehouse_id": activity.warehouse_ref,
        "file": {"path": f"../src/{sql_rel_path}", "source": "WORKSPACE"},
    }
    if activity.parameters:
        sql_task["parameters"] = dict(activity.parameters)
    task["sql_task"] = sql_task

    return PreparedActivity(task=task, notebooks=notebooks)
