"""Shared naming helpers used by every activity preparer."""

from __future__ import annotations

import re


def to_snake_case(name: str) -> str:
    """Converts PascalCase / camelCase / mixed identifiers to snake_case.

    Examples:
        ``BronzeIngest`` -> ``bronze_ingest``
        ``copySQLToBlob`` -> ``copy_sql_to_blob``
        ``ETL_Main`` -> ``etl_main``
        ``spaces and dashes-here`` -> ``spaces_and_dashes_here``
    """
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    s2 = re.sub(r"[^a-zA-Z0-9_]+", "_", s2)
    s2 = re.sub(r"_+", "_", s2).strip("_")
    return s2.lower()


def notebook_filename(task_key: str, activity_name: str | None = None) -> str:
    """Derive a snake_case ``<name>.py`` filename for a generated notebook."""
    source = activity_name or task_key
    snake = to_snake_case(source)
    if not snake:
        snake = task_key.lower() or "notebook"
    return f"{snake}.py"


def workspace_notebook_filename(workspace_path: str) -> str:
    """Derive a bundle filename from a workspace notebook path's basename.

    Preserves the workspace name verbatim (case, underscores, digits) so a
    downloaded notebook lands at ``src/notebooks/<basename>.py`` instead of
    being renamed to the ADF activity's task_key.  Sanitises filesystem-unsafe
    characters and ensures a ``.py`` extension.

    Returns an empty string when the path yields no usable segment so the
    caller can fall back to the activity-name-based naming.
    """
    if not workspace_path:
        return ""
    basename = workspace_path.rsplit("/", 1)[-1].strip()
    if not basename:
        return ""
    stem, _, ext = basename.rpartition(".")
    if not stem:
        stem, ext = basename, ""
    safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._-")
    if not safe_stem:
        return ""
    if ext.lower() == "py":
        return f"{safe_stem}.py"
    if ext:
        safe_ext = re.sub(r"[^A-Za-z0-9]+", "", ext)
        return f"{safe_stem}_{safe_ext}.py" if safe_ext else f"{safe_stem}.py"
    return f"{safe_stem}.py"
