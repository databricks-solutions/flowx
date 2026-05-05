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
