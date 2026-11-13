"""Files and packages a generated dbt task ships with: the dbt project copied into the bundle and dbt's pins."""

from __future__ import annotations

from pathlib import Path

from flowx.models.dab import DabNotebook

_EXCLUDED_DBT_PATH_PARTS = frozenset({".git", ".venv", "__pycache__", "logs", "target"})
# The dbt packages every generated dbt task installs.
DBT_PACKAGES = ("dbt-databricks==1.12.2", "dbt-core==1.11.12")


def tree_artifacts(
    source_root: Path, destination_root: str, *, excluded_parts: frozenset[str] = frozenset()
) -> list[DabNotebook]:
    """Returns bundle artifacts for files beneath a local source directory.

    Files under build, cache, and log folders are skipped, as are any under *excluded_parts*.
    """
    if not source_root.is_dir():
        return []
    return [
        DabNotebook(
            relative_path=(Path(destination_root) / source.relative_to(source_root)).as_posix(),
            binary_content=source.read_bytes(),
        )
        for source in sorted(source_root.rglob("*"))
        if source.is_file()
        and not source.is_symlink()
        and not ((_EXCLUDED_DBT_PATH_PARTS | excluded_parts) & set(source.relative_to(source_root).parts))
    ]
