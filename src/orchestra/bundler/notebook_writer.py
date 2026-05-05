"""Utility to write DabNotebook objects to files on disk."""

from __future__ import annotations

from pathlib import Path

from flowx.models.dab import DabNotebook


def write_notebooks(notebooks: list[DabNotebook], output_dir: Path) -> list[Path]:
    """Writes each notebook to ``output_dir/<relative_path>`` and returns the absolute paths."""
    created: list[Path] = []
    for notebook in notebooks:
        destination = output_dir / notebook.relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        if notebook.binary_content is not None:
            destination.write_bytes(notebook.binary_content)
        else:
            content = notebook.content if notebook.content.endswith("\n") else notebook.content + "\n"
            destination.write_text(content, encoding="utf-8")
        created.append(destination.resolve())
    return created
