"""Utility to write DabNotebook objects to files on disk."""

from __future__ import annotations

import logging
from pathlib import Path

from flowx.models.dab import DabNotebook

logger = logging.getLogger(__name__)


def _content_signature(notebook: DabNotebook) -> object:
    """Return a value suitable for comparing two notebooks for byte-equivalence."""
    if notebook.binary_content is not None:
        return ("binary", notebook.binary_content)
    return ("text", notebook.content)


def _disambiguate(relative_path: str, taken: set[str]) -> str:
    """Append a numeric suffix to ``relative_path`` until it is unique within ``taken``."""
    if relative_path not in taken:
        return relative_path
    parent, _, name = relative_path.rpartition("/")
    stem, dot, ext = name.rpartition(".")
    if not stem:
        stem, dot, ext = name, "", ""
    n = 1
    while True:
        candidate_name = f"{stem}__{n}{dot}{ext}" if dot else f"{stem}__{n}"
        candidate = f"{parent}/{candidate_name}" if parent else candidate_name
        if candidate not in taken:
            return candidate
        n += 1


def write_notebooks(notebooks: list[DabNotebook], output_dir: Path) -> list[Path]:
    """Writes each notebook to ``output_dir/<relative_path>`` and returns the absolute paths.

    Two notebooks may legitimately share a ``relative_path`` (e.g., when two
    ADF activities reference the same workspace notebook).  Identical writes
    are coalesced into one.  When the contents differ — which usually means
    two workspace paths produced the same basename — the second write is
    given a ``__N`` suffix and a warning is logged so the user can decide
    whether to disambiguate the source activity names or workspace paths.
    """
    created: list[Path] = []
    written_signatures: dict[str, object] = {}
    taken_paths: set[str] = set()

    for notebook in notebooks:
        target = notebook.relative_path
        signature = _content_signature(notebook)
        existing = written_signatures.get(target)
        if existing is not None:
            if existing == signature:
                # Identical write — skip the duplicate file operation.
                continue
            new_target = _disambiguate(target, taken_paths)
            logger.warning(
                "Two notebooks resolved to the same bundle path %s with different "
                "contents; writing the second copy to %s.  This typically means "
                "two workspace notebook paths share a basename — rename one in "
                "the workspace or adjust the ADF activity name to disambiguate.",
                target,
                new_target,
            )
            target = new_target
        destination = output_dir / target
        destination.parent.mkdir(parents=True, exist_ok=True)
        if notebook.binary_content is not None:
            destination.write_bytes(notebook.binary_content)
        else:
            content = notebook.content if notebook.content.endswith("\n") else notebook.content + "\n"
            destination.write_text(content, encoding="utf-8")
        written_signatures[target] = signature
        taken_paths.add(target)
        created.append(destination.resolve())
    return created
