"""Finding files an Airflow task reads, by the rules Airflow itself uses.

Airflow's Jinja loader looks a template name up in the DAG file's folder and then each
``template_searchpath`` directory. flowx follows the same rules and reads nothing outside those folders,
because whatever it reads is copied into the migration output.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath


@dataclass(slots=True, kw_only=True)
class TemplateFile:
    """The outcome of loading a template file.

    Attributes:
        name: The template name as the task gives it.
        path: Where the file was found, or ``None`` when it was not loaded.
        content: The file's text as Airflow renders it, or ``None`` when it was not loaded.
        reason: Why the file was not loaded, when it was not.
    """

    name: str
    path: Path | None = None
    content: str | None = None
    reason: str | None = None

    def record(self) -> dict[str, str]:
        """Returns the file's details for a placeholder or a task's provenance."""
        details = {"path": self.name}
        if self.path is not None:
            details["resolved_path"] = str(self.path)
        if self.content is not None:
            details["content"] = self.content
        if self.reason is not None:
            details["unresolved"] = self.reason
        return details


def find_template_file(name: str, search_paths: tuple[Path, ...]) -> tuple[Path | None, str]:
    """Finds a template file the way Airflow's Jinja loader does, or explains why it cannot.

    Jinja splits the name on ``/``, ignores empty and ``.`` segments, refuses ``..``, and returns the
    first match across the search paths. A match that is a symbolic link leading outside its search
    folder is refused, since its contents would be copied into the output. On success the second value
    is the resolved path as text.
    """
    parts = [part for part in name.split("/") if part and part != "."]
    if ".." in parts:
        return None, "climbs out of the template search path"
    for root in search_paths:
        candidate = root.joinpath(*parts)
        if candidate.is_file():
            if not candidate.resolve().is_relative_to(root.resolve()):
                return None, f"is a symbolic link that leads outside {root}"
            return candidate, str(candidate)
    searched = ", ".join(str(root) for root in search_paths) or "an empty search path"
    return None, f"was not found in {searched}"


def read_template_file(name: str, search_paths: tuple[Path, ...]) -> TemplateFile:
    """Loads a template file's text as Airflow renders it, or records why it could not be loaded."""
    path, detail = find_template_file(name, search_paths)
    if path is None:
        return TemplateFile(name=name, reason=detail)
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        return TemplateFile(name=name, path=path, reason=f"could not be read as UTF-8 text ({error})")
    # Airflow's Jinja environment drops a single trailing newline (keep_trailing_newline=False).
    return TemplateFile(name=name, path=path, content=content.removesuffix("\n"))


def find_exported_file(path: str, search_paths: tuple[Path, ...], source_root: Path | None) -> tuple[Path | None, str]:
    """Finds a file a task names by its path on the Airflow machine, reading only inside discover's folders.

    The path usually points at the Airflow worker or a user's home folder (``/home/<user>/...``), which the
    export copies somewhere else. A relative path follows the template lookup rules. An absolute path is used
    as-is only when it lies inside the search paths or *source_root*. Otherwise the file is looked for under
    *source_root* by name; when several files share the name, the one whose trailing folders match the most of
    the original path wins, and a tie is reported instead of guessed.

    Args:
        path: The file's path as the DAG names it.
        search_paths: The DAG folder, then the DAG's ``template_searchpath``.
        source_root: The folder discover was pointed at.

    Returns:
        The file's local path and that path as text, or None and the reason it was not found.
    """
    roots = tuple(root for root in (*search_paths, source_root) if root is not None)
    if not PurePosixPath(path).is_absolute():
        found, _ = find_template_file(path, search_paths)
        if found is not None:
            return found, str(found)
    candidate = Path(path)
    if candidate.is_absolute() and candidate.is_file() and _inside_any(candidate, roots):
        return candidate, str(candidate)
    if source_root is None:
        return None, "was not found in the DAG folder or template_searchpath"
    return _match_by_name(PurePosixPath(path), source_root)


def _inside_any(path: Path, roots: tuple[Path, ...]) -> bool:
    resolved = path.resolve()
    return any(resolved.is_relative_to(root.resolve()) for root in roots)


def _match_by_name(original: PurePosixPath, source_root: Path) -> tuple[Path | None, str]:
    matches = sorted(
        path
        for path in source_root.rglob(f"*{original.suffix}")
        if path.name == original.name
        and path.is_file()
        and ".ipynb_checkpoints" not in path.parts
        and _inside_any(path, (source_root,))
    )
    if not matches:
        return None, f"was not found at that path or as a file named {original.name!r} under {source_root}"
    if len(matches) == 1:
        return matches[0], str(matches[0])
    scores = {match: _trailing_parts_in_common(original, match.relative_to(source_root)) for match in matches}
    best = max(scores.values())
    leaders = [match for match, score in scores.items() if score == best]
    if len(leaders) == 1 and best > 1:
        return leaders[0], str(leaders[0])
    names = ", ".join(str(match.relative_to(source_root)) for match in matches)
    return None, f"matches {len(matches)} files named {original.name!r} under {source_root} ({names}); keep one"


def _trailing_parts_in_common(original: PurePosixPath, found: Path) -> int:
    count = 0
    for left, right in zip(reversed(original.parts), reversed(found.parts)):
        if left != right:
            break
        count += 1
    return count


def find_script_file(path: str, search_paths: tuple[Path, ...]) -> tuple[Path | None, str]:
    """Finds a script a shell command names, reading only inside the DAG folder and template search path.

    A relative path follows the template lookup rules. An absolute path is used only when it, after
    following symbolic links, lies inside one of the search paths, so discover never copies an unrelated
    file from the machine it runs on.
    """
    if not Path(path).is_absolute():
        return find_template_file(path, search_paths)
    if ".." in Path(path).parts:
        return None, "climbs out of the template search path"
    candidate = Path(path)
    if not any(candidate.resolve().is_relative_to(root.resolve()) for root in search_paths):
        return None, "is outside the DAG folder and template_searchpath, so discover does not read it"
    if not candidate.is_file():
        return None, "was not found"
    return candidate, str(candidate)
