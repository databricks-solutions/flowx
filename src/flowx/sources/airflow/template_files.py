"""Finding files an Airflow task reads, by the rules Airflow itself uses.

Airflow's Jinja loader looks a template name up in the DAG file's folder and then each
``template_searchpath`` directory. flowx follows the same rules, and reads nothing outside those folders.
"""

from __future__ import annotations

from pathlib import Path


def find_template_file(name: str, search_paths: tuple[Path, ...]) -> tuple[Path | None, str]:
    """Finds a template file the way Airflow's Jinja loader does, or explains why it cannot.

    Jinja splits the name on ``/``, ignores empty and ``.`` segments, refuses ``..``, and returns the
    first match across the search paths. On success the second value is the resolved path as text.
    """
    parts = [part for part in name.split("/") if part and part != "."]
    if ".." in parts:
        return None, "climbs out of the template search path"
    for root in search_paths:
        candidate = root.joinpath(*parts)
        if candidate.is_file():
            return candidate, str(candidate)
    searched = ", ".join(str(root) for root in search_paths) or "an empty search path"
    return None, f"was not found in {searched}"


def find_script_file(path: str, search_paths: tuple[Path, ...]) -> tuple[Path | None, str]:
    """Finds a script a shell command names, reading only inside the DAG folder and template search path.

    A relative path follows the template lookup rules. An absolute path is used only when it lies inside
    one of the search paths, so discover never copies an unrelated file from the machine it runs on.
    """
    if not Path(path).is_absolute():
        return find_template_file(path, search_paths)
    if ".." in Path(path).parts:
        return None, "climbs out of the template search path"
    candidate = Path(path)
    if not any(candidate.is_relative_to(root) for root in search_paths):
        return None, "is outside the DAG folder and template_searchpath, so discover does not read it"
    if not candidate.is_file():
        return None, "was not found"
    return candidate, str(candidate)
