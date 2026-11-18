"""Files and packages a generated dbt task ships with: the dbt project copied into the bundle and dbt's pins."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from flowx.models.dab import DabNotebook

_EXCLUDED_DBT_PATH_PARTS = frozenset({".git", ".venv", "__pycache__", "logs", "target"})
# The dbt packages every generated dbt task installs.
DBT_PACKAGES = ("dbt-databricks==1.12.2", "dbt-core==1.11.12")
# Files at a dbt project's root that dbt reads.
_PROJECT_FILES = ("dbt_project.yml", "packages.yml", "dependencies.yml", "package-lock.yml", "selectors.yml")
# The files dbt reads from a resource folder; used when a folder setting names the project root itself.
_RESOURCE_SUFFIXES = frozenset({".sql", ".py", ".yml", ".yaml", ".md", ".csv"})
_ENV_VAR = re.compile(r"""env_var\(\s*['"]([^'"]+)['"]""")
# The dbt_project.yml settings that name the project's folders, with dbt's defaults.
_PROJECT_FOLDER_SETTINGS = {
    "model-paths": ["models"],
    "macro-paths": ["macros"],
    "seed-paths": ["seeds"],
    "snapshot-paths": ["snapshots"],
    "test-paths": ["tests"],
    "analysis-paths": ["analyses"],
    "docs-paths": [],
    "asset-paths": [],
}


def tree_artifacts(
    source_root: Path,
    destination_root: str,
    *,
    excluded_parts: frozenset[str] = frozenset(),
    suffixes: frozenset[str] | None = None,
) -> list[DabNotebook]:
    """Returns bundle artifacts for files beneath a local source directory.

    Hidden files and folders (``.env``, ``.git``, ...) and files under build, cache, and log folders are
    skipped, as are any under *excluded_parts*. With *suffixes*, only files with one of those suffixes are kept.
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
        and not any(part.startswith(".") for part in source.relative_to(source_root).parts)
        and (suffixes is None or source.suffix in suffixes)
    ]


def _project_settings(project: Path) -> dict:
    try:
        settings = yaml.safe_load((project / "dbt_project.yml").read_text(encoding="utf-8")) or {}
    except (OSError, UnicodeDecodeError, yaml.YAMLError):
        return {}
    return settings if isinstance(settings, dict) else {}


def _project_folders(project: Path, *, include_packages: bool = False) -> list[str]:
    """Returns the folders a dbt project's ``dbt_project.yml`` names, or dbt's defaults for those it leaves out.

    With *include_packages*, the folder ``dbt deps`` installs packages into is included too.
    """
    settings = _project_settings(project)
    folders: list[str] = []
    for setting, default in _PROJECT_FOLDER_SETTINGS.items():
        value = settings.get(setting, default)
        for folder in value if isinstance(value, list) else default:
            if isinstance(folder, str) and folder not in folders:
                folders.append(folder)
    if include_packages:
        packages = settings.get("packages-install-path", "dbt_packages")
        folders.append(packages if isinstance(packages, str) else "dbt_packages")
    return folders


def project_artifacts(project: Path, destination_root: str, *, include_packages: bool = False) -> list[DabNotebook]:
    """Returns bundle artifacts for the files of a dbt project that dbt reads.

    That is ``dbt_project.yml``, the package and selector files, and the folders ``dbt_project.yml`` names,
    plus the installed packages folder with *include_packages*, for a runner that does not run ``dbt deps``.
    Anything else in the project folder, such as ``profiles.yml``, a ``.env`` file, or a credentials file,
    is left out, and so is a folder that would lead outside the project. A setting that names the project root,
    such as ``model-paths: ["."]``, copies only the dbt resource files (SQL, Python, YAML, Markdown, and CSV)
    found under the root, without ``profiles.yml``, installed packages, or build output.
    """
    artifacts = [
        DabNotebook(relative_path=f"{destination_root}/{name}", binary_content=(project / name).read_bytes())
        for name in _PROJECT_FILES
        if (project / name).is_file() and not (project / name).is_symlink()
    ]
    for folder in _project_folders(project, include_packages=include_packages):
        source = (project / folder).resolve()
        if not source.is_relative_to(project.resolve()):
            continue
        if source == project.resolve():
            excluded = frozenset({"profiles.yml"} | (set() if include_packages else {"dbt_packages"}))
            artifacts.extend(
                tree_artifacts(project, destination_root, excluded_parts=excluded, suffixes=_RESOURCE_SUFFIXES)
            )
        else:
            artifacts.extend(tree_artifacts(project / folder, f"{destination_root}/{folder}"))
    unique: dict[str, DabNotebook] = {}
    for artifact in artifacts:
        unique.setdefault(artifact.relative_path, artifact)
    return list(unique.values())


def environment_variables(project: Path) -> list[str]:
    """Returns the environment variables a dbt project reads with ``env_var()``, in the files dbt reads."""
    names: list[str] = []
    for artifact in project_artifacts(project, "project"):
        text = (artifact.binary_content or b"").decode("utf-8", errors="ignore")
        for name in _ENV_VAR.findall(text):
            if name not in names:
                names.append(name)
    return names
