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
# An ``env_var()`` call without a default, which fails when the variable is not set.
_ENV_VAR = re.compile(r"""env_var\(\s*['"]([^'"]+)['"]\s*\)""")
# Files that declare dbt packages, which a fresh task environment installs with ``dbt deps``.
_PACKAGE_FILES = ("packages.yml", "dependencies.yml")
# The most commands a Databricks dbt task runs.
MAX_TASK_COMMANDS = 10
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


def _package_entries(project: Path) -> list[object] | None:
    """Returns the package entries of a project's package files, or None when one cannot be read."""
    entries: list[object] = []
    for name in _PACKAGE_FILES:
        path = project / name
        if not path.is_file():
            continue
        try:
            content = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, yaml.YAMLError):
            return None
        if content is None:
            continue
        if not isinstance(content, dict):
            return None
        for key in ("packages", "projects"):
            value = content.get(key) or []
            entries.extend(value if isinstance(value, list) else [value])
    return entries


def declares_packages(project: Path) -> bool:
    """Returns whether the project's package files list any package; one that cannot be read counts as yes."""
    entries = _package_entries(project)
    return entries is None or bool(entries)


def local_packages(project: Path) -> list[str]:
    """Returns the folders of the project's ``local:`` packages, as written, which ``dbt deps`` copies from."""
    return [
        entry["local"]
        for entry in _package_entries(project) or []
        if isinstance(entry, dict) and isinstance(entry.get("local"), str)
    ]


def local_packages_outside(project: Path) -> list[str]:
    """Returns the ``local:`` package folders that lie outside the project, so the bundle cannot carry them."""
    root = project.resolve()
    return [folder for folder in local_packages(project) if not (project / folder).resolve().is_relative_to(root)]


def task_commands(commands: list[str], project: Path) -> list[str]:
    """Returns the commands a dbt task runs: *commands*, with ``dbt deps`` added when the project needs it.

    A task starts without the project's packages installed, so ``dbt deps`` goes after any leading
    ``dbt clean`` (which removes them) when the project declares packages and the commands do not install them.
    """
    commands = list(commands)
    if any(command.split()[:2] == ["dbt", "deps"] for command in commands) or not declares_packages(project):
        return commands
    position = next(
        (index for index, command in enumerate(commands) if command.split()[:2] != ["dbt", "clean"]), len(commands)
    )
    commands.insert(position, "dbt deps")
    return commands


def _project_folders(project: Path, *, include_packages: bool = False) -> list[str]:
    """Returns the folders a dbt project reads: those ``dbt_project.yml`` names, or dbt's defaults for those it
    leaves out, and its ``local:`` packages.

    With *include_packages*, the folder ``dbt deps`` installs packages into is included too.
    """
    settings = _project_settings(project)
    folders: list[str] = []
    for setting, default in _PROJECT_FOLDER_SETTINGS.items():
        value = settings.get(setting, default)
        for folder in value if isinstance(value, list) else default:
            if isinstance(folder, str) and folder not in folders:
                folders.append(folder)
    for folder in local_packages(project):
        if folder not in folders:
            folders.append(folder)
    if include_packages:
        folders.append(_packages_folder(project))
    return folders


def _packages_folder(project: Path) -> str:
    """Returns the folder ``dbt deps`` installs packages into."""
    packages = _project_settings(project).get("packages-install-path", "dbt_packages")
    return packages if isinstance(packages, str) else "dbt_packages"


def _output_folders(project: Path) -> list[str]:
    """Returns the folders dbt writes build output and logs into."""
    settings = _project_settings(project)
    return [
        value
        for value in (settings.get("target-path", "target"), settings.get("log-path", "logs"))
        if isinstance(value, str)
    ]


def project_artifacts(project: Path, destination_root: str, *, include_packages: bool = False) -> list[DabNotebook]:
    """Returns bundle artifacts for the files of a dbt project that dbt reads.

    That is ``dbt_project.yml``, the package and selector files, and the folders ``dbt_project.yml`` names,
    plus the installed packages folder with *include_packages*, for a runner that does not run ``dbt deps``.
    Anything else in the project folder, such as ``profiles.yml``, a ``.env`` file, or a credentials file,
    is left out, and so is a folder that would lead outside the project. A setting that names the project root,
    such as ``model-paths: ["."]``, copies the dbt resource files (SQL, Python, YAML, Markdown, and CSV) found
    under the root, since dbt reads them all, without ``profiles.yml``, installed packages, build and log
    output, or Python virtual environments.
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
            excluded = frozenset(
                {
                    "profiles.yml",
                    *_output_folders(project),
                    *(marker.parent.name for marker in project.rglob("pyvenv.cfg")),
                }
                | (set() if include_packages else {_packages_folder(project)})
            )
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
