"""Find Oozie application directories in an export and resolve the files their actions reference.

An export is a copy of HDFS application directories: each holds a ``workflow.xml`` and usually a
``job.properties``, a ``lib/`` of JARs and scripts, and sometimes ``coordinator.xml``. Actions name their
files in several ways (relative, ``${nameNode}/...`` absolute, ``${wf:appPath()}/...``), so resolution
looks for the file inside the app directory by its relative path, its ``lib/...`` tail, or its bare name.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

_EXPRESSION = re.compile(r"\$\{[^}]*\}")
_URI_AUTHORITY = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://[^/]*")


@dataclass(slots=True, kw_only=True)
class OozieApp:
    """One Oozie application directory found in the export."""

    name: str
    directory: Path
    workflow_path: Path
    coordinator_paths: list[Path] = field(default_factory=list)
    properties_path: Path | None = None


def discover_apps(source_dir: Path, *, output_dir: Path | None = None) -> list[OozieApp]:
    """Returns every directory under *source_dir* that holds a ``workflow.xml``, in a stable order.

    App names come from the directory path relative to *source_dir* (``finance/daily`` becomes
    ``finance_daily``). When two paths flatten to the same name (``a/b_c`` and ``a_b/c``), the later ones in
    sorted order get ``_2``, ``_3``, and so on. Anything under *output_dir* is skipped: discover keeps a
    verbatim copy of each ``workflow.xml`` there, and an output dir inside the source dir would otherwise turn
    those copies into new apps on the next run.
    """
    root = source_dir.resolve()
    skipped = output_dir.resolve() if output_dir is not None else None
    apps: list[OozieApp] = []
    used_names: set[str] = set()
    for workflow_path in sorted(root.rglob("workflow.xml")):
        directory = workflow_path.parent
        if skipped is not None and directory.is_relative_to(skipped):
            continue
        relative = directory.relative_to(root)
        base_name = "_".join(relative.parts) if relative.parts else root.name
        properties_path = directory / "job.properties"
        apps.append(
            OozieApp(
                name=_unique_name(base_name, used_names),
                directory=directory,
                workflow_path=workflow_path,
                coordinator_paths=sorted(directory.glob("coordinator*.xml")),
                properties_path=properties_path if properties_path.is_file() else None,
            )
        )
    return apps


def _unique_name(base_name: str, used_names: set[str]) -> str:
    candidate = base_name
    suffix = 2
    while candidate in used_names:
        candidate = f"{base_name}_{suffix}"
        suffix += 1
    used_names.add(candidate)
    return candidate


def resolve_app_file(app: OozieApp, reference: str) -> Path | None:
    """Finds the local copy of a file an action references, or ``None`` when the export doesn't have it.

    Only files inside the app directory are returned, so a reference like ``../../etc/passwd`` can't make
    flowx copy arbitrary local files into a bundle. Symlinks are followed before that check, so a link
    pointing outside the app directory is rejected too.
    """
    if "\x00" in reference:
        return None
    cleaned = _URI_AUTHORITY.sub("", _EXPRESSION.sub("", reference)).split("#", 1)[0].strip()
    if not cleaned:
        return None
    parts = PurePosixPath(cleaned).parts
    file_name = PurePosixPath(cleaned).name
    candidates: list[str] = ["/".join(part for part in parts if part != "/")]
    if "lib" in parts:
        lib_index = len(parts) - 1 - parts[::-1].index("lib")
        candidates.append("/".join(parts[lib_index:]))
    candidates.extend([file_name, f"lib/{file_name}"])
    app_root = app.directory.resolve()
    for candidate in candidates:
        if not candidate:
            continue
        path = (app.directory / candidate).resolve()
        if path.is_file() and path.is_relative_to(app_root):
            return path
    return None
