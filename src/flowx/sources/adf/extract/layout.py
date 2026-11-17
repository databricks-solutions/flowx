"""Check that a path holds ADF definitions discover can read, whichever way they were obtained.

The same check runs after every route in the ``flowx-extract-metadata`` skill: an ADF Studio
ARM export, a Git clone, or a live extraction. It catches the common traps before discover
silently produces an empty inventory -- most importantly pointing at the unzipped ADF Studio
export folder, whose ``factory/`` subfolder makes the loader treat it as a native layout with
no pipelines, when the definitions actually live in ``ARMTemplateForFactory.json``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from flowx.sources.adf.extract.core import MANIFEST_NAME
from flowx.sources.adf.loader import load_adf_definitions

ARM_TEMPLATE_NAME = "ARMTemplateForFactory.json"
_NATIVE_PIPELINE_FOLDERS = ("pipeline", "pipelines")
_MANIFEST_FOLDERS_TO_COUNTS = {
    "pipeline": "pipelines",
    "dataset": "datasets",
    "linkedService": "linked_services",
    "trigger": "triggers",
}


@dataclass(slots=True, kw_only=True)
class LayoutReport:
    """What was found at a path and the exact path to hand to discover.

    Attributes:
        requested_path: The path that was checked.
        kind: ``native``, ``arm_template``, ``arm_export_folder``, ``multiple_factories``, ``zip``,
            ``missing`` or ``unknown``.
        discover_path: The path to give discover, or ``None`` when there is no single answer.
        counts: What discover parsed at ``discover_path`` (pipelines, datasets, ...).
        warnings: Problems and hints to show the user.
        candidates: For ``multiple_factories``, the path of each factory found.
    """

    requested_path: str
    kind: str
    discover_path: str | None
    counts: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    candidates: list[str] = field(default_factory=list)

    @property
    def usable(self) -> bool:
        """True when discover will find at least one pipeline at ``discover_path``."""
        return self.discover_path is not None and self.counts.get("pipelines", 0) > 0

    def as_dict(self) -> dict[str, Any]:
        """Returns the report as a JSON-ready dict, including ``usable``."""
        return {
            "requested_path": self.requested_path,
            "kind": self.kind,
            "discover_path": self.discover_path,
            "usable": self.usable,
            "counts": self.counts,
            "warnings": self.warnings,
            "candidates": self.candidates,
        }


def _has_native_layout(folder: Path) -> bool:
    return any((folder / name).is_dir() for name in _NATIVE_PIPELINE_FOLDERS) or any(
        child.is_dir() and child.name.lower() in _NATIVE_PIPELINE_FOLDERS for child in folder.iterdir()
    )


def _find_arm_templates(folder: Path) -> list[Path]:
    """Finds ``ARMTemplateForFactory.json`` at the top of *folder*, or else one level down.

    One level down covers the ``adf_publish`` branch, which keeps the template in a folder named after
    the factory.
    """
    direct = folder / ARM_TEMPLATE_NAME
    if direct.is_file():
        return [direct]
    nested = sorted(child / ARM_TEMPLATE_NAME for child in folder.iterdir() if child.is_dir())
    return [candidate for candidate in nested if candidate.is_file()]


def _find_native_roots(folder: Path) -> list[Path]:
    """Finds subfolders with the native layout (a Git repo whose ADF root folder isn't ``/``)."""
    return [child for child in sorted(folder.iterdir()) if child.is_dir() and _has_native_layout(child)]


def _ask_which_factory(report: LayoutReport, candidates: list[Path]) -> LayoutReport:
    report.kind = "multiple_factories"
    report.candidates = [str(candidate) for candidate in candidates]
    report.warnings.append(
        f"{report.requested_path} holds definitions for {len(candidates)} factories; ask which one to migrate "
        "and check that path: " + ", ".join(report.candidates)
    )
    return report


def _count_definitions(path: Path) -> dict[str, int]:
    definitions = load_adf_definitions(path)
    return {
        "pipelines": len(definitions.pipelines),
        "datasets": len(definitions.datasets),
        "linked_services": len(definitions.linked_services),
        "triggers": len(definitions.triggers),
        "global_parameters": len(definitions.global_parameters),
    }


def _compare_with_manifest(folder: Path, counts: dict[str, int]) -> list[str]:
    """Returns a warning for an incomplete extraction or each count discover didn't match."""
    manifest_path = folder / MANIFEST_NAME
    if not manifest_path.is_file():
        return []
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return [f"{MANIFEST_NAME} could not be read: {error}"]
    warnings: list[str] = []
    if not manifest.get("complete", False):
        warnings.append(
            "The extraction was incomplete: " + "; ".join(manifest.get("warnings") or ["no details recorded"])
        )
    for folder_name, count_name in _MANIFEST_FOLDERS_TO_COUNTS.items():
        expected = (manifest.get("counts") or {}).get(folder_name)
        if expected is not None and expected != counts.get(count_name):
            warnings.append(
                f"{MANIFEST_NAME} lists {expected} {folder_name} file(s) but discover parsed "
                f"{counts.get(count_name)}; some definitions may not have loaded."
            )
    return warnings


def inspect_source_path(path: str | Path) -> LayoutReport:
    """Works out what *path* contains and which path discover should be given.

    Args:
        path: An ADF Studio export (folder or template file), a Git folder or clone, or an earlier
            live extraction.

    Returns:
        The report; ``discover_path`` is the exact path to give discover, and ``usable`` says whether
        discover will find pipelines there.
    """
    requested = Path(path).expanduser()
    report = LayoutReport(requested_path=str(requested), kind="missing", discover_path=None)
    if not requested.exists():
        report.warnings.append(f"{requested} does not exist.")
        return report

    if requested.is_file():
        if requested.suffix.lower() == ".zip":
            report.kind = "zip"
            report.warnings.append("Unzip the ADF Studio export first, then point at ARMTemplateForFactory.json.")
            return report
        if requested.suffix != ".json":
            report.kind = "unknown"
            report.warnings.append(f"{requested} is not a .json file; point at {ARM_TEMPLATE_NAME} or its folder.")
            return report
        report.kind = "arm_template"
        report.discover_path = str(requested)
    elif _has_native_layout(requested):
        report.kind = "native"
        report.discover_path = str(requested)
    elif templates := _find_arm_templates(requested):
        if len(templates) > 1:
            return _ask_which_factory(report, templates)
        report.kind = "arm_export_folder"
        report.discover_path = str(templates[0])
        report.warnings.append(
            f"{requested} is an ARM template export. Point discover at the template file "
            f"{templates[0]}, not the folder: the folder's factory/ subfolder holds only the factory itself, "
            "so discover would find no pipelines."
        )
    elif native_roots := _find_native_roots(requested):
        if len(native_roots) > 1:
            return _ask_which_factory(report, native_roots)
        report.kind = "native"
        report.discover_path = str(native_roots[0])
        report.warnings.append(
            f"The definitions are in {native_roots[0]} (the factory's Git root folder); use that path for discover."
        )
    else:
        report.kind = "unknown"
        report.warnings.append(
            f"No ADF definitions found in {requested}. Expected a pipeline/ folder (Git or live extraction) "
            f"or {ARM_TEMPLATE_NAME} (ADF Studio export or the adf_publish branch)."
        )
        return report

    discover_path = Path(report.discover_path)
    try:
        report.counts = _count_definitions(discover_path)
    except Exception as error:  # noqa: BLE001 -- surface any parse failure to the user as a warning
        report.warnings.append(f"discover could not parse {discover_path}: {error}")
        return report
    if report.counts.get("pipelines", 0) == 0:
        report.warnings.append(f"No pipelines were found in {discover_path}.")
    if report.kind == "native":
        report.warnings.extend(_compare_with_manifest(discover_path, report.counts))
    return report
