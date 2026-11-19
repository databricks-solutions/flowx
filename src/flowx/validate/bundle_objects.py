"""Reads the reviewable objects (jobs and their tasks) out of the generated bundles, and hashes and diffs them."""

from __future__ import annotations

import difflib
import hashlib
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from flowx.discovery_serde import canonical_sha256
from flowx.models.validation import ValidationFinding
from flowx.validate.bundle_invariants import check_bundle_dir


@dataclass(slots=True, kw_only=True, frozen=True)
class BundleObject:
    """A job or task under review.

    Attributes:
        object_id: ``<job>`` or ``<job>/<task>``.
        object_kind: ``job`` or ``task``.
        job: The job key the object belongs to.
        bundle: The bundle directory name.
        definition: The YAML definition as data; a job includes its tasks.
    """

    object_id: str
    object_kind: str
    job: str
    bundle: str
    definition: dict[str, Any]

    @property
    def content_hash(self) -> str:
        """Hash of the definition."""
        return "sha256:" + canonical_sha256(self.definition)


def find_bundles(output_dir: Path, pipeline: str = "") -> list[Path]:
    """Returns *output_dir* when it holds a ``databricks.yml``, else its children that do.

    When *pipeline* names one of the child bundles, only that bundle is returned.
    """
    if (output_dir / "databricks.yml").is_file():
        return [output_dir]
    bundles = sorted(child for child in output_dir.iterdir() if (child / "databricks.yml").is_file())
    return [bundle for bundle in bundles if bundle.name == pipeline] or bundles


def _load_jobs(bundle_dir: Path) -> dict[str, dict[str, Any]]:
    """Returns the jobs declared in a bundle's resource YAML, by job key."""
    jobs: dict[str, dict[str, Any]] = {}
    for path in sorted((bundle_dir / "resources").glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        jobs.update(((document.get("resources") or {}).get("jobs")) or {})
    return jobs


def collect_objects(output_dir: Path, pipeline: str = "") -> list[BundleObject]:
    """Returns every job and top-level task in the bundles, limited to one job or bundle when *pipeline* is set."""
    objects: list[BundleObject] = []
    for bundle_dir in find_bundles(output_dir, pipeline):
        for job_key, job in _load_jobs(bundle_dir).items():
            if pipeline and pipeline not in (job_key, bundle_dir.name):
                continue
            objects.append(
                BundleObject(object_id=job_key, object_kind="job", job=job_key, bundle=bundle_dir.name, definition=job)
            )
            objects.extend(
                BundleObject(
                    object_id=f"{job_key}/{task['task_key']}",
                    object_kind="task",
                    job=job_key,
                    bundle=bundle_dir.name,
                    definition=task,
                )
                for task in job.get("tasks") or []
            )
    return objects


def bundle_hash(bundle_dirs: list[Path]) -> str:
    """Returns one hash over every non-hidden file in *bundle_dirs*."""
    digest = hashlib.sha256()
    for bundle_dir in bundle_dirs:
        for path in sorted(bundle_dir.rglob("*")):
            relative = path.relative_to(bundle_dir)
            if path.is_file() and not any(part.startswith(".") for part in relative.parts):
                digest.update(f"{bundle_dir.name}/{relative.as_posix()}\0".encode())
                digest.update(path.read_bytes())
    return "sha256:" + digest.hexdigest()


def object_diff(object_id: str, before: Any, after: Any) -> str:
    """Returns a unified diff of two definitions of *object_id*, or empty when they match."""
    lines = difflib.unified_diff(
        yaml.safe_dump(before, sort_keys=True).splitlines(keepends=True) if before is not None else [],
        yaml.safe_dump(after, sort_keys=True).splitlines(keepends=True) if after is not None else [],
        fromfile=f"{object_id} (before)",
        tofile=f"{object_id} (after)",
    )
    return "".join(lines)


def text_diff(path: str, before: str, after: str) -> str:
    """Returns a unified diff of two versions of the file at *path*."""
    lines = difflib.unified_diff(
        before.splitlines(keepends=True), after.splitlines(keepends=True), fromfile=path, tofile=path
    )
    return "".join(lines)


def run_static_checks(
    bundle_dirs: list[Path], *, bundle_validate: bool = False, profile: str = ""
) -> dict[str, list[ValidationFinding]]:
    """Runs the structural checks, and optionally ``databricks bundle validate``, on each bundle.

    Args:
        bundle_dirs: The bundles to check.
        bundle_validate: Also run ``databricks bundle validate`` in each bundle.
        profile: Databricks CLI profile for ``bundle validate``.

    Returns:
        Blocker findings by bundle name; bundles without problems are left out.

    Raises:
        ValueError: When *bundle_validate* is set and the ``databricks`` CLI is not installed.
    """
    if bundle_validate and shutil.which("databricks") is None:
        raise ValueError("bundle_validate needs the databricks CLI on PATH; it is not available on serverless.")
    findings: dict[str, list[ValidationFinding]] = {}
    for bundle_dir in bundle_dirs:
        bundle_findings = _structural_findings(bundle_dir)
        if bundle_validate:
            bundle_findings.extend(_cli_validate(bundle_dir, profile))
        if bundle_findings:
            findings[bundle_dir.name] = bundle_findings
    return findings


def _structural_findings(bundle_dir: Path) -> list[ValidationFinding]:
    """Returns the bundle-invariant violations of *bundle_dir*, or one finding when its YAML does not parse."""
    try:
        violations = check_bundle_dir(bundle_dir).violations
    except yaml.YAMLError as error:
        return [
            ValidationFinding(
                code="STATIC-INVALID-YAML",
                severity="blocker",
                message=str(error),
                location=bundle_dir.name,
                suggested_fix="Fix the YAML syntax.",
            )
        ]
    return [
        ValidationFinding(
            code=f"STATIC-{violation.code.upper().replace('_', '-')}",
            severity="blocker",
            message=violation.message,
            location=f"{bundle_dir.name}: {violation.location}",
            suggested_fix="Fix the bundle YAML so the structural check passes.",
        )
        for violation in violations
    ]


def _cli_validate(bundle_dir: Path, profile: str) -> list[ValidationFinding]:
    """Runs ``databricks bundle validate`` in *bundle_dir* and returns a finding when it fails."""
    command = ["databricks", "bundle", "validate", *(["--profile", profile] if profile else [])]
    result = subprocess.run(command, cwd=bundle_dir, capture_output=True, text=True, check=False)
    if result.returncode == 0:
        return []
    return [
        ValidationFinding(
            code="STATIC-BUNDLE-VALIDATE",
            severity="blocker",
            message=(result.stderr or result.stdout).strip()[-2000:],
            location=bundle_dir.name,
            suggested_fix="Fix the errors databricks bundle validate reports.",
        )
    ]
