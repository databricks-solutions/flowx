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

DEPLOYABLE_DIRS = frozenset({"resources", "src"})
BUNDLE_VALIDATE_CHECK = "databricks bundle validate"
STRUCTURE_CHECK = "structure"


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
    """Returns the bundles under *output_dir*, limited to the one that matches *pipeline* when it is set.

    The bundle is *output_dir* itself when it holds a ``databricks.yml``, else each child that does.
    *pipeline* matches a bundle directory name or a job key.

    Raises:
        ValueError: When no bundle or job matches *pipeline*.
    """
    if (output_dir / "databricks.yml").is_file():
        bundles = [output_dir]
    else:
        bundles = sorted(child for child in output_dir.iterdir() if (child / "databricks.yml").is_file())
    if not pipeline:
        return bundles
    selected = [bundle for bundle in bundles if bundle.name == pipeline] or [
        bundle for bundle in bundles if pipeline in _job_keys(bundle)
    ]
    if not selected:
        raise ValueError(f"Pipeline {pipeline!r} not found: no bundle directory or job key has that name.")
    return selected


def _job_keys(bundle_dir: Path) -> set[str]:
    try:
        return set(_load_jobs(bundle_dir))
    except yaml.YAMLError:
        return set()


def _load_jobs(bundle_dir: Path) -> dict[str, dict[str, Any]]:
    """Returns the jobs declared in a bundle's resource YAML, by job key."""
    jobs: dict[str, dict[str, Any]] = {}
    for path in sorted((bundle_dir / "resources").glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        jobs.update(((document.get("resources") or {}).get("jobs")) or {})
    return jobs


def is_deployable(relative: Path) -> bool:
    """Says whether a bundle-relative path is ``databricks.yml`` or sits under ``resources/`` or ``src/``."""
    return relative.as_posix() == "databricks.yml" or (len(relative.parts) > 1 and relative.parts[0] in DEPLOYABLE_DIRS)


def is_editable(relative: Path) -> bool:
    """Says whether remediation may change a bundle-relative path: ``databricks.yml`` or YAML in ``resources/``."""
    return relative.suffix in (".yml", ".yaml") and is_deployable(relative) and relative.parts[0] != "src"


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
    """Returns one hash over the deployable inputs of *bundle_dirs*, leaving out ``metadata/`` and other phase state."""
    digest = hashlib.sha256()
    for bundle_dir in bundle_dirs:
        for path in sorted(bundle_dir.rglob("*")):
            relative = path.relative_to(bundle_dir)
            if path.is_file() and is_deployable(relative):
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
    bundle_dirs: list[Path], *, bundle_validate: bool = True, profile: str = ""
) -> tuple[dict[str, list[ValidationFinding]], list[dict[str, str]]]:
    """Runs the structural checks and ``databricks bundle validate`` on each bundle.

    ``bundle validate`` runs when the ``databricks`` CLI is on PATH. When it is turned off or the CLI is
    missing, as on serverless or Genie Code, the check is recorded as ``skipped`` with the reason.

    Args:
        bundle_dirs: The bundles to check.
        bundle_validate: Run ``databricks bundle validate`` when the CLI is available.
        profile: Databricks CLI profile for ``bundle validate``.

    Returns:
        ``(findings, checks)``: blocker findings by bundle name (bundles without problems are left out),
        and one ``{check, bundle, status, reason}`` record per check run or skipped.
    """
    skip_reason = _bundle_validate_skip_reason(bundle_validate)
    findings: dict[str, list[ValidationFinding]] = {}
    checks: list[dict[str, str]] = []
    for bundle_dir in bundle_dirs:
        structural = _structural_findings(bundle_dir)
        checks.append(_check(STRUCTURE_CHECK, bundle_dir, "failed" if structural else "passed"))
        cli = [] if skip_reason else _cli_validate(bundle_dir, profile)
        status = "skipped" if skip_reason else "failed" if cli else "passed"
        checks.append(_check(BUNDLE_VALIDATE_CHECK, bundle_dir, status, skip_reason))
        if structural or cli:
            findings[bundle_dir.name] = structural + cli
    return findings, checks


def _bundle_validate_skip_reason(bundle_validate: bool) -> str:
    if not bundle_validate:
        return "turned off with --no-bundle-validate"
    if shutil.which("databricks") is None:
        return "databricks CLI not found on PATH; it is not available on serverless or Genie Code"
    return ""


def _check(name: str, bundle_dir: Path, status: str, reason: str = "") -> dict[str, str]:
    return {"check": name, "bundle": bundle_dir.name, "status": status, "reason": reason}


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
    try:
        result = subprocess.run(command, cwd=bundle_dir, capture_output=True, text=True, check=False, timeout=300)
    except subprocess.TimeoutExpired:
        return [_cli_finding(bundle_dir, "databricks bundle validate timed out after 300 s.")]
    if result.returncode == 0:
        return []
    return [_cli_finding(bundle_dir, (result.stderr or result.stdout).strip()[-2000:])]


def _cli_finding(bundle_dir: Path, message: str) -> ValidationFinding:
    return ValidationFinding(
        code="STATIC-BUNDLE-VALIDATE",
        severity="blocker",
        message=message,
        location=bundle_dir.name,
        suggested_fix="Fix the errors databricks bundle validate reports.",
    )
