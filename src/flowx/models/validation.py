"""Records written by the validate phase: findings, per-object stamps, iterations, and the run report."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

RULESET_VERSION = "1.0.0"
SEVERITIES: tuple[str, ...] = ("blocker", "major", "minor", "advisory")
VERDICTS: tuple[str, ...] = ("approved", "approved_with_findings", "rejected")
TERMINATION_REASONS: tuple[str, ...] = ("threshold_met", "budget_exhausted", "oscillation", "dry_run")


@dataclass(slots=True, kw_only=True)
class ValidationFinding:
    """One problem a reviewer found in a converted object.

    Attributes:
        code: Stable identifier, e.g. ``"ORCH-MISSING-DEPENDENCY"``.
        severity: One of :data:`SEVERITIES`.
        message: What is wrong, compared with the source pipeline.
        location: The file, job, or task the finding concerns.
        suggested_fix: How a person or an agent can resolve it.
        customization: The ``metadata/configuration.json`` option the difference follows from, when the
            user asked for it. Such findings are recorded but do not lower the score.
    """

    code: str
    severity: str
    message: str
    location: str = ""
    suggested_fix: str = ""
    customization: str = ""


@dataclass(slots=True, kw_only=True)
class ValidationStamp:
    """The approval metadata attached to one reviewed object.

    Attributes:
        object_id: ``<job>`` for a job, ``<job>/<task>`` for a task, or the bundle name when static
            checks rejected the bundle.
        object_kind: ``job``, ``task``, or ``pipeline``.
        verdict: One of :data:`VERDICTS`.
        score: 0-100.
        findings: The findings behind the score.
        reviewer: The model or tool that reviewed the object.
        ruleset_version: The scoring rules the stamp was computed with.
        reviewed_at: ISO-8601 UTC time of the review.
        iteration: The iteration the object was reviewed in.
        content_hash: Hash of the reviewed definition.
    """

    object_id: str
    object_kind: str
    verdict: str
    score: int
    findings: list[ValidationFinding] = field(default_factory=list)
    reviewer: str
    ruleset_version: str
    reviewed_at: str
    iteration: int
    content_hash: str


@dataclass(slots=True, kw_only=True)
class ValidationIteration:
    """Everything recorded for one pass of the validation loop.

    Attributes:
        run_id: The run this iteration belongs to.
        iteration: 1-based iteration number.
        approval_rate: Weighted approval rate of the job stamps, 0-1.
        content_hash: Hash of every bundle file at review time.
        terminated: The stop reason when the loop ended here, else empty.
        stamps: Every current stamp, including ones carried forward unchanged.
        static: Static-check findings; when present no agentic review ran.
        diffs: Unified diff per object whose definition changed since the previous iteration.
        snapshots: The reviewed definition of every object.
        remediation: Bundle files changed after this iteration.
    """

    run_id: str
    iteration: int
    approval_rate: float
    content_hash: str
    terminated: str = ""
    stamps: list[ValidationStamp] = field(default_factory=list)
    static: list[ValidationFinding] = field(default_factory=list)
    diffs: dict[str, str] = field(default_factory=dict)
    snapshots: dict[str, Any] = field(default_factory=dict)
    remediation: list[str] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class ValidationReport:
    """The run-level report written to ``metadata/validation/validation_report.json``.

    Attributes:
        run_id: Unique id of the run.
        run_by: User who started the run.
        ruleset_version: Scoring rules in effect.
        reviewer: Reviewer of the latest iteration.
        terminated: Stop reason, or empty while the loop is still running.
        iterations: Number of iterations recorded.
        approval_rate: Approval rate of the latest iteration.
        content_hash: Bundle hash of the latest iteration.
        stamps: Stamps of the latest iteration.
        threshold: Approval rate that ends the loop.
        max_iterations: Iteration budget.
        dry_run: When true the bundle is never changed.
        require_approval: When true remediation waits for user approval.
        pipeline: Job or bundle name the run is limited to, or empty for all.
        trajectory: ``iteration`` / ``approval_rate`` / ``content_hash`` per iteration.
    """

    run_id: str
    run_by: str
    ruleset_version: str = RULESET_VERSION
    reviewer: str = ""
    terminated: str = ""
    iterations: int = 0
    approval_rate: float = 0.0
    content_hash: str = ""
    stamps: list[ValidationStamp] = field(default_factory=list)
    threshold: float = 0.9
    max_iterations: int = 3
    dry_run: bool = False
    require_approval: bool = False
    pipeline: str = ""
    trajectory: list[dict[str, Any]] = field(default_factory=list)


def to_dict(record: ValidationIteration | ValidationReport) -> dict[str, Any]:
    """Returns *record* as plain JSON-ready data."""
    return asdict(record)


def finding_from_dict(raw: dict[str, Any]) -> ValidationFinding:
    """Builds a finding from its JSON form."""
    return ValidationFinding(**raw)


def stamp_from_dict(raw: dict[str, Any]) -> ValidationStamp:
    """Builds a stamp from its JSON form."""
    return ValidationStamp(**{**raw, "findings": [finding_from_dict(item) for item in raw.get("findings", [])]})


def iteration_from_dict(raw: dict[str, Any]) -> ValidationIteration:
    """Builds an iteration record from its JSON form."""
    return ValidationIteration(
        **{
            **raw,
            "stamps": [stamp_from_dict(item) for item in raw.get("stamps", [])],
            "static": [finding_from_dict(item) for item in raw.get("static", [])],
        }
    )


def report_from_dict(raw: dict[str, Any]) -> ValidationReport:
    """Builds a run report from its JSON form."""
    return ValidationReport(**{**raw, "stamps": [stamp_from_dict(item) for item in raw.get("stamps", [])]})
