"""The validate loop: static checks, agent review, stamping, reports, and in-place remediation.

flowx runs no model. Each iteration the host agent calls :func:`prepare_validation` to get the objects to
review, reviews them against the source pipeline, sends its findings to :func:`record_validation`, and,
while the loop has not stopped, fixes the bundle through :func:`apply_remediation`. All state lives under
``metadata/validation/`` so a run can be resumed from any step.
"""

from __future__ import annotations

import getpass
import json
import uuid
from dataclasses import asdict, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from flowx.bundler.constants import KEPT_TRANSLATION_REPORT
from flowx.models.validation import (
    RULESET_VERSION,
    SEVERITIES,
    ValidationFinding,
    ValidationIteration,
    ValidationReport,
    ValidationStamp,
    finding_from_dict,
    iteration_from_dict,
    report_from_dict,
    to_dict,
)
from flowx.validate.bundle_objects import (
    BundleObject,
    bundle_hash,
    collect_objects,
    find_bundles,
    is_editable,
    object_diff,
    run_static_checks,
    text_diff,
)
from flowx.validate.scoring import (
    REJECTION_SCORE,
    SEVERITY_PENALTIES,
    approval_rate,
    roll_up_job,
    score_findings,
    verdict_for,
)

STATIC_REVIEWER = "flowx-static"
_FINDING_KEYS = frozenset(field.name for field in fields(ValidationFinding))


def validation_dir(output_dir: Path) -> Path:
    """Returns the folder holding the validation state for *output_dir*."""
    return Path(output_dir) / "metadata" / "validation"


def report_path(output_dir: Path) -> Path:
    """Returns the path of ``validation_report.json``."""
    return validation_dir(output_dir) / "validation_report.json"


def run_dir(output_dir: Path, run_id: str) -> Path:
    """Returns the folder holding the iteration records of one run."""
    return validation_dir(output_dir) / "runs" / run_id


def iteration_path(output_dir: Path, run_id: str, iteration: int) -> Path:
    """Returns the path of one iteration record."""
    return run_dir(output_dir, run_id) / f"iteration_{iteration:03d}.json"


def _pending_path(output_dir: Path) -> Path:
    return validation_dir(output_dir) / "pending.json"


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def load_report(output_dir: Path) -> ValidationReport | None:
    """Returns the saved run report, or ``None`` before the first run."""
    path = report_path(output_dir)
    return report_from_dict(_read_json(path)) if path.is_file() else None


def load_iterations(output_dir: Path) -> list[ValidationIteration]:
    """Returns the saved iteration records of the current run, oldest first."""
    report = load_report(output_dir)
    if report is None:
        return []
    return [
        iteration_from_dict(_read_json(iteration_path(output_dir, report.run_id, i)))
        for i in range(1, report.iterations + 1)
    ]


def _customizations(output_dir: Path) -> dict[str, Any]:
    path = Path(output_dir) / "metadata" / "configuration.json"
    return _read_json(path) if path.is_file() else {}


def _translation_entries(output_dir: Path, translation_report: Path | None) -> dict[str, Any]:
    path = translation_report or Path(output_dir) / KEPT_TRANSLATION_REPORT
    if not path.is_file():
        raise ValueError(
            f"No translation report at {path}. Run package, which keeps it at {KEPT_TRANSLATION_REPORT}, "
            "or pass the convert report with --report."
        )
    pipelines = _read_json(path).get("pipelines") or []
    return {
        entry["name"]: {key: value for key, value in entry.items() if key != "tasks"}
        for entry in pipelines
        if "name" in entry
    }


def _source_definition(output_dir: Path, job: str, job_definition: dict[str, Any]) -> Any:
    metadata = Path(output_dir) / "metadata"
    for name in (job, job_definition.get("name", "")):
        arm = metadata / f"{name}.arm.json"
        if name and arm.is_file():
            return _read_json(arm)
    inventory = metadata / "inventory.json"
    if inventory.is_file():
        names = {job, job_definition.get("name", "")}
        return next((entry for entry in _read_json(inventory).get("pipelines", []) if entry.get("name") in names), None)
    return None


def _start_run(previous: ValidationReport | None, settings: dict[str, Any]) -> ValidationReport:
    chosen = {key: value for key, value in settings.items() if value is not None}
    return ValidationReport(
        run_id=str(uuid.uuid4()),
        run_by=getpass.getuser(),
        stamps=previous.stamps if previous else [],
        **chosen,
    )


def prepare_validation(
    output_dir: Path,
    *,
    pipeline: str | None = None,
    threshold: float | None = None,
    max_iterations: int | None = None,
    dry_run: bool | None = None,
    require_approval: bool | None = None,
    reset: bool = False,
    bundle_validate: bool = True,
    profile: str = "",
    translation_report: Path | None = None,
) -> dict[str, Any]:
    """Starts or resumes a run and returns the objects the agent must review this iteration.

    Static checks run first. When they fail, the iteration is recorded straight away with every bundle
    rejected and nothing is sent for review. Objects whose definition still matches an ``approved``
    stamp are skipped. Run settings only take effect when a new run starts; a saved run resumes with
    its own settings.

    Args:
        output_dir: The migration output folder holding the bundles and ``metadata/``.
        pipeline: Limit the run to one job key or bundle name.
        threshold: Approval rate, 0-1, that ends the loop.
        max_iterations: Iteration budget.
        dry_run: Review only; never change the bundle.
        require_approval: Make remediation wait for user approval.
        reset: Start a new run even when one is in progress. Approved stamps still carry over.
        bundle_validate: Run ``databricks bundle validate`` as a static check when the CLI is available;
            otherwise the check is recorded as skipped with the reason.
        profile: Databricks CLI profile for ``bundle validate``.
        translation_report: The convert report; defaults to the copy package keeps in ``metadata/``.

    Returns:
        ``status`` is ``review`` (review ``targets``), ``static_failed`` (fix ``static`` findings), or
        ``terminated`` (the run already ended; pass ``reset`` to start again).

    Raises:
        ValueError: When the output folder holds no bundle, *pipeline* matches nothing, the translation
            report is missing, or settings are out of range.
    """
    output_dir = Path(output_dir)
    if not find_bundles(output_dir, pipeline or ""):
        raise ValueError(f"No bundle (databricks.yml) under {output_dir}; run package first.")
    translation = _translation_entries(output_dir, translation_report)
    if threshold is not None and not 0 < threshold <= 1:
        raise ValueError("threshold must be greater than 0 and at most 1.")
    if max_iterations is not None and max_iterations < 1:
        raise ValueError("max_iterations must be at least 1.")
    report = load_report(output_dir)
    if report is not None and report.terminated and not reset:
        return {"ok": True, "status": "terminated", "report": to_dict(report)}
    if report is not None and reset:
        _write_json(run_dir(output_dir, report.run_id) / "validation_report.json", to_dict(report))
    if report is None or reset:
        settings = {
            "pipeline": pipeline,
            "threshold": threshold,
            "max_iterations": max_iterations,
            "dry_run": dry_run,
            "require_approval": require_approval,
        }
        report = _start_run(report, settings)
        _write_json(report_path(output_dir), to_dict(report))
    _pending_path(output_dir).unlink(missing_ok=True)
    iteration = report.iterations + 1
    bundle_dirs = find_bundles(output_dir, report.pipeline)
    content_hash = bundle_hash(bundle_dirs)
    static, checks = run_static_checks(bundle_dirs, bundle_validate=bundle_validate, profile=profile)
    if static:
        stamps = [_static_stamp(bundle, findings, iteration) for bundle, findings in static.items()]
        summary = _finish_iteration(
            output_dir, report, iteration, content_hash, stamps, [], STATIC_REVIEWER, static, checks
        )
        return {"ok": True, "status": "static_failed", **summary}
    objects = collect_objects(output_dir, report.pipeline)
    prior = _prior_stamps(output_dir, report)
    targets = [item for item in objects if not _is_unchanged_approval(prior.get(item.object_id), item)]
    _write_json(
        _pending_path(output_dir),
        {
            "run_id": report.run_id,
            "iteration": iteration,
            "content_hash": content_hash,
            "targets": [item.object_id for item in targets],
            "checks": checks,
        },
    )
    jobs = {item.job: item for item in objects if item.object_kind == "job"}
    return {
        "ok": True,
        "status": "review",
        "run_id": report.run_id,
        "iteration": iteration,
        "settings": _settings(report),
        "checks": checks,
        "targets": [_target_view(item) for item in targets],
        "skipped": [item.object_id for item in objects if item not in targets],
        "sources": {job: _source_definition(output_dir, job, item.definition) for job, item in jobs.items()},
        "translation": {name: entry for name, entry in translation.items() if name in jobs},
        "customizations": _customizations(output_dir),
        "ruleset": {"version": RULESET_VERSION, "severities": SEVERITY_PENALTIES, "rejection_score": REJECTION_SCORE},
    }


def _prior_stamps(output_dir: Path, report: ValidationReport) -> dict[str, ValidationStamp]:
    reviewed = next(
        (record.stamps for record in reversed(load_iterations(output_dir)) if not record.static), report.stamps
    )
    return {stamp.object_id: stamp for stamp in reviewed}


def _settings(report: ValidationReport) -> dict[str, Any]:
    return {
        "pipeline": report.pipeline,
        "threshold": report.threshold,
        "max_iterations": report.max_iterations,
        "dry_run": report.dry_run,
        "require_approval": report.require_approval,
    }


def _target_view(item: BundleObject) -> dict[str, Any]:
    return {
        "object_id": item.object_id,
        "object_kind": item.object_kind,
        "job": item.job,
        "bundle": item.bundle,
        "definition": item.definition,
    }


def _is_unchanged_approval(stamp: ValidationStamp | None, item: BundleObject) -> bool:
    return stamp is not None and stamp.verdict == "approved" and stamp.content_hash == item.content_hash


def _static_stamp(bundle: str, findings: list[ValidationFinding], iteration: int) -> ValidationStamp:
    return ValidationStamp(
        object_id=bundle,
        object_kind="pipeline",
        verdict="rejected",
        score=0,
        findings=findings,
        reviewer=STATIC_REVIEWER,
        ruleset_version=RULESET_VERSION,
        reviewed_at=_now(),
        iteration=iteration,
        content_hash="",
    )


def record_validation(output_dir: Path, reviews: dict[str, Any]) -> dict[str, Any]:
    """Validates the agent's findings, stamps every object, and writes the iteration and run reports.

    Args:
        output_dir: The migration output folder.
        reviews: ``{"reviewer": "<model id>", "reviews": [{"object_id": ..., "findings": [...]}]}`` with
            one review per target from :func:`prepare_validation`.

    Returns:
        ``ok`` false with ``violations`` when the reviews are invalid (nothing is written); otherwise the
        iteration summary with ``approval_rate``, ``terminated``, and the ``actionable`` findings.
    """
    output_dir = Path(output_dir)
    report = load_report(output_dir)
    pending_path = _pending_path(output_dir)
    if report is None or not pending_path.is_file():
        return {"ok": False, "violations": ["No review is pending; run validate prepare first."]}
    pending = _read_json(pending_path)
    content_hash = bundle_hash(find_bundles(output_dir, report.pipeline))
    if content_hash != pending["content_hash"]:
        return {"ok": False, "violations": ["The bundle changed after prepare; run validate prepare again."]}
    findings_by_object, violations = _parse_reviews(reviews, set(pending["targets"]), _customizations(output_dir))
    reviewer = reviews.get("reviewer") if isinstance(reviews, dict) else None
    if not isinstance(reviewer, str) or not reviewer:
        violations.append("reviewer must name the reviewing model, e.g. 'claude-opus-5'.")
    if violations or not isinstance(reviewer, str):
        return {"ok": False, "violations": violations}
    objects = collect_objects(output_dir, report.pipeline)
    prior = _prior_stamps(output_dir, report)
    iteration = pending["iteration"]
    stamps = _stamp_objects(objects, findings_by_object, prior, reviewer, iteration)
    summary = _finish_iteration(
        output_dir, report, iteration, content_hash, stamps, objects, reviewer, {}, pending.get("checks", [])
    )
    pending_path.unlink()
    return {"ok": True, **summary}


def _parse_reviews(
    reviews: Any, targets: set[str], customizations: dict[str, Any]
) -> tuple[dict[str, list[ValidationFinding]], list[str]]:
    entries = reviews.get("reviews") if isinstance(reviews, dict) else None
    if not isinstance(entries, list):
        return {}, ["reviews must be an object with a 'reviews' list."]
    violations: list[str] = []
    findings_by_object: dict[str, list[ValidationFinding]] = {}
    for index, entry in enumerate(entries):
        object_id = entry.get("object_id") if isinstance(entry, dict) else None
        if object_id not in targets:
            violations.append(f"reviews[{index}]: object_id {object_id!r} is not a target of this iteration.")
            continue
        if object_id in findings_by_object:
            violations.append(f"reviews[{index}]: {object_id} is reviewed more than once.")
            continue
        raw_findings = entry.get("findings", [])
        if not isinstance(raw_findings, list):
            violations.append(f"reviews[{index}]: findings must be a list.")
            continue
        parsed = [
            _parse_finding(raw, f"{object_id} finding {i}", customizations, violations)
            for i, raw in enumerate(raw_findings)
        ]
        findings_by_object[object_id] = [finding for finding in parsed if finding is not None]
    violations.extend(f"{object_id} has no review." for object_id in sorted(targets - set(findings_by_object)))
    return findings_by_object, violations


def _parse_finding(
    raw: Any, label: str, customizations: dict[str, Any], violations: list[str]
) -> ValidationFinding | None:
    if not isinstance(raw, dict) or set(raw) - _FINDING_KEYS:
        violations.append(f"{label}: must be an object with only {sorted(_FINDING_KEYS)}.")
        return None
    problems = [
        f"{key} must be a non-empty string"
        for key in ("code", "message", "suggested_fix")
        if not isinstance(raw.get(key), str) or not raw[key]
    ]
    problems += [
        f"{key} must be a string"
        for key in ("location", "customization", "customization_value")
        if not isinstance(raw.get(key, ""), str)
    ]
    if raw.get("severity") not in SEVERITIES:
        problems.append(f"severity must be one of {list(SEVERITIES)}")
    problems += _waiver_problems(raw, customizations)
    if problems:
        violations.extend(f"{label}: {problem}." for problem in problems)
        return None
    return finding_from_dict(raw)


def _waiver_problems(raw: dict[str, Any], customizations: dict[str, Any]) -> list[str]:
    option = raw.get("customization", "")
    value = raw.get("customization_value", "")
    if not option:
        return ["customization_value needs a customization option"] if value else []
    if raw.get("severity") == "blocker":
        return ["waiver rejected: a blocker cannot be excused by a customization"]
    if option not in customizations:
        return [f"waiver rejected: {option!r} is not an option in metadata/configuration.json"]
    if value != str(customizations[option]):
        return [
            f"waiver rejected: customization_value {value!r} does not match the chosen "
            f"{option}={customizations[option]!r} in metadata/configuration.json"
        ]
    return []


def _stamp_objects(
    objects: list[BundleObject],
    findings_by_object: dict[str, list[ValidationFinding]],
    prior: dict[str, ValidationStamp],
    reviewer: str,
    iteration: int,
) -> list[ValidationStamp]:
    reviewed_at = _now()

    def stamp(item: BundleObject, score: int, verdict: str, findings: list[ValidationFinding]) -> ValidationStamp:
        return ValidationStamp(
            object_id=item.object_id,
            object_kind=item.object_kind,
            verdict=verdict,
            score=score,
            findings=findings,
            reviewer=reviewer,
            ruleset_version=RULESET_VERSION,
            reviewed_at=reviewed_at,
            iteration=iteration,
            content_hash=item.content_hash,
        )

    task_stamps: dict[str, ValidationStamp] = {}
    for item in objects:
        if item.object_kind != "task":
            continue
        if item.object_id not in findings_by_object:
            task_stamps[item.object_id] = prior[item.object_id]
            continue
        findings = findings_by_object[item.object_id]
        score = score_findings(findings)
        task_stamps[item.object_id] = stamp(item, score, verdict_for(findings, score), findings)
    stamps: list[ValidationStamp] = []
    for item in objects:
        if item.object_kind == "task":
            stamps.append(task_stamps[item.object_id])
        elif item.object_id not in findings_by_object:
            stamps.append(prior[item.object_id])
        else:
            findings = findings_by_object[item.object_id]
            score, verdict = roll_up_job(
                findings, [value for key, value in task_stamps.items() if key.startswith(f"{item.job}/")]
            )
            stamps.append(stamp(item, score, verdict, findings))
    return stamps


def _finish_iteration(
    output_dir: Path,
    report: ValidationReport,
    iteration: int,
    content_hash: str,
    stamps: list[ValidationStamp],
    objects: list[BundleObject],
    reviewer: str,
    static: dict[str, list[ValidationFinding]],
    checks: list[dict[str, str]],
) -> dict[str, Any]:
    previous = load_iterations(output_dir)
    snapshots = {item.object_id: item.definition for item in objects}
    earlier = next((record.snapshots for record in reversed(previous) if record.snapshots), {})
    diffs = {
        object_id: object_diff(object_id, earlier.get(object_id), snapshots.get(object_id))
        for object_id in sorted(set(earlier) | set(snapshots))
        if earlier and earlier.get(object_id) != snapshots.get(object_id)
    }
    rate = approval_rate(stamps)
    terminated = _stop_reason(report, iteration, rate, content_hash, previous)
    record = ValidationIteration(
        run_id=report.run_id,
        iteration=iteration,
        approval_rate=rate,
        content_hash=content_hash,
        terminated=terminated,
        stamps=stamps,
        static=[finding for findings in static.values() for finding in findings],
        checks=checks,
        diffs=diffs,
        snapshots=snapshots,
    )
    _write_json(iteration_path(output_dir, report.run_id, iteration), to_dict(record))
    report.reviewer = reviewer
    report.terminated = terminated
    report.iterations = iteration
    report.approval_rate = rate
    report.content_hash = content_hash
    report.stamps = stamps
    report.static_checks = checks
    report.trajectory = [
        *report.trajectory,
        {"iteration": iteration, "approval_rate": rate, "content_hash": content_hash, "terminated": terminated},
    ]
    _write_json(report_path(output_dir), to_dict(report))
    return {
        "run_id": report.run_id,
        "iteration": iteration,
        "approval_rate": rate,
        "threshold": report.threshold,
        "terminated": terminated,
        "static": [asdict(finding) for finding in record.static],
        "checks": checks,
        "actionable": actionable_findings(stamps),
        "changed_objects": sorted(diffs),
        "report_path": str(report_path(output_dir)),
    }


def _stop_reason(
    report: ValidationReport, iteration: int, rate: float, content_hash: str, previous: list[ValidationIteration]
) -> str:
    if rate >= report.threshold:
        return "threshold_met"
    if report.dry_run:
        return "dry_run"
    if any(record.content_hash == content_hash for record in previous):
        return "oscillation"
    if iteration >= report.max_iterations:
        return "budget_exhausted"
    return ""


def actionable_findings(stamps: list[ValidationStamp]) -> list[dict[str, Any]]:
    """Returns the findings worth fixing: everything except advisories and user customizations."""
    return [
        {"object_id": stamp.object_id, **asdict(finding)}
        for stamp in stamps
        for finding in stamp.findings
        if finding.severity != "advisory" and not finding.customization
    ]


def apply_remediation(output_dir: Path, edits: list[dict[str, Any]], *, approve: bool = False) -> dict[str, Any]:
    """Writes the agent's fixes to the bundle's YAML files, unless the run is a dry run or awaits approval.

    Args:
        output_dir: The migration output folder.
        edits: ``[{"path": "<path relative to output_dir>", "content": "<full new file text>"}]``. Only
            ``.yml`` / ``.yaml`` files inside a bundle may change.
        approve: The user approved these edits; required when the run has ``require_approval``.

    Returns:
        ``applied`` says whether the files were written; ``diffs`` holds the change per file. ``status``
        is ``applied``, ``dry_run``, or ``needs_approval``.
    """
    output_dir = Path(output_dir).resolve()
    report = load_report(output_dir)
    if report is None or report.iterations == 0:
        return {"ok": False, "error": "Nothing to remediate; record an iteration first."}
    if _pending_path(output_dir).is_file():
        return {"ok": False, "error": "A review is pending; record it before remediating."}
    bundle_dirs = [path.resolve() for path in find_bundles(output_dir, report.pipeline)]
    resolved, errors = _resolve_edits(output_dir, bundle_dirs, edits)
    if errors:
        return {"ok": False, "error": "; ".join(errors)}
    diffs = {
        relative: text_diff(relative, path.read_text(encoding="utf-8") if path.is_file() else "", content)
        for relative, path, content in resolved
    }
    if report.dry_run:
        return {"ok": True, "status": "dry_run", "applied": False, "diffs": diffs}
    if report.terminated:
        return {
            "ok": False,
            "error": f"The run has ended ({report.terminated}); run validate prepare with reset to start again.",
        }
    if report.require_approval and not approve:
        return {"ok": True, "status": "needs_approval", "applied": False, "diffs": diffs}
    for _relative, path, content in resolved:
        path.write_text(content, encoding="utf-8")
    latest = iteration_path(output_dir, report.run_id, report.iterations)
    record = iteration_from_dict(_read_json(latest))
    record.remediation = sorted({*record.remediation, *diffs})
    _write_json(latest, to_dict(record))
    return {"ok": True, "status": "applied", "applied": True, "diffs": diffs}


def _resolve_edits(
    output_dir: Path, bundle_dirs: list[Path], edits: Any
) -> tuple[list[tuple[str, Path, str]], list[str]]:
    if not isinstance(edits, list) or not edits:
        return [], ["edits must be a non-empty list of {path, content}."]
    resolved: list[tuple[str, Path, str]] = []
    errors: list[str] = []
    for index, edit in enumerate(edits):
        relative = edit.get("path") if isinstance(edit, dict) else None
        content = edit.get("content") if isinstance(edit, dict) else None
        if not isinstance(relative, str) or not isinstance(content, str):
            errors.append(f"edits[{index}] needs a string path and content")
            continue
        path = (output_dir / relative).resolve()
        bundle = next((bundle for bundle in bundle_dirs if path.is_relative_to(bundle)), None)
        if bundle is None or not is_editable(path.relative_to(bundle)):
            errors.append(f"edits[{index}]: {relative} is not databricks.yml or resource YAML inside a bundle")
            continue
        resolved.append((path.relative_to(output_dir).as_posix(), path, content))
    return resolved, errors
