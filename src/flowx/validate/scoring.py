"""Turns review findings into scores, verdicts, job roll-ups, and the run's approval rate.

Notes:
    A task's score is ``100`` minus a penalty per finding (blocker 100, major 20, minor 5,
    advisory 0), floored at 0. Findings tied to a user customization carry no penalty. A task with a
    blocker, or a score below 60, is rejected. A job's score is the lower of its own job-level score
    and the mean of its task scores, and it is rejected when it has a blocker or any task is
    rejected. The approval rate is the mean over jobs of ``score / 100``, counting rejected jobs as 0.
"""

from __future__ import annotations

from statistics import mean

from flowx.models.validation import ValidationFinding, ValidationStamp

SEVERITY_PENALTIES: dict[str, int] = {"blocker": 100, "major": 20, "minor": 5, "advisory": 0}
REJECTION_SCORE = 60
ROLLUP_KINDS = frozenset({"job", "pipeline"})


def score_findings(findings: list[ValidationFinding]) -> int:
    """Returns the 0-100 score for an object with *findings*."""
    penalty = sum(SEVERITY_PENALTIES[finding.severity] for finding in findings if not finding.customization)
    return max(0, 100 - penalty)


def verdict_for(findings: list[ValidationFinding], score: int) -> str:
    """Returns the verdict for an object with *findings* and *score*."""
    if score < REJECTION_SCORE or any(finding.severity == "blocker" for finding in findings):
        return "rejected"
    return "approved_with_findings" if findings else "approved"


def roll_up_job(job_findings: list[ValidationFinding], task_stamps: list[ValidationStamp]) -> tuple[int, str]:
    """Returns the ``(score, verdict)`` of a job from its own findings and its task stamps."""
    own_score = score_findings(job_findings)
    score = min(own_score, round(mean(stamp.score for stamp in task_stamps))) if task_stamps else own_score
    verdicts = {verdict_for(job_findings, score), *(stamp.verdict for stamp in task_stamps)}
    if "rejected" in verdicts:
        return score, "rejected"
    return score, "approved_with_findings" if "approved_with_findings" in verdicts else "approved"


def approval_rate(stamps: list[ValidationStamp]) -> float:
    """Returns the weighted approval rate, 0-1, over the job-level stamps in *stamps*."""
    credits = [
        0.0 if stamp.verdict == "rejected" else stamp.score / 100
        for stamp in stamps
        if stamp.object_kind in ROLLUP_KINDS
    ]
    return round(mean(credits), 4) if credits else 0.0
