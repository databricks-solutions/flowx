"""Build per-pipeline migration-coverage rows from the discover-phase metadata.

Joins the two artifacts the discover phase writes into ``<output_dir>/metadata/``:

* ``profile_report.csv`` -- per-pipeline complexity (activity/dataset/linked-service
  counts, collapsible patterns, activity-category counts, complexity score + size).
* ``inventory.json`` -- per-activity translation strategy plus source-audit counts
  and reconciliation status when the source supports independent auditing.

The result is one metric row per pipeline (no run metadata -- ``run_id`` /
``run_date`` / ``run_by`` are stamped on at write time by :mod:`reporting.results`).
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from flowx.agentic import AgenticContractError, summarize_persisted_agentic_resolutions

# Metric columns (order matters: it drives the results-table column order).
COVERAGE_METRIC_COLUMNS: tuple[str, ...] = (
    "pipeline",
    "activities",
    "audited_activities",
    "datasets",
    "linked_services",
    "collapsible_patterns",
    "databricks_native_activities",
    "control_flow_activities",
    "other_activities",
    "deterministic_activities",
    "agentic_activities",
    "resolved_agentic_count",
    "unresolved_agentic_count",
    "agentic_resolution_outcomes",
    "agentic_provider_version",
    "unsupported_activities",
    "failed_activities",
    "excluded_activities",
    "reconciliation_status",
    "migration_status",
    "coverage_pct",
    "deterministic_coverage_pct",
    "code_attached_coverage_pct",
    "finding_count",
    "finding_fingerprints",
    "complexity_score",
    "complexity_size",
)

_CSV_INT_COLUMNS: tuple[str, ...] = (
    "datasets",
    "linked_services",
    "collapsible_patterns",
    "databricks_native_activities",
    "control_flow_activities",
    "other_activities",
    "complexity_score",
)


def _coverage_pct(deterministic: int, agentic: int, total: int) -> float:
    """Coverage % = (deterministic + agentic) / total activities, rounded to 1dp."""
    if total <= 0:
        return 0.0
    return round((deterministic + agentic) / total * 100, 1)


def _deterministic_coverage_pct(deterministic: int, total: int) -> float:
    """Deterministic coverage over audited activity candidates, rounded to 1dp."""
    if total <= 0:
        return 0.0
    return round(deterministic / total * 100, 1)


def _code_attached_coverage_pct(deterministic: int, resolved: int, total: int) -> float:
    """Mechanically code-attached coverage over audited activity candidates."""
    if total <= 0:
        return 0.0
    return round((deterministic + resolved) / total * 100, 1)


def build_coverage_rows(metadata_dir: Path) -> list[dict[str, Any]]:
    """Builds per-pipeline coverage rows from a migration ``metadata/`` directory.

    Args:
        metadata_dir: The bundle's ``metadata/`` folder containing ``inventory.json``
            and ``profile_report.csv``.

    Returns:
        One dict per pipeline keyed by :data:`COVERAGE_METRIC_COLUMNS`, ordered by
        pipeline name.  The inventory's pipeline set is authoritative; complexity
        columns are looked up from the CSV (defaulting to 0 / "" when absent).

    Raises:
        FileNotFoundError: When ``inventory.json`` is missing.
    """
    inventory_path = metadata_dir / "inventory.json"
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    is_airflow = inventory.get("source") == "airflow"
    agentic_summary = summarize_persisted_agentic_resolutions(metadata_dir / "agentic") if is_airflow else {}
    provider_version = str(agentic_summary.get("provider_version", ""))
    resolution_pipelines = agentic_summary.get("pipelines", {})
    if not isinstance(resolution_pipelines, dict):
        raise AgenticContractError("agentic resolution summary pipelines must be an object")
    inventory_names = {
        str(pipeline.get("name", "")) for pipeline in inventory.get("pipelines", []) if isinstance(pipeline, dict)
    }
    unknown_pipelines = sorted(set(resolution_pipelines) - inventory_names)
    if unknown_pipelines:
        raise AgenticContractError(
            "agentic resolution evidence references unknown inventory pipeline(s): " + ", ".join(unknown_pipelines)
        )

    csv_by_pipeline: dict[str, dict[str, str]] = {}
    csv_path = metadata_dir / "profile_report.csv"
    if csv_path.exists():
        with csv_path.open(encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                csv_by_pipeline[row["pipeline"]] = row

    rows: list[dict[str, Any]] = []
    for pipeline in inventory.get("pipelines", []):
        name = pipeline.get("name", "")
        strategies = [activity.get("strategy") for activity in pipeline.get("activities", [])]
        has_audit = "audited_activity_count" in pipeline
        deterministic = int(pipeline.get("deterministic_count", 0)) if has_audit else strategies.count("deterministic")
        agentic = int(pipeline.get("agentic_count", 0)) if has_audit else strategies.count("agentic")
        unsupported = strategies.count("unsupported")
        failed = int(pipeline.get("failed_count", 0)) if has_audit else 0
        excluded = int(pipeline.get("excluded_count", 0)) if has_audit else 0
        total = int(pipeline.get("audited_activity_count", 0)) if has_audit else len(strategies)
        if is_airflow:
            empty_outcomes = {"resolved": 0, "needs_input": 0, "deferred": 0, "declined": 0, "unreviewed": agentic}
            if agentic_summary:
                outcomes = resolution_pipelines.get(name, empty_outcomes)
                if not isinstance(outcomes, dict) or any(
                    not isinstance(outcomes.get(key), int)
                    for key in ("resolved", "needs_input", "deferred", "declined", "unreviewed")
                ):
                    raise AgenticContractError(f"invalid agentic resolution outcomes for pipeline {name!r}")
                if sum(outcomes.values()) != agentic:
                    raise AgenticContractError(
                        f"agentic resolution evidence accounts for {sum(outcomes.values())} of "
                        f"{agentic} agentic activities in pipeline {name!r}"
                    )
            else:
                outcomes = empty_outcomes
            resolved_agentic = outcomes["resolved"]
            unresolved_agentic = agentic - resolved_agentic
            code_attached_coverage = _code_attached_coverage_pct(deterministic, resolved_agentic, total)
        else:
            outcomes = {}
            resolved_agentic = agentic
            unresolved_agentic = 0
            code_attached_coverage = _coverage_pct(deterministic, agentic, total)
        findings = pipeline.get("findings", [])
        fingerprints = [
            finding["fingerprint"]
            for finding in findings
            if isinstance(finding, dict) and isinstance(finding.get("fingerprint"), str)
        ]
        csv_row = csv_by_pipeline.get(name, {})

        def _csv_int(col: str, _csv_row: dict[str, str] = csv_row) -> int:
            try:
                return int(_csv_row.get(col, 0) or 0)
            except (TypeError, ValueError):
                return 0

        rows.append(
            {
                "pipeline": name,
                "activities": total,
                "audited_activities": total,
                "datasets": _csv_int("datasets"),
                "linked_services": _csv_int("linked_services"),
                "collapsible_patterns": _csv_int("collapsible_patterns"),
                "databricks_native_activities": _csv_int("databricks_native_activities"),
                "control_flow_activities": _csv_int("control_flow_activities"),
                "other_activities": _csv_int("other_activities"),
                "deterministic_activities": deterministic,
                "agentic_activities": agentic,
                "resolved_agentic_count": resolved_agentic,
                "unresolved_agentic_count": unresolved_agentic,
                "agentic_resolution_outcomes": json.dumps(outcomes, sort_keys=True, separators=(",", ":")),
                "agentic_provider_version": provider_version if is_airflow else "",
                "unsupported_activities": unsupported,
                "failed_activities": failed,
                "excluded_activities": excluded,
                "reconciliation_status": (
                    "verified_with_reviewed_resolutions"
                    if is_airflow and outcomes.get("resolved", 0) > 0
                    else pipeline.get("reconciliation_status", "not_applicable")
                ),
                "migration_status": pipeline.get("migration_status", "included"),
                "coverage_pct": _coverage_pct(deterministic, agentic, total),
                "deterministic_coverage_pct": _deterministic_coverage_pct(deterministic, total),
                "code_attached_coverage_pct": code_attached_coverage,
                "finding_count": len(findings),
                "finding_fingerprints": json.dumps(fingerprints, separators=(",", ":")),
                "complexity_score": _csv_int("complexity_score"),
                "complexity_size": csv_row.get("complexity_size", "") or "",
            }
        )
    rows.sort(key=lambda row: row["pipeline"])
    return rows
