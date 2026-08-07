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
    "unsupported_activities",
    "failed_activities",
    "excluded_activities",
    "reconciliation_status",
    "migration_status",
    "coverage_pct",
    "deterministic_coverage_pct",
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
                "unsupported_activities": unsupported,
                "failed_activities": failed,
                "excluded_activities": excluded,
                "reconciliation_status": pipeline.get("reconciliation_status", "not_applicable"),
                "migration_status": pipeline.get("migration_status", "included"),
                "coverage_pct": _coverage_pct(deterministic, agentic, total),
                "deterministic_coverage_pct": _deterministic_coverage_pct(deterministic, total),
                "finding_count": len(findings),
                "finding_fingerprints": json.dumps(fingerprints, separators=(",", ":")),
                "complexity_score": _csv_int("complexity_score"),
                "complexity_size": csv_row.get("complexity_size", "") or "",
            }
        )
    rows.sort(key=lambda row: row["pipeline"])
    return rows
