"""Build per-pipeline migration-coverage rows from the discover-phase metadata.

Joins the two artifacts the discover phase writes into ``<output_dir>/metadata/``:

* ``profile_report.csv`` -- per-pipeline complexity (activity/dataset/linked-service
  counts, collapsible patterns, activity-category counts, complexity score + size).
* ``inventory.json`` -- per-activity translation strategy, from which the
  deterministic / agentic / unsupported counts and coverage % are derived.

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
    "datasets",
    "linked_services",
    "collapsible_patterns",
    "databricks_native_activities",
    "control_flow_activities",
    "other_activities",
    "deterministic_activities",
    "agentic_activities",
    "unsupported_activities",
    "coverage_pct",
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
        deterministic = strategies.count("deterministic")
        agentic = strategies.count("agentic")
        unsupported = strategies.count("unsupported")
        total = len(strategies)
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
                "datasets": _csv_int("datasets"),
                "linked_services": _csv_int("linked_services"),
                "collapsible_patterns": _csv_int("collapsible_patterns"),
                "databricks_native_activities": _csv_int("databricks_native_activities"),
                "control_flow_activities": _csv_int("control_flow_activities"),
                "other_activities": _csv_int("other_activities"),
                "deterministic_activities": deterministic,
                "agentic_activities": agentic,
                "unsupported_activities": unsupported,
                "coverage_pct": _coverage_pct(deterministic, agentic, total),
                "complexity_score": _csv_int("complexity_score"),
                "complexity_size": csv_row.get("complexity_size", "") or "",
            }
        )
    rows.sort(key=lambda row: row["pipeline"])
    return rows
