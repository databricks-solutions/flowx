"""Source-neutral discover-phase inventory and profile reporting.

Builds the ``metadata/inventory.json`` payload and ``metadata/profile_report.csv``
every source's discover phase emits, keyed off the shared Pipeline IR. The shape
matches the one the ADF discover phase established, so the shared
``reporting.coverage`` / dashboard consume any source's output unchanged.

The Airflow discover phase predates this module and still carries an equivalent
inline copy; it should migrate to these helpers.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from flowx.adapter.predicates import walk_activities
from flowx.models.ir import Pipeline, PlaceholderActivity

_PROFILE_COLUMNS: tuple[str, ...] = (
    "pipeline",
    "activities",
    "datasets",
    "linked_services",
    "collapsible_patterns",
    "databricks_native_activities",
    "control_flow_activities",
    "other_activities",
    "complexity_score",
    "complexity_size",
)

_NATIVE_TYPES = frozenset(
    {"NotebookActivity", "SparkPythonActivity", "SparkJarActivity", "SqlActivity", "RunJobActivity"}
)
_CONTROL_FLOW_TYPES = frozenset({"ForEachActivity", "IfConditionActivity", "SwitchActivity"})


def classify_tasks(pipeline: Pipeline) -> list[dict[str, str]]:
    """Classifies each task (descending into control-flow bodies) for the inventory.

    Args:
        pipeline: Translated pipeline IR.

    Returns:
        One ``{"name", "task_key", "strategy"}`` entry per task, where
        ``strategy`` is ``"agentic"`` for placeholders and ``"deterministic"``
        otherwise.
    """
    items: list[dict[str, str]] = []
    for task in walk_activities(pipeline.tasks):
        strategy = "agentic" if isinstance(task, PlaceholderActivity) else "deterministic"
        items.append({"name": task.name, "task_key": task.task_key, "strategy": strategy})
    return items


def build_inventory_dict(pipelines: list[Pipeline], source_dir: str, *, source: str) -> dict[str, Any]:
    """Builds the ``inventory.json`` payload for *pipelines* under a named *source*."""
    pipeline_entries: list[dict[str, Any]] = []
    audited = deterministic = agentic = failed = excluded = 0
    for pipeline in pipelines:
        items = classify_tasks(pipeline)
        pipeline_audited = int(pipeline.audit.get("audited_activity_count", len(items)))
        pipeline_deterministic = int(
            pipeline.audit.get("deterministic_count", sum(1 for item in items if item["strategy"] == "deterministic"))
        )
        pipeline_agentic = int(
            pipeline.audit.get("agentic_count", sum(1 for item in items if item["strategy"] == "agentic"))
        )
        pipeline_failed = int(pipeline.audit.get("failed_count", 0))
        pipeline_excluded = int(pipeline.audit.get("excluded_count", 0))
        coverage = (
            round(100.0 * (pipeline_deterministic + pipeline_agentic) / pipeline_audited, 1)
            if pipeline_audited
            else 0.0
        )
        deterministic_coverage = (
            round(100.0 * pipeline_deterministic / pipeline_audited, 1) if pipeline_audited else 0.0
        )
        audited += pipeline_audited
        deterministic += pipeline_deterministic
        agentic += pipeline_agentic
        failed += pipeline_failed
        excluded += pipeline_excluded
        pipeline_entries.append(
            {
                "name": pipeline.name,
                "activities": items,
                "audited_activity_count": pipeline_audited,
                "deterministic_count": pipeline_deterministic,
                "agentic_count": pipeline_agentic,
                "failed_count": pipeline_failed,
                "excluded_count": pipeline_excluded,
                "reconciliation_status": pipeline.reconciliation_status or "verified",
                "migration_status": pipeline.migration_status,
                "coverage_pct": coverage,
                "deterministic_coverage_pct": deterministic_coverage,
                "findings": pipeline.not_translatable,
                "transformations": pipeline.audit.get("transformations", []),
            }
        )
    total_coverage = round(100.0 * (deterministic + agentic) / audited, 1) if audited else 0.0
    total_deterministic = round(100.0 * deterministic / audited, 1) if audited else 0.0
    return {
        "source": source,
        "source_dir": source_dir,
        "pipelines": pipeline_entries,
        "summary": {
            "pipeline_count": len(pipelines),
            "activity_count": audited,
            "audited_activity_count": audited,
            "deterministic_count": deterministic,
            "agentic_count": agentic,
            "unsupported_count": 0,
            "failed_count": failed,
            "excluded_count": excluded,
            "coverage_pct": total_coverage,
            "deterministic_coverage_pct": total_deterministic,
            "reconciliation_status": "verified",
        },
    }


def write_profile_csv(pipelines: list[Pipeline], path: Path) -> None:
    """Writes the per-pipeline complexity report with the shared column set."""
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(_PROFILE_COLUMNS))
        writer.writeheader()
        for pipeline in pipelines:
            writer.writerow(_profile_row(pipeline))


def _profile_row(pipeline: Pipeline) -> dict[str, Any]:
    """Computes one profile row for *pipeline* over the shared column set."""
    type_names = [type(task).__name__ for task in pipeline.tasks if not task.task_key.startswith("__flowx_")]
    total = len(type_names)
    native = sum(1 for name in type_names if name in _NATIVE_TYPES)
    control = sum(1 for name in type_names if name in _CONTROL_FLOW_TYPES)
    other = total - native - control
    score = native * 1 + control * 2 + other * 3
    size = "S" if score <= 5 else "M" if score <= 15 else "L" if score <= 30 else "XL"
    return {
        "pipeline": pipeline.name,
        "activities": total,
        "datasets": 0,
        "linked_services": 0,
        "collapsible_patterns": 0,
        "databricks_native_activities": native,
        "control_flow_activities": control,
        "other_activities": other,
        "complexity_score": score,
        "complexity_size": size,
    }
