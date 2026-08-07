"""Airflow discover phase: parse DAGs into a classified inventory.

Mirrors the ADF discover contract: writes ``metadata/inventory.json`` and
``metadata/profile_report.csv`` under the shared output dir. Independently audited
task candidates drive deterministic, agentic, failed, and excluded counts; emitted
IR tasks remain available for the per-task inventory.
Exposes ``main(argv)`` so the adapter runs it in-process, like the ADF loader.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path
from typing import Any

from flowx.adapter.predicates import walk_activities
from flowx.models.ir import NotebookActivity, Pipeline, PlaceholderActivity
from flowx.sources.adf.loader import clear_stale_outputs
from flowx.sources.airflow.loader import load_pipelines

logger = logging.getLogger(__name__)


def _classify(pipeline: Pipeline) -> list[dict[str, str]]:
    """Classifies each task in *pipeline* for the inventory.

    Descends into for_each bodies so a mapped operator's nested placeholder is counted as agentic
    rather than being invisible to the inventory (which would report full coverage).
    """
    items: list[dict[str, str]] = []
    for task in walk_activities(pipeline.tasks):
        if isinstance(task, NotebookActivity):
            strategy = "deterministic"
        elif isinstance(task, PlaceholderActivity):
            strategy = "agentic"
        else:
            strategy = "deterministic"
        items.append({"name": task.name, "task_key": task.task_key, "strategy": strategy})
    return items


def build_inventory_dict(pipelines: list[Pipeline], source_dir: str) -> dict[str, Any]:
    """Builds the inventory.json payload matching the ADF discover shape."""
    pipeline_entries: list[dict[str, Any]] = []
    audited = deterministic = agentic = failed = excluded = 0
    for pipeline in pipelines:
        items = _classify(pipeline)
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
    coverage = round(100.0 * (deterministic + agentic) / audited, 1) if audited else 0.0
    deterministic_coverage = round(100.0 * deterministic / audited, 1) if audited else 0.0
    reconciliation_status = (
        "failed"
        if any(pipeline.reconciliation_status == "failed" for pipeline in pipelines)
        else "verified_with_gaps"
        if any(pipeline.reconciliation_status == "verified_with_gaps" for pipeline in pipelines)
        else "excluded"
        if pipelines and all(pipeline.migration_status == "excluded" for pipeline in pipelines)
        else "verified"
    )
    return {
        "source": "airflow",
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
            "coverage_pct": coverage,
            "deterministic_coverage_pct": deterministic_coverage,
            "reconciliation_status": reconciliation_status,
        },
    }


# Full profile column set the shared reporting.coverage / dashboard consume. Airflow has no
# dataset/linked-service/motif concept, so those are 0; the rest are computed from task types.
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
_CONTROL_FLOW_TYPES = frozenset({"ForEachActivity"})


def _profile_row(pipeline: Pipeline) -> dict[str, Any]:
    """Computes one profile row for *pipeline* over the full column set."""
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


def _write_profile_csv(pipelines: list[Pipeline], path: Path) -> None:
    """Writes the per-pipeline complexity report with the full shared column set."""
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(_PROFILE_COLUMNS))
        writer.writeheader()
        for pipeline in pipelines:
            writer.writerow(_profile_row(pipeline))


def main(argv: list[str] | None = None) -> int:
    """Discover-phase entry point for the Airflow source."""
    parser = argparse.ArgumentParser(description="Parse Airflow DAGs into a flowx inventory.")
    parser.add_argument("--source-dir", required=True, type=Path, help="A DAG .py file or directory of DAGs.")
    parser.add_argument("--output-dir", type=Path, default=Path("./flowx_output"), help="Shared migration output dir.")
    parser.add_argument("--pipeline", type=str, default=None, help="Filter to a single DAG by dag_id.")
    parser.add_argument(
        "--exclude-dag",
        action="append",
        default=[],
        help="Exclude a DAG from bundle emission while retaining it in audit and coverage reporting. Repeatable.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    pipelines = load_pipelines(args.source_dir, pipeline=args.pipeline, exclude_dags=set(args.exclude_dag))
    if not pipelines:
        logger.error("No Airflow DAGs found under %s (or none matched --pipeline).", args.source_dir)
        return 1
    logger.info("Parsed %d DAG(s) from %s", len(pipelines), args.source_dir)

    output_dir: Path = args.output_dir.resolve()
    clear_stale_outputs(output_dir)
    metadata_dir = output_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    inventory = build_inventory_dict(pipelines, str(args.source_dir))
    (metadata_dir / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    _write_profile_csv(pipelines, metadata_dir / "profile_report.csv")

    summary = inventory["summary"]
    print("\nAirflow Discover Summary")
    print("========================")
    print(f"DAGs parsed:        {summary['pipeline_count']}")
    print(f"Total tasks:        {summary['activity_count']}")
    print(f"  Deterministic:    {summary['deterministic_count']}")
    print(f"  Agentic:          {summary['agentic_count']}")
    print(f"  Failed:           {summary['failed_count']}")
    print(f"  Excluded:         {summary['excluded_count']}")
    print(f"Translation path:   {summary['coverage_pct']}%")
    print(f"Deterministic:      {summary['deterministic_coverage_pct']}%")
    print(f"Reconciliation:     {summary['reconciliation_status']}")
    return 1 if any(pipeline.reconciliation_status == "failed" for pipeline in pipelines) else 0


if __name__ == "__main__":
    raise SystemExit(main())
