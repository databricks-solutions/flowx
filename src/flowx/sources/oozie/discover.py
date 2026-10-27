"""Oozie discover phase: classify every app's actions and decisions into the shared inventory.

Writes ``metadata/inventory.json`` and ``metadata/profile_report.csv`` in the same shape as ADF and Airflow,
adds an ``oozie`` section per pipeline (HDFS paths, missing files, dropped Spark settings, ...), and keeps a
verbatim copy of each ``workflow.xml`` under ``metadata/<app>/``. Exposes ``main(argv)`` for the adapter.
"""

from __future__ import annotations

import csv
import json
import logging
import shutil
from pathlib import Path
from typing import Any

from flowx.adapter.predicates import walk_activities
from flowx.models.ir import Pipeline
from flowx.sources.adf.loader import clear_stale_outputs
from flowx.sources.oozie.cli import phase_arguments
from flowx.sources.oozie.loader import load_pipelines

logger = logging.getLogger(__name__)


def _classify(pipeline: Pipeline) -> list[dict[str, Any]]:
    """One inventory item per audited Oozie node (actions and decisions), in audit order."""
    audited_keys = list(pipeline.audit.get("audited_task_keys", []))
    agentic = set(pipeline.audit.get("agentic_task_keys", []))
    oozie = pipeline.audit.get("oozie", {})
    node_types = oozie.get("node_types", {})
    reasons = oozie.get("agentic_reasons", {})
    names = {task.task_key: task.name for task in walk_activities(pipeline.tasks)}
    items: list[dict[str, Any]] = []
    for task_key in audited_keys:
        item: dict[str, Any] = {
            "name": names.get(task_key, task_key),
            "task_key": task_key,
            "strategy": "agentic" if task_key in agentic else "deterministic",
            "oozie_type": node_types.get(task_key, "decision"),
        }
        if task_key in agentic:
            item["reason"] = reasons.get(task_key) or "Needs agentic translation."
        items.append(item)
    return items


def _percent(part: int, whole: int) -> float:
    return round(100.0 * part / whole, 1) if whole else 0.0


def build_oozie_inventory(pipelines: list[Pipeline], source_dir: str) -> dict[str, Any]:
    """Builds inventory.json in the same shape the Airflow and ADF discover phases write."""
    entries: list[dict[str, Any]] = []
    totals = {"audited": 0, "deterministic": 0, "agentic": 0, "failed": 0, "excluded": 0}
    for pipeline in pipelines:
        audit = pipeline.audit
        counts = {
            "audited": int(audit.get("audited_activity_count", 0)),
            "deterministic": int(audit.get("deterministic_count", 0)),
            "agentic": int(audit.get("agentic_count", 0)),
            "failed": int(audit.get("failed_count", 0)),
            "excluded": int(audit.get("excluded_count", 0)),
        }
        for name, value in counts.items():
            totals[name] += value
        entries.append(
            {
                "name": pipeline.name,
                "activities": _classify(pipeline),
                "audited_activity_count": counts["audited"],
                "deterministic_count": counts["deterministic"],
                "agentic_count": counts["agentic"],
                "failed_count": counts["failed"],
                "excluded_count": counts["excluded"],
                "reconciliation_status": pipeline.reconciliation_status or "verified",
                "migration_status": pipeline.migration_status,
                "coverage_pct": _percent(counts["deterministic"] + counts["agentic"], counts["audited"]),
                "deterministic_coverage_pct": _percent(counts["deterministic"], counts["audited"]),
                "findings": pipeline.not_translatable,
                "transformations": audit.get("transformations", []),
                "oozie": audit.get("oozie", {}),
            }
        )
    statuses = {pipeline.reconciliation_status for pipeline in pipelines}
    reconciliation = (
        "failed" if "failed" in statuses else "verified_with_gaps" if "verified_with_gaps" in statuses else "verified"
    )
    return {
        "source": "oozie",
        "source_dir": source_dir,
        "pipelines": entries,
        "summary": {
            "pipeline_count": len(pipelines),
            "activity_count": totals["audited"],
            "audited_activity_count": totals["audited"],
            "deterministic_count": totals["deterministic"],
            "agentic_count": totals["agentic"],
            "unsupported_count": 0,
            "failed_count": totals["failed"],
            "excluded_count": totals["excluded"],
            "coverage_pct": _percent(totals["deterministic"] + totals["agentic"], totals["audited"]),
            "deterministic_coverage_pct": _percent(totals["deterministic"], totals["audited"]),
            "reconciliation_status": reconciliation,
        },
    }


# Same columns the shared reporting/coverage and dashboard read; Oozie has no datasets, linked services, or motifs.
_PROFILE_COLUMNS = (
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


def write_profile_csv(pipelines: list[Pipeline], path: Path) -> None:
    """Writes one complexity row per app: native Spark tasks score 1, decisions 2, agentic items 3."""
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(_PROFILE_COLUMNS))
        writer.writeheader()
        for pipeline in pipelines:
            items = _classify(pipeline)
            agentic = [item for item in items if item["strategy"] == "agentic"]
            control = sum(1 for item in items if item["strategy"] != "agentic" and item["oozie_type"] == "decision")
            native = len(items) - control - len(agentic)
            score = native + control * 2 + len(agentic) * 3
            size = "S" if score <= 5 else "M" if score <= 15 else "L" if score <= 30 else "XL"
            writer.writerow(
                {
                    "pipeline": pipeline.name,
                    "activities": len(items),
                    "datasets": 0,
                    "linked_services": 0,
                    "collapsible_patterns": 0,
                    "databricks_native_activities": native,
                    "control_flow_activities": control,
                    "other_activities": len(agentic),
                    "complexity_score": score,
                    "complexity_size": size,
                }
            )


def main(argv: list[str] | None = None) -> int:
    """Discover-phase entry point for the Oozie source."""
    arguments = phase_arguments(argv, "Parse Oozie applications into a flowx inventory.")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    pipelines = load_pipelines(
        arguments.source_dir,
        pipeline=arguments.pipeline,
        exclude=frozenset(arguments.exclude_pipeline),
        output_dir=arguments.output_dir,
    )
    if not pipelines:
        logger.error("No Oozie applications (workflow.xml) found under %s.", arguments.source_dir)
        return 1
    output_dir: Path = arguments.output_dir.resolve()
    clear_stale_outputs(output_dir)
    metadata_dir = output_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    inventory = build_oozie_inventory(pipelines, str(arguments.source_dir))
    (metadata_dir / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    write_profile_csv(pipelines, metadata_dir / "profile_report.csv")
    for pipeline in pipelines:
        source_file = Path(pipeline.audit.get("source_file", ""))
        if source_file.is_file():
            (metadata_dir / pipeline.name).mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_file, metadata_dir / pipeline.name / "workflow.xml")
    summary = inventory["summary"]
    print("\nOozie Discover Summary")
    print("======================")
    print(f"Apps parsed:        {summary['pipeline_count']}")
    print(f"Audited nodes:      {summary['activity_count']}")
    print(f"  Deterministic:    {summary['deterministic_count']}")
    print(f"  Agentic:          {summary['agentic_count']}")
    print(f"  Failed apps:      {summary['failed_count']}")
    print(f"Deterministic:      {summary['deterministic_coverage_pct']}%")
    return 1 if all(pipeline.reconciliation_status == "failed" for pipeline in pipelines) else 0


if __name__ == "__main__":
    raise SystemExit(main())
