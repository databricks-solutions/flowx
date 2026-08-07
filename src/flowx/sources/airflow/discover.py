"""Airflow discover phase: parse DAGs into a classified inventory.

Mirrors the ADF discover contract: writes ``metadata/inventory.json`` and
``metadata/profile_report.csv`` under the shared output dir, classifying each
task as deterministic (a mapped operator -> NotebookActivity) or agentic (an
unmapped operator -> PlaceholderActivity, needing LLM-assisted translation).
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
    deterministic = agentic = 0
    for pipeline in pipelines:
        items = _classify(pipeline)
        deterministic += sum(1 for i in items if i["strategy"] == "deterministic")
        agentic += sum(1 for i in items if i["strategy"] == "agentic")
        pipeline_entries.append({"name": pipeline.name, "activities": items})
    activity_count = deterministic + agentic
    # Coverage counts both deterministic and agentic as "has a translation path", matching the
    # shared reporting.coverage formula (agentic gaps are translated in the convert phase).
    coverage = round(100.0 * (deterministic + agentic) / activity_count, 1) if activity_count else 0.0
    return {
        "source": "airflow",
        "source_dir": source_dir,
        "pipelines": pipeline_entries,
        "summary": {
            "pipeline_count": len(pipelines),
            "activity_count": activity_count,
            "deterministic_count": deterministic,
            "agentic_count": agentic,
            "unsupported_count": 0,
            "coverage_pct": coverage,
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
    type_names = [type(task).__name__ for task in pipeline.tasks]
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
    print(f"Coverage:           {summary['coverage_pct']}%")
    return 1 if any(pipeline.reconciliation_status == "failed" for pipeline in pipelines) else 0


if __name__ == "__main__":
    raise SystemExit(main())
