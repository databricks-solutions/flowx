"""Airflow convert phase: parse DAGs into the shared translation report.

Writes ``.work/translation_report.json`` (single pipeline dict, or a
``{"pipelines": [...]}`` wrapper for many) in the exact shape the ADF convert
phase emits, so the shared package phase consumes it unchanged.  Reuses the
source-neutral ``flowx.ir_serde.pipeline_to_dict`` so both sources converge on
one report format.  Exposes ``main(argv)`` for the adapter to run in-process.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from flowx.adapter.predicates import walk_activities
from flowx.ir_serde import pipeline_to_dict
from flowx.models.ir import PlaceholderActivity
from flowx.sources.airflow.loader import load_pipelines

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    """Convert-phase entry point for the Airflow source."""
    parser = argparse.ArgumentParser(description="Translate Airflow DAGs into flowx Pipeline IR.")
    parser.add_argument("--source-dir", required=False, type=Path, help="A DAG .py file or directory of DAGs.")
    parser.add_argument("--output-dir", type=Path, default=Path("./flowx_output"), help="Shared migration output dir.")
    parser.add_argument("--pipeline", type=str, default=None, help="Translate only the named DAG (default: all).")
    parser.add_argument(
        "--exclude-dag",
        action="append",
        default=[],
        help="Exclude a DAG from bundle emission while retaining it in audit and coverage reporting. Repeatable.",
    )
    parser.add_argument(
        "--dbt-mode",
        choices=("static", "pydabs"),
        default="static",
        help="dbt-factory render mode: 'static' (inner job of per-node tasks, default) or 'pydabs' "
        "(a deploy-time PyDABs hook that builds the dbt job from the live manifest).",
    )
    parser.add_argument(
        "--merge-agentic",
        action="store_true",
        help="Deprecated and disabled for Airflow; retained only to return a migration error.",
    )
    parser.add_argument("--report", type=Path, default=None, help="Translation report to merge agentic results into.")
    parser.add_argument(
        "--agentic-results",
        type=Path,
        default=None,
        help="Directory of per-activity agentic result JSON files.",
    )
    parser.add_argument("--output", type=Path, default=None, help="Merged report destination; defaults to --report.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    if args.merge_agentic:
        logger.error("Airflow agentic merge is disabled; use the fingerprint-bound resolve-agentic workflow.")
        return 2

    if not args.source_dir:
        parser.error("--source-dir is required")

    pipelines = load_pipelines(
        args.source_dir,
        pipeline=args.pipeline,
        dbt_mode=args.dbt_mode,
        exclude_dags=set(args.exclude_dag),
    )
    if not pipelines:
        logger.error("No Airflow DAGs found under %s (or none matched --pipeline).", args.source_dir)
        return 1

    output_dir: Path = args.output_dir.resolve()
    work_dir = output_dir / ".work"
    work_dir.mkdir(parents=True, exist_ok=True)

    pipeline_dicts = [pipeline_to_dict(pipeline) for pipeline in pipelines]
    payload = pipeline_dicts[0] if len(pipeline_dicts) == 1 else {"pipelines": pipeline_dicts}
    report_file = work_dir / "translation_report.json"
    report_file.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    # Preserve unmapped operator context for review and the future fingerprint-bound resolver.
    gaps = _collect_gaps(pipelines)
    if gaps:
        (work_dir / "gaps.json").write_text(json.dumps(gaps, indent=2, default=str), encoding="utf-8")

    total_tasks = sum(len(p.tasks) for p in pipelines)
    print("\nAirflow Translation Summary")
    print("===========================")
    print(f"DAGs translated:    {len(pipelines)}")
    print(f"Total tasks:        {total_tasks}")
    print(f"Agentic gaps:       {len(gaps)}")
    print(f"\nTranslation report (intermediate): {report_file}")
    return 1 if any(pipeline.reconciliation_status == "failed" for pipeline in pipelines) else 0


def _collect_gaps(pipelines: list) -> list[dict]:
    """Returns one AgenticGap-shaped dict per PlaceholderActivity across all pipelines.

    Each carries the placeholder's ``activity_name``, ``activity_type`` (the Airflow
    operator), and ``raw_definition`` (the operator's source).
    """
    gaps: list[dict] = []
    for pipeline in pipelines:
        # Descend into for_each bodies: a mapped operator's placeholder lives in inner_activities, and
        # a gap the agentic round never sees is guidance generated and dropped.
        for task in walk_activities(pipeline.tasks):
            if isinstance(task, PlaceholderActivity):
                gaps.append(
                    {
                        "activity_name": task.name,
                        "activity_type": task.original_type,
                        "raw_definition": task.raw_definition,
                    }
                )
    return gaps


if __name__ == "__main__":
    raise SystemExit(main())
