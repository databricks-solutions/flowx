"""Step Functions convert phase: translate state machines into the shared report.

Writes ``.work/translation_report.json`` (a single pipeline dict, or a
``{"pipelines": [...]}`` wrapper for many) in the shape the ADF and Airflow
convert phases emit, so the shared package phase consumes it unchanged. Reuses
the source-neutral ``flowx.ir_serde.pipeline_to_dict``. Exposes ``main(argv)``
for the adapter to run in-process.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from flowx.adapter.predicates import walk_activities
from flowx.ir_serde import pipeline_to_dict
from flowx.models.ir import Pipeline, PlaceholderActivity
from flowx.sources.stepfunctions.loader import load_pipelines

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    """Convert-phase entry point for the Step Functions source."""
    parser = argparse.ArgumentParser(description="Translate AWS Step Functions state machines into flowx Pipeline IR.")
    parser.add_argument(
        "--source-dir",
        required=True,
        type=Path,
        help="A state machine .json file or a directory of exported definitions.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("./flowx_output"), help="Shared migration output dir.")
    parser.add_argument("--pipeline", type=str, default=None, help="Translate only the named state machine.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    pipelines = load_pipelines(args.source_dir, pipeline=args.pipeline)
    if not pipelines:
        logger.error("No state machines found under %s (or none matched --pipeline).", args.source_dir)
        return 1

    output_dir: Path = args.output_dir.resolve()
    work_dir = output_dir / ".work"
    work_dir.mkdir(parents=True, exist_ok=True)

    pipeline_dicts = [pipeline_to_dict(pipeline) for pipeline in pipelines]
    payload = pipeline_dicts[0] if len(pipeline_dicts) == 1 else {"pipelines": pipeline_dicts}
    report_file = work_dir / "translation_report.json"
    report_file.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    gaps = _collect_gaps(pipelines)
    if gaps:
        (work_dir / "gaps.json").write_text(json.dumps(gaps, indent=2, default=str), encoding="utf-8")

    total_tasks = sum(len(pipeline.tasks) for pipeline in pipelines)
    print("\nStep Functions Translation Summary")
    print("==================================")
    print(f"State machines:     {len(pipelines)}")
    print(f"Total tasks:        {total_tasks}")
    print(f"Agentic gaps:       {len(gaps)}")
    print(f"\nTranslation report (intermediate): {report_file}")
    return 0


def _collect_gaps(pipelines: list[Pipeline]) -> list[dict]:
    """Returns one gap-shaped dict per placeholder across all pipelines.

    Each carries the placeholder's ``activity_name``, ``activity_type`` (the
    state's original type), and ``raw_definition`` (the ASL state body).
    """
    gaps: list[dict] = []
    for pipeline in pipelines:
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
