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

from flowx.ir_serde import pipeline_to_dict
from flowx.sources.airflow.loader import load_pipelines

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    """Convert-phase entry point for the Airflow source."""
    parser = argparse.ArgumentParser(description="Translate Airflow DAGs into flowx Pipeline IR.")
    parser.add_argument("--source-dir", required=True, type=Path, help="A DAG .py file or directory of DAGs.")
    parser.add_argument("--output-dir", type=Path, default=Path("./flowx_output"), help="Shared migration output dir.")
    parser.add_argument("--pipeline", type=str, default=None, help="Translate only the named DAG (default: all).")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    pipelines = load_pipelines(args.source_dir, pipeline=args.pipeline)
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

    total_tasks = sum(len(p.tasks) for p in pipelines)
    print("\nAirflow Translation Summary")
    print("===========================")
    print(f"DAGs translated:    {len(pipelines)}")
    print(f"Total tasks:        {total_tasks}")
    print(f"\nTranslation report (intermediate): {report_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
