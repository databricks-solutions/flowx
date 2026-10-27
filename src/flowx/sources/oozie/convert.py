"""Oozie convert phase: write the shared translation report the package phase consumes.

Failed apps are kept out of the report (the package gate fails closed on them) and listed in
``.work/failed_apps.json`` instead, so one broken app doesn't block packaging the rest.
"""

from __future__ import annotations

import json
import logging

from flowx.adapter.predicates import walk_activities
from flowx.ir_serde import pipeline_to_dict
from flowx.models.ir import PlaceholderActivity
from flowx.sources.oozie.cli import phase_arguments
from flowx.sources.oozie.loader import load_pipelines

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    """Convert-phase entry point for the Oozie source."""
    arguments = phase_arguments(argv, "Translate Oozie applications into flowx Pipeline IR.")
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
    translated = [pipeline for pipeline in pipelines if pipeline.reconciliation_status != "failed"]
    failed = [pipeline for pipeline in pipelines if pipeline.reconciliation_status == "failed"]
    work_dir = arguments.output_dir.resolve() / ".work"
    work_dir.mkdir(parents=True, exist_ok=True)
    (work_dir / "failed_apps.json").write_text(
        json.dumps([{"name": pipeline.name, "findings": pipeline.not_translatable} for pipeline in failed], indent=2),
        encoding="utf-8",
    )
    if not translated:
        logger.error("No Oozie application under %s could be translated.", arguments.source_dir)
        return 1
    dictionaries = [pipeline_to_dict(pipeline) for pipeline in translated]
    payload = dictionaries[0] if len(dictionaries) == 1 else {"pipelines": dictionaries}
    report_file = work_dir / "translation_report.json"
    report_file.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    gaps = [
        {
            "pipeline": pipeline.name,
            "activity_name": task.name,
            "activity_type": task.original_type,
            "raw_definition": task.raw_definition,
        }
        for pipeline in translated
        for task in walk_activities(pipeline.tasks)
        if isinstance(task, PlaceholderActivity)
    ]
    (work_dir / "gaps.json").write_text(json.dumps(gaps, indent=2, default=str), encoding="utf-8")
    print("\nOozie Translation Summary")
    print("=========================")
    print(f"Apps translated:    {len(translated)}")
    print(f"Apps failed:        {len(failed)}")
    print(f"Agentic gaps:       {len(gaps)}")
    print(f"\nTranslation report (intermediate): {report_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
