"""Load Glue Workflows into flowx Pipeline IR.

Both phase modules call :func:`load_pipelines`: ``discover`` classifies the
resulting IR into an inventory, and ``convert`` serialises it to the shared
translation report. This mirrors the Step Functions and Airflow sources, where
parsing and translation happen together in the loader.
"""

from __future__ import annotations

import logging
from pathlib import Path

from flowx.models.ir import Pipeline
from flowx.sources.glue.translate import translate_workflow
from flowx.sources.glue.workflow import load_workflow_files, parse_workflow

logger = logging.getLogger(__name__)


def load_pipelines(source_dir: Path, *, pipeline: str | None = None) -> list[Pipeline]:
    """Parses and translates every Glue Workflow under *source_dir*.

    Args:
        source_dir: A single workflow ``.json`` file or a directory of exports
            (``aws glue get-workflow --include-graph`` payloads).
        pipeline: When set, keep only the workflow whose name matches.

    Returns:
        One :class:`Pipeline` per translated workflow, in sorted file order.
    """
    pipelines: list[Pipeline] = []
    for stem, raw in load_workflow_files(Path(source_dir)):
        try:
            workflow = parse_workflow(raw, default_name=stem)
        except ValueError as error:
            logger.warning("Skipping %s: %s", stem, error)
            continue
        if pipeline is not None and workflow.name != pipeline:
            continue
        pipelines.append(translate_workflow(workflow))
    return pipelines
