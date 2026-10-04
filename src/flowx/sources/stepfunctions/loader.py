"""Load Step Functions state machines into flowx Pipeline IR.

Both phase modules call :func:`load_pipelines`: ``discover`` classifies the
resulting IR into an inventory, and ``convert`` serialises it to the shared
translation report. This mirrors the Airflow source, where parsing and
translation happen together in the loader rather than in a separate convert
pass.
"""

from __future__ import annotations

import logging
from pathlib import Path

from flowx.models.ir import Pipeline
from flowx.sources.stepfunctions.asl import load_state_machine_files, parse_state_machine
from flowx.sources.stepfunctions.translate import translate_state_machine

logger = logging.getLogger(__name__)


def load_pipelines(source_dir: Path, *, pipeline: str | None = None) -> list[Pipeline]:
    """Parses and translates every state machine under *source_dir*.

    Args:
        source_dir: A single state machine ``.json`` file or a directory of them
            (bare ASL documents or ``describe-state-machine`` payloads).
        pipeline: When set, keep only the state machine whose name matches.

    Returns:
        One :class:`Pipeline` per translated state machine, in sorted file order.
    """
    pipelines: list[Pipeline] = []
    for stem, raw in load_state_machine_files(Path(source_dir)):
        try:
            machine = parse_state_machine(raw, default_name=stem)
        except ValueError as error:
            logger.warning("Skipping %s: %s", stem, error)
            continue
        if pipeline is not None and machine.name != pipeline:
            continue
        pipelines.append(translate_state_machine(machine))
    return pipelines
