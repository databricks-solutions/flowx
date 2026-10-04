"""Step Functions discover phase: parse state machines into a classified inventory.

Mirrors the ADF and Airflow discover contract: writes ``metadata/inventory.json``
and ``metadata/profile_report.csv`` under the shared output dir. Exposes
``main(argv)`` so the adapter runs it in-process.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from flowx.sources.adf.loader import clear_stale_outputs
from flowx.sources.inventory import build_inventory_dict, write_profile_csv
from flowx.sources.stepfunctions.loader import load_pipelines

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    """Discover-phase entry point for the Step Functions source."""
    parser = argparse.ArgumentParser(description="Parse AWS Step Functions state machines into a flowx inventory.")
    parser.add_argument(
        "--source-dir",
        required=True,
        type=Path,
        help="A state machine .json file or a directory of exported definitions.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("./flowx_output"), help="Shared migration output dir.")
    parser.add_argument("--pipeline", type=str, default=None, help="Filter to a single state machine by name.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    pipelines = load_pipelines(args.source_dir, pipeline=args.pipeline)
    if not pipelines:
        logger.error("No state machines found under %s (or none matched --pipeline).", args.source_dir)
        return 1
    logger.info("Parsed %d state machine(s) from %s", len(pipelines), args.source_dir)

    output_dir: Path = args.output_dir.resolve()
    clear_stale_outputs(output_dir)
    metadata_dir = output_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    inventory = build_inventory_dict(pipelines, str(args.source_dir), source="stepfunctions")
    (metadata_dir / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    write_profile_csv(pipelines, metadata_dir / "profile_report.csv")

    summary = inventory["summary"]
    print("\nStep Functions Discover Summary")
    print("===============================")
    print(f"State machines:     {summary['pipeline_count']}")
    print(f"Total states:       {summary['activity_count']}")
    print(f"  Deterministic:    {summary['deterministic_count']}")
    print(f"  Agentic:          {summary['agentic_count']}")
    print(f"Translation path:   {summary['coverage_pct']}%")
    print(f"Deterministic:      {summary['deterministic_coverage_pct']}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
