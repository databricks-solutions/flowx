"""Command-line arguments shared by the Oozie discover and convert phases."""

from __future__ import annotations

import argparse
from pathlib import Path


def phase_arguments(argv: list[str] | None, description: str) -> argparse.Namespace:
    """Parses the options both Oozie phases take: source dir, output dir, and app filters."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--source-dir", required=True, type=Path, help="Directory of Oozie application directories.")
    parser.add_argument("--output-dir", type=Path, default=Path("./flowx_output"), help="Shared migration output dir.")
    parser.add_argument("--pipeline", type=str, default=None, help="Only this app (its directory-derived name).")
    # --exclude-dag is the spelling the MCP server forwards for every source.
    parser.add_argument(
        "--exclude-pipeline",
        "--exclude-dag",
        dest="exclude_pipeline",
        action="append",
        default=[],
        help="Exclude an app; repeatable.",
    )
    return parser.parse_args(argv)
