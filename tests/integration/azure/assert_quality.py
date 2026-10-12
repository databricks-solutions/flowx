"""Fail CI when a live ADF export loses expected translation coverage."""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

from flowx.sources.adf.loader import build_inventory, load_adf_definitions

EXPECTED_COUNTS = {"deterministic": 36, "agentic": 15, "unsupported": 0}


def _classification_matrix(source_dir: Path) -> tuple[Counter[tuple[str, str]], dict[str, int]]:
    inventory = build_inventory(load_adf_definitions(source_dir))
    matrix = Counter((item.activity_type, item.strategy.value) for item in inventory.items)
    counts = Counter(item.strategy.value for item in inventory.items)
    return matrix, {strategy: counts[strategy] for strategy in EXPECTED_COUNTS}


def main() -> int:
    """Validate inventory counts and the generated bundle's minimum structure."""
    fixture_root = Path(sys.argv[1])
    live_export_root = Path(sys.argv[2])
    inventory_path = Path(sys.argv[3])
    bundle_root = Path(sys.argv[4])
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    summary = inventory["summary"]

    fixture_matrix, fixture_counts = _classification_matrix(fixture_root)
    live_matrix, live_counts = _classification_matrix(live_export_root)
    assert fixture_matrix == live_matrix, (
        f"classification mismatch: checked-in fixtures={fixture_matrix}; live export={live_matrix}"
    )
    assert fixture_counts == EXPECTED_COUNTS
    assert live_counts == EXPECTED_COUNTS
    assert summary["deterministic_count"] == 36, summary
    assert summary["agentic_count"] == 15, summary
    assert summary["unsupported_count"] == 0, summary
    bundle_files = list(bundle_root.glob("*/databricks.yml"))
    resource_files = list(bundle_root.glob("*/resources/*.yml"))
    source_files = [path for path in bundle_root.glob("*/src/**/*") if path.is_file()]
    assert len(bundle_files) == summary["pipeline_count"], "one bundle is required per pipeline"
    assert resource_files, "bundles contain no resource YAML"
    assert source_files, "bundles contain no generated source files"
    assert all(path.stat().st_size > 0 for path in bundle_files)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
