"""Consumer-safety tests for the ADF inventory emitted via the shared AST + emitter.

Two guarantees:

* **Superset** -- the new ``inventory.json`` keeps every key the historical shape
  had (per-activity ``name`` / ``type`` / ``strategy`` / ``depends_on`` and the
  ``summary`` count block) byte-compatibly, and only *adds* fields on top. The
  historical values are reconstructed here from :func:`build_inventory` (the
  authoritative classifier, untouched by this slice).
* **Golden coverage** -- feeding the new inventory through the real consumer
  (:func:`flowx.reporting.coverage.build_coverage_rows`) reproduces a committed
  snapshot, so downstream reporting is provably unchanged.
"""

from __future__ import annotations

import json
from pathlib import Path

from flowx.reporting.coverage import build_coverage_rows
from flowx.sources.adf.loader import build_inventory, load_adf_definitions, main

FIXTURES_DIR = Path(__file__).parent.parent / "resources" / "json"
GOLDEN_COVERAGE = Path(__file__).parent.parent / "resources" / "golden" / "adf_fixture_coverage.json"


def _run_discover(tmp_path: Path) -> Path:
    """Run the ADF discover entry point against the fixtures; return metadata dir."""
    exit_code = main(["--source-dir", str(FIXTURES_DIR), "--output-dir", str(tmp_path)])
    assert exit_code == 0
    return tmp_path / "metadata"


def _legacy_pipeline_map() -> dict[str, list[dict]]:
    """Reconstruct the pre-change per-pipeline activity shape from build_inventory.

    This is exactly what the removed ``_inventory_to_dict`` used to emit, rebuilt
    from the authoritative classifier so the superset check does not depend on a
    frozen copy of the old serializer.
    """
    inventory = build_inventory(load_adf_definitions(FIXTURES_DIR))
    pipeline_map: dict[str, list[dict]] = {}
    for item in inventory.items:
        entry: dict = {"name": item.activity_name, "type": item.activity_type, "strategy": item.strategy.value}
        if item.depends_on:
            entry["depends_on"] = item.depends_on
        pipeline_map.setdefault(item.pipeline_name, []).append(entry)
    return pipeline_map


def test_inventory_top_level_is_superset(tmp_path: Path) -> None:
    """Top-level keys include the legacy set plus the new ``source`` discriminator."""
    metadata = _run_discover(tmp_path)
    inventory = json.loads((metadata / "inventory.json").read_text())

    # Legacy top-level keys still present.
    for key in ("source_dir", "pipelines", "summary"):
        assert key in inventory
    # Additive discriminator.
    assert inventory["source"] == "adf"


def test_summary_counts_match_legacy_classifier(tmp_path: Path) -> None:
    """The summary count block is byte-compatible with the legacy classifier."""
    metadata = _run_discover(tmp_path)
    summary = json.loads((metadata / "inventory.json").read_text())["summary"]

    legacy = build_inventory(load_adf_definitions(FIXTURES_DIR))
    total = legacy.deterministic_count + legacy.agentic_count + legacy.unsupported_count
    assert summary["pipeline_count"] == legacy.pipeline_count
    assert summary["activity_count"] == total
    assert summary["deterministic_count"] == legacy.deterministic_count
    assert summary["agentic_count"] == legacy.agentic_count
    assert summary["unsupported_count"] == legacy.unsupported_count
    assert summary["coverage_pct"] == round((legacy.deterministic_count + legacy.agentic_count) / total * 100, 1)


def test_activity_entries_superset_legacy_shape(tmp_path: Path) -> None:
    """Every activity keeps its legacy keys/values and only gains additive fields."""
    metadata = _run_discover(tmp_path)
    inventory = json.loads((metadata / "inventory.json").read_text())
    legacy_map = _legacy_pipeline_map()

    # Same pipeline membership (ADF omits zero-activity pipelines; fixtures have none empty).
    new_names = [pipeline["name"] for pipeline in inventory["pipelines"]]
    assert sorted(new_names) == sorted(legacy_map)

    for pipeline in inventory["pipelines"]:
        legacy_entries = legacy_map[pipeline["name"]]
        new_entries = pipeline["activities"]
        assert len(new_entries) == len(legacy_entries)
        for legacy_entry, new_entry in zip(legacy_entries, new_entries):
            # Every legacy key/value survives byte-for-byte.
            for key, value in legacy_entry.items():
                assert new_entry[key] == value, (pipeline["name"], key)
            # Additive standardized fields are present.
            assert new_entry["original_type"] == legacy_entry["type"]
            assert "dependencies" in new_entry
            assert "raw" in new_entry


def test_dependencies_field_carries_conditions(tmp_path: Path) -> None:
    """The additive ``dependencies`` field carries upstream + conditions per edge."""
    metadata = _run_discover(tmp_path)
    inventory = json.loads((metadata / "inventory.json").read_text())

    # The all-dependency-conditions fixture exercises non-default conditions.
    pipeline = next(p for p in inventory["pipelines"] if p["name"] == "pipeline_all_dependency_conditions")
    edges = [dependency for activity in pipeline["activities"] for dependency in activity["dependencies"]]
    assert edges, "expected at least one dependency edge"
    for edge in edges:
        assert set(edge.keys()) == {"upstream", "conditions", "resolved"}
        assert isinstance(edge["conditions"], list)


def test_coverage_output_matches_golden(tmp_path: Path) -> None:
    """The real coverage consumer reproduces the committed golden snapshot.

    This is the no-consumer-breakage guarantee: regenerate the golden with
    ``make test`` after an intentional change and review the diff.
    """
    metadata = _run_discover(tmp_path)
    rows = json.loads(json.dumps(build_coverage_rows(metadata), sort_keys=True))
    golden = json.loads(GOLDEN_COVERAGE.read_text())
    assert rows == golden
