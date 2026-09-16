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


def test_inventory_carries_per_pipeline_lineage(tmp_path: Path) -> None:
    """The emitted inventory surfaces the lineage #62b derived over the ADF fixtures.

    Lineage rides additively on each pipeline entry, so the aggregate across the
    per-pipeline blocks must match what the discovery lineage pass computed: on
    these fixtures that is 11 cross-pipeline control edges and 0 data edges.
    """
    metadata = _run_discover(tmp_path)
    inventory = json.loads((metadata / "inventory.json").read_text())

    control_edges = 0
    data_edges = 0
    for pipeline in inventory["pipelines"]:
        assert "lineage" in pipeline, pipeline["name"]
        # Additive block only -- historical per-pipeline keys are untouched.
        assert set(pipeline["lineage"].keys()) == {"control_edges", "data_edges", "motifs"}
        control_edges += len(pipeline["lineage"]["control_edges"])
        data_edges += len(pipeline["lineage"]["data_edges"])

    assert control_edges == 11
    assert data_edges == 0


def test_inventory_surfaces_detected_motifs_without_collapsing(tmp_path: Path) -> None:
    """Discover surfaces the profiler's detected motifs additively, members intact.

    ``pipeline_complex_etl`` carries a detectable metadata-driven bulk-copy motif.
    It must appear in the pipeline's additive ``motifs`` list -- carrying its type,
    Databricks replacement target, and participating activities -- while every
    member still appears as its own entry in ``activities`` (no discover-time
    collapse) and the lineage block's own motif slot stays empty (decoupled).
    """
    metadata = _run_discover(tmp_path)
    inventory = json.loads((metadata / "inventory.json").read_text())

    pipeline = next(p for p in inventory["pipelines"] if p["name"] == "pipeline_complex_etl")
    bulk = next(m for m in pipeline["motifs"] if m["motif_id"] == "metadata_driven_bulk_copy")

    assert set(bulk.keys()) == {
        "motif_id",
        "display_name",
        "databricks_replacement",
        "member_task_keys",
        "source_type_hint",
        "confidence_notes",
    }
    assert bulk["databricks_replacement"] == "for_each_ingestion"
    assert bulk["member_task_keys"], "motif must name its participating activities"

    # No collapse at discover: each member survives as its own activity entry.
    activity_names = {activity["name"] for activity in pipeline["activities"]}
    for member in bulk["member_task_keys"]:
        assert member in activity_names, member

    # Decoupled from lineage: the additive motifs key is separate from lineage.motifs.
    assert pipeline["lineage"]["motifs"] == []


def test_pipelines_without_a_motif_omit_the_motifs_key(tmp_path: Path) -> None:
    """The motifs field is additive: a pipeline with no detected motif omits it."""
    metadata = _run_discover(tmp_path)
    inventory = json.loads((metadata / "inventory.json").read_text())

    plain = next(p for p in inventory["pipelines"] if p["name"] == "pipeline_notebook_basic")
    assert "motifs" not in plain


def test_coverage_output_matches_golden(tmp_path: Path) -> None:
    """The real coverage consumer reproduces the committed golden snapshot.

    This is the no-consumer-breakage guarantee: regenerate the golden with
    ``make test`` after an intentional change and review the diff.
    """
    metadata = _run_discover(tmp_path)
    rows = json.loads(json.dumps(build_coverage_rows(metadata), sort_keys=True))
    golden = json.loads(GOLDEN_COVERAGE.read_text())
    assert rows == golden
