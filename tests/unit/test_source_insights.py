"""Enrich writes its own ``source_insights.json`` and the inventory is rendered from it.

The inventory's ``insights`` block is always a copy of ``source_insights.json``, which records
the saved ``source_graphs.json`` hash it was checked against and its own content hash, so a
later phase can tell exactly which graph and which insights a decision was made on.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from flowx.discovery_insights import (
    INSIGHTS_KEY,
    SOURCE_INSIGHTS_FILENAME,
    enrich_inventory,
    inventory_fingerprint,
    source_insights_hash_violations,
    validate_insights,
)
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.discovery_serde import SOURCE_GRAPHS_FILENAME, write_source_graphs
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode


def _graphs() -> list[SourceGraph]:
    node = SourceNode(
        source_id="load",
        task_key="load",
        concept=CONCEPT_NOTEBOOK,
        source="adf",
        name="load",
        native_type="DatabricksNotebook",
        properties={STRATEGY_PROPERTY: "deterministic"},
    )
    return [SourceGraph(name="orders", source="adf", tasks=[node])]


def _discover(output_dir: Path) -> Path:
    """Write source_graphs.json and the inventory projected from it, as ADF discover does."""
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    document = write_source_graphs(metadata / SOURCE_GRAPHS_FILENAME, _graphs(), source="adf")
    inventory = build_source_inventory(
        _graphs(), source="adf", source_dir="/src", source_graphs_sha256=document["document_sha256"]
    )
    path = metadata / "inventory.json"
    path.write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    return path


def _insights() -> dict[str, Any]:
    return {
        "overview": "One ingestion pipeline.",
        "pipeline_insights": [{"pipeline": "orders", "intent": "Load orders nightly."}],
    }


def test_enrich_writes_source_insights_and_renders_the_inventory_from_it(tmp_path: Path) -> None:
    inventory_path = _discover(tmp_path)
    before = json.loads(inventory_path.read_text(encoding="utf-8"))

    result = enrich_inventory(tmp_path, insights=_insights())

    assert result["ok"] is True
    source_insights = json.loads((tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).read_text(encoding="utf-8"))
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    assert inventory[INSIGHTS_KEY] == source_insights
    assert {key: value for key, value in inventory.items() if key != INSIGHTS_KEY} == before
    assert source_insights["source_graphs_sha256"] == before["source_graphs_sha256"]
    assert source_insights["inventory_sha256"] == inventory_fingerprint(before)
    assert result["source_insights_sha256"] == source_insights["source_insights_sha256"]
    assert source_insights_hash_violations(source_insights) == []


def test_re_enriching_is_byte_identical_and_keeps_the_routing_fingerprint(tmp_path: Path) -> None:
    inventory_path = _discover(tmp_path)
    fingerprint_before = inventory_fingerprint(json.loads(inventory_path.read_text(encoding="utf-8")))

    enrich_inventory(tmp_path, insights=_insights())
    first = (inventory_path.read_bytes(), (tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).read_bytes())
    enrich_inventory(tmp_path, insights=_insights())
    second = (inventory_path.read_bytes(), (tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).read_bytes())

    assert first == second
    assert inventory_fingerprint(json.loads(inventory_path.read_text(encoding="utf-8"))) == fingerprint_before


def test_enrich_refuses_an_inventory_from_a_different_source_graphs_file(tmp_path: Path) -> None:
    inventory_path = _discover(tmp_path)
    write_source_graphs(tmp_path / "metadata" / SOURCE_GRAPHS_FILENAME, [], source="adf")
    inventory_bytes = inventory_path.read_bytes()

    result = enrich_inventory(tmp_path, insights=_insights())

    assert result["ok"] is False
    assert any("different source_graphs.json" in violation for violation in result["violations"])
    assert inventory_path.read_bytes() == inventory_bytes
    assert not (tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).exists()


def test_inventory_without_a_recorded_graph_hash_still_enriches(tmp_path: Path) -> None:
    """An inventory from a source that does not persist graphs yet has nothing to check."""
    metadata = tmp_path / "metadata"
    metadata.mkdir(parents=True)
    inventory = build_source_inventory(_graphs(), source="airflow", source_dir="/dags")
    (metadata / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")

    result = enrich_inventory(tmp_path, insights=_insights())

    assert result["ok"] is True
    source_insights = json.loads((metadata / SOURCE_INSIGHTS_FILENAME).read_text(encoding="utf-8"))
    assert "source_graphs_sha256" not in source_insights


def test_authored_insights_cannot_set_the_library_hashes() -> None:
    inventory = build_source_inventory(_graphs(), source="adf", source_dir="/src")
    raw = {**_insights(), "source_graphs_sha256": "x", "source_insights_sha256": "y"}

    violations = validate_insights(raw, inventory)

    assert any("source_graphs_sha256" in violation and "library" in violation for violation in violations)
    assert any("source_insights_sha256" in violation and "library" in violation for violation in violations)


def test_an_edited_source_insights_file_fails_its_hash_check(tmp_path: Path) -> None:
    _discover(tmp_path)
    enrich_inventory(tmp_path, insights=_insights())
    document = json.loads((tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).read_text(encoding="utf-8"))
    document["overview"] = "Edited after enrich."

    assert source_insights_hash_violations(document) != []
