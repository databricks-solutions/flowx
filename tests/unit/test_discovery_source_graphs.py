"""Tests for the persisted ``source_graphs.json`` envelope in :mod:`flowx.discovery_serde`."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from flowx.discovery_serde import (
    SOURCE_GRAPHS_CONTRACT_VERSION,
    read_source_graphs,
    source_graph_to_dict,
    source_graphs_document,
    source_graphs_from_document,
    write_source_graphs,
)
from flowx.ir_serde import lineage_to_dict
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceDependency, SourceGraph, SourceNode
from flowx.models.ir import Lineage, MotifAnnotation


def _graph(name: str) -> SourceGraph:
    return SourceGraph(
        name=name,
        source="unit",
        tasks=[
            SourceNode(
                source_id=f"{name}.a",
                task_key="a",
                concept=CONCEPT_NOTEBOOK,
                source="unit",
                native_type="Notebook",
                properties={"strategy": "deterministic"},
            ),
            SourceNode(
                source_id=f"{name}.b",
                task_key="b",
                concept=CONCEPT_NOTEBOOK,
                source="unit",
                native_type="Notebook",
                dependencies=[SourceDependency(upstream="a", conditions=["Succeeded"])],
            ),
        ],
        raw={"name": name},
    )


def test_written_document_round_trips_and_carries_version_and_hashes(tmp_path: Path) -> None:
    """Discover's file reads back to the same graphs and records its version and hashes."""
    graphs = [_graph("first"), _graph("second")]
    path = tmp_path / "source_graphs.json"

    written = write_source_graphs(path, graphs, source="unit")
    on_disk = json.loads(path.read_text(encoding="utf-8"))

    assert on_disk == written
    assert on_disk["contract_version"] == SOURCE_GRAPHS_CONTRACT_VERSION
    assert on_disk["source"] == "unit"
    assert len(on_disk["graph_sha256"]) == 2
    assert len(on_disk["document_sha256"]) == 64
    assert [source_graph_to_dict(graph) for graph in read_source_graphs(path)] == [
        source_graph_to_dict(graph) for graph in graphs
    ]


def test_hashes_do_not_depend_on_formatting(tmp_path: Path) -> None:
    """Re-saving the same content without indentation still verifies."""
    document = source_graphs_document([_graph("first")], source="unit")
    compact = tmp_path / "compact.json"
    compact.write_text(json.dumps(document, separators=(",", ":")), encoding="utf-8")

    assert [graph.name for graph in read_source_graphs(compact)] == ["first"]


def test_document_without_hashes_is_accepted() -> None:
    """A version-1 file written without hashes (the Airflow discover shape) still reads."""
    document = {
        "contract_version": "1",
        "source": "airflow",
        "graphs": [source_graph_to_dict(_graph("dag"))],
    }

    assert [graph.name for graph in source_graphs_from_document(document)] == ["dag"]


def test_edited_graph_fails_closed() -> None:
    """Changing a graph after discover wrote it is caught by the per-graph hash."""
    document = source_graphs_document([_graph("first")], source="unit")
    document["graphs"][0]["tasks"][0]["native_type"] = "Edited"

    with pytest.raises(ValueError, match="graph_sha256"):
        source_graphs_from_document(document)


def test_edited_envelope_fails_closed() -> None:
    """Changing envelope fields is caught by the document hash even when graphs are untouched."""
    document = source_graphs_document([_graph("first")], source="unit")
    document["source"] = "other"

    with pytest.raises(ValueError, match="document_sha256"):
        source_graphs_from_document(document)


def test_unknown_contract_version_is_rejected() -> None:
    """A newer contract version is refused rather than read with the wrong rules."""
    document = source_graphs_document([_graph("first")], source="unit")
    document["contract_version"] = "2"

    with pytest.raises(ValueError, match="contract_version"):
        source_graphs_from_document(document)


def _motif(hint: str | None) -> MotifAnnotation:
    return MotifAnnotation(
        motif_id="activity_and_notify",
        member_task_keys=["a", "b"],
        display_name="Activity and notify",
        databricks_replacement="task_with_notification",
        notes=["b looks like a notification"],
        source_type_hint=hint,
    )


def test_motifs_round_trip_inside_the_hashed_graph() -> None:
    """Motifs recorded on a graph are saved with it, so the graph hash covers them."""
    graph = _graph("first")
    graph.lineage = Lineage(motifs=[_motif("database")])

    document = source_graphs_document([graph], source="unit")
    restored = source_graphs_from_document(json.loads(json.dumps(document)))

    assert restored[0].lineage is not None
    assert restored[0].lineage.motifs == [_motif("database")]
    document["graphs"][0]["lineage"]["motifs"][0]["member_task_keys"] = ["a"]
    with pytest.raises(ValueError, match="graph_sha256"):
        source_graphs_from_document(document)


def test_lineage_without_a_motif_hint_serialises_as_before() -> None:
    """source_type_hint is written only when set, so existing lineage blocks stay byte-identical."""
    without_hint = lineage_to_dict(Lineage(motifs=[_motif(None)]))
    with_hint = lineage_to_dict(Lineage(motifs=[_motif("database")]))

    assert list(without_hint["motifs"][0]) == [
        "motif_id",
        "member_task_keys",
        "display_name",
        "databricks_replacement",
        "notes",
    ]
    assert with_hint["motifs"][0]["source_type_hint"] == "database"
