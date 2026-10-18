"""Tests for the persisted ``source_graphs.json`` envelope in :mod:`flowx.discovery_serde`."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from flowx.discovery_serde import (
    _HASHED_DOCUMENT_KEYS,
    SOURCE_GRAPHS_CONTRACT_VERSION,
    _canonical_sha256,
    read_source_graphs,
    source_graph_to_dict,
    source_graphs_document,
    source_graphs_from_document,
    write_source_graphs,
)
from flowx.ir_serde import lineage_to_dict
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceDependency, SourceGraph, SourceNode
from flowx.models.ir import ControlEdge, Lineage, MotifAnnotation


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


@pytest.mark.parametrize("missing_key", ["graph_sha256", "document_sha256"])
def test_document_missing_a_hash_is_rejected(missing_key: str) -> None:
    """Deleting a hash field cannot get an edited file past verification."""
    document = source_graphs_document([_graph("first")], source="unit")
    del document[missing_key]

    with pytest.raises(ValueError, match=f"missing {missing_key}"):
        source_graphs_from_document(document)


def test_edited_graph_with_both_hashes_deleted_fails_closed() -> None:
    document = source_graphs_document([_graph("first")], source="unit")
    document["graphs"][0]["tasks"][0]["native_type"] = "Edited"
    del document["graph_sha256"]
    del document["document_sha256"]

    with pytest.raises(ValueError, match="missing graph_sha256"):
        source_graphs_from_document(document)


def test_graph_from_another_source_is_rejected() -> None:
    """A document labelled with one source cannot carry graphs from another, even with matching hashes."""
    document = source_graphs_document([_graph("first")], source="unit")
    document["source"] = "other"
    document["document_sha256"] = _canonical_sha256({key: document[key] for key in _HASHED_DOCUMENT_KEYS})

    with pytest.raises(ValueError, match="holds a graph from another source"):
        source_graphs_from_document(document)


def test_writer_refuses_a_graph_from_another_source(tmp_path: Path) -> None:
    """Discover cannot save a document that the reader would refuse later."""
    path = tmp_path / "source_graphs.json"

    with pytest.raises(ValueError, match="holds a graph from another source"):
        source_graphs_document([_graph("first")], source="other")
    with pytest.raises(ValueError, match="holds a graph from another source"):
        write_source_graphs(path, [_graph("first")], source="other")
    assert not path.exists()


def test_unknown_top_level_key_is_rejected() -> None:
    """A key the hashes do not cover cannot ride along unverified."""
    document = source_graphs_document([_graph("first")], source="unit")
    document["extra"] = {"injected": True}

    with pytest.raises(ValueError, match="extra"):
        source_graphs_from_document(document)


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


def _rehash(document: dict) -> dict:
    """Recompute both hashes after an edit, so a test reaches the checks that come after them."""
    document["graph_sha256"] = [_canonical_sha256(graph) for graph in document["graphs"]]
    document["document_sha256"] = _canonical_sha256({key: document[key] for key in _HASHED_DOCUMENT_KEYS})
    return document


@pytest.mark.parametrize("recorded", ["missing", "false", 1])
def test_missing_or_non_boolean_resolved_reads_as_unresolved(recorded) -> None:
    """Only an explicit ``true`` means resolved; anything else read back is unresolved."""
    graph = _graph("first")
    graph.lineage = Lineage(
        control_edges=[ControlEdge(source_workflow="first", target_workflow="missing", via_task_key="a")]
    )
    document = source_graphs_document([graph], source="unit")
    edge = document["graphs"][0]["lineage"]["control_edges"][0]
    dependency = document["graphs"][0]["tasks"][1]["dependencies"][0]
    for raw in (edge, dependency):
        if recorded == "missing":
            del raw["resolved"]
        else:
            raw["resolved"] = recorded

    [restored] = source_graphs_from_document(_rehash(document))

    assert restored.lineage.control_edges[0].resolved is False
    assert restored.tasks[1].dependencies[0].resolved is False


@pytest.mark.parametrize("recorded", ["true", 0])
def test_non_boolean_wait_for_completion_reads_as_unset(recorded) -> None:
    """A saved wait flag that is not a boolean comes back as unknown, never verbatim."""
    graph = _graph("first")
    graph.lineage = Lineage(
        control_edges=[
            ControlEdge(source_workflow="first", target_workflow="child", via_task_key="a", wait_for_completion=True)
        ]
    )
    document = source_graphs_document([graph], source="unit")
    document["graphs"][0]["lineage"]["control_edges"][0]["wait_for_completion"] = recorded

    [restored] = source_graphs_from_document(_rehash(document))

    assert restored.lineage.control_edges[0].wait_for_completion is None


def _graph_with_parameters_list(document: dict) -> None:
    document["graphs"][0]["parameters"] = [1]


def _graph_with_tasks_map(document: dict) -> None:
    document["graphs"][0]["tasks"] = {"a": {}}


def _graph_that_is_a_string(document: dict) -> None:
    document["graphs"][0] = "first"


@pytest.mark.parametrize("edit", [_graph_with_parameters_list, _graph_with_tasks_map, _graph_that_is_a_string])
def test_malformed_graph_shape_raises_value_error(edit) -> None:
    """A graph whose shape is wrong is refused with the reader's documented error, not a crash."""
    document = source_graphs_document([_graph("first")], source="unit")
    edit(document)

    with pytest.raises(ValueError, match="malformed"):
        source_graphs_from_document(_rehash(document))


def test_document_that_is_not_an_object_raises_value_error() -> None:
    with pytest.raises(ValueError, match="JSON object"):
        source_graphs_from_document([])  # type: ignore[arg-type]


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
