"""Unit tests for the shared discovery AST (models/discovery.py + discovery_serde.py).

Covers construction of the node set and an exact serialize<->deserialize round
trip, including a ContainerNode with labelled branches, a node carrying
data_reads/data_writes and properties, a GapNode, a schedule, and parameters.
"""

from __future__ import annotations

import json

from flowx.discovery_serde import _node_from_dict, source_graph_from_dict, source_graph_to_dict
from flowx.models.discovery import (
    CONCEPT_COPY_DATA,
    CONCEPT_GAP,
    CONCEPT_LOOP,
    CONCEPT_NOTEBOOK,
    SOURCE_ADF,
    SOURCE_AIRFLOW,
    ContainerNode,
    GapNode,
    ParameterSpec,
    PolicySpec,
    ScheduleSpec,
    SourceDependency,
    SourceGraph,
    SourceNode,
)
from flowx.models.ir import DataAsset


def test_gap_node_defaults_to_gap_concept():
    """A GapNode is a gap without the caller having to restate the concept."""
    gap = GapNode(source_id="a1", task_key="a1", source=SOURCE_ADF, reason="unmapped ExecuteDataFlow")

    assert gap.concept == CONCEPT_GAP
    assert gap.reason == "unmapped ExecuteDataFlow"


def test_partial_gap_node_dict_rehydrates_with_gap_concept():
    """A GapNode dict with no `concept` key falls back to the CONCEPT_GAP default."""
    partial = {"node_type": "GapNode", "source_id": "d1", "task_key": "dataflow", "source": SOURCE_ADF}

    node = _node_from_dict(partial)

    assert isinstance(node, GapNode)
    assert node.concept == CONCEPT_GAP


def test_partial_source_node_dict_falls_back_to_model_defaults():
    """Omitted optional fields on a plain SourceNode dict use model defaults, not empty strings."""
    partial = {
        "node_type": "SourceNode",
        "source_id": "n1",
        "task_key": "run",
        "concept": CONCEPT_NOTEBOOK,
        "source": SOURCE_AIRFLOW,
    }

    node = _node_from_dict(partial)

    assert type(node) is SourceNode
    assert node.name is None
    assert node.native_type is None
    assert node.policy is None
    assert node.dependencies == []
    assert node.data_reads == []
    assert node.data_writes == []
    assert node.properties == {}
    assert node.raw is None


def test_container_node_holds_labelled_branches():
    """A ContainerNode nests child nodes under source-named branch labels."""
    container = ContainerNode(
        source_id="fe",
        task_key="for_each",
        concept=CONCEPT_LOOP,
        source=SOURCE_ADF,
        native_type="ForEach",
        branches={"body": [SourceNode(source_id="c", task_key="copy", concept=CONCEPT_COPY_DATA, source=SOURCE_ADF)]},
    )

    assert list(container.branches) == ["body"]
    assert container.branches["body"][0].task_key == "copy"


def _sample_graph() -> SourceGraph:
    return SourceGraph(
        name="ingest_orders",
        source=SOURCE_ADF,
        description="Loads orders and fans out per region",
        parameters={"region": ParameterSpec(type="String", default="us")},
        variables={"batch": ParameterSpec(type="String")},
        schedule=ScheduleSpec(
            kind="schedule",
            expression={"frequency": "Day", "interval": 1},
            extensions={"timezone": "UTC"},
        ),
        tags=["prod", "orders"],
        tasks=[
            SourceNode(
                source_id="copy_orders",
                task_key="copy_orders",
                concept=CONCEPT_COPY_DATA,
                source=SOURCE_ADF,
                name="Copy Orders",
                native_type="Copy",
                policy=PolicySpec(timeout_seconds=3600, max_retries=2, extensions={"secure_output": True}),
                data_reads=[DataAsset(signature="ds_raw_orders", asset_type="file")],
                data_writes=[
                    DataAsset(
                        signature="ds_curated_orders",
                        identity="curated.orders",
                        asset_type="table",
                        properties={"format": "delta"},
                    )
                ],
                properties={"linked_service": "AzureSqlDatabase1"},
                raw={"type": "Copy", "name": "Copy Orders"},
            ),
            ContainerNode(
                source_id="per_region",
                task_key="per_region",
                concept=CONCEPT_LOOP,
                source=SOURCE_ADF,
                native_type="ForEach",
                dependencies=[SourceDependency(upstream="copy_orders", conditions=["Succeeded", "Skipped"])],
                branches={
                    "body": [
                        SourceNode(
                            source_id="run_region",
                            task_key="run_region",
                            concept=CONCEPT_NOTEBOOK,
                            source=SOURCE_ADF,
                            native_type="DatabricksNotebook",
                        )
                    ]
                },
            ),
            GapNode(
                source_id="dataflow",
                task_key="mapping_dataflow",
                source=SOURCE_ADF,
                native_type="ExecuteDataFlow",
                reason="ExecuteDataFlow has no deterministic mapping",
                raw={"type": "ExecuteDataFlow"},
            ),
        ],
        properties={"folder": "ingest"},
        extensions={"annotations": ["team:data"]},
        raw={"name": "ingest_orders"},
    )


def test_source_graph_round_trip_is_exact():
    """A fully-populated graph survives to_dict -> JSON -> from_dict unchanged."""
    graph = _sample_graph()

    reloaded = source_graph_from_dict(json.loads(json.dumps(source_graph_to_dict(graph))))

    assert reloaded == graph


def test_round_trip_preserves_container_branches_and_gap():
    """Subclass identity (Container/Gap) and their extra fields survive the round trip."""
    graph = _sample_graph()

    reloaded = source_graph_from_dict(json.loads(json.dumps(source_graph_to_dict(graph))))

    container = reloaded.tasks[1]
    assert isinstance(container, ContainerNode)
    assert container.branches["body"][0].task_key == "run_region"
    assert container.dependencies[0].conditions == ["Succeeded", "Skipped"]

    gap = reloaded.tasks[2]
    assert isinstance(gap, GapNode)
    assert gap.concept == CONCEPT_GAP
    assert gap.reason == "ExecuteDataFlow has no deterministic mapping"


def test_round_trip_preserves_data_assets_and_extension_bags():
    """data_reads/data_writes (reused DataAsset) and the extension bags round-trip."""
    graph = _sample_graph()

    reloaded = source_graph_from_dict(json.loads(json.dumps(source_graph_to_dict(graph))))

    copy_node = reloaded.tasks[0]
    assert copy_node.data_reads == [DataAsset(signature="ds_raw_orders", asset_type="file")]
    assert copy_node.data_writes == [
        DataAsset(
            signature="ds_curated_orders", identity="curated.orders", asset_type="table", properties={"format": "delta"}
        )
    ]
    assert copy_node.properties == {"linked_service": "AzureSqlDatabase1"}
    assert reloaded.extensions == {"annotations": ["team:data"]}
    assert reloaded.schedule is not None
    # Source-faithful schedule: the ADF recurrence rides verbatim in `expression`.
    assert reloaded.schedule.expression == {"frequency": "Day", "interval": 1}
    assert reloaded.schedule.extensions == {"timezone": "UTC"}


def test_empty_graph_emits_lists_and_dicts_never_null():
    """A minimal graph serialises its collections as empty containers, not null."""
    serialised = source_graph_to_dict(SourceGraph(name="empty", source=SOURCE_AIRFLOW))

    assert serialised["tasks"] == []
    assert serialised["tags"] == []
    assert serialised["parameters"] == {}
    assert serialised["variables"] == {}
    # Airflow has no graph-scoped variables; the field stays empty rather than absent.
    assert source_graph_from_dict(serialised).variables == {}
