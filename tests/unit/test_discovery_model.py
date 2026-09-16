"""Unit tests for the shared discovery AST (models/discovery.py + discovery_serde.py).

Covers construction of the node set and an exact serialize<->deserialize round
trip, including a ContainerNode with labelled branches, a node carrying
data_reads/data_writes and properties, a GapNode, a schedule, and parameters.
"""

from __future__ import annotations

import json

from flowx.discovery_serde import _node_from_dict, _node_to_dict, source_graph_from_dict, source_graph_to_dict
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
from flowx.models.ir import ControlEdge, DataAsset, DataEdge, Lineage


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
            timezone="UTC",
            extensions={"runtime_state": "Started"},
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
    # Source-faithful schedule: the ADF recurrence rides verbatim in `expression`,
    # the source-declared timezone is a typed field, and no Databricks-target
    # shape (Quartz cron / pause_status) is baked in.
    assert reloaded.schedule.expression == {"frequency": "Day", "interval": 1}
    assert reloaded.schedule.timezone == "UTC"
    assert reloaded.schedule.extensions == {"runtime_state": "Started"}


def test_empty_graph_emits_lists_and_dicts_never_null():
    """A minimal graph serialises its collections as empty containers, not null."""
    serialised = source_graph_to_dict(SourceGraph(name="empty", source=SOURCE_AIRFLOW))

    assert serialised["tasks"] == []
    assert serialised["tags"] == []
    assert serialised["parameters"] == {}
    assert serialised["variables"] == {}
    # Airflow has no graph-scoped variables; the field stays empty rather than absent.
    assert source_graph_from_dict(serialised).variables == {}


def test_lineage_block_round_trips_and_is_absent_when_none():
    """A graph's derived lineage survives serialise<->deserialise; None stays absent."""
    graph = SourceGraph(
        name="pl",
        source=SOURCE_ADF,
        lineage=Lineage(
            control_edges=[
                ControlEdge(
                    source_workflow="pl",
                    target_workflow="child",
                    via_task_key="Run Child",
                    wait_for_completion=False,
                )
            ],
            data_edges=[
                DataEdge(
                    source_task_key="writer",
                    target_task_key="reader",
                    match_kind="identity",
                    match_key="curated.orders",
                    identity="curated.orders",
                    asset_type="table",
                )
            ],
        ),
    )
    reloaded = source_graph_from_dict(json.loads(json.dumps(source_graph_to_dict(graph))))
    assert reloaded == graph

    # A graph with no derived lineage omits the key entirely and rehydrates to None.
    bare = source_graph_to_dict(SourceGraph(name="bare", source=SOURCE_ADF))
    assert "lineage" not in bare
    assert source_graph_from_dict(bare).lineage is None


def test_node_run_condition_round_trips_and_defaults_none():
    """Airflow's node-level trigger_rule round-trips; ADF-style nodes leave it None."""
    node = SourceNode(
        source_id="join",
        task_key="join",
        concept=CONCEPT_NOTEBOOK,
        source=SOURCE_AIRFLOW,
        run_condition="none_failed_min_one_success",
    )
    graph = SourceGraph(name="dag", source=SOURCE_AIRFLOW, tasks=[node])

    reloaded = source_graph_from_dict(json.loads(json.dumps(source_graph_to_dict(graph))))
    assert reloaded == graph
    assert reloaded.tasks[0].run_condition == "none_failed_min_one_success"

    # ADF/substrate default: a node that sets no run_condition omits the key and stays None.
    plain = SourceNode(source_id="a", task_key="a", concept=CONCEPT_NOTEBOOK, source=SOURCE_ADF)
    assert "run_condition" not in _node_to_dict(plain)
    assert _node_from_dict(_node_to_dict(plain)).run_condition is None


def test_graph_default_policy_and_run_timeout_round_trip_and_default_none():
    """Airflow DAG default_args cascade + dagrun_timeout round-trip; ADF leaves both None."""
    graph = SourceGraph(
        name="dag",
        source=SOURCE_AIRFLOW,
        default_policy=PolicySpec(max_retries=3, retry_interval_seconds=300, extensions={"owner": "data"}),
        run_timeout_seconds=7200,
    )

    reloaded = source_graph_from_dict(json.loads(json.dumps(source_graph_to_dict(graph))))
    assert reloaded == graph
    assert reloaded.default_policy == PolicySpec(
        max_retries=3, retry_interval_seconds=300, extensions={"owner": "data"}
    )
    assert reloaded.run_timeout_seconds == 7200

    # ADF/substrate default: no graph-level policy or run timeout -> keys absent, fields None.
    bare = source_graph_to_dict(SourceGraph(name="pl", source=SOURCE_ADF))
    assert "default_policy" not in bare
    assert "run_timeout_seconds" not in bare
    rehydrated = source_graph_from_dict(bare)
    assert rehydrated.default_policy is None
    assert rehydrated.run_timeout_seconds is None


def test_non_physical_asset_type_and_empty_reads_writes_are_valid():
    """A value/logical asset_type round-trips, and empty reads/writes are valid (best-effort)."""
    producer = SourceNode(
        source_id="extract",
        task_key="extract",
        concept=CONCEPT_NOTEBOOK,
        source=SOURCE_AIRFLOW,
        # An Airflow XCom / TaskFlow return value: no physical identity, an open non-physical kind.
        data_writes=[DataAsset(signature="extract:return_value", asset_type="value")],
    )
    # Best-effort population: a node may record nothing at all.
    consumer = SourceNode(
        source_id="load",
        task_key="load",
        concept=CONCEPT_NOTEBOOK,
        source=SOURCE_AIRFLOW,
    )
    graph = SourceGraph(name="dag", source=SOURCE_AIRFLOW, tasks=[producer, consumer])

    reloaded = source_graph_from_dict(json.loads(json.dumps(source_graph_to_dict(graph))))
    assert reloaded == graph
    assert reloaded.tasks[0].data_writes == [DataAsset(signature="extract:return_value", asset_type="value")]
    assert reloaded.tasks[0].data_writes[0].identity is None
    # Empty reads/writes survive as empty lists, never None.
    assert reloaded.tasks[1].data_reads == []
    assert reloaded.tasks[1].data_writes == []
