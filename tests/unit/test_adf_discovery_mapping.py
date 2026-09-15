"""Tests for the ADF -> shared discovery AST mapper (:mod:`flowx.sources.adf.discovery_mapping`).

Proves the mapping is 1:1 and lossless: every activity becomes one discovery
node, the ADF type is retained verbatim as ``native_type`` / ``original_type``,
the source dict is preserved on ``raw``, dependency edges keep all of their
outcome conditions, and control-flow nesting maps to labelled container branches.
"""

from __future__ import annotations

from flowx.discovery_inventory import STRATEGY_PROPERTY
from flowx.models.discovery import (
    SOURCE_ADF,
    ContainerNode,
    GapNode,
    SourceGraph,
    SourceNode,
)
from flowx.sources.adf.discovery_mapping import (
    adf_definitions_to_source_graphs,
    adf_pipeline_to_source_graph,
)
from flowx.sources.adf.loader import _parse_pipeline_json


def _pipeline(activities: list[dict], **props) -> SourceGraph:
    data = {"name": props.pop("name", "pl"), "properties": {"activities": activities, **props}}
    return adf_pipeline_to_source_graph(_parse_pipeline_json(data))


def test_activity_maps_1to1_retaining_type_and_raw() -> None:
    """Each activity becomes one node with its ADF type and raw dict preserved."""
    raw_activity = {
        "name": "Run Notebook",
        "type": "DatabricksNotebook",
        "typeProperties": {"notebookPath": "/nb"},
    }
    graph = _pipeline([raw_activity])

    assert len(graph.tasks) == 1
    node = graph.tasks[0]
    assert isinstance(node, SourceNode)
    assert node.source == SOURCE_ADF
    assert node.name == "Run Notebook"
    assert node.task_key == "Run Notebook"
    # ADF type is retained verbatim -- native_type is the original_type source.
    assert node.native_type == "DatabricksNotebook"
    # Verbatim source dict preserved for lossless fallback.
    assert node.raw == raw_activity
    # Target-side strategy stashed in the properties seam (not a typed field).
    assert node.properties[STRATEGY_PROPERTY] == "deterministic"


def test_dependencies_capture_all_conditions() -> None:
    """A dependency edge keeps every outcome condition, not just the first."""
    activities = [
        {"name": "A", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}},
        {
            "name": "B",
            "type": "DatabricksNotebook",
            "dependsOn": [{"activity": "A", "dependencyConditions": ["Succeeded", "Skipped"]}],
        },
    ]
    graph = _pipeline(activities)

    node_b = next(node for node in graph.tasks if node.name == "B")
    assert len(node_b.dependencies) == 1
    assert node_b.dependencies[0].upstream == "A"
    assert node_b.dependencies[0].conditions == ["Succeeded", "Skipped"]


def test_no_motif_collapse_every_activity_is_a_node() -> None:
    """Activities that a motif would merge each stay their own node (no collapse)."""
    activities = [
        {"name": "Load", "type": "Copy"},
        {"name": "Notify", "type": "WebActivity", "dependsOn": [{"activity": "Load"}]},
    ]
    graph = _pipeline(activities)
    assert [node.name for node in graph.tasks] == ["Load", "Notify"]


def test_control_flow_maps_to_container_branches() -> None:
    """An IfCondition maps to a ContainerNode with true/false branches nested."""
    activities = [
        {
            "name": "Check",
            "type": "IfCondition",
            "typeProperties": {
                "ifTrueActivities": [{"name": "T", "type": "DatabricksNotebook"}],
                "ifFalseActivities": [{"name": "F", "type": "Wait"}],
            },
        }
    ]
    graph = _pipeline(activities)

    container = graph.tasks[0]
    assert isinstance(container, ContainerNode)
    assert container.native_type == "IfCondition"
    assert list(container.branches.keys()) == ["true", "false"]
    assert container.branches["true"][0].name == "T"
    assert container.branches["false"][0].name == "F"


def test_switch_maps_cases_and_default_to_branches() -> None:
    """A Switch maps each case value plus default to its own labelled branch."""
    activities = [
        {
            "name": "Route",
            "type": "Switch",
            "typeProperties": {
                "cases": [
                    {"value": "gold", "activities": [{"name": "G", "type": "Copy"}]},
                    {"value": "silver", "activities": [{"name": "S", "type": "Copy"}]},
                ],
                "defaultActivities": [{"name": "D", "type": "Wait"}],
            },
        }
    ]
    graph = _pipeline(activities)

    container = graph.tasks[0]
    assert isinstance(container, ContainerNode)
    assert list(container.branches.keys()) == ["gold", "silver", "default"]
    assert container.branches["gold"][0].name == "G"
    assert container.branches["default"][0].name == "D"


def test_unsupported_activity_becomes_gap_node() -> None:
    """An unsupported ADF type maps to a GapNode carrying the reason and raw."""
    activities = [{"name": "Weird", "type": "TotallyUnknownType"}]
    graph = _pipeline(activities)

    node = graph.tasks[0]
    assert isinstance(node, GapNode)
    assert node.reason is not None and "TotallyUnknownType" in node.reason
    assert node.raw == {"name": "Weird", "type": "TotallyUnknownType"}
    assert node.properties[STRATEGY_PROPERTY] == "unsupported"


def test_policy_maps_retry_and_preserves_timeout_verbatim() -> None:
    """Retry count/interval map to typed fields; the ISO timeout rides in extensions."""
    activities = [
        {
            "name": "Copy",
            "type": "Copy",
            "policy": {
                "timeout": "0.12:00:00",
                "retry": 3,
                "retryIntervalInSeconds": 30,
                "secureInput": True,
            },
        }
    ]
    graph = _pipeline(activities)

    policy = graph.tasks[0].policy
    assert policy is not None
    assert policy.max_retries == 3
    assert policy.retry_interval_seconds == 30
    # Timeout normalisation to seconds is a target concern -> preserved verbatim.
    assert policy.extensions["timeout"] == "0.12:00:00"
    assert policy.extensions["secure_input"] is True


def test_graph_carries_parameters_variables_tags_and_folder() -> None:
    """Graph-level metadata maps onto the shared fields / properties seam."""
    data = {
        "name": "pl",
        "properties": {
            "activities": [{"name": "N", "type": "DatabricksNotebook"}],
            "parameters": {"env": {"type": "String", "defaultValue": "dev"}},
            "variables": {"counter": {"type": "Integer"}},
            "annotations": ["team-a", "prod"],
            "folder": {"name": "ingest/bronze"},
        },
    }
    graph = adf_pipeline_to_source_graph(_parse_pipeline_json(data))

    assert graph.source == SOURCE_ADF
    assert graph.parameters["env"].type == "String"
    assert graph.parameters["env"].default == "dev"
    assert graph.variables["counter"].type == "Integer"
    assert graph.tags == ["team-a", "prod"]
    assert graph.properties["folder"] == "ingest/bronze"
    assert graph.raw is not None  # verbatim pipeline dict preserved


def test_definitions_map_preserves_pipeline_order(adf_definitions) -> None:
    """The definitions-level mapper yields one graph per pipeline, in order."""
    graphs = adf_definitions_to_source_graphs(adf_definitions)
    assert [g.name for g in graphs] == [p.name for p in adf_definitions.pipelines]
    assert all(g.source == SOURCE_ADF for g in graphs)
