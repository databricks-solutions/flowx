"""Tests for the ADF -> shared discovery AST mapper (:mod:`flowx.sources.adf.discovery_mapping`).

Proves the mapping is 1:1 and lossless: every activity becomes one discovery
node, the ADF type is retained verbatim as ``native_type`` / ``original_type``,
the source dict is preserved on ``raw``, dependency edges keep all of their
outcome conditions, and control-flow nesting maps to labelled container branches.
"""

from __future__ import annotations

from flowx.discovery_inventory import STRATEGY_PROPERTY
from flowx.models.adf_ast import AdfDefinitions
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
from flowx.sources.adf.loader import _parse_pipeline_json, _parse_trigger_json


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


# ---------------------------------------------------------------------------
# Triggers / schedules (BLOCKING 1)
# ---------------------------------------------------------------------------


def _definitions_with_trigger(trigger: dict) -> AdfDefinitions:
    pipeline = _parse_pipeline_json({"name": "pl_sched", "properties": {"activities": []}})
    return AdfDefinitions(pipelines=[pipeline], triggers=[_parse_trigger_json(trigger)])


def test_schedule_trigger_lands_in_source_graph_schedule() -> None:
    """A ScheduleTrigger referencing a pipeline populates that graph's schedule."""
    trigger = {
        "name": "tr_daily",
        "properties": {
            "type": "ScheduleTrigger",
            "typeProperties": {
                "recurrence": {"frequency": "Day", "interval": 1, "timeZone": "UTC"},
            },
            "pipelines": [{"pipelineReference": {"referenceName": "pl_sched", "type": "PipelineReference"}}],
        },
    }
    graphs = adf_definitions_to_source_graphs(_definitions_with_trigger(trigger))

    schedule = graphs[0].schedule
    assert schedule is not None
    assert schedule.kind == "schedule"
    # Recurrence payload preserved verbatim as the expression.
    assert schedule.expression == {"frequency": "Day", "interval": 1, "timeZone": "UTC"}
    assert schedule.timezone == "UTC"
    # Full trigger properties preserved losslessly in extensions.
    assert schedule.extensions["trigger_name"] == "tr_daily"
    assert schedule.extensions["trigger_type"] == "ScheduleTrigger"
    assert "properties" in schedule.extensions


def test_tumbling_window_trigger_expression_from_type_properties() -> None:
    """A TumblingWindowTrigger keeps its typeProperties as the expression."""
    trigger = {
        "name": "tr_tumble",
        "properties": {
            "type": "TumblingWindowTrigger",
            "typeProperties": {"frequency": "Hour", "interval": 1, "startTime": "2024-01-01T00:00:00Z"},
            "pipelines": [{"pipelineReference": {"referenceName": "pl_sched"}}],
        },
    }
    graphs = adf_definitions_to_source_graphs(_definitions_with_trigger(trigger))

    schedule = graphs[0].schedule
    assert schedule is not None
    assert schedule.kind == "interval"
    assert schedule.expression == {"frequency": "Hour", "interval": 1, "startTime": "2024-01-01T00:00:00Z"}


def test_unreferenced_pipeline_has_no_schedule() -> None:
    """A pipeline no trigger references keeps ``schedule is None``."""
    trigger = {
        "name": "tr_other",
        "properties": {
            "type": "ScheduleTrigger",
            "typeProperties": {"recurrence": {"frequency": "Day", "interval": 1}},
            "pipelines": [{"pipelineReference": {"referenceName": "some_other_pipeline"}}],
        },
    }
    graphs = adf_definitions_to_source_graphs(_definitions_with_trigger(trigger))
    assert graphs[0].schedule is None


def test_multiple_triggers_preserve_extras_in_extensions() -> None:
    """A second trigger for the same pipeline is preserved, not overwritten."""
    pipeline = _parse_pipeline_json({"name": "pl_sched", "properties": {"activities": []}})
    triggers = [
        _parse_trigger_json(
            {
                "name": "tr_first",
                "properties": {
                    "type": "ScheduleTrigger",
                    "typeProperties": {"recurrence": {"frequency": "Day", "interval": 1}},
                    "pipelines": [{"pipelineReference": {"referenceName": "pl_sched"}}],
                },
            }
        ),
        _parse_trigger_json(
            {
                "name": "tr_second",
                "properties": {
                    "type": "ScheduleTrigger",
                    "typeProperties": {"recurrence": {"frequency": "Hour", "interval": 6}},
                    "pipelines": [{"pipelineReference": {"referenceName": "pl_sched"}}],
                },
            }
        ),
    ]
    graphs = adf_definitions_to_source_graphs(AdfDefinitions(pipelines=[pipeline], triggers=triggers))

    schedule = graphs[0].schedule
    assert schedule is not None
    assert schedule.extensions["trigger_name"] == "tr_first"  # first wins the typed slot
    additional = schedule.extensions["additional_triggers"]
    assert len(additional) == 1
    # The additional trigger retains its NAME (not just properties).
    assert additional[0]["trigger_name"] == "tr_second"
    assert additional[0]["trigger_type"] == "ScheduleTrigger"
    assert additional[0]["properties"]["typeProperties"]["recurrence"]["interval"] == 6


def test_triggers_do_not_leak_across_pipelines() -> None:
    """A ScheduleSpec is per-pipeline: a later A-only trigger must not appear on B.

    Guards the shared-instance aliasing bug -- trigger_ab references A and B, then
    trigger_a references only A. B must keep exactly trigger_ab and gain nothing
    from trigger_a.
    """
    pipeline_a = _parse_pipeline_json({"name": "pl_a", "properties": {"activities": []}})
    pipeline_b = _parse_pipeline_json({"name": "pl_b", "properties": {"activities": []}})
    triggers = [
        _parse_trigger_json(
            {
                "name": "tr_ab",
                "properties": {
                    "type": "ScheduleTrigger",
                    "typeProperties": {"recurrence": {"frequency": "Day", "interval": 1}},
                    "pipelines": [
                        {"pipelineReference": {"referenceName": "pl_a"}},
                        {"pipelineReference": {"referenceName": "pl_b"}},
                    ],
                },
            }
        ),
        _parse_trigger_json(
            {
                "name": "tr_a_only",
                "properties": {
                    "type": "ScheduleTrigger",
                    "typeProperties": {"recurrence": {"frequency": "Hour", "interval": 2}},
                    "pipelines": [{"pipelineReference": {"referenceName": "pl_a"}}],
                },
            }
        ),
    ]
    graphs = {
        graph.name: graph
        for graph in adf_definitions_to_source_graphs(
            AdfDefinitions(pipelines=[pipeline_a, pipeline_b], triggers=triggers)
        )
    }

    schedule_a = graphs["pl_a"].schedule
    schedule_b = graphs["pl_b"].schedule
    assert schedule_a is not None and schedule_b is not None
    assert schedule_a is not schedule_b  # distinct instances, no aliasing
    # A picked up the second trigger; B must NOT have leaked it.
    assert schedule_a.extensions["additional_triggers"][0]["trigger_name"] == "tr_a_only"
    assert "additional_triggers" not in schedule_b.extensions


def test_fixture_scheduled_pipeline_gets_schedule(adf_definitions) -> None:
    """End-to-end over the fixtures: a trigger-referenced pipeline gets a schedule."""
    graphs = {graph.name: graph for graph in adf_definitions_to_source_graphs(adf_definitions)}
    # tr_daily_schedule references pipeline_copy_csv_to_delta in the fixtures.
    assert graphs["pipeline_copy_csv_to_delta"].schedule is not None


# ---------------------------------------------------------------------------
# Empty / one-sided control flow (BLOCKING 2)
# ---------------------------------------------------------------------------


def test_empty_if_condition_stays_a_container_with_both_branches() -> None:
    """An IfCondition with no children still maps to a ContainerNode, both branches present."""
    graph = _pipeline([{"name": "Gate", "type": "IfCondition", "typeProperties": {}}])

    node = graph.tasks[0]
    assert isinstance(node, ContainerNode)
    assert node.native_type == "IfCondition"
    assert list(node.branches.keys()) == ["true", "false"]
    assert node.branches["true"] == []
    assert node.branches["false"] == []


def test_empty_for_each_stays_a_container_with_body_branch() -> None:
    """A ForEach with no children still maps to a ContainerNode with an empty body."""
    graph = _pipeline([{"name": "Loop", "type": "ForEach", "typeProperties": {}}])

    node = graph.tasks[0]
    assert isinstance(node, ContainerNode)
    assert list(node.branches.keys()) == ["body"]
    assert node.branches["body"] == []


def test_empty_until_stays_a_container_with_body_branch() -> None:
    """An Until with no children still maps to a ContainerNode with an empty body."""
    graph = _pipeline([{"name": "Retry", "type": "Until", "typeProperties": {}}])

    node = graph.tasks[0]
    assert isinstance(node, ContainerNode)
    assert node.native_type == "Until"
    assert list(node.branches.keys()) == ["body"]
    assert node.branches["body"] == []


def test_one_sided_if_keeps_empty_false_branch() -> None:
    """An If with only a true branch keeps its false branch present-but-empty."""
    graph = _pipeline(
        [
            {
                "name": "Gate",
                "type": "IfCondition",
                "typeProperties": {"ifTrueActivities": [{"name": "T", "type": "Wait"}]},
            }
        ]
    )

    node = graph.tasks[0]
    assert isinstance(node, ContainerNode)
    assert [child.name for child in node.branches["true"]] == ["T"]
    assert "false" in node.branches  # empty branch is present, not dropped
    assert node.branches["false"] == []


def test_empty_switch_stays_a_container_with_default_branch() -> None:
    """A Switch with no cases still maps to a ContainerNode with an empty default."""
    graph = _pipeline([{"name": "Route", "type": "Switch", "typeProperties": {}}])

    node = graph.tasks[0]
    assert isinstance(node, ContainerNode)
    assert list(node.branches.keys()) == ["default"]
    assert node.branches["default"] == []
