"""Unit tests for the AWS Glue Workflows source: parsing, translation, inventory."""

from __future__ import annotations

from pathlib import Path

from flowx.ir_serde import pipeline_to_dict
from flowx.models.ir import PlaceholderActivity
from flowx.sources.glue.loader import load_pipelines
from flowx.sources.glue.translate import translate_workflow
from flowx.sources.glue.workflow import parse_workflow
from flowx.sources.inventory import build_inventory_dict, classify_tasks

_FIXTURE = Path(__file__).parent.parent / "resources" / "glue" / "sales_workflow.json"


def _sales_pipeline():
    """Returns the single translated pipeline from the sales_workflow fixture."""
    pipelines = load_pipelines(_FIXTURE)
    assert len(pipelines) == 1
    return pipelines[0]


def test_parse_unwraps_workflow_and_collects_nodes() -> None:
    """The Workflow wrapper is unwrapped and job/crawler/trigger nodes are collected."""
    raw = {"Workflow": {"Name": "wf", "Graph": {"Nodes": [{"Type": "JOB", "Name": "a"}]}}}
    workflow = parse_workflow(raw, default_name="ignored")
    assert workflow.name == "wf"
    assert workflow.jobs == ["a"]


def test_tasks_are_one_placeholder_per_job_and_crawler() -> None:
    """Each job and crawler node becomes an agentic placeholder task."""
    pipeline = _sales_pipeline()
    assert pipeline.name == "sales_workflow"
    assert all(isinstance(task, PlaceholderActivity) for task in pipeline.tasks)
    by_name = {task.name: task for task in pipeline.tasks}
    assert set(by_name) == {"ingest_sales", "transform_sales", "publish_sales", "sales_crawler"}
    assert by_name["sales_crawler"].original_type == "GlueCrawler"
    assert by_name["ingest_sales"].original_type == "GlueJob"


def test_scheduled_trigger_sets_quartz_schedule() -> None:
    """A scheduled start trigger's cron becomes a UTC Quartz schedule (seconds prefixed)."""
    pipeline = _sales_pipeline()
    assert pipeline.schedule == {
        "kind": "schedule",
        "quartz_cron_expression": "0 0 7 * * ? *",
        "timezone_id": "UTC",
    }


def test_conditional_trigger_builds_dependency_edges() -> None:
    """A conditional trigger wires each predicate node to each action node."""
    pipeline = _sales_pipeline()
    by_name = {task.name: task for task in pipeline.tasks}

    assert by_name["ingest_sales"].depends_on is None

    for action in ("sales_crawler", "transform_sales"):
        deps = by_name[action].depends_on
        assert deps is not None
        assert [d.task_key for d in deps] == ["ingest_sales"]
        assert deps[0].outcome is None

    publish_deps = by_name["publish_sales"].depends_on
    assert publish_deps is not None
    assert {d.task_key for d in publish_deps} == {"sales_crawler", "transform_sales"}


def test_run_if_outcomes_map_from_predicate_state() -> None:
    """ALL/SUCCEEDED predicate yields the default run_if (outcome None, no run_if key)."""
    pipeline = _sales_pipeline()
    by_name = {task.name: task for task in pipeline.tasks}
    assert all(dep.outcome is None for dep in by_name["publish_sales"].depends_on)


def test_any_logical_failed_predicate_maps_to_run_if_constant() -> None:
    """An ANY predicate over FAILED states maps to AT_LEAST_ONE_FAILED on each edge."""
    raw = {
        "Workflow": {
            "Name": "retry_wf",
            "Graph": {
                "Nodes": [
                    {"Type": "JOB", "Name": "main"},
                    {"Type": "JOB", "Name": "fallback"},
                    {
                        "Type": "TRIGGER",
                        "Name": "on-fail",
                        "TriggerDetails": {
                            "Trigger": {
                                "Type": "CONDITIONAL",
                                "Predicate": {
                                    "Logical": "ANY",
                                    "Conditions": [{"JobName": "main", "State": "FAILED"}],
                                },
                                "Actions": [{"JobName": "fallback"}],
                            }
                        },
                    },
                ]
            },
        }
    }
    pipeline = translate_workflow(parse_workflow(raw, default_name="retry_wf"))
    fallback = next(task for task in pipeline.tasks if task.name == "fallback")
    assert fallback.depends_on[0].outcome == "AT_LEAST_ONE_FAILED"


def test_inventory_classifies_all_nodes_agentic() -> None:
    """Job and crawler bodies are agentic gaps this increment; the graph is the deterministic part."""
    pipeline = _sales_pipeline()
    strategies = [item["strategy"] for item in classify_tasks(pipeline)]
    assert strategies == ["agentic"] * 4

    inventory = build_inventory_dict([pipeline], str(_FIXTURE), source="glue")
    assert inventory["source"] == "glue"
    assert inventory["summary"]["agentic_count"] == 4
    assert inventory["summary"]["deterministic_count"] == 0


def test_pipeline_serialises_through_shared_serde() -> None:
    """The IR round-trips through the source-neutral serializer the package phase consumes."""
    pipeline = _sales_pipeline()
    payload = pipeline_to_dict(pipeline)
    assert payload["name"] == "sales_workflow"
    assert payload["schedule"]["quartz_cron_expression"] == "0 0 7 * * ? *"
    assert payload["tags"]["source"] == "glue"
    publish = next(task for task in payload["tasks"] if task["name"] == "publish_sales")
    assert {dep["task_key"] for dep in publish["depends_on"]} == {"sales_crawler", "transform_sales"}


def test_unknown_action_node_is_noted() -> None:
    """An action naming a node absent from the graph is recorded in not_translatable."""
    raw = {
        "Workflow": {
            "Name": "broken",
            "Graph": {
                "Nodes": [
                    {"Type": "JOB", "Name": "known"},
                    {
                        "Type": "TRIGGER",
                        "Name": "t",
                        "TriggerDetails": {
                            "Trigger": {
                                "Type": "CONDITIONAL",
                                "Predicate": {
                                    "Logical": "AND",
                                    "Conditions": [{"JobName": "known", "State": "SUCCEEDED"}],
                                },
                                "Actions": [{"JobName": "ghost"}],
                            }
                        },
                    },
                ]
            },
        }
    }
    pipeline = translate_workflow(parse_workflow(raw, default_name="broken"))
    assert any("ghost" in note.get("issue", "") for note in pipeline.not_translatable)
