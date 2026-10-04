"""Unit tests for the AWS Step Functions source: parsing, translation, inventory."""

from __future__ import annotations

import json
from pathlib import Path

from flowx.ir_serde import pipeline_to_dict
from flowx.models.ir import (
    ForEachActivity,
    IfConditionActivity,
    PlaceholderActivity,
    RunJobActivity,
    SetVariableActivity,
    WaitActivity,
)
from flowx.sources.inventory import build_inventory_dict, classify_tasks
from flowx.sources.stepfunctions.asl import parse_state_machine
from flowx.sources.stepfunctions.loader import load_pipelines
from flowx.sources.stepfunctions.translate import translate_state_machine

_FIXTURE = Path(__file__).parent.parent / "resources" / "stepfunctions" / "retail_etl.json"


def _retail_pipeline():
    """Returns the single translated pipeline from the retail_etl fixture."""
    pipelines = load_pipelines(_FIXTURE)
    assert len(pipelines) == 1
    return pipelines[0]


def test_parse_describe_wrapper_unwraps_definition() -> None:
    """A describe-state-machine payload is unwrapped and named from its arn."""
    raw = {
        "stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:orders",
        "definition": json.dumps({"StartAt": "Only", "States": {"Only": {"Type": "Succeed"}}}),
    }
    machine = parse_state_machine(raw, default_name="ignored")
    assert machine.name == "orders"
    assert machine.start_at == "Only"
    assert set(machine.states) == {"Only"}


def test_top_level_task_types_and_order() -> None:
    """The flat ASL graph becomes the expected top-level IR task sequence."""
    pipeline = _retail_pipeline()
    assert pipeline.name == "retail_etl"
    kinds = [type(task).__name__ for task in pipeline.tasks]
    assert kinds == ["PlaceholderActivity", "IfConditionActivity", "RunJobActivity", "WaitActivity"]


def test_glue_task_is_agentic_placeholder() -> None:
    """A Glue Task state becomes an agentic placeholder carrying its raw ASL body."""
    pipeline = _retail_pipeline()
    ingest = pipeline.tasks[0]
    assert isinstance(ingest, PlaceholderActivity)
    assert ingest.original_type == "Task:glue"
    assert ingest.raw_definition is not None


def test_nested_state_machine_task_is_run_job() -> None:
    """A states:startExecution Task becomes a RunJob targeting the nested machine name."""
    pipeline = _retail_pipeline()
    run_job = pipeline.tasks[2]
    assert isinstance(run_job, RunJobActivity)
    assert run_job.job_name == "publish_marts"


def test_choice_nests_branches_and_computes_join() -> None:
    """The Choice reduces to a condition whose branches reconverge at PublishMarts."""
    pipeline = _retail_pipeline()
    condition = pipeline.tasks[1]
    assert isinstance(condition, IfConditionActivity)
    assert (condition.op, condition.left, condition.right) == (">", "rowCount", "0")

    assert [type(task).__name__ for task in condition.if_true_activities] == ["ForEachActivity"]
    assert [type(task).__name__ for task in condition.if_false_activities] == ["SetVariableActivity"]

    for_each = condition.if_true_activities[0]
    assert isinstance(for_each, ForEachActivity)
    assert for_each.items_expression == "partitions"
    assert for_each.concurrency == 4
    assert [type(task).__name__ for task in for_each.inner_activities] == ["PlaceholderActivity"]

    no_data = condition.if_false_activities[0]
    assert isinstance(no_data, SetVariableActivity)


def test_dependency_edges_follow_transitions() -> None:
    """Top-level depends_on edges mirror the ASL Next transitions; the join rejoins the condition."""
    pipeline = _retail_pipeline()
    by_name = {task.name: task for task in pipeline.tasks}
    assert by_name["IngestRaw"].depends_on is None
    assert [d.task_key for d in by_name["CheckRowCount"].depends_on] == [by_name["IngestRaw"].task_key]
    assert [d.task_key for d in by_name["PublishMarts"].depends_on] == [by_name["CheckRowCount"].task_key]
    assert [d.task_key for d in by_name["Settle"].depends_on] == [by_name["PublishMarts"].task_key]


def test_wait_seconds_translated() -> None:
    """A Wait state's literal Seconds maps to wait_time_seconds."""
    pipeline = _retail_pipeline()
    settle = pipeline.tasks[3]
    assert isinstance(settle, WaitActivity)
    assert settle.wait_time_seconds == 30


def test_inventory_counts_deterministic_and_agentic() -> None:
    """Classification descends into control-flow bodies and splits the two Glue tasks out as agentic."""
    pipeline = _retail_pipeline()
    strategies = [item["strategy"] for item in classify_tasks(pipeline)]
    assert strategies.count("agentic") == 2
    assert strategies.count("deterministic") == 5

    inventory = build_inventory_dict([pipeline], str(_FIXTURE), source="stepfunctions")
    assert inventory["source"] == "stepfunctions"
    assert inventory["summary"]["agentic_count"] == 2
    assert inventory["summary"]["deterministic_count"] == 5


def test_pipeline_serialises_through_shared_serde() -> None:
    """The IR round-trips through the source-neutral serializer the package phase consumes."""
    pipeline = _retail_pipeline()
    payload = pipeline_to_dict(pipeline)
    assert payload["name"] == "retail_etl"
    assert [task["type"] for task in payload["tasks"]] == [
        "PlaceholderActivity",
        "IfConditionActivity",
        "RunJobActivity",
        "WaitActivity",
    ]
    condition = payload["tasks"][1]
    assert condition["if_true_activities"][0]["type"] == "ForEachActivity"


def test_cyclic_graph_falls_back_to_placeholder() -> None:
    """A cycle is routed to an agentic placeholder instead of looping forever."""
    machine = parse_state_machine(
        {
            "StartAt": "A",
            "States": {
                "A": {"Type": "Pass", "Result": {"x": 1}, "Next": "B"},
                "B": {"Type": "Pass", "Result": {"x": 2}, "Next": "A"},
            },
        },
        default_name="cyclic",
    )
    pipeline = translate_state_machine(machine)
    assert any(isinstance(task, PlaceholderActivity) for task in pipeline.tasks)
    assert any("cyclic" in note["issue"] for note in pipeline.not_translatable)


def _translate_one(definition: dict) -> object:
    """Translates a single-Task state machine and returns its one task."""
    machine = parse_state_machine({"StartAt": "T", "States": {"T": {**definition, "End": True}}}, default_name="m")
    return translate_state_machine(machine).tasks[0]


def test_glue_start_workflow_run_becomes_run_job() -> None:
    """A Task starting a Glue workflow becomes a run-job targeting the normalised workflow key."""
    task = _translate_one(
        {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:glue:startWorkflowRun.sync",
            "Parameters": {"Name": "Sales-Workflow"},
        }
    )
    assert isinstance(task, RunJobActivity)
    assert task.job_name == "sales_workflow"


def test_glue_start_workflow_run_without_literal_name_is_placeholder() -> None:
    """A dynamic workflow name (Name.$) cannot be wired, so it stays an agentic placeholder."""
    machine = parse_state_machine(
        {
            "StartAt": "T",
            "States": {
                "T": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:glue:startWorkflowRun",
                    "Parameters": {"Name.$": "$.workflowName"},
                    "End": True,
                }
            },
        },
        default_name="m",
    )
    pipeline = translate_state_machine(machine)
    assert isinstance(pipeline.tasks[0], PlaceholderActivity)
    assert any("startWorkflowRun has no literal Name" in note["issue"] for note in pipeline.not_translatable)


def test_glue_single_job_task_stays_placeholder() -> None:
    """A plain Glue job start (not a workflow) remains an agentic placeholder."""
    task = _translate_one(
        {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {"JobName": "ingest"},
        }
    )
    assert isinstance(task, PlaceholderActivity)
    assert task.original_type == "Task:glue"
