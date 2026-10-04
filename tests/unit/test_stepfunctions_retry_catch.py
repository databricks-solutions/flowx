"""Tests for Step Functions Retry and Catch mapping to retries and failure deps."""

from __future__ import annotations

from pathlib import Path

from flowx.sources.stepfunctions.asl import parse_state_machine
from flowx.sources.stepfunctions.loader import load_pipelines
from flowx.sources.stepfunctions.translate import translate_state_machine

_FIXTURE = Path(__file__).parent.parent / "resources" / "stepfunctions" / "retry_catch.json"


def _pipeline():
    return load_pipelines(_FIXTURE)[0]


def _translate(definition: dict) -> object:
    machine = parse_state_machine({"StartAt": "T", "States": {"T": {**definition, "End": True}}}, default_name="m")
    return translate_state_machine(machine).tasks[0]


def test_retry_sets_max_retries_from_highest_attempt_entry() -> None:
    """Multiple Retry entries: the entry with the highest MaxAttempts governs."""
    pipeline = _pipeline()
    extract = next(task for task in pipeline.tasks if task.name == "Extract")
    assert extract.max_retries == 6


def test_retry_sets_interval_millis() -> None:
    """IntervalSeconds * 1000 is stored as min_retry_interval_millis."""
    pipeline = _pipeline()
    extract = next(task for task in pipeline.tasks if task.name == "Extract")
    assert extract.min_retry_interval_millis == 2000


def test_backoff_rate_noted() -> None:
    """BackoffRate has no IR field; it is recorded in not_translatable."""
    pipeline = _pipeline()
    assert any("BackoffRate" in note.get("issue", "") for note in pipeline.not_translatable)


def test_no_retry_leaves_fields_unset() -> None:
    """A Task without Retry has max_retries=None."""
    task = _translate({"Type": "Task", "Resource": "arn:aws:lambda:::function:f"})
    assert task.max_retries is None
    assert task.min_retry_interval_millis is None


def test_catch_handler_translated_as_sibling_task() -> None:
    """HandleError is translated and present in the pipeline tasks."""
    pipeline = _pipeline()
    names = {task.name for task in pipeline.tasks}
    assert "HandleError" in names
    assert "Load" in names


def test_catch_handler_has_failure_dep_on_caught_task() -> None:
    """HandleError depends on Extract with outcome ALL_FAILED."""
    pipeline = _pipeline()
    extract = next(task for task in pipeline.tasks if task.name == "Extract")
    handler = next(task for task in pipeline.tasks if task.name == "HandleError")
    assert handler.depends_on is not None
    failure_deps = [dep for dep in handler.depends_on if dep.outcome == "ALL_FAILED"]
    assert len(failure_deps) == 1
    assert failure_deps[0].task_key == extract.task_key


def test_success_path_unaffected() -> None:
    """Load (success path from Extract) has no failure outcome on its dep."""
    pipeline = _pipeline()
    extract = next(task for task in pipeline.tasks if task.name == "Extract")
    load = next(task for task in pipeline.tasks if task.name == "Load")
    assert load.depends_on is not None
    assert all(dep.outcome is None for dep in load.depends_on)
    assert load.depends_on[0].task_key == extract.task_key


def test_catch_on_main_path_handler_patches_dep() -> None:
    """When the catch handler is the same state as the success path, failure dep is patched in."""
    machine = parse_state_machine(
        {
            "StartAt": "A",
            "States": {
                "A": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:::function:f",
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Notify"}],
                    "Next": "Notify",
                },
                "Notify": {"Type": "Task", "Resource": "arn:aws:states:::sns:publish", "End": True},
            },
        },
        default_name="shared",
    )
    pipeline = translate_state_machine(machine)
    by_name = {task.name: task for task in pipeline.tasks}
    notify = by_name["Notify"]
    assert notify.depends_on is not None
    outcomes = {dep.outcome for dep in notify.depends_on}
    assert "ALL_FAILED" in outcomes


def test_catch_handler_on_separate_path_not_duplicated() -> None:
    """A catch handler not on the success path appears exactly once in tasks."""
    pipeline = _pipeline()
    handler_tasks = [task for task in pipeline.tasks if task.name == "HandleError"]
    assert len(handler_tasks) == 1
