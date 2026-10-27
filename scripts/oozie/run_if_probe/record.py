"""Runs the run_if probe jobs on a workspace and records what Lakeflow actually did.

Usage: python scripts/oozie/run_if_probe/record.py --profile <profile>
Deploy first with: databricks bundle deploy -t dev --profile <profile> (from this directory).
Rewrites the whole semantics file, so every key it holds is derived here.
"""

from __future__ import annotations

import argparse
import datetime
import json
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import OperationFailed
from databricks.sdk.service.jobs import Run

_OUTPUT = Path(__file__).resolve().parents[3] / "src" / "flowx" / "sources" / "oozie" / "lakeflow_run_semantics.json"
# The recorded file is checked into a public repo, so it names the kind of workspace, never its URL or ID.
_WORKSPACE_LABEL = "internal dev workspace (AWS, serverless jobs)"
# A run with a failed task reports INTERNAL_ERROR at run level (result_state FAILED), so it counts as finished.
_FINISHED_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def _describe_tasks(workspace: WorkspaceClient, run: Run) -> dict[str, dict[str, str]]:
    """Per-task life-cycle state, result state, condition outcome, and echoed notebook output."""
    described: dict[str, dict[str, str]] = {}
    for task in run.tasks or []:
        state = task.state
        entry = {
            "life_cycle_state": state.life_cycle_state.value if state and state.life_cycle_state else "NONE",
            "result_state": state.result_state.value if state and state.result_state else "NONE",
        }
        if task.condition_task is not None and entry["result_state"] == "SUCCESS":
            if task.condition_task.outcome is None:
                raise RuntimeError(f"condition task {task.task_key} terminated without an outcome")
            entry["condition_outcome"] = str(task.condition_task.outcome)
        if task.task_key.startswith("echo_") and entry["result_state"] == "SUCCESS":
            output = workspace.jobs.get_run_output(run_id=task.run_id).notebook_output
            entry["echo"] = output.result if output else ""
        described[task.task_key] = entry
    return described


def _run(workspace: WorkspaceClient, job_name_suffix: str) -> tuple[int, dict[str, dict[str, str]], str]:
    job = next(job for job in workspace.jobs.list() if (job.settings.name or "").endswith(job_name_suffix))
    waiter = workspace.jobs.run_now(job_id=job.job_id)
    try:
        waiter.result()
    except OperationFailed:
        pass  # a run that ends FAILED raises; the run itself is still what we want to inspect
    run = workspace.jobs.get_run(waiter.run_id)
    life_cycle_state = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "NONE"
    if life_cycle_state not in _FINISHED_STATES:
        raise RuntimeError(
            f"run {waiter.run_id} of {job_name_suffix} ended in {life_cycle_state}, expected a finished state"
        )
    overall = run.state.result_state.value if run.state.result_state else "NONE"
    return waiter.run_id, _describe_tasks(workspace, run), overall


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    arguments = parser.parse_args()
    workspace = WorkspaceClient(profile=arguments.profile)

    failures_run, failures, failures_result = _run(workspace, "probe-failures")
    handler_run, handler_tasks, handler_result = _run(workspace, "probe-result-with-handler")
    leaf_run, leaf_tasks, leaf_result = _run(workspace, "probe-leaf-upstream-failed")
    conditions_run, conditions, _ = _run(workspace, "probe-conditions")
    gates_run, gates, gates_result = _run(workspace, "probe-result-state-gates")
    runtime_run, runtime, _ = _run(workspace, "probe-string-greater-runtime")

    raw = {
        "failures": failures,
        "failures_run_result_state": failures_result,
        "handler": handler_tasks,
        "handler_run_result_state": handler_result,
        "leaf_upstream_failed": leaf_tasks,
        "leaf_upstream_failed_run_result_state": leaf_result,
        "conditions": conditions,
        "result_state_gates": gates,
        "result_state_gates_run_result_state": gates_result,
        "string_greater_runtime": runtime,
        "condition_outcomes_source": "read programmatically from jobs.get_run tasks[*].condition_task.outcome",
        "string_greater_than": (
            "GREATER_THAN with literal operands 'abc'/'abd' is rejected by jobs/create (400 INVALID_PARAMETER_VALUE: "
            'Numeric operand "left" is not a valid number), so it errors before any run'
        ),
    }
    print(json.dumps(raw, indent=2))

    runtime_task = runtime["string_greater_runtime"]
    if runtime_task["result_state"] == "SUCCESS":
        runtime_measurement = f"condition task succeeded with outcome {runtime_task['condition_outcome']}"
    else:
        runtime_measurement = f"condition task ended {runtime_task['life_cycle_state']}/{runtime_task['result_state']}"

    literals = {
        "SUCCESS": gates["echo_ok_task"]["echo"],
        "FAILED": gates["echo_fail_task"]["echo"],
        "UPSTREAM_FAILED": gates["echo_y_skipped"]["echo"],
        "EXCLUDED": gates.get("echo_excluded_task", {}).get("echo"),
    }
    semantics = {
        "upstream_failed_counts_as_failed": failures["handler_on_a2_failed"]["result_state"] == "SUCCESS",
        "excluded_counts_as_done_for_all_done": failures["all_done_after_excluded"]["result_state"] == "SUCCESS",
        "condition_outcome_dependency_counts_as_success": failures["merge_after_condition"]["result_state"]
        == "SUCCESS",
        "leaf_upstream_failed_fails_run": leaf_result == "FAILED",
        "failed_non_leaf_with_successful_leaves": handler_result,
        "condition_numeric_string_equal": conditions["numeric_equal"]["condition_outcome"].lower() == "true",
        "condition_string_greater_than_rejected_at_create": True,
        "condition_string_greater_than_runtime": runtime_measurement,
        "result_state_reference_supported": all(
            gates[key].get("echo") is not None and "{{" not in gates[key]["echo"]
            for key in ("echo_ok_task", "echo_fail_task", "echo_y_skipped")
        ),
        "result_state_literals": literals,
        "all_done_runs_after_upstream_failed": gates["echo_y_skipped"]["result_state"] == "SUCCESS"
        and gates["cond_y_skipped"]["life_cycle_state"] == "TERMINATED",
        "all_done_runs_after_failed": gates["echo_fail_task"]["result_state"] == "SUCCESS",
        "condition_on_result_state_distinguishes_failed_from_upstream_failed": gates["after_cond_fail"]["result_state"]
        == "SUCCESS"
        and gates["after_cond_y"]["result_state"] == "EXCLUDED",
        "result_state_gates_run_result_state": gates_result,
        "result_state_gates_run_id": gates_run,
        "probe_run_ids": [failures_run, handler_run, leaf_run, conditions_run, gates_run, runtime_run],
        "recorded_on": datetime.date.today().isoformat(),
        "workspace_host": _WORKSPACE_LABEL,
        "notes": {
            "condition_numeric_string_equal": "false means EQUAL_TO '05' vs '5' is false, i.e. no numeric coercion",
            "result_state_literals.EXCLUDED": (
                "unobservable: ALL_DONE dependents of an EXCLUDED task are themselves EXCLUDED; "
                "TIMEDOUT and CANCELED are unmeasured"
            ),
        },
        "raw_task_states": raw,
    }
    _OUTPUT.write_text(json.dumps(semantics, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {_OUTPUT}")


if __name__ == "__main__":
    main()
