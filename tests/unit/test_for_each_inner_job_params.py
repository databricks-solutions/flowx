"""Unit tests for orchestra.bundler.inner_job_params.

Covers C-06 (VAREX-004): variable references in a ForEach inner-job body
must not produce undeclared inner-job parameters.  When the parent job has
a known setter task for the variable, the inner job receives a
``{{tasks.<setter>.values.<var>}}`` reference; when no setter is known the
name still surfaces as an inner parameter (legacy fallback so test-only
flows that omit the mapping continue to work).
"""

from __future__ import annotations

from orchestra.bundler.inner_job_params import collect_inner_job_params


def _notebook_task(base_parameters: dict[str, str]) -> dict[str, object]:
    return {
        "task_key": "inner_nb",
        "notebook_task": {
            "notebook_path": "/Shared/inner",
            "base_parameters": base_parameters,
        },
    }


class TestVariableTaskKeysRouting:
    def test_variable_with_known_setter_routes_via_task_value(self):
        """C-06: a variable referenced inside the inner job with a known
        parent-side setter is NOT declared as an inner job parameter."""
        inner_tasks = [_notebook_task({"continue": "@variables('continue')"})]
        parameters, job_parameters = collect_inner_job_params(
            inner_tasks,
            variable_task_keys={"continue": "_init_continue"},
        )
        # No inner parameter declared for `continue` -- the parent passes
        # the task-value reference through job_parameters instead.
        param_names = {p["name"] for p in parameters}
        assert "continue" not in param_names
        assert job_parameters["continue"] == "{{tasks._init_continue.values.continue}}"

    def test_variable_without_setter_falls_back_to_parent_job_parameter(self):
        """Legacy fallback for tests that don't supply a setter map."""
        inner_tasks = [_notebook_task({"continue": "@variables('continue')"})]
        parameters, job_parameters = collect_inner_job_params(inner_tasks)
        param_names = {p["name"] for p in parameters}
        # Without variable_task_keys we still emit the (broken) job.parameters
        # reference so prior behaviour is preserved when callers don't opt in.
        assert "continue" in param_names
        assert job_parameters["continue"] == "{{job.parameters.continue}}"

    def test_pipeline_parameter_still_uses_job_parameters_ref(self):
        """C-06 only redirects variables -- pipeline parameters still flow
        via the inner job's parameter declarations as before."""
        inner_tasks = [_notebook_task({"env": "@pipeline().parameters.env"})]
        parameters, job_parameters = collect_inner_job_params(
            inner_tasks,
            variable_task_keys={"continue": "_init_continue"},  # unrelated var
        )
        param_names = {p["name"] for p in parameters}
        assert "env" in param_names
        assert job_parameters["env"] == "{{job.parameters.env}}"

    def test_multi_child_for_each_threads_variable_task_keys(self, monkeypatch):
        """CF3-006: ForEach preparer's multi-child path must thread
        variable_task_keys into collect_inner_job_params just like the
        single-child escalation path does, so the same parent->setter map
        is honoured regardless of how many children the ForEach has.

        Asserts the kwarg is forwarded by intercepting collect_inner_job_params.
        """
        from orchestra.models.ir import ForEachActivity, NotebookActivity
        from orchestra.preparer.activity_preparers import for_each as for_each_module
        from orchestra.preparer.workflow_preparer import prepare_activity

        def _base(name: str, key: str) -> dict[str, object]:
            return {
                "name": name,
                "task_key": key,
                "description": None,
                "timeout_seconds": None,
                "max_retries": None,
                "min_retry_interval_millis": None,
                "depends_on": None,
                "cluster": None,
            }

        captured: list[dict[str, str] | None] = []
        from orchestra.bundler import inner_job_params as ijp_module

        original = ijp_module.collect_inner_job_params

        def _spy(tasks, *, raw_ir_tasks=None, variable_task_keys=None):
            captured.append(variable_task_keys)
            return original(tasks, raw_ir_tasks=raw_ir_tasks, variable_task_keys=variable_task_keys)

        monkeypatch.setattr(for_each_module, "collect_inner_job_params", _spy)

        nb_a = NotebookActivity(
            **_base("InnerA", "inner_a"),
            notebook_path="/Shared/a",
            base_parameters={"continue": "@variables('continue')"},
        )
        nb_b = NotebookActivity(
            **_base("InnerB", "inner_b"),
            notebook_path="/Shared/b",
            base_parameters={"continue": "@variables('continue')"},
        )
        loop = ForEachActivity(
            **_base("Loop", "loop"),
            items_expression="@output.value",
            inner_activities=[nb_a, nb_b],
            concurrency=2,
        )
        prepared = prepare_activity(
            loop,
            variable_task_keys={"continue": "_init_continue"},
        )
        # Multi-child escalation -> exactly one collect call from for_each preparer.
        for_each_call = next((m for m in captured if m and "continue" in m), None)
        assert for_each_call is not None, "variable_task_keys must be forwarded"
        assert for_each_call["continue"] == "_init_continue"
        # Multi-child path -> inner_workflows populated.
        assert prepared.inner_workflows

    def test_mixed_variable_and_pipeline_param_payload(self):
        """A base_parameters block with both a variable and a pipeline param."""
        inner_tasks = [
            _notebook_task(
                {
                    "ctx_continue": "@variables('continue')",
                    "ctx_env": "@pipeline().parameters.env",
                }
            )
        ]
        parameters, job_parameters = collect_inner_job_params(
            inner_tasks,
            variable_task_keys={"continue": "_init_continue"},
        )
        param_names = {p["name"] for p in parameters}
        # Only the pipeline parameter is declared on the inner job.
        assert "continue" not in param_names
        assert "env" in param_names
        assert job_parameters["continue"] == "{{tasks._init_continue.values.continue}}"
        assert job_parameters["env"] == "{{job.parameters.env}}"
