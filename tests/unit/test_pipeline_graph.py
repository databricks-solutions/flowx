"""Unit tests for the pipeline-level Run Pipeline dependency graph."""

from __future__ import annotations

import pytest

from flowx.bundler.pipeline_graph import (
    PipelineCycleError,
    build_pipeline_dependencies,
    connected_components,
    topo_order,
)
from flowx.models.ir import ExecutePipelineActivity, Pipeline, WaitActivity
from flowx.preparer.workflow_preparer import prepare_workflow


def _workflow(name: str, calls: list[str] | None = None):
    """Builds a PreparedWorkflow named *name* that ExecutePipeline-calls each pipeline in *calls*."""
    tasks: list = [WaitActivity(name="wait", task_key="wait", wait_time_seconds=1)]
    for index, callee in enumerate(calls or []):
        tasks.append(
            ExecutePipelineActivity(
                name=f"call_{index}",
                task_key=f"call_{index}",
                pipeline_name=callee,
            )
        )
    return prepare_workflow(Pipeline(name=name, tasks=tasks))


class TestBuildPipelineDependencies:
    def test_extracts_run_pipeline_edges(self):
        workflows = [_workflow("a", ["b"]), _workflow("b", []), _workflow("c", ["a", "b"])]
        deps = build_pipeline_dependencies(workflows)
        assert deps == {"a": {"b"}, "b": set(), "c": {"a", "b"}}

    def test_every_workflow_is_a_key_even_without_deps(self):
        deps = build_pipeline_dependencies([_workflow("solo", [])])
        assert deps == {"solo": set()}

    def test_call_to_pipeline_outside_migration_is_retained(self):
        # 'b' is not a workflow in the migration; the edge is still recorded (deploy-time concern).
        deps = build_pipeline_dependencies([_workflow("a", ["b"])])
        assert deps == {"a": {"b"}}


class TestConnectedComponents:
    def test_splits_disjoint_pipelines(self):
        deps = {"a": {"b"}, "b": set(), "c": set()}
        assert connected_components(deps) == [["a", "b"], ["c"]]

    def test_transitive_calls_form_one_component(self):
        deps = {"a": {"b"}, "b": {"c"}, "c": set()}
        assert connected_components(deps) == [["a", "b", "c"]]

    def test_components_are_deterministic_and_sorted(self):
        deps = {"z": set(), "m": {"n"}, "n": set()}
        assert connected_components(deps) == [["m", "n"], ["z"]]

    def test_edge_to_outside_pipeline_ignored_for_grouping(self):
        deps = {"a": {"external"}}
        assert connected_components(deps) == [["a"]]


class TestTopoOrder:
    def test_callees_before_callers(self):
        deps = {"a": {"b"}, "b": set(), "c": {"a"}}
        order = topo_order(deps)
        assert order.index("b") < order.index("a") < order.index("c")

    def test_independent_nodes_sorted(self):
        assert topo_order({"y": set(), "x": set()}) == ["x", "y"]

    def test_cycle_raises(self):
        with pytest.raises(PipelineCycleError):
            topo_order({"a": {"b"}, "b": {"a"}})
