"""The profiler result models are slotted, keyword-only dataclasses."""

from __future__ import annotations

import pytest

from flowx.sources.adf.profiler import models


def test_pipeline_cost_is_slotted_and_kw_only():
    row = models.PipelineCost(
        factory_name="f",
        pipeline_name="p",
        total_runs=3,
        orchestration_cost=0.003,
        data_movement_cost=0.0,
        pipeline_activity_cost=0.0,
        external_cost=0.0,
        total_cost=0.003,
    )
    assert row.total_runs == 3
    with pytest.raises(TypeError):
        models.PipelineCost("f", "p", 3, 0, 0, 0, 0, 0)  # type: ignore[misc]  # positional rejected (kw_only)
    assert not hasattr(row, "__dict__")  # slots


def test_profile_result_defaults_to_empty_collections():
    result = models.ProfileResult(region="eastus", pricing_source="default list rates", days=90, total_pipeline_runs=0)
    assert result.pipeline_costs == []
    assert result.actual_costs == []
    assert result.activity_runs == []
    assert result.factory_subscriptions == {}
    assert result.cost_management_denied == []
    assert result.permission_warnings == []


def test_profile_result_collections_are_not_shared_between_instances():
    first = models.ProfileResult(region="eastus", pricing_source="x", days=1, total_pipeline_runs=0)
    second = models.ProfileResult(region="eastus", pricing_source="x", days=1, total_pipeline_runs=0)
    first.permission_warnings.append("denied")
    assert second.permission_warnings == []
