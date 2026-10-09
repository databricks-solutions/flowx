"""Exact cost math for ADF activity runs, ported from the standalone profiler."""

from __future__ import annotations

import pytest

from flowx.sources.adf.profiler import cost_model as cm

AZURE_IR = {
    "orchestration_per_1000": 1.00,
    "data_movement_per_diu_hour": 0.25,
    "pipeline_activity_per_hour": 0.005,
    "external_activity_per_hour": 0.00025,
}
SHIR = {
    "orchestration_per_1000": 1.50,
    "data_movement_per_hour": 0.10,
    "pipeline_activity_per_hour": 0.005,
    "external_activity_per_hour": 0.00025,
}


def test_billable_duration_has_one_minute_floor():
    # 10 seconds bills as 1 minute = 1/60 h; never zero.
    assert cm.billable_duration_hours(10_000) == pytest.approx(1 / 60)
    assert cm.billable_duration_hours(120_000) == pytest.approx(2 / 60)
    assert cm.billable_duration_hours(0) == pytest.approx(1 / 60)


def test_orchestration_cost_is_per_run():
    assert cm.orchestration_cost(AZURE_IR) == pytest.approx(0.001)


def test_copy_cost_uses_diu_on_azure_ir():
    out = {"usedDataIntegrationUnits": 4}
    cost = cm.activity_execution_cost("Copy", "azure_ir", 60_000, out, AZURE_IR)
    # 1 min = 1/60 h * 4 DIU * $0.25 = 0.016666...
    assert cost == pytest.approx((1 / 60) * 4 * 0.25)


def test_copy_cost_defaults_to_four_diu_when_absent():
    assert cm.activity_execution_cost("Copy", "azure_ir", 60_000, {}, AZURE_IR) == pytest.approx((1 / 60) * 4 * 0.25)


def test_copy_cost_uses_hourly_rate_on_shir():
    cost = cm.activity_execution_cost("Copy", "shir", 120_000, {}, SHIR)
    assert cost == pytest.approx((2 / 60) * 0.10)


def test_duration_comes_from_argument_not_output():
    # A stray durationInMs inside `output` must be ignored; the record's duration wins.
    out = {"durationInMs": 600_000}
    cost = cm.activity_execution_cost("Lookup", "azure_ir", 120_000, out, AZURE_IR)
    assert cost == pytest.approx((2 / 60) * 0.005)


def test_pipeline_activity_cost():
    assert cm.activity_execution_cost("Lookup", "azure_ir", 60_000, {}, AZURE_IR) == pytest.approx((1 / 60) * 0.005)


def test_external_activity_cost():
    cost = cm.activity_execution_cost("DatabricksNotebook", "azure_ir", 60_000, {}, AZURE_IR)
    assert cost == pytest.approx((1 / 60) * 0.00025)


def test_dataflow_cost_uses_min_vcores():
    cost = cm.activity_execution_cost("ExecuteDataFlow", "azure_ir", 60_000, {}, AZURE_IR)
    assert cost == pytest.approx((1 / 60) * cm.DATAFLOW_MIN_VCORES * cm.DATAFLOW_VCORE_RATE)


def test_unknown_activity_falls_back_to_pipeline_rate():
    cost = cm.activity_execution_cost("SomethingNew", "azure_ir", 60_000, {}, AZURE_IR)
    assert cost == pytest.approx((1 / 60) * 0.005)
