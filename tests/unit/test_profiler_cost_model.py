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


def _billed(activity_type: str, *entries: tuple[float, str, str]) -> dict:
    """An activity output carrying Azure's billingReference (shapes from a live Azure IR factory)."""
    return {
        "billingReference": {
            "activityType": activity_type,
            "billableDuration": [
                {"duration": duration, "meterType": meter_type, "unit": unit} for duration, meter_type, unit in entries
            ],
        }
    }


def test_billed_copy_uses_azures_diu_hours():
    output = _billed("DataMovement", (0.0667, "AzureIR", "DIUHours"))
    assert cm.billed_execution(output, AZURE_IR) == ("data_movement", pytest.approx(0.0667 * 0.25))


def test_billed_self_hosted_copy_uses_the_hourly_rate():
    output = _billed("DataMovement", (0.05, "SelfhostedIR", "Hours"))
    assert cm.billed_execution(output, SHIR) == ("data_movement", pytest.approx(0.05 * 0.10))


def test_billed_pipeline_activity_uses_azures_hours():
    output = _billed("PipelineActivity", (1 / 60, "AzureIR", "Hours"))
    assert cm.billed_execution(output, AZURE_IR) == ("pipeline_activity", pytest.approx((1 / 60) * 0.005))


def test_billed_external_activity_uses_azures_hours():
    # WebActivity and DatabricksJob both bill as ExternalActivity.
    output = _billed("ExternalActivity", (2 / 60, "AzureIR", "Hours"))
    assert cm.billed_execution(output, AZURE_IR) == ("external", pytest.approx((2 / 60) * 0.00025))


def test_billed_data_flow_uses_the_vcore_hours_azure_reports():
    # Azure reports the vCore-hours the cluster actually used, not 8 vCores x the activity's duration.
    output = _billed("executedataflow", (0.354, "Data Flow", "coreHour"))
    assert cm.billed_execution(output, AZURE_IR) == ("external", pytest.approx(0.354 * cm.DATAFLOW_VCORE_RATE))


def test_billed_entries_are_summed():
    output = _billed("PipelineActivity", (1 / 60, "AzureIR", "Hours"), (1 / 60, "AzureIR", "Hours"))
    assert cm.billed_execution(output, AZURE_IR) == ("pipeline_activity", pytest.approx((2 / 60) * 0.005))


@pytest.mark.parametrize(
    "output",
    [
        {},  # managed-VNet runs carry no billingReference
        {"billingReference": None},
        {"billingReference": {"activityType": "ExternalActivity", "billableDuration": []}},
        _billed("SomethingNew", (1.0, "AzureIR", "Hours")),
        _billed("PipelineActivity", (1.0, "AzureIR", "Fortnights")),
        _billed("DataMovement", (1.0, "SelfhostedIR", "Hours")),  # no hourly data-movement rate on Azure IR
    ],
)
def test_without_a_usable_billing_reference_there_is_no_billed_cost(output):
    assert cm.billed_execution(output, AZURE_IR) is None


def test_databricks_job_is_an_external_activity_when_priced_by_duration():
    # Azure bills DatabricksJob as ExternalActivity; the duration fallback (managed VNet) must agree.
    cost = cm.activity_execution_cost("DatabricksJob", "azure_ir", 60_000, {}, AZURE_IR)
    assert cost == pytest.approx((1 / 60) * AZURE_IR["external_activity_per_hour"])
