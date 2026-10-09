"""The scanner produces costs from recorded responses via an injected transport."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from flowx.sources.adf.profiler import pricing
from flowx.sources.adf.profiler.azure_client import AdfScanner

from .profiler_replay import ReplayTransport

FIXTURES = Path(__file__).parents[1] / "resources" / "azure" / "handauthored"
DEMO = "/factories/demo-factory"
SECOND = "/factories/second-factory"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def _mapping(**overrides) -> dict:
    mapping = {
        "/subscriptions?": _load("subscriptions.json"),
        "/subscriptions/SUB/providers/Microsoft.DataFactory/factories?": _load("factories.json"),
        "/subscriptions/OTHER/providers/Microsoft.DataFactory/factories?": {"value": []},
        "/integrationRuntimes?": _load("integration_runtimes.json"),
        f"{DEMO}/queryPipelineRuns": [_load("pipeline_runs_page1.json"), _load("pipeline_runs_page2.json")],
        f"{SECOND}/queryPipelineRuns": {"value": []},
        "/queryActivityruns": _load("activity_runs.json"),
        "Microsoft.CostManagement/query": {"properties": {"columns": [], "rows": []}},
    }
    mapping.update(overrides)
    return mapping


@pytest.fixture(autouse=True)
def offline_pricing(monkeypatch):
    regions: list[str] = []

    def fake_fetch(region: str):
        regions.append(region)
        return None

    monkeypatch.setattr(pricing, "fetch_live_pricing", fake_fetch)
    return regions


def _scan(mapping: dict, **scanner_kwargs):
    transport = ReplayTransport(mapping)
    scanner = AdfScanner(credential=None, days=90, transport=transport, **scanner_kwargs)
    return scanner.scan(), transport


def test_scan_builds_pipeline_costs_from_recordings():
    result, _ = _scan(_mapping())
    assert result.total_pipeline_runs == 2
    copy_pipeline = next(cost for cost in result.pipeline_costs if cost.pipeline_name == "copy_pipeline")
    assert copy_pipeline.factory_name == "demo-factory"
    assert copy_pipeline.total_runs == 2
    # Per run: 3 billable minutes x 4 DIU x $0.25/DIU-h = $0.05, plus $0.001 activity + $0.001 trigger orchestration.
    assert copy_pipeline.data_movement_cost == pytest.approx(0.1)
    assert copy_pipeline.orchestration_cost == pytest.approx(0.004)
    assert copy_pipeline.total_cost == pytest.approx(0.104)
    assert result.factory_subscriptions == {"demo-factory": "Demo Subscription", "second-factory": "Demo Subscription"}


def test_activity_duration_comes_from_run_record():
    result, _ = _scan(_mapping())
    copy_run = result.activity_runs[0]
    # The record says 180 s (3 billable minutes); output's 60 s must be ignored.
    assert copy_run.execution_cost == pytest.approx((3 / 60) * 4 * 0.25)
    assert copy_run.ir_type == "azure_ir"


def test_pipeline_runs_follow_continuation_token():
    result, transport = _scan(_mapping())
    run_queries = [body for method, url, body in transport.calls if url.find(f"{DEMO}/queryPipelineRuns") >= 0]
    assert len(run_queries) == 2
    assert "continuationToken" not in run_queries[0]
    assert run_queries[1]["continuationToken"] == "tok"
    assert result.total_pipeline_runs == 2


def test_scan_with_no_runs_is_empty_but_valid():
    result, _ = _scan(_mapping(**{f"{DEMO}/queryPipelineRuns": {"value": []}}))
    assert result.total_pipeline_runs == 0
    assert result.pipeline_costs == []
    assert result.activity_runs == []


def test_region_resolved_from_first_factory(offline_pricing):
    result, _ = _scan(_mapping())
    assert offline_pricing == ["eastus"]
    assert result.region == "eastus"
    assert result.pricing_source == "default list rates"


def test_live_pricing_is_labelled_with_region(monkeypatch):
    monkeypatch.setattr(pricing, "fetch_live_pricing", lambda region: pricing.DEFAULT_PRICING)
    result, _ = _scan(_mapping())
    assert result.pricing_source == "Azure Retail Prices API (eastus)"


def test_explicit_pricing_is_labelled_custom(offline_pricing):
    result, _ = _scan(_mapping(), pricing=pricing.DEFAULT_PRICING)
    assert result.pricing_source == "custom"
    assert offline_pricing == []


def test_run_query_forbidden_degrades_gracefully():
    result, _ = _scan(_mapping(**{f"{DEMO}/queryPipelineRuns": 403}))
    assert result.total_pipeline_runs == 0
    assert any("Data Factory Contributor" in warning for warning in result.permission_warnings)


def test_failed_activity_query_skips_that_run_but_counts_it():
    result, _ = _scan(_mapping(**{"/queryActivityruns": 500}))
    assert result.total_pipeline_runs == 2
    assert result.pipeline_costs == []


def test_no_factories_yields_empty_result():
    result, _ = _scan(_mapping(**{"/subscriptions/SUB/providers/Microsoft.DataFactory/factories?": {"value": []}}))
    assert result.total_pipeline_runs == 0
    assert result.region == "eastus"


COST_QUERY = "/subscriptions/SUB/providers/Microsoft.CostManagement/query"


def test_actuals_matched_from_cost_management():
    result, transport = _scan(_mapping(**{COST_QUERY: _load("cost_management.json")}))
    demo_actuals = [actual for actual in result.actual_costs if actual.factory_name == "demo-factory"]
    assert [(actual.cost, actual.meter_subcategory, actual.currency) for actual in demo_actuals] == [
        (12.34, "", "USD"),
        (3.5, "Managed Airflow", "USD"),
    ]
    # Rows for resources that aren't a discovered factory are dropped.
    assert {actual.factory_name for actual in result.actual_costs} == {"demo-factory"}
    cost_bodies = [body for method, url, body in transport.calls if COST_QUERY in url]
    assert len(cost_bodies) == 1  # one query per subscription, not per factory
    assert cost_bodies[0]["type"] == "ActualCost"


def test_actuals_survive_a_window_with_no_runs():
    result, _ = _scan(
        _mapping(**{COST_QUERY: _load("cost_management.json"), f"{DEMO}/queryPipelineRuns": {"value": []}})
    )
    assert result.total_pipeline_runs == 0
    assert result.actual_costs


def test_cost_management_forbidden_yields_empty_actuals():
    result, _ = _scan(_mapping(**{COST_QUERY: 403}))
    assert result.actual_costs == []
    assert result.cost_management_denied == ["SUB"]
    assert any("Cost Management Reader" in warning for warning in result.permission_warnings)


def test_empty_cost_management_is_valid():
    result, _ = _scan(_mapping(**{COST_QUERY: {"properties": {"columns": [], "rows": []}}}))
    assert result.actual_costs == []
    assert result.cost_management_denied == []


def test_factory_match_is_by_substring_as_in_the_original():
    # Known follow-up, ported as-is: "demo-factory" also claims "demo-factory-old"'s billing.
    rows = {
        "properties": {
            "columns": [{"name": "PreTaxCost"}, {"name": "MeterSubcategory"}, {"name": "ResourceId"}],
            "rows": [
                [
                    1.0,
                    "",
                    "/subscriptions/SUB/resourceGroups/RG/providers/Microsoft.DataFactory/factories/demo-factory-old",
                ]
            ],
        }
    }
    result, _ = _scan(_mapping(**{COST_QUERY: rows}))
    assert [(actual.factory_name, actual.currency) for actual in result.actual_costs] == [("demo-factory", "USD")]


def test_subscription_names_recorded_for_the_report():
    result, _ = _scan(_mapping())
    assert result.subscription_names == {"SUB": "Demo Subscription", "OTHER": "Other Subscription"}


def test_uncostable_runs_are_reported_not_silently_dropped():
    result, _ = _scan(_mapping(**{"/queryActivityruns": 500}))
    assert any("2 of 2 pipeline runs could not be costed" in warning for warning in result.permission_warnings)


def test_bad_activity_output_drops_only_that_run():
    bad = {"value": [{"activityType": "Copy", "durationInMs": 60000, "output": {"usedDataIntegrationUnits": None}}]}
    good = _load("activity_runs.json")
    result, _ = _scan(
        _mapping(**{"/pipelineruns/r1/queryActivityruns": bad, "/pipelineruns/r2/queryActivityruns": good})
    )
    copy_pipeline = next(cost for cost in result.pipeline_costs if cost.pipeline_name == "copy_pipeline")
    assert copy_pipeline.total_runs == 1
    assert any("1 of 2 pipeline runs could not be costed" in warning for warning in result.permission_warnings)


def test_no_subscriptions_warns_about_identity():
    result, _ = _scan(_mapping(**{"/subscriptions?": {"value": []}}))
    assert any(
        "No accessible subscriptions" in warning and "tenant" in warning for warning in result.permission_warnings
    )


def test_no_factories_warns():
    empty = {"value": []}
    result, _ = _scan(_mapping(**{"/subscriptions/SUB/providers/Microsoft.DataFactory/factories?": empty}))
    assert any("No Data Factory factories found" in warning for warning in result.permission_warnings)


def test_managed_vnet_notebook_is_priced_at_the_vnet_external_rate():
    # Shapes from a live factory: the run names its IR "managedvnetir (West US)" and billingReference is null.
    result, _ = _scan(
        _mapping(
            **{
                "/integrationRuntimes?": _load("integration_runtimes_managed_vnet.json"),
                "/queryActivityruns": _load("activity_runs_managed_vnet.json"),
            }
        )
    )
    notebook_run = result.activity_runs[0]
    assert notebook_run.ir_type == "managed_vnet_ir"
    # 118.6 s bills as 2 minutes at the managed-VNet external rate ($1.00/h list); rounded to 6 dp.
    assert notebook_run.execution_cost == pytest.approx(
        (2 / 60) * pricing.DEFAULT_PRICING["managed_vnet_ir"]["external_activity_per_hour"], abs=1e-6
    )
