"""The scanner produces costs from recorded responses via an injected transport."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from flowx.sources.adf.profiler import cost_model, pricing
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
        raise pricing.LivePricingUnavailable("offline")

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


def test_empty_run_page_with_token_keeps_paging():
    run = {"runId": "r1", "pipelineName": "copy_pipeline"}
    pages = [{"value": [], "continuationToken": "a"}, {"value": [run]}]
    result, _ = _scan(_mapping(**{f"{DEMO}/queryPipelineRuns": pages}))
    assert result.total_pipeline_runs == 1


def test_repeated_continuation_token_stops_paging():
    run = {"runId": "r1", "pipelineName": "copy_pipeline"}
    page = {"value": [run], "continuationToken": "same"}
    unexpected = AssertionError("paged past a repeated continuation token")
    result, transport = _scan(_mapping(**{f"{DEMO}/queryPipelineRuns": [page, page, unexpected]}))
    assert len([url for method, url, body in transport.calls if f"{DEMO}/queryPipelineRuns" in url]) == 2
    assert result.total_pipeline_runs == 2


def test_scan_with_no_runs_is_empty_but_valid():
    result, _ = _scan(_mapping(**{f"{DEMO}/queryPipelineRuns": {"value": []}}))
    assert result.total_pipeline_runs == 0
    assert result.pipeline_costs == []
    assert result.activity_runs == []


def test_each_factory_region_is_priced(offline_pricing):
    result, _ = _scan(_mapping())
    assert offline_pricing == ["eastus", "westeurope"]
    assert result.region == "eastus, westeurope"
    assert result.pricing_source == "default list rates"


def test_live_pricing_is_labelled_with_region(monkeypatch):
    monkeypatch.setattr(pricing, "fetch_live_pricing", lambda region: pricing.DEFAULT_PRICING)
    result, _ = _scan(_mapping())
    assert result.pricing_source == "eastus: Azure Retail Prices API; westeurope: Azure Retail Prices API"


def test_pricing_source_names_the_region_that_fell_back(monkeypatch):
    monkeypatch.setattr(pricing, "fetch_live_pricing", _live_only({"eastus": pricing.DEFAULT_PRICING}))
    result, _ = _scan(_mapping())
    assert result.pricing_source == "eastus: Azure Retail Prices API; westeurope: default list rates"


def test_list_rate_fallback_warns_once_per_region(monkeypatch):
    monkeypatch.setattr(pricing, "fetch_live_pricing", _live_only({"eastus": pricing.DEFAULT_PRICING}))
    factories = {
        "value": [
            {"name": "eu-one", "location": "West Europe", "id": FACTORY_ID + "eu-one"},
            {"name": "eu-two", "location": "West Europe", "id": FACTORY_ID + "eu-two"},
            {"name": "us-one", "location": "East US", "id": FACTORY_ID + "us-one"},
        ]
    }
    result, _ = _scan(
        _mapping(
            **{
                "/subscriptions/SUB/providers/Microsoft.DataFactory/factories?": factories,
                "/queryPipelineRuns": {"value": []},
            }
        )
    )
    assert [warning for warning in result.warnings if "list rates" in warning] == [
        "westeurope: couldn't fetch live Azure prices (HTTP 503); used built-in list rates, so estimates may be off"
    ]


def test_live_pricing_adds_no_warning(monkeypatch):
    monkeypatch.setattr(pricing, "fetch_live_pricing", lambda region: pricing.DEFAULT_PRICING)
    result, _ = _scan(_mapping())
    assert result.warnings == []


def test_custom_pricing_adds_no_warning(offline_pricing):
    result, _ = _scan(_mapping(), pricing=pricing.DEFAULT_PRICING)
    assert result.warnings == []


def _live_only(rates_by_region: dict):
    def fetch(region: str):
        if region not in rates_by_region:
            raise pricing.LivePricingUnavailable("HTTP 503")
        return rates_by_region[region]

    return fetch


def test_each_factory_is_priced_with_its_own_regions_rates(monkeypatch):
    west_europe = {card: dict(rates) for card, rates in pricing.DEFAULT_PRICING.items()}
    west_europe["azure_ir"]["data_movement_per_diu_hour"] = 0.5
    rates_by_region = {"eastus": pricing.DEFAULT_PRICING, "westeurope": west_europe}
    monkeypatch.setattr(pricing, "fetch_live_pricing", rates_by_region.get)
    eu_run = {"value": [{"runId": "r3", "pipelineName": "eu_pipeline"}]}
    result, _ = _scan(_mapping(**{f"{SECOND}/queryPipelineRuns": eu_run}))
    data_movement = {cost.pipeline_name: cost.data_movement_cost for cost in result.pipeline_costs}
    assert data_movement == {"copy_pipeline": pytest.approx(0.1), "eu_pipeline": pytest.approx(0.1)}
    eu_activity = next(run for run in result.activity_runs if run.factory_name == "second-factory")
    assert eu_activity.execution_cost == pytest.approx(0.1)


def test_explicit_pricing_is_labelled_custom(offline_pricing):
    result, _ = _scan(_mapping(), pricing=pricing.DEFAULT_PRICING)
    assert result.pricing_source == "custom"
    assert offline_pricing == []


def test_run_query_forbidden_degrades_gracefully():
    result, _ = _scan(_mapping(**{f"{DEMO}/queryPipelineRuns": 403}))
    assert result.total_pipeline_runs == 0
    assert any("Data Factory Contributor" in warning for warning in result.warnings)


def test_activity_runs_follow_continuation_token():
    first_page = {**_load("activity_runs.json"), "continuationToken": "next"}
    second_page = {"value": [{**_load("activity_runs.json")["value"][0], "activityName": "copy2"}]}
    result, transport = _scan(
        _mapping(
            **{
                "/pipelineruns/r1/queryActivityruns": [first_page, second_page],
                "/pipelineruns/r2/queryActivityruns": {"value": []},
            }
        )
    )
    bodies = [body for method, url, body in transport.calls if "/pipelineruns/r1/queryActivityruns" in url]
    assert [body.get("continuationToken") for body in bodies] == [None, "next"]
    assert [run.activity_name for run in result.activity_runs] == ["copy1", "copy2"]


def test_activity_query_forbidden_warns_once_about_the_role():
    result, _ = _scan(_mapping(**{"/queryActivityruns": 403}))
    assert result.pipeline_costs == []
    denied = [warning for warning in result.warnings if "Cannot query activity runs" in warning]
    assert denied == ["demo-factory: Cannot query activity runs (need Data Factory Contributor)"]
    assert any("2 of 2 pipeline runs could not be costed" in warning for warning in result.warnings)


def test_failed_activity_query_skips_that_run_but_counts_it():
    result, _ = _scan(_mapping(**{"/queryActivityruns": 500}))
    assert result.total_pipeline_runs == 2
    assert result.pipeline_costs == []


def test_no_factories_yields_empty_result():
    result, _ = _scan(_mapping(**{"/subscriptions/SUB/providers/Microsoft.DataFactory/factories?": {"value": []}}))
    assert result.total_pipeline_runs == 0
    assert result.region == ""


def test_no_factories_skips_the_price_lookup(offline_pricing):
    result, _ = _scan(_mapping(**{"/subscriptions/SUB/providers/Microsoft.DataFactory/factories?": {"value": []}}))
    assert offline_pricing == []
    assert len(result.warnings) == 1
    assert "No Data Factory factories found" in result.warnings[0]


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


def test_actuals_follow_next_link_pages():
    next_link = "https://management.azure.com/subscriptions/SUB/providers/Microsoft.CostManagement/query?skiptoken=2"
    first_page = _cost_rows((1.0, FACTORY_ID + "demo-factory"))
    first_page["properties"]["nextLink"] = next_link
    second_page = _cost_rows((2.0, FACTORY_ID + "demo-factory"))
    result, transport = _scan(_mapping(**{COST_QUERY: [first_page, second_page]}))
    assert [actual.cost for actual in result.actual_costs] == [1.0, 2.0]
    cost_calls = [(url, body) for method, url, body in transport.calls if COST_QUERY in url]
    assert cost_calls[1][0] == next_link
    assert cost_calls[1][1] == cost_calls[0][1]


def test_repeated_next_link_stops_paging():
    page = _cost_rows((1.0, FACTORY_ID + "demo-factory"))
    page["properties"]["nextLink"] = (
        "https://management.azure.com/subscriptions/SUB/providers/Microsoft.CostManagement/query?skiptoken=2"
    )
    unexpected = AssertionError("paged past a repeated nextLink")
    result, transport = _scan(_mapping(**{COST_QUERY: [page, page, unexpected]}))
    assert len([url for method, url, body in transport.calls if COST_QUERY in url]) == 2
    assert [actual.cost for actual in result.actual_costs] == [1.0, 1.0]


def test_each_cost_page_is_decoded_with_its_own_columns():
    first_page = _cost_rows((1.0, FACTORY_ID + "demo-factory"))
    first_page["properties"]["nextLink"] = (
        "https://management.azure.com/subscriptions/SUB/providers/Microsoft.CostManagement/query?skiptoken=2"
    )
    second_page = {
        "properties": {
            "columns": [{"name": "ResourceId"}, {"name": "MeterSubcategory"}, {"name": "PreTaxCost"}],
            "rows": [[FACTORY_ID + "demo-factory", "", 2.0]],
        }
    }
    result, _ = _scan(_mapping(**{COST_QUERY: [first_page, second_page]}))
    assert [(actual.factory_name, actual.cost) for actual in result.actual_costs] == [
        ("demo-factory", 1.0),
        ("demo-factory", 2.0),
    ]


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
    assert any("Cost Management Reader" in warning for warning in result.warnings)


def test_empty_cost_management_is_valid():
    result, _ = _scan(_mapping(**{COST_QUERY: {"properties": {"columns": [], "rows": []}}}))
    assert result.actual_costs == []
    assert result.cost_management_denied == []


def _cost_rows(*rows: tuple[float, str]) -> dict:
    """A Cost Management response with one row per (cost, factory resource ID)."""
    return {
        "properties": {
            "columns": [{"name": "PreTaxCost"}, {"name": "MeterSubcategory"}, {"name": "ResourceId"}],
            "rows": [[cost, "", resource_id] for cost, resource_id in rows],
        }
    }


FACTORY_ID = "/subscriptions/SUB/resourceGroups/RG/providers/Microsoft.DataFactory/factories/"


def test_factory_name_that_prefixes_another_does_not_claim_its_billing():
    # The original matched by substring, so "demo-factory" also took "demo-factory-old"'s billing.
    result, _ = _scan(_mapping(**{COST_QUERY: _cost_rows((1.0, FACTORY_ID + "demo-factory-old"))}))
    assert result.actual_costs == []


def test_factories_whose_names_prefix_each_other_each_get_their_own_billing():
    # Live case: "flowx-profiler-test-adf" and "flowx-profiler-test-adf-vnet" in one resource group.
    factories = {
        "value": [
            {"name": "test-adf", "location": "West Europe", "id": FACTORY_ID + "test-adf"},
            {"name": "test-adf-vnet", "location": "West Europe", "id": FACTORY_ID + "test-adf-vnet"},
        ]
    }
    # Cost Management lowercases resource IDs.
    rows = _cost_rows((1.0, FACTORY_ID.lower() + "test-adf-vnet"), (2.0, FACTORY_ID.lower() + "test-adf"))
    result, _ = _scan(
        _mapping(
            **{
                "/subscriptions/SUB/providers/Microsoft.DataFactory/factories?": factories,
                "/queryPipelineRuns": {"value": []},
                COST_QUERY: rows,
            }
        )
    )
    assert sorted((actual.factory_name, actual.cost) for actual in result.actual_costs) == [
        ("test-adf", 2.0),
        ("test-adf-vnet", 1.0),
    ]


def test_subscription_names_recorded_for_the_report():
    result, _ = _scan(_mapping())
    assert result.subscription_names == {"SUB": "Demo Subscription", "OTHER": "Other Subscription"}


def test_uncostable_runs_are_reported_not_silently_dropped():
    result, _ = _scan(_mapping(**{"/queryActivityruns": 500}))
    assert any("2 of 2 pipeline runs could not be costed" in warning for warning in result.warnings)


def test_bad_activity_output_drops_only_that_run():
    bad = {"value": [{"activityType": "Copy", "durationInMs": 60000, "output": {"usedDataIntegrationUnits": None}}]}
    good = _load("activity_runs.json")
    result, _ = _scan(
        _mapping(**{"/pipelineruns/r1/queryActivityruns": bad, "/pipelineruns/r2/queryActivityruns": good})
    )
    copy_pipeline = next(cost for cost in result.pipeline_costs if cost.pipeline_name == "copy_pipeline")
    assert copy_pipeline.total_runs == 1
    assert any("1 of 2 pipeline runs could not be costed" in warning for warning in result.warnings)


def test_no_subscriptions_warns_about_identity():
    result, _ = _scan(_mapping(**{"/subscriptions?": {"value": []}}))
    assert any("No accessible subscriptions" in warning and "tenant" in warning for warning in result.warnings)


def test_no_factories_warns():
    empty = {"value": []}
    result, _ = _scan(_mapping(**{"/subscriptions/SUB/providers/Microsoft.DataFactory/factories?": empty}))
    assert any("No Data Factory factories found" in warning for warning in result.warnings)


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


def test_billed_quantities_from_azure_take_precedence_over_duration():
    # Azure IR runs report what Azure billed. Here a 3-minute Lookup is billed as 1 minute of pipeline
    # activity, and a data flow is billed for the 0.354 vCore-hours its cluster used; the duration
    # estimate would be 3 minutes, and 8 vCores x 3 minutes = 0.4 vCore-hours.
    activities = {
        "value": [
            _activity(
                "Lookup",
                180_000,
                _reference("PipelineActivity", [{"duration": 1 / 60, "meterType": "AzureIR", "unit": "Hours"}]),
            ),
            _activity(
                "ExecuteDataFlow",
                174_759,
                _reference("executedataflow", [{"duration": 0.354, "meterType": "Data Flow", "unit": "coreHour"}]),
            ),
        ]
    }
    result, _ = _scan(_mapping(**{"/queryActivityruns": activities}))
    lookup_run, data_flow_run = result.activity_runs[:2]
    lookup_rate = pricing.DEFAULT_PRICING["azure_ir"]["pipeline_activity_per_hour"]
    assert lookup_run.execution_cost == pytest.approx((1 / 60) * lookup_rate, abs=1e-6)
    assert data_flow_run.execution_cost == pytest.approx(0.354 * cost_model.DATAFLOW_VCORE_RATE, abs=1e-6)


def test_billed_bucket_comes_from_azure_not_the_activity_type():
    # An activity type the duration fallback doesn't know would land in the pipeline-activity bucket;
    # Azure says it bills as ExternalActivity, so it lands there.
    reference = _reference("ExternalActivity", [{"duration": 2 / 60, "meterType": "AzureIR", "unit": "Hours"}])
    result, _ = _scan(_mapping(**{"/queryActivityruns": {"value": [_activity("SomeNewActivity", 110_713, reference)]}}))
    pipeline = next(cost for cost in result.pipeline_costs if cost.pipeline_name == "copy_pipeline")
    assert pipeline.pipeline_activity_cost == 0
    external_rate = pricing.DEFAULT_PRICING["azure_ir"]["external_activity_per_hour"]
    assert pipeline.external_cost == pytest.approx(2 * (2 / 60) * external_rate, abs=1e-4)


def _reference(activity_type: str, billable_duration: list[dict]) -> dict:
    return {"activityType": activity_type, "billableDuration": billable_duration}


def _activity(activity_type: str, duration_ms: int, billing_reference: dict) -> dict:
    return {
        "activityName": activity_type.lower(),
        "activityType": activity_type,
        "pipelineName": "copy_pipeline",
        "durationInMs": duration_ms,
        "output": {
            "effectiveIntegrationRuntime": "AutoResolveIntegrationRuntime",
            "billingReference": billing_reference,
        },
    }
