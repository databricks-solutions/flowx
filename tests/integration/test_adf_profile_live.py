"""Live checks for `flowx profile --source adf`: the public price list, and a scan of the ephemeral factory."""

from __future__ import annotations

import csv
import os
import subprocess
import sys
from pathlib import Path

import pytest
import requests

from flowx.sources.adf.profile import pricing
from flowx.sources.adf.profile.core import AdfScanner

pytestmark = pytest.mark.integration

LIVE_FACTORY_VARIABLES = ("AZURE_SUBSCRIPTION_ID", "AZURE_RESOURCE_GROUP", "ADF_FACTORY_NAME", "ADF_CASE_LOCATION")
SUBSCRIPTION_ID = "00000000-0000-0000-0000-000000000001"
EXPECTED_METERS = {
    ("azure_ir", "orchestration_per_1000"): "Cloud Orchestration Activity Run",
    ("azure_ir", "data_movement_per_diu_hour"): "Cloud Data Movement",
    ("azure_ir", "pipeline_activity_per_hour"): "Cloud Pipeline Activity",
    ("azure_ir", "external_activity_per_hour"): "Cloud External Pipeline Activity",
    ("shir", "orchestration_per_1000"): "Self Hosted Orchestration Activity Run",
    ("shir", "data_movement_per_hour"): "Self Hosted Data Movement",
    ("shir", "pipeline_activity_per_hour"): "Self Hosted Pipeline Activity",
    ("shir", "external_activity_per_hour"): "Self Hosted External Pipeline Activity",
    ("managed_vnet_ir", "orchestration_per_1000"): "Azure Managed VNET Orchestration Activity Run",
    ("managed_vnet_ir", "data_movement_per_diu_hour"): "Azure Managed VNET Data Movement",
    ("managed_vnet_ir", "pipeline_activity_per_hour"): "Azure Managed VNET Pipeline Activity",
    ("managed_vnet_ir", "external_activity_per_hour"): "Azure Managed VNET External Pipeline Activity",
}


def test_live_retail_prices_cover_every_meter_and_price_a_scan(replay_transport, monkeypatch):
    pages: list[dict] = []
    real_get = requests.get

    def recording_get(*args, **kwargs):
        response = real_get(*args, **kwargs)
        pages.append(response.json())
        return response

    monkeypatch.setattr(requests, "get", recording_get)
    rates = pricing.fetch_live_pricing("eastus")

    assert {(card, rate) for card, meters in rates.items() for rate in meters} == set(EXPECTED_METERS)
    items = [item for page in pages for item in page.get("Items", [])]
    tagged = pricing.parse_retail_prices(
        [{**item, "retailPrice": item.get("retailPrice", 0) * (1000 + index)} for index, item in enumerate(items)]
    )
    assert tagged is not None
    for (card, rate), meter in EXPECTED_METERS.items():
        matches = [
            index
            for index, item in enumerate(items)
            if item.get("productName") == "Azure Data Factory v2" and item.get("meterName") == meter
        ]
        assert len(matches) == 1, meter
        live_price = items[matches[0]]["retailPrice"]
        assert rates[card][rate] == live_price, meter
        assert tagged[card][rate] == pytest.approx(live_price * (1000 + matches[0])), meter

    monkeypatch.setattr(pricing, "fetch_live_pricing", lambda region: rates)

    factory_id = f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/rg/providers/Microsoft.DataFactory/factories/f"
    transport = replay_transport(
        {
            "/subscriptions?": {"value": [{"subscriptionId": SUBSCRIPTION_ID, "displayName": "live-prices"}]},
            "/factories?": {"value": [{"id": factory_id, "name": "f", "location": "East US"}]},
            "/integrationRuntimes?": {"value": []},
            "/queryPipelineRuns": {"value": []},
            "Microsoft.CostManagement/query": {"properties": {"columns": [], "rows": []}},
        }
    )
    result = AdfScanner(credential=None, days=1, transport=transport).scan()
    assert result.pricing_source == "Azure Retail Prices API (eastus)"
    assert result.warnings == []


def test_profile_scans_the_ephemeral_factory(tmp_path):
    missing = [name for name in LIVE_FACTORY_VARIABLES if not os.environ.get(name)]
    if missing and os.environ.get("GITHUB_ACTIONS"):
        pytest.fail(f"the live workflow didn't set {', '.join(missing)}")
    if missing:
        pytest.skip(f"needs the live workflow's {', '.join(missing)} (set by its Azure login and export steps)")
    subscription_id, resource_group, factory_name, location = (os.environ[name] for name in LIVE_FACTORY_VARIABLES)
    output_dir = Path(os.environ["RUNNER_TEMP"]) / "flowx-profile" if os.environ.get("RUNNER_TEMP") else tmp_path

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "flowx.adapter",
            "profile",
            "--source",
            "adf",
            "--output-dir",
            str(output_dir),
            "--days",
            "1",
            "--subscription-id",
            subscription_id,
            "--resource-group",
            resource_group,
            "--factory-name",
            factory_name,
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": "src"},
        timeout=600,
    )
    assert completed.returncode == 0, completed.stderr

    tco = output_dir / "metadata" / "tco"
    assert (tco / "cost_comparison.md").is_file()
    with (tco / "cost_comparison.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["factory"] for row in rows if row["level"] == "factory"] == [factory_name]

    assert f"(Azure Retail Prices API ({location.lower().replace(' ', '')}))." in completed.stdout

    warnings = [
        line.removeprefix("warning: ") for line in completed.stderr.splitlines() if line.startswith("warning: ")
    ]
    cost_management_denied = (
        f"Subscription {subscription_id[:8]}...: Cost Management access denied "
        "(need Billing Reader or Cost Management Reader on this subscription)"
    )
    assert warnings == [cost_management_denied], completed.stderr
