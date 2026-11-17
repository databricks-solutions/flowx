from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

PROFILE_DIR = Path(__file__).parents[1] / "integration" / "azure" / "profile"
SUBSCRIPTIONS_URL = "https://management.azure.com/subscriptions?api-version=2022-12-01"


@pytest.fixture(scope="module")
def redact():
    spec = importlib.util.spec_from_file_location("redact", PROFILE_DIR / "redact.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_committed_capture_has_no_leaks(redact):
    assert redact.find_leaks(PROFILE_DIR / "capture") == []


@pytest.mark.parametrize(
    ("response", "leak"),
    [
        ({"id": "0f8fad5b-d9cb-469f-a165-70867728950e"}, "a GUID"),
        ({"owner": "someone@example.org"}, "an email address"),
        ({"tags": {"Owner": "someone"}}, "a tags object"),
        ({"Authorization": "Bearer abc"}, "an authorization-like key"),
        ({"link": "https://store.blob.core.windows.net/c?sv=2022-11-02&sig=abc"}, "a SAS query parameter"),
        ({"value": [{"subscriptionId": "<SUBSCRIPTION_2>"}]}, "<SUBSCRIPTION_2>, not the scanned subscription"),
        (
            {"properties": {"rows": [[1.0, "", "/subscriptions/<SUBSCRIPTION_3>/resourcegroups/rg"]]}},
            "<SUBSCRIPTION_3>, not the scanned subscription",
        ),
    ],
)
def test_find_leaks_reports_each_kind(redact, tmp_path, response, leak):
    (tmp_path / "capture.json").write_text(json.dumps({"subscription_id": "<SUBSCRIPTION_1>"}))
    exchange = {"body": None, "method": "GET", "response": response, "status": 200, "url": SUBSCRIPTIONS_URL}
    (tmp_path / "exchanges.jsonl").write_text(json.dumps(exchange) + "\n")
    assert any(leak in found for found in redact.find_leaks(tmp_path))


def test_keep_read_fields_keeps_only_the_scanned_subscription(redact):
    response = {
        "count": {"type": "Total", "value": 2},
        "value": [
            {"subscriptionId": "SUB", "displayName": "demo", "tags": {"Owner": "someone"}, "state": "Enabled"},
            {"subscriptionId": "OTHER", "displayName": "someone else's", "tags": {}},
        ],
    }
    exchange = {"body": None, "method": "GET", "response": response, "status": 200, "url": SUBSCRIPTIONS_URL}
    kept = redact.keep_read_fields(exchange, "SUB")
    assert kept["response"] == {"value": [{"subscriptionId": "SUB", "displayName": "demo"}]}


def test_keep_read_fields_keeps_only_what_activity_costing_reads(redact):
    activity = {
        "activityName": "copy1",
        "activityType": "Copy",
        "durationInMs": 180000,
        "input": {"source": {"type": "BlobSource"}},
        "output": {
            "usedDataIntegrationUnits": 4,
            "effectiveIntegrationRuntime": "AutoResolveIntegrationRuntime (West Europe)",
            "billingReference": {
                "activityType": "DataMovement",
                "billableDuration": [{"duration": 0.05, "meterType": "AzureIR", "unit": "DIUHours"}],
            },
            "executionDetails": [{"source": {"type": "AzureBlobStorage"}}],
        },
    }
    url = "https://management.azure.com/subscriptions/SUB/resourceGroups/RG/providers/Microsoft.DataFactory/factories/f/pipelineruns/r1/queryActivityruns"
    exchange = {"body": {}, "method": "POST", "response": {"value": [activity]}, "status": 200, "url": url}
    assert redact.keep_read_fields(exchange, "SUB")["response"] == {
        "value": [
            {
                "activityType": "Copy",
                "durationInMs": 180000,
                "output": {
                    "usedDataIntegrationUnits": 4,
                    "effectiveIntegrationRuntime": "AutoResolveIntegrationRuntime (West Europe)",
                    "billingReference": {
                        "activityType": "DataMovement",
                        "billableDuration": [{"duration": 0.05, "unit": "DIUHours"}],
                    },
                },
            }
        ]
    }


@pytest.mark.parametrize("response", [{"value": []}, {}, None])
def test_keep_read_fields_fails_on_an_endpoint_it_has_no_field_list_for(redact, response):
    exchange = {
        "body": None,
        "method": "GET",
        "response": response,
        "status": 200,
        "url": "https://management.azure.com/x",
    }
    with pytest.raises(ValueError):
        redact.keep_read_fields(exchange, "SUB")


def test_find_leaks_reports_another_subscription_in_a_url(redact, tmp_path):
    (tmp_path / "capture.json").write_text(json.dumps({"subscription_id": "<SUBSCRIPTION_1>"}))
    url = "https://management.azure.com/subscriptions/<SUBSCRIPTION_2>/providers/Microsoft.CostManagement/query"
    exchange = {"body": None, "method": "POST", "response": {}, "status": 200, "url": url}
    (tmp_path / "exchanges.jsonl").write_text(json.dumps(exchange) + "\n")
    assert redact.find_leaks(tmp_path) == ["exchanges.jsonl mentions <SUBSCRIPTION_2>, not the scanned subscription"]
