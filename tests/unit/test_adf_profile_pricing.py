"""Pricing rate cards and the pure Retail-Prices parser."""

from __future__ import annotations

import sys
import types

import pytest

from flowx.sources.adf.profile import azure_client, pricing


def test_default_pricing_has_three_rate_cards():
    assert set(pricing.DEFAULT_PRICING) == {"azure_ir", "shir", "managed_vnet_ir"}
    assert pricing.DEFAULT_PRICING["azure_ir"]["orchestration_per_1000"] == 1.00


def test_parse_retail_prices_maps_meters():
    items = [
        {
            "productName": "Azure Data Factory v2",
            "meterName": "Cloud Orchestration Activity Run",
            "retailPrice": 1.0,
            "unitOfMeasure": "1K",
        },
        {
            "productName": "Azure Data Factory v2",
            "meterName": "Cloud Data Movement",
            "retailPrice": 0.25,
            "unitOfMeasure": "1 Hour",
        },
        {
            "productName": "Azure Data Factory v2",
            "meterName": "Cloud Pipeline Activity",
            "retailPrice": 0.005,
            "unitOfMeasure": "1 Hour",
        },
    ]
    parsed = pricing.parse_retail_prices(items)
    assert parsed is not None
    assert parsed["azure_ir"]["orchestration_per_1000"] == 1.0
    assert parsed["azure_ir"]["data_movement_per_diu_hour"] == 0.25


def test_parse_retail_prices_falls_back_per_meter():
    items = [
        {
            "productName": "Azure Data Factory v2",
            "meterName": "Cloud Orchestration Activity Run",
            "retailPrice": 0.8,
            "unitOfMeasure": "1K",
        }
    ]
    parsed = pricing.parse_retail_prices(items)
    assert parsed is not None
    assert parsed["azure_ir"]["orchestration_per_1000"] == 0.8
    assert parsed["shir"]["data_movement_per_hour"] == 0.10


def test_parse_retail_prices_returns_none_on_empty():
    assert pricing.parse_retail_prices([]) is None


def test_parse_retail_prices_ignores_zero_prices():
    assert (
        pricing.parse_retail_prices(
            [{"productName": "Azure Data Factory v2", "meterName": "Cloud Data Movement", "retailPrice": 0}]
        )
        is None
    )


def test_fetch_live_pricing_follows_next_page_link(monkeypatch):
    pages = {
        pricing.RETAIL_PRICES_URL: {
            "Items": [
                {
                    "productName": "Azure Data Factory v2",
                    "meterName": "Cloud Orchestration Activity Run",
                    "retailPrice": 0.8,
                }
            ],
            "NextPageLink": "https://prices.azure.com/api/retail/prices?$skip=100",
        },
        "https://prices.azure.com/api/retail/prices?$skip=100": {
            "Items": [{"productName": "Azure Data Factory v2", "meterName": "Cloud Data Movement", "retailPrice": 0.3}],
            "NextPageLink": None,
        },
    }
    requested: list[tuple[str, object]] = []

    def fake_get(url, params=None, timeout=None):
        requested.append((url, params))
        return types.SimpleNamespace(status_code=200, json=lambda: pages[url])

    requests = types.ModuleType("requests")
    requests.get = fake_get  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "requests", requests)
    rates = pricing.fetch_live_pricing("westeurope")
    assert rates is not None
    assert rates["azure_ir"]["orchestration_per_1000"] == 0.8
    assert rates["azure_ir"]["data_movement_per_diu_hour"] == 0.3
    assert requested[1] == ("https://prices.azure.com/api/retail/prices?$skip=100", None)
    assert "westeurope" in requested[0][1]["$filter"]  # type: ignore[index]


def test_fetch_live_pricing_stops_on_a_repeated_next_page_link(monkeypatch):
    page = {
        "Items": [
            {
                "productName": "Azure Data Factory v2",
                "meterName": "Cloud Orchestration Activity Run",
                "retailPrice": 0.8,
            }
        ],
        "NextPageLink": "https://prices.azure.com/api/retail/prices?$skip=100",
    }
    requested: list[str] = []

    def fake_get(url, params=None, timeout=None):
        requested.append(url)
        if len(requested) > 2:
            raise AssertionError("paged past a repeated NextPageLink")
        return types.SimpleNamespace(status_code=200, json=lambda: page)

    requests = types.ModuleType("requests")
    requests.get = fake_get  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "requests", requests)
    assert pricing.fetch_live_pricing("westeurope") is not None
    assert requested == [pricing.RETAIL_PRICES_URL, "https://prices.azure.com/api/retail/prices?$skip=100"]


class ReadTimeout(Exception):
    pass


def _fake_requests(monkeypatch, get) -> None:
    requests = types.ModuleType("requests")
    requests.get = get  # type: ignore[attr-defined]
    requests.Timeout = ReadTimeout  # type: ignore[attr-defined]
    requests.ConnectionError = ConnectionError  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "requests", requests)
    monkeypatch.setattr(pricing, "RETRY_PAUSE_SECONDS", 0)


def _answers(*outcomes):
    calls: list[float] = []

    def get(url, params=None, timeout=None):
        calls.append(timeout)
        outcome = outcomes[len(calls) - 1]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return get, calls


LIVE_PAGE = types.SimpleNamespace(
    status_code=200,
    json=lambda: {
        "Items": [{"productName": "Azure Data Factory v2", "meterName": "Cloud Data Movement", "retailPrice": 0.3}]
    },
)


def test_a_timeout_is_retried_once_with_the_shared_azure_timeout(monkeypatch):
    get, calls = _answers(ReadTimeout(), LIVE_PAGE)
    _fake_requests(monkeypatch, get)
    rates = pricing.fetch_live_pricing("eastus2")
    assert rates["azure_ir"]["data_movement_per_diu_hour"] == 0.3
    assert calls == [azure_client.REQUEST_TIMEOUT_SECONDS, azure_client.REQUEST_TIMEOUT_SECONDS]


def test_a_second_timeout_gives_up_with_its_name(monkeypatch):
    get, calls = _answers(ReadTimeout(), ReadTimeout(), LIVE_PAGE)
    _fake_requests(monkeypatch, get)
    with pytest.raises(pricing.LivePricingUnavailable, match="^ReadTimeout$"):
        pricing.fetch_live_pricing("eastus2")
    assert len(calls) == 2


def test_an_http_error_is_not_retried(monkeypatch):
    get, calls = _answers(types.SimpleNamespace(status_code=404), LIVE_PAGE)
    _fake_requests(monkeypatch, get)
    with pytest.raises(pricing.LivePricingUnavailable, match="^HTTP 404$"):
        pricing.fetch_live_pricing("eastus2")
    assert len(calls) == 1


def test_fetch_live_pricing_names_an_http_failure(monkeypatch):
    _fake_requests(monkeypatch, lambda url, params=None, timeout=None: types.SimpleNamespace(status_code=503))
    with pytest.raises(pricing.LivePricingUnavailable, match="^HTTP 503$"):
        pricing.fetch_live_pricing("westeurope")


def test_fetch_live_pricing_names_a_network_failure(monkeypatch):
    def unreachable(url, params=None, timeout=None):
        raise ConnectionError("no route to host")

    _fake_requests(monkeypatch, unreachable)
    with pytest.raises(pricing.LivePricingUnavailable, match="^ConnectionError$"):
        pricing.fetch_live_pricing("westeurope")


def test_fetch_live_pricing_rejects_a_response_with_no_usable_prices(monkeypatch):
    empty = types.SimpleNamespace(status_code=200, json=lambda: {"Items": []})
    _fake_requests(monkeypatch, lambda url, params=None, timeout=None: empty)
    with pytest.raises(pricing.LivePricingUnavailable, match="no usable prices"):
        pricing.fetch_live_pricing("westeurope")


@pytest.mark.parametrize(
    "next_page_link",
    [
        "https://evil.example.com/api/retail/prices?$skip=100",
        "https://prices.azure.com.evil.com/api/retail/prices?$skip=100",
        "http://prices.azure.com/api/retail/prices?$skip=100",
        "https://prices.azure.com@evil.com/api/retail/prices?$skip=100",
        "https://user@prices.azure.com/api/retail/prices?$skip=100",
        "https://prices.azure.com:444/api/retail/prices?$skip=100",
        "https://prices.azure.com:443/api/retail/prices?$skip=100",
        "https://prices.azure.com./api/retail/prices?$skip=100",
        "https://pri\u0441es.azure.com/api/retail/prices?$skip=100",
        "https://xn--pries-3ye.azure.com/api/retail/prices?$skip=100",
    ],
)
def test_fetch_live_pricing_stops_at_a_next_page_link_off_prices_azure_com(monkeypatch, next_page_link):
    page = {
        "Items": [
            {
                "productName": "Azure Data Factory v2",
                "meterName": "Cloud Orchestration Activity Run",
                "retailPrice": 0.8,
            }
        ],
        "NextPageLink": next_page_link,
    }
    requested: list[str] = []

    def fake_get(url, params=None, timeout=None):
        requested.append(url)
        return types.SimpleNamespace(status_code=200, json=lambda: page)

    _fake_requests(monkeypatch, fake_get)
    rates = pricing.fetch_live_pricing("westeurope")
    assert rates["azure_ir"]["orchestration_per_1000"] == 0.8
    assert requested == [pricing.RETAIL_PRICES_URL]


def test_fetch_live_pricing_follows_the_real_host_in_upper_case(monkeypatch):
    second_page_url = "https://PRICES.AZURE.COM/api/retail/prices?$skip=100"
    pages = {
        pricing.RETAIL_PRICES_URL: {
            "Items": [
                {
                    "productName": "Azure Data Factory v2",
                    "meterName": "Cloud Orchestration Activity Run",
                    "retailPrice": 0.8,
                }
            ],
            "NextPageLink": second_page_url,
        },
        second_page_url: {
            "Items": [{"productName": "Azure Data Factory v2", "meterName": "Cloud Data Movement", "retailPrice": 0.3}]
        },
    }
    requested: list[str] = []

    def fake_get(url, params=None, timeout=None):
        requested.append(url)
        return types.SimpleNamespace(status_code=200, json=lambda: pages[url])

    _fake_requests(monkeypatch, fake_get)
    rates = pricing.fetch_live_pricing("westeurope")
    assert rates["azure_ir"]["data_movement_per_diu_hour"] == 0.3
    assert requested == [pricing.RETAIL_PRICES_URL, second_page_url]


def _adf_meter(name: str, price: float, product: str = "Azure Data Factory v2") -> dict:
    return {"productName": product, "meterName": name, "retailPrice": price, "unitOfMeasure": "1 Hour"}


def test_pipeline_activity_is_not_taken_from_the_external_pipeline_meter():
    parsed = pricing.parse_retail_prices(
        [
            _adf_meter("Cloud Pipeline Activity", 9.0, product="Azure Data Factory v2 - Managed Airflow"),
            _adf_meter("Cloud External Pipeline Activity", 0.00025),
            _adf_meter("Cloud Pipeline Activity", 0.005),
            _adf_meter("Azure Managed VNET External Pipeline Activity", 2.0),
            _adf_meter("Azure Managed VNET Pipeline Activity", 3.0),
        ]
    )
    assert parsed is not None
    assert parsed["azure_ir"]["pipeline_activity_per_hour"] == 0.005
    assert parsed["azure_ir"]["external_activity_per_hour"] == 0.00025
    assert parsed["managed_vnet_ir"]["pipeline_activity_per_hour"] == 3.0
    assert parsed["managed_vnet_ir"]["external_activity_per_hour"] == 2.0


def test_self_hosted_rates_come_from_the_self_hosted_meters():
    parsed = pricing.parse_retail_prices(
        [
            _adf_meter("Self Hosted Monitoring Operations", 0.25),
            _adf_meter("Self Hosted Read Write Operations", 0.5),
            _adf_meter("Self Hosted External Pipeline Activity", 0.0001),
            _adf_meter("Self Hosted Pipeline Activity", 0.002),
            _adf_meter("Self Hosted Data Movement", 0.2),
            _adf_meter("Self Hosted Orchestration Activity Run", 1.75),
        ]
    )
    assert parsed is not None
    assert parsed["shir"] == {
        "orchestration_per_1000": 1.75,
        "data_movement_per_hour": 0.2,
        "pipeline_activity_per_hour": 0.002,
        "external_activity_per_hour": 0.0001,
    }
