"""ADF pricing: static list-rate cards plus an optional live fetch from the public Azure
Retail Prices API. The parser is pure (testable); the HTTP fetch is a thin wrapper that
lazy-imports requests so importing this module needs no network stack.
"""

from __future__ import annotations

from typing import Any

from flowx.sources.adf.profile.azure_client import is_https_on

RateCards = dict[str, dict[str, float]]

DEFAULT_PRICING: RateCards = {
    "azure_ir": {
        "orchestration_per_1000": 1.00,
        "data_movement_per_diu_hour": 0.25,
        "pipeline_activity_per_hour": 0.005,
        "external_activity_per_hour": 0.00025,
    },
    "shir": {
        "orchestration_per_1000": 1.50,
        "data_movement_per_hour": 0.10,
        "pipeline_activity_per_hour": 0.005,
        "external_activity_per_hour": 0.00025,
    },
    "managed_vnet_ir": {
        "orchestration_per_1000": 1.00,
        "data_movement_per_diu_hour": 0.25,
        "pipeline_activity_per_hour": 1.00,
        "external_activity_per_hour": 1.00,
    },
}

RETAIL_PRICES_URL = "https://prices.azure.com/api/retail/prices"


class LivePricingUnavailable(Exception):
    """Raised when live Retail Prices can't be used; the message is a short, plain reason."""


METER_NAMES: dict[str, dict[str, str]] = {
    "azure_ir": {
        "orchestration_per_1000": "Cloud Orchestration Activity Run",
        "data_movement_per_diu_hour": "Cloud Data Movement",
        "pipeline_activity_per_hour": "Cloud Pipeline Activity",
        "external_activity_per_hour": "Cloud External Pipeline Activity",
    },
    "shir": {
        "orchestration_per_1000": "Self Hosted Orchestration Activity Run",
        "data_movement_per_hour": "Self Hosted Data Movement",
        "pipeline_activity_per_hour": "Self Hosted Pipeline Activity",
        "external_activity_per_hour": "Self Hosted External Pipeline Activity",
    },
    "managed_vnet_ir": {
        "orchestration_per_1000": "Azure Managed VNET Orchestration Activity Run",
        "data_movement_per_diu_hour": "Azure Managed VNET Data Movement",
        "pipeline_activity_per_hour": "Azure Managed VNET Pipeline Activity",
        "external_activity_per_hour": "Azure Managed VNET External Pipeline Activity",
    },
}
PRODUCT_NAME = "Azure Data Factory v2"


def parse_retail_prices(items: list[dict[str, Any]]) -> RateCards | None:
    """Map Azure Retail Prices API `Items` to rate cards, or None if nothing usable came back.

    Each rate is the price of the `Azure Data Factory v2` meter named exactly as in METER_NAMES;
    any meter the API didn't return keeps its list-price fallback.
    """
    prices_by_meter = {
        item.get("meterName", ""): item["retailPrice"]
        for item in items
        if item.get("retailPrice", 0) > 0 and item.get("productName") == PRODUCT_NAME
    }
    if not prices_by_meter:
        return None
    return {
        card: {rate: prices_by_meter.get(meter, DEFAULT_PRICING[card][rate]) for rate, meter in meters.items()}
        for card, meters in METER_NAMES.items()
    }


def fetch_live_pricing(region: str) -> RateCards:
    """Live rates for `region`, following `NextPageLink` pages until none or one repeats.

    Raises LivePricingUnavailable with a short reason on any failure, so the caller can fall back
    to list rates and say why.
    """
    try:
        import requests

        items: list[dict[str, Any]] = []
        next_url: str | None = RETAIL_PRICES_URL
        params: dict[str, str] | None = {
            "api-version": "2023-01-01-preview",
            "$filter": (
                f"serviceName eq 'Azure Data Factory v2' and armRegionName eq '{region}' and priceType eq 'Consumption'"
            ),
        }
        seen_urls: set[str] = set()
        while next_url and next_url not in seen_urls:
            if not is_https_on(next_url, "prices.azure.com"):
                break
            seen_urls.add(next_url)
            response = requests.get(next_url, params=params, timeout=5)
            if response.status_code != 200:
                raise LivePricingUnavailable(f"HTTP {response.status_code}")
            page = response.json()
            items.extend(page.get("Items", []))
            next_url = page.get("NextPageLink")
            params = None
    except LivePricingUnavailable:
        raise
    except Exception as error:
        raise LivePricingUnavailable(type(error).__name__) from error
    rates = parse_retail_prices(items)
    if rates is None:
        raise LivePricingUnavailable("no usable prices returned")
    return rates
