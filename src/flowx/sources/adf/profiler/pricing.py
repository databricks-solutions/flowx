"""ADF pricing: static list-rate cards plus an optional live fetch from the public Azure
Retail Prices API. The parser is pure (testable); the HTTP fetch is a thin wrapper that
lazy-imports requests so importing this module needs no network stack.
"""

from __future__ import annotations

from typing import Any

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


def parse_retail_prices(items: list[dict[str, Any]]) -> RateCards | None:
    """Map Azure Retail Prices API `Items` to rate cards, or None if nothing usable came back.

    Meters are matched by keyword (e.g. "cloud" + "orchestration"); any meter the API didn't
    return keeps its list-price fallback.
    """
    prices_by_meter: dict[str, float] = {}
    for item in items:
        meter = item.get("meterName", "").lower()
        price = item.get("retailPrice", 0)
        if price > 0:
            prices_by_meter[meter] = price
    if not prices_by_meter:
        return None

    def find_price(keywords: list[str], fallback: float) -> float:
        for meter, price in prices_by_meter.items():
            if all(keyword in meter for keyword in keywords):
                return price
        return fallback

    return {
        "azure_ir": {
            "orchestration_per_1000": find_price(["cloud", "orchestration"], 1.00),
            "data_movement_per_diu_hour": find_price(["cloud", "data movement"], 0.25),
            "pipeline_activity_per_hour": find_price(["cloud", "pipeline"], 0.005),
            "external_activity_per_hour": find_price(["cloud", "external"], 0.00025),
        },
        "shir": {
            "orchestration_per_1000": find_price(["on premises", "orchestration"], 1.50),
            "data_movement_per_hour": find_price(["on premises", "data movement"], 0.10),
            "pipeline_activity_per_hour": find_price(["on premises", "pipeline"], 0.005),
            "external_activity_per_hour": find_price(["on premises", "external"], 0.00025),
        },
        "managed_vnet_ir": {
            "orchestration_per_1000": find_price(["managed vnet", "orchestration"], 1.00),
            "data_movement_per_diu_hour": find_price(["managed vnet", "data movement"], 0.25),
            "pipeline_activity_per_hour": find_price(["managed vnet", "pipeline"], 1.00),
            "external_activity_per_hour": find_price(["managed vnet", "external"], 1.00),
        },
    }


def fetch_live_pricing(region: str) -> RateCards | None:
    """Best-effort live rates for `region`; returns None on any failure so the caller falls back."""
    try:
        import requests

        response = requests.get(
            RETAIL_PRICES_URL,
            params={
                "api-version": "2023-01-01-preview",
                "$filter": (
                    "serviceName eq 'Azure Data Factory v2' "
                    f"and armRegionName eq '{region}' and priceType eq 'Consumption'"
                ),
            },
            timeout=5,
        )
        if response.status_code != 200:
            return None
        return parse_retail_prices(response.json().get("Items", []))
    except Exception:
        return None
