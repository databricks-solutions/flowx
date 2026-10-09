"""Pricing rate cards and the pure Retail-Prices parser."""

from __future__ import annotations

from flowx.sources.adf.profiler import pricing


def test_default_pricing_has_three_rate_cards():
    assert set(pricing.DEFAULT_PRICING) == {"azure_ir", "shir", "managed_vnet_ir"}
    assert pricing.DEFAULT_PRICING["azure_ir"]["orchestration_per_1000"] == 1.00


def test_parse_retail_prices_maps_meters():
    items = [
        {"meterName": "Cloud Orchestration Activity Run", "retailPrice": 1.0, "unitOfMeasure": "1K"},
        {"meterName": "Cloud Data Movement", "retailPrice": 0.25, "unitOfMeasure": "1 Hour"},
        {"meterName": "Cloud Pipeline Activity", "retailPrice": 0.005, "unitOfMeasure": "1 Hour"},
    ]
    parsed = pricing.parse_retail_prices(items)
    assert parsed is not None
    assert parsed["azure_ir"]["orchestration_per_1000"] == 1.0
    assert parsed["azure_ir"]["data_movement_per_diu_hour"] == 0.25


def test_parse_retail_prices_falls_back_per_meter():
    items = [{"meterName": "Cloud Orchestration Activity Run", "retailPrice": 0.8, "unitOfMeasure": "1K"}]
    parsed = pricing.parse_retail_prices(items)
    assert parsed is not None
    assert parsed["azure_ir"]["orchestration_per_1000"] == 0.8
    assert parsed["shir"]["data_movement_per_hour"] == 0.10


def test_parse_retail_prices_returns_none_on_empty():
    assert pricing.parse_retail_prices([]) is None


def test_parse_retail_prices_ignores_zero_prices():
    assert pricing.parse_retail_prices([{"meterName": "Cloud Data Movement", "retailPrice": 0}]) is None
