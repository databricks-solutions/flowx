"""Tests for the platform limits file the ``flowx-route`` skill checks its suggested patterns against.

The file is data the route agent reads, so its contract is its shape: every limit names the connector it
applies to, its value, whether the docs say it is fixed, the page it came from and the day it was read.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pytest

LIMITS_PATH = Path(__file__).parents[2] / "skills" / "flowx-route" / "platform-limits.json"
FIXED_VALUES = {"yes", "no", "not stated"}


@pytest.fixture(scope="module")
def limits_file() -> dict[str, Any]:
    return json.loads(LIMITS_PATH.read_text(encoding="utf-8"))


def _is_https_url(value: object) -> bool:
    if not isinstance(value, str):
        return False
    parsed = urlparse(value)
    return parsed.scheme == "https" and bool(parsed.netloc)


def test_every_limit_names_its_connector_value_fixedness_source_and_read_date(limits_file: dict[str, Any]) -> None:
    limits = limits_file["limits"]
    assert limits

    for row in limits:
        assert isinstance(row["connector"], str) and row["connector"], row["id"]
        assert row["value"] not in (None, ""), row["id"]
        assert row["fixed"] in FIXED_VALUES, row["id"]
        assert _is_https_url(row["source_url"]), row["id"]
        assert date.fromisoformat(row["page_date"]) <= date.fromisoformat(row["read_on"]), row["id"]


def test_limit_ids_are_unique(limits_file: dict[str, Any]) -> None:
    ids = [row["id"] for row in limits_file["limits"] + limits_file["unconfirmed"]]

    assert len(ids) == len(set(ids))


def test_every_unconfirmed_claim_says_where_it_was_looked_for_and_when(limits_file: dict[str, Any]) -> None:
    for row in limits_file["unconfirmed"]:
        assert row["claim"] and row["why"], row["id"]
        assert row["checked_urls"] and all(_is_https_url(url) for url in row["checked_urls"]), row["id"]
        date.fromisoformat(row["read_on"])
