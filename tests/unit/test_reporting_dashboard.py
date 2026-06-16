"""Tests for the coverage dashboard builder + installer."""

from __future__ import annotations

import json

import pytest

from flowx.reporting import dashboard as D


def test_build_serialized_dashboard_injects_table_and_is_valid_json():
    serialized = D.build_serialized_dashboard("cat.sch.results")
    spec = json.loads(serialized)
    assert "{{RESULTS_TABLE}}" not in serialized
    # every dataset query references the fully-qualified table
    joined = " ".join(line for ds in spec["datasets"] for line in ds["queryLines"])
    assert "cat.sch.results" in joined
    assert spec["pages"][0]["pageType"] == "PAGE_TYPE_CANVAS"
    # widget field names match their dataset fields (counter references a real column)
    widget_names = {w["widget"]["name"] for w in spec["pages"][0]["layout"]}
    assert {"kpi-coverage", "by-size", "coverage-trend", "pipeline-table"} <= widget_names


def test_build_serialized_dashboard_requires_table():
    with pytest.raises(ValueError):
        D.build_serialized_dashboard("")


class _Created:
    dashboard_id = "dash-123"


class _FakeLakeview:
    def __init__(self):
        self.created = None
        self.published = None

    def create(self, dashboard):
        self.created = dashboard
        return _Created()

    def publish(self, dashboard_id, warehouse_id):
        self.published = (dashboard_id, warehouse_id)


class _FakeWarehousesAPI:
    def list(self):
        class _W:
            id = "wh1"
            name = "wh1"
            state = "RUNNING"
            enable_serverless_compute = True
            warehouse_type = "PRO"

        return [_W()]


class _Me:
    user_name = "greg@databricks.com"


class _FakeCurrentUser:
    def me(self):
        return _Me()


class _FakeConfig:
    host = "https://example.cloud.databricks.com"


class _FakeClient:
    def __init__(self):
        self.lakeview = _FakeLakeview()
        self.warehouses = _FakeWarehousesAPI()
        self.current_user = _FakeCurrentUser()
        self.config = _FakeConfig()


def test_install_dashboard_creates_and_publishes():
    client = _FakeClient()
    dashboard_id, url = D.install_dashboard("cat.sch.results", client=client)
    assert dashboard_id == "dash-123"
    assert url == "https://example.cloud.databricks.com/sql/dashboardsv3/dash-123"
    # created with resolved warehouse, table-bound spec, and default parent path = user home
    created = client.lakeview.created
    assert created.warehouse_id == "wh1"
    assert created.parent_path == "/Workspace/Users/greg@databricks.com"
    assert "cat.sch.results" in created.serialized_dashboard
    assert client.lakeview.published == ("dash-123", "wh1")


def test_install_dashboard_respects_overrides():
    client = _FakeClient()
    D.install_dashboard(
        "cat.sch.results", warehouse_id="whX", display_name="My Dash", parent_path="/Workspace/Shared", client=client
    )
    created = client.lakeview.created
    assert created.warehouse_id == "whX"
    assert created.display_name == "My Dash"
    assert created.parent_path == "/Workspace/Shared"
