"""Tests for the UC-table results writer (SQL builders, warehouse resolution, write)."""

from __future__ import annotations

import csv
import json
import uuid
from pathlib import Path

import pytest

from flowx.reporting import results as R


def test_create_table_sql_has_run_metadata_and_all_columns():
    sql = R.build_create_table_sql("cat.sch.tbl")
    assert sql.startswith("CREATE TABLE IF NOT EXISTS cat.sch.tbl")
    assert "run_id STRING" in sql
    assert "run_date TIMESTAMP" in sql
    assert "run_by STRING" in sql
    assert "coverage_pct DOUBLE" in sql
    assert "complexity_size STRING" in sql


def test_insert_sql_stamps_run_metadata_and_escapes():
    rows = [
        {
            "pipeline": "p1",
            "activities": 3,
            "datasets": 1,
            "linked_services": 0,
            "collapsible_patterns": 0,
            "databricks_native_activities": 1,
            "control_flow_activities": 0,
            "other_activities": 2,
            "deterministic_activities": 2,
            "agentic_activities": 1,
            "unsupported_activities": 0,
            "coverage_pct": 100.0,
            "complexity_score": 7,
            "complexity_size": "M",
        },
        {
            "pipeline": "O'Brien's pipe",
            "activities": 1,
            "datasets": 0,
            "linked_services": 0,
            "collapsible_patterns": 0,
            "databricks_native_activities": 0,
            "control_flow_activities": 0,
            "other_activities": 1,
            "deterministic_activities": 0,
            "agentic_activities": 0,
            "unsupported_activities": 1,
            "coverage_pct": 0.0,
            "complexity_score": 3,
            "complexity_size": "S",
        },
    ]
    run_id = "abc-123"
    sql = R.build_insert_sql("cat.sch.tbl", rows, run_id)
    assert "INSERT INTO cat.sch.tbl (run_id, run_date, run_by," in sql
    # run metadata: literal run_id + SQL functions on every row
    assert sql.count("'abc-123'") == 2
    assert sql.count("CURRENT_TIMESTAMP()") == 2
    assert sql.count("CURRENT_USER()") == 2
    # apostrophe escaped by doubling
    assert "'O''Brien''s pipe'" in sql
    # numeric + float rendered unquoted
    assert "100.0" in sql


class _FakeWarehouse:
    def __init__(self, id, state, serverless=False):
        self.id = id
        self.name = id
        self.state = state
        self.enable_serverless_compute = serverless
        self.warehouse_type = "PRO"


class _FakeWarehousesAPI:
    def __init__(self, items):
        self._items = items

    def list(self):
        return list(self._items)


class _FakeStmtAPI:
    def __init__(self):
        self.statements = []

    def execute_statement(self, statement, warehouse_id, wait_timeout=None):
        self.statements.append((warehouse_id, statement))

        class _Resp:
            class status:
                state = "SUCCEEDED"

        return _Resp()


class _FakeClient:
    def __init__(self, warehouses):
        self.warehouses = _FakeWarehousesAPI(warehouses)
        self.statement_execution = _FakeStmtAPI()


def test_resolve_warehouse_prefers_running_serverless():
    client = _FakeClient(
        [
            _FakeWarehouse("w_stopped", "STOPPED", serverless=True),
            _FakeWarehouse("w_running_classic", "RUNNING", serverless=False),
            _FakeWarehouse("w_running_serverless", "RUNNING", serverless=True),
        ]
    )
    assert R.resolve_warehouse_id(client) == "w_running_serverless"
    # explicit id passes through
    assert R.resolve_warehouse_id(client, "explicit") == "explicit"


def test_resolve_warehouse_none_raises():
    with pytest.raises(RuntimeError):
        R.resolve_warehouse_id(_FakeClient([]))


def _metadata(tmp_path: Path) -> Path:
    md = tmp_path / "metadata"
    md.mkdir()
    inv = {
        "pipelines": [
            {"name": "p1", "activities": [{"name": "a", "type": "DatabricksNotebook", "strategy": "deterministic"}]}
        ]
    }
    (md / "inventory.json").write_text(json.dumps(inv))
    with (md / "profile_report.csv").open("w", newline="") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=[
                "pipeline",
                "activities",
                "datasets",
                "linked_services",
                "collapsible_patterns",
                "databricks_native_activities",
                "control_flow_activities",
                "other_activities",
                "complexity_score",
                "complexity_size",
            ],
        )
        w.writeheader()
        w.writerow(
            {
                "pipeline": "p1",
                "activities": 1,
                "datasets": 0,
                "linked_services": 1,
                "collapsible_patterns": 0,
                "databricks_native_activities": 1,
                "control_flow_activities": 0,
                "other_activities": 0,
                "complexity_score": 2,
                "complexity_size": "S",
            }
        )
    return md


def test_write_results_executes_create_then_insert(tmp_path: Path):
    client = _FakeClient([_FakeWarehouse("wh1", "RUNNING", serverless=True)])
    run_id, rows = R.write_results(_metadata(tmp_path), "cat.sch.tbl", client=client)
    assert rows == 1
    uuid.UUID(run_id)  # valid uuid
    stmts = client.statement_execution.statements
    assert len(stmts) == 2
    assert stmts[0][0] == "wh1" and stmts[0][1].startswith("CREATE TABLE IF NOT EXISTS")
    assert stmts[1][1].startswith("INSERT INTO cat.sch.tbl")
    assert run_id in stmts[1][1]
