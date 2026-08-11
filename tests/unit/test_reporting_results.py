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
    assert "audited_activities INT" in sql
    assert "failed_activities INT" in sql
    assert "excluded_activities INT" in sql
    assert "reconciliation_status STRING" in sql
    assert "deterministic_coverage_pct DOUBLE" in sql
    assert "code_attached_coverage_pct DOUBLE" in sql
    assert "resolved_agentic_count INT" in sql
    assert "unresolved_agentic_count INT" in sql
    assert "agentic_resolution_outcomes STRING" in sql
    assert "agentic_provider_version STRING" in sql
    assert "finding_fingerprints STRING" in sql
    assert "complexity_size STRING" in sql


def test_schema_evolution_sql_adds_only_missing_metric_columns() -> None:
    sql = R.build_add_columns_sql(
        "cat.sch.tbl",
        existing_columns={"RUN_ID", "PIPELINE", "activities", "coverage_pct"},
    )

    assert sql.startswith("ALTER TABLE cat.sch.tbl ADD COLUMNS")
    assert "audited_activities INT" in sql
    assert "deterministic_coverage_pct DOUBLE" in sql
    assert "code_attached_coverage_pct DOUBLE" in sql
    assert "pipeline STRING" not in sql
    assert "\n  coverage_pct DOUBLE" not in sql


def test_insert_sql_stamps_run_metadata_and_escapes():
    rows = [
        {
            "pipeline": "p1",
            "activities": 3,
            "audited_activities": 3,
            "datasets": 1,
            "linked_services": 0,
            "collapsible_patterns": 0,
            "databricks_native_activities": 1,
            "control_flow_activities": 0,
            "other_activities": 2,
            "deterministic_activities": 2,
            "agentic_activities": 1,
            "resolved_agentic_count": 0,
            "unresolved_agentic_count": 1,
            "agentic_resolution_outcomes": '{"unreviewed":1}',
            "agentic_provider_version": "0.2.2",
            "unsupported_activities": 0,
            "failed_activities": 0,
            "excluded_activities": 0,
            "reconciliation_status": "verified_with_gaps",
            "migration_status": "included",
            "coverage_pct": 100.0,
            "deterministic_coverage_pct": 66.7,
            "code_attached_coverage_pct": 66.7,
            "finding_count": 1,
            "finding_fingerprints": '["abc"]',
            "complexity_score": 7,
            "complexity_size": "M",
        },
        {
            "pipeline": "O'Brien's pipe",
            "activities": 1,
            "audited_activities": 1,
            "datasets": 0,
            "linked_services": 0,
            "collapsible_patterns": 0,
            "databricks_native_activities": 0,
            "control_flow_activities": 0,
            "other_activities": 1,
            "deterministic_activities": 0,
            "agentic_activities": 0,
            "resolved_agentic_count": 0,
            "unresolved_agentic_count": 0,
            "agentic_resolution_outcomes": "{}",
            "agentic_provider_version": "",
            "unsupported_activities": 1,
            "failed_activities": 0,
            "excluded_activities": 0,
            "reconciliation_status": "not_applicable",
            "migration_status": "included",
            "coverage_pct": 0.0,
            "deterministic_coverage_pct": 0.0,
            "code_attached_coverage_pct": 0.0,
            "finding_count": 0,
            "finding_fingerprints": "[]",
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
    assert "'verified_with_gaps'" in sql
    assert "'[\"abc\"]'" in sql
    assert "'{\"unreviewed\":1}'" in sql


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
    def __init__(self, columns=None):
        self.statements = []
        self.columns = columns or [name for name, _sql_type in R.RESULTS_COLUMNS]

    def execute_statement(self, statement, warehouse_id, wait_timeout=None):
        self.statements.append((warehouse_id, statement))

        class _Resp:
            class status:
                state = "SUCCEEDED"

        if statement.startswith("SHOW COLUMNS"):

            class _Result:
                data_array = [[column] for column in self.columns]

            _Resp.result = _Result()

        return _Resp()


class _FakeClient:
    def __init__(self, warehouses, columns=None):
        self.warehouses = _FakeWarehousesAPI(warehouses)
        self.statement_execution = _FakeStmtAPI(columns)


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


def test_write_results_executes_create_schema_check_then_insert(tmp_path: Path):
    client = _FakeClient([_FakeWarehouse("wh1", "RUNNING", serverless=True)])
    run_id, rows = R.write_results(_metadata(tmp_path), "cat.sch.tbl", client=client)
    assert rows == 1
    uuid.UUID(run_id)  # valid uuid
    stmts = client.statement_execution.statements
    assert len(stmts) == 3
    assert stmts[0][0] == "wh1" and stmts[0][1].startswith("CREATE TABLE IF NOT EXISTS")
    assert stmts[1][1] == "SHOW COLUMNS IN cat.sch.tbl"
    assert stmts[2][1].startswith("INSERT INTO cat.sch.tbl")
    assert run_id in stmts[2][1]


def test_write_results_evolves_an_existing_legacy_schema_before_insert(tmp_path: Path) -> None:
    legacy_columns = {
        "run_id",
        "run_date",
        "run_by",
        "pipeline",
        "activities",
        "datasets",
        "linked_services",
        "collapsible_patterns",
        "databricks_native_activities",
        "control_flow_activities",
        "other_activities",
        "deterministic_activities",
        "agentic_activities",
        "unsupported_activities",
        "coverage_pct",
        "complexity_score",
        "complexity_size",
    }
    client = _FakeClient([_FakeWarehouse("wh1", "RUNNING", serverless=True)], columns=legacy_columns)

    R.write_results(_metadata(tmp_path), "cat.sch.tbl", client=client)

    statements = [statement for _warehouse, statement in client.statement_execution.statements]
    assert len(statements) == 4
    assert statements[2].startswith("ALTER TABLE cat.sch.tbl ADD COLUMNS")
    assert "audited_activities INT" in statements[2]
    assert "deterministic_coverage_pct DOUBLE" in statements[2]
    assert "code_attached_coverage_pct DOUBLE" in statements[2]
    assert "agentic_resolution_outcomes STRING" in statements[2]
    assert statements[3].startswith("INSERT INTO cat.sch.tbl")
