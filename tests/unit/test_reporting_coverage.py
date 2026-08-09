"""Tests for building per-pipeline coverage rows from migration metadata."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from flowx.reporting.coverage import build_coverage_rows


def _write_metadata(tmp_path: Path) -> Path:
    md = tmp_path / "metadata"
    md.mkdir()
    inventory = {
        "pipelines": [
            {
                "name": "p_alpha",
                "activities": [
                    {"name": "a1", "type": "DatabricksNotebook", "strategy": "deterministic"},
                    {"name": "a2", "type": "Copy", "strategy": "deterministic"},
                    {"name": "a3", "type": "ExecuteDataFlow", "strategy": "agentic"},
                    {"name": "a4", "type": "Custom", "strategy": "unsupported"},
                ],
            },
            {
                "name": "p_beta",
                "activities": [
                    {"name": "b1", "type": "DatabricksNotebook", "strategy": "deterministic"},
                ],
            },
        ],
        "summary": {"pipeline_count": 2},
    }
    (md / "inventory.json").write_text(json.dumps(inventory), encoding="utf-8")
    rows = [
        {
            "pipeline": "p_alpha",
            "activities": 4,
            "datasets": 2,
            "linked_services": 1,
            "collapsible_patterns": 1,
            "databricks_native_activities": 1,
            "control_flow_activities": 0,
            "other_activities": 3,
            "complexity_score": 12,
            "complexity_size": "M",
        },
        {
            "pipeline": "p_beta",
            "activities": 1,
            "datasets": 0,
            "linked_services": 1,
            "collapsible_patterns": 0,
            "databricks_native_activities": 1,
            "control_flow_activities": 0,
            "other_activities": 0,
            "complexity_score": 2,
            "complexity_size": "S",
        },
    ]
    with (md / "profile_report.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    return md


def test_build_coverage_rows_joins_inventory_and_csv(tmp_path: Path):
    rows = build_coverage_rows(_write_metadata(tmp_path))
    assert [r["pipeline"] for r in rows] == ["p_alpha", "p_beta"]  # sorted by name
    alpha = rows[0]
    assert alpha["activities"] == 4
    assert alpha["deterministic_activities"] == 2
    assert alpha["agentic_activities"] == 1
    assert alpha["unsupported_activities"] == 1
    # coverage = (det + agentic) / total = 3/4 = 75.0
    assert alpha["coverage_pct"] == 75.0
    assert alpha["runnable_coverage_pct"] == 75.0
    assert alpha["unresolved_agentic_activities"] == 0
    assert alpha["agentic_resolution_outcomes"] == "{}"
    # complexity columns come from the CSV
    assert alpha["datasets"] == 2 and alpha["linked_services"] == 1
    assert alpha["collapsible_patterns"] == 1 and alpha["complexity_size"] == "M"


def test_build_coverage_rows_full_coverage_and_missing_csv(tmp_path: Path):
    md = _write_metadata(tmp_path)
    (md / "profile_report.csv").unlink()  # CSV optional -> complexity columns default
    rows = {r["pipeline"]: r for r in build_coverage_rows(md)}
    beta = rows["p_beta"]
    assert beta["coverage_pct"] == 100.0  # 1/1 deterministic
    assert beta["datasets"] == 0 and beta["complexity_size"] == ""  # defaulted, no CSV


def test_audited_counts_drive_translation_and_deterministic_coverage(tmp_path: Path) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    inventory = {
        "source": "airflow",
        "pipelines": [
            {
                "name": "verified_with_gap",
                "activities": [],
                "audited_activity_count": 8,
                "deterministic_count": 7,
                "agentic_count": 1,
                "failed_count": 0,
                "excluded_count": 0,
                "reconciliation_status": "verified_with_gaps",
                "migration_status": "included",
                "findings": [{"fingerprint": "abc123", "severity": "gap"}],
            },
            {
                "name": "failed",
                "activities": [],
                "audited_activity_count": 9,
                "deterministic_count": 7,
                "agentic_count": 1,
                "failed_count": 1,
                "excluded_count": 0,
                "reconciliation_status": "failed",
                "migration_status": "included",
                "findings": [{"fingerprint": "def456", "severity": "failed"}],
            },
        ],
    }
    (metadata / "inventory.json").write_text(json.dumps(inventory), encoding="utf-8")

    rows = {row["pipeline"]: row for row in build_coverage_rows(metadata)}

    verified = rows["verified_with_gap"]
    assert verified["activities"] == 8
    assert verified["audited_activities"] == 8
    assert verified["coverage_pct"] == 100.0
    assert verified["deterministic_coverage_pct"] == 87.5
    assert verified["runnable_coverage_pct"] == 87.5
    assert verified["unresolved_agentic_activities"] == 1
    assert json.loads(verified["agentic_resolution_outcomes"]) == {
        "resolved": 0,
        "needs_input": 0,
        "deferred": 0,
        "declined": 0,
        "unreviewed": 1,
    }
    assert verified["finding_count"] == 1
    assert json.loads(verified["finding_fingerprints"]) == ["abc123"]

    failed = rows["failed"]
    assert failed["activities"] == 9
    assert failed["failed_activities"] == 1
    assert failed["coverage_pct"] == 88.9
    assert failed["deterministic_coverage_pct"] == 77.8
    assert failed["runnable_coverage_pct"] == 77.8
    assert failed["reconciliation_status"] == "failed"


def test_excluded_activities_remain_in_coverage_denominator(tmp_path: Path) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    inventory = {
        "source": "airflow",
        "pipelines": [
            {
                "name": "excluded",
                "activities": [],
                "audited_activity_count": 3,
                "deterministic_count": 0,
                "agentic_count": 0,
                "failed_count": 0,
                "excluded_count": 3,
                "reconciliation_status": "verified",
                "migration_status": "excluded",
                "findings": [],
            }
        ],
    }
    (metadata / "inventory.json").write_text(json.dumps(inventory), encoding="utf-8")

    row = build_coverage_rows(metadata)[0]

    assert row["activities"] == 3
    assert row["excluded_activities"] == 3
    assert row["coverage_pct"] == 0.0
    assert row["deterministic_coverage_pct"] == 0.0
    assert row["runnable_coverage_pct"] == 0.0
    assert row["migration_status"] == "excluded"
