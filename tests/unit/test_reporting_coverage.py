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
