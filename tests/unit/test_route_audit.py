"""Tests for the routing/gaps audit artifact the package phase persists (FIX 5).

Package prunes the transient ``.work/`` folder by default, which erases the routing trail
(translation report + ``gaps.json``). ``_write_route_audit`` summarises the recorded plan's routed
components + decisions + the gaps routing introduced into ``metadata/route_audit.json`` so the trail
survives the prune. It must no-op (write nothing) when no plan was recorded, keeping the no-route
path byte-identical.
"""

from __future__ import annotations

import json
from pathlib import Path

from flowx.bundler.dab_writer import _write_route_audit


def _write_plan(output_dir: Path) -> None:
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    plan = {
        "schema_version": "1",
        "inventory_sha256": "abc123",
        "components": [
            {"component_id": "component-1", "members": ["parent", "child"], "decision": "agentic"},
            {"component_id": "component-2", "members": ["solo"], "decision": "deterministic"},
        ],
    }
    (metadata / "conversion_plan.json").write_text(json.dumps(plan, indent=2), encoding="utf-8")


def _write_gaps(output_dir: Path) -> None:
    work = output_dir / ".work"
    work.mkdir(parents=True, exist_ok=True)
    gaps = [
        {
            "activity_name": "Extract",
            "activity_type": "CopyActivity",
            "raw_definition": {"name": "Extract", "type": "CopyActivity"},
            "pipeline": "parent",
        },
        {
            "activity_name": "Load",
            "activity_type": "NotebookActivity",
            "raw_definition": {"name": "Load"},
            "pipeline": "child",
        },
    ]
    (work / "gaps.json").write_text(json.dumps(gaps, indent=2), encoding="utf-8")


def test_route_audit_summarises_components_decisions_and_gaps(tmp_path: Path) -> None:
    _write_plan(tmp_path)
    _write_gaps(tmp_path)

    audit_path = _write_route_audit(tmp_path)

    assert audit_path == tmp_path / "metadata" / "route_audit.json"
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    assert audit["recorded_against_inventory_sha256"] == "abc123"
    # Both routed components and their decisions are recorded.
    decisions = {component["component_id"]: component["decision"] for component in audit["components"]}
    assert decisions == {"component-1": "agentic", "component-2": "deterministic"}
    # Only the agentic component's members are surfaced as agentic pipelines.
    assert audit["agentic_pipelines"] == ["child", "parent"]
    # The gaps introduced survive as a compact summary (no verbose raw_definition).
    assert audit["gaps_count"] == 2
    assert {gap["pipeline"] for gap in audit["gaps_introduced"]} == {"parent", "child"}
    assert all("raw_definition" not in gap for gap in audit["gaps_introduced"])


def test_route_audit_noops_without_a_recorded_plan(tmp_path: Path) -> None:
    # No conversion_plan.json -> no routing happened -> nothing is written (no-route path stays clean).
    (tmp_path / "metadata").mkdir(parents=True, exist_ok=True)

    audit_path = _write_route_audit(tmp_path)

    assert audit_path is None
    assert not (tmp_path / "metadata" / "route_audit.json").exists()


def test_route_audit_handles_missing_gaps_file(tmp_path: Path) -> None:
    # A recorded plan but no gaps.json (e.g. all-deterministic route) still writes an audit with zero gaps.
    _write_plan(tmp_path)

    audit_path = _write_route_audit(tmp_path)

    assert audit_path is not None
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    assert audit["gaps_count"] == 0
    assert audit["gaps_introduced"] == []
