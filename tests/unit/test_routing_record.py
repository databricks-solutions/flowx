"""Tests for validating an authored conversion plan and recording it (routing, #77).

The agent authors ONLY the per-component decision (and an optional rationale); the library recomputes
members, recommended, and both options' evidence on record, and binds the plan to the inventory with
a SHA-256 fingerprint. The recorded ``conversion_plan.json`` is a separate additive artifact -- it
never mutates ``inventory.json``. Fixtures are built through the source-agnostic emitter so the engine
is proven against the production inventory shape.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from flowx.adapter.__main__ import main as adapter_cli_main
from flowx.discovery_insights import inventory_fingerprint
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.models.conversion_plan import (
    SCHEMA_VERSION,
    ComponentPlan,
    ConversionPlan,
)
from flowx.models.discovery import CONCEPT_NOTEBOOK, CONCEPT_RUN_WORKFLOW, SourceGraph, SourceNode
from flowx.models.ir import ControlEdge, Lineage
from flowx.routing import (
    PLAN_FILENAME,
    build_recommendation,
    load_plan,
    record_plan,
    validate_plan,
)


def _node(task_key: str, native_type: str, *, strategy: str = "deterministic") -> SourceNode:
    return SourceNode(
        source_id=task_key,
        task_key=task_key,
        concept=CONCEPT_NOTEBOOK,
        source="unit",
        name=task_key,
        native_type=native_type,
        properties={STRATEGY_PROPERTY: strategy},
        raw={"name": task_key, "type": native_type},
    )


def _inventory() -> dict[str, Any]:
    """A two-pipeline component (parent -> child, both engine-capable) plus a standalone 'solo'.

    'child' carries a Lakeflow Connect simplification insight, so its component is deterministic-capable
    AND has a prominent agentic re-architecture option -- the deterministic-vs-LFC choice #77 surfaces.
    """
    parent = SourceGraph(
        name="parent",
        source="unit",
        tasks=[_node("call_child", "ExecutePipeline")],
        lineage=Lineage(
            control_edges=[ControlEdge(source_workflow="parent", target_workflow="child", via_task_key="call_child")]
        ),
    )
    child = SourceGraph(name="child", source="unit", tasks=[_node("copy_orders", "Copy")])
    solo = SourceGraph(name="solo", source="unit", tasks=[_node("load", "Notebook")])
    inventory = build_source_inventory([parent, child, solo], source="unit", source_dir="/tmp/src")
    inventory["insights"] = {
        "pipeline_insights": [
            {
                "pipeline": "child",
                "recommended_patterns": [
                    {
                        "pattern": "Lakeflow Connect SQL Server connector",
                        "fit": "Replaces the bespoke Copy extractor with a managed pipeline",
                        "simplification_pattern": True,
                    }
                ],
            }
        ]
    }
    return inventory


def _authored_plan() -> dict[str, Any]:
    """Accept-the-recommendation plan: decision == recommended for both components."""
    return {
        "components": [
            {"component_id": "component-1", "members": ["child", "parent"], "decision": "deterministic"},
            {"component_id": "component-2", "members": ["solo"], "decision": "deterministic"},
        ]
    }


def _write_inventory(output_dir: Path, inventory: dict[str, Any]) -> Path:
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    path = metadata / "inventory.json"
    path.write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    return path


# --------------------------------------------------------------------------- #
# Models.
# --------------------------------------------------------------------------- #


def test_models_construct_and_document_the_contract() -> None:
    plan = ConversionPlan(
        inventory_sha256="abc",
        components=[
            ComponentPlan(
                component_id="component-1",
                members=["child", "parent"],
                recommended="deterministic",
                decision="agentic",
            )
        ],
    )
    assert plan.schema_version == SCHEMA_VERSION
    assert plan.components[0].decision == "agentic"


# --------------------------------------------------------------------------- #
# Validation: success (including the default plan round-trip).
# --------------------------------------------------------------------------- #


def test_valid_plan_passes_validation() -> None:
    assert validate_plan(_authored_plan(), _inventory()) == []


def test_default_plan_from_recommendation_validates() -> None:
    inventory = _inventory()
    default_plan = build_recommendation(inventory)["default_plan"]
    assert validate_plan(default_plan, inventory) == []


# --------------------------------------------------------------------------- #
# Validation: failure modes.
# --------------------------------------------------------------------------- #


def test_non_dict_payload_is_a_violation() -> None:
    assert validate_plan([1, 2], _inventory()) == ["conversion plan must be a JSON object, got list"]


def test_unknown_top_level_key_including_library_owned_fields() -> None:
    raw = _authored_plan()
    raw["schema_version"] = "1"
    raw["bogus"] = True
    violations = validate_plan(raw, _inventory())
    assert any("unknown top-level key: 'schema_version' (set by the library, not the author)" in v for v in violations)
    assert any("unknown top-level key: 'bogus'" in v for v in violations)


def test_library_owned_component_fields_are_rejected() -> None:
    raw = _authored_plan()
    raw["components"][0]["recommended"] = "deterministic"
    raw["components"][0]["options"] = {}
    violations = validate_plan(raw, _inventory())
    assert any("unknown field 'recommended' (set by the library, not the author)" in v for v in violations)
    assert any("unknown field 'options' (set by the library, not the author)" in v for v in violations)


def test_invalid_decision_value_is_rejected() -> None:
    raw = _authored_plan()
    raw["components"][0]["decision"] = "maybe"
    violations = validate_plan(raw, _inventory())
    assert any("'decision' must be one of" in v for v in violations)


def test_unknown_pipeline_in_members_is_rejected() -> None:
    raw = _authored_plan()
    raw["components"][0]["members"] = ["child", "ghost"]
    violations = validate_plan(raw, _inventory())
    assert any("pipeline 'ghost' not in inventory" in v for v in violations)


def test_members_that_do_not_form_a_component_are_rejected() -> None:
    # 'parent' and 'solo' are in different components; pairing them is not a coherent route.
    raw = {
        "components": [
            {"component_id": "component-1", "members": ["parent", "solo"], "decision": "deterministic"},
        ]
    }
    violations = validate_plan(raw, _inventory())
    assert any("do not form a connected component" in v for v in violations)


def test_partial_plan_missing_a_component_is_rejected() -> None:
    raw = {
        "components": [
            {"component_id": "component-1", "members": ["child", "parent"], "decision": "deterministic"},
        ]
    }
    violations = validate_plan(raw, _inventory())
    assert any("every component must be routed" in v for v in violations)


def test_duplicate_decision_for_one_component_is_rejected() -> None:
    raw = {
        "components": [
            {"component_id": "component-1", "members": ["child", "parent"], "decision": "deterministic"},
            {"component_id": "component-1", "members": ["child", "parent"], "decision": "agentic"},
            {"component_id": "component-2", "members": ["solo"], "decision": "deterministic"},
        ]
    }
    violations = validate_plan(raw, _inventory())
    assert any("decided" in v and "times" in v for v in violations)


def test_component_id_mismatch_is_rejected() -> None:
    raw = _authored_plan()
    raw["components"][0]["component_id"] = "component-9"
    violations = validate_plan(raw, _inventory())
    assert any("does not match" in v for v in violations)


def test_duplicate_members_are_rejected() -> None:
    # A frozenset would collapse ["solo", "solo"] to {"solo"} and wrongly match component-2; the
    # members-match/bijection contract must reject the duplicate explicitly.
    raw = _authored_plan()
    raw["components"][1]["members"] = ["solo", "solo"]
    violations = validate_plan(raw, _inventory())
    assert any("duplicate" in v.lower() and "solo" in v for v in violations)


def test_rationale_must_be_non_empty_when_present() -> None:
    raw = _authored_plan()
    raw["components"][0]["rationale"] = "   "
    violations = validate_plan(raw, _inventory())
    assert any("'rationale' must be a non-empty string" in v for v in violations)


# --------------------------------------------------------------------------- #
# Record: fingerprint binding, both-options shape, atomicity, idempotency.
# --------------------------------------------------------------------------- #


def test_record_writes_plan_with_both_options_recommended_and_fingerprint(tmp_path: Path) -> None:
    inventory = _inventory()
    _write_inventory(tmp_path, inventory)
    result = record_plan(tmp_path, plan=_authored_plan())
    assert result["ok"] is True
    assert result["components"] == 2

    plan = json.loads((tmp_path / "metadata" / PLAN_FILENAME).read_text(encoding="utf-8"))
    assert plan["schema_version"] == SCHEMA_VERSION
    assert plan["inventory_sha256"] == inventory_fingerprint(inventory)
    component = plan["components"][0]
    assert component["members"] == ["child", "parent"]
    assert component["recommended"] == "deterministic"
    assert component["decision"] == "deterministic"
    assert set(component["options"]) == {"deterministic", "agentic"}
    # The agentic option's simplification pattern is surfaced as a first-class peer, not buried.
    assert component["options"]["agentic"]["has_simplification"] is True
    assert component["options"]["deterministic"]["capable"] is True


def test_record_preserves_a_user_override_with_both_recommended_and_decision(tmp_path: Path) -> None:
    inventory = _inventory()
    _write_inventory(tmp_path, inventory)
    raw = _authored_plan()
    # User overrides the deterministic recommendation to take the Lakeflow Connect re-architecture.
    raw["components"][0]["decision"] = "agentic"
    raw["components"][0]["rationale"] = "Adopt the managed connector to retire the extractor"
    record_plan(tmp_path, plan=raw)
    plan = json.loads((tmp_path / "metadata" / PLAN_FILENAME).read_text(encoding="utf-8"))
    component = plan["components"][0]
    assert component["recommended"] == "deterministic"
    assert component["decision"] == "agentic"
    assert component["rationale"] == "Adopt the managed connector to retire the extractor"


def test_record_leaves_inventory_byte_identical(tmp_path: Path) -> None:
    inventory = _inventory()
    inventory_path = _write_inventory(tmp_path, inventory)
    original_bytes = inventory_path.read_bytes()
    record_plan(tmp_path, plan=_authored_plan())
    assert inventory_path.read_bytes() == original_bytes


def test_record_is_idempotent(tmp_path: Path) -> None:
    inventory = _inventory()
    _write_inventory(tmp_path, inventory)
    plan_path = tmp_path / "metadata" / PLAN_FILENAME
    record_plan(tmp_path, plan=_authored_plan())
    first = plan_path.read_bytes()
    record_plan(tmp_path, plan=_authored_plan())
    assert plan_path.read_bytes() == first


def test_record_leaves_plan_untouched_on_validation_failure(tmp_path: Path) -> None:
    inventory = _inventory()
    _write_inventory(tmp_path, inventory)
    record_plan(tmp_path, plan=_authored_plan())  # write a good plan first
    good_bytes = (tmp_path / "metadata" / PLAN_FILENAME).read_bytes()

    bad = _authored_plan()
    bad["components"][0]["decision"] = "nonsense"
    result = record_plan(tmp_path, plan=bad)
    assert result["ok"] is False and result["violations"]
    assert (tmp_path / "metadata" / PLAN_FILENAME).read_bytes() == good_bytes


def test_record_raises_when_inventory_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        record_plan(tmp_path, plan=_authored_plan())


def test_load_plan_requires_exactly_one_source(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        load_plan()
    with pytest.raises(ValueError):
        load_plan(plan={}, plan_path=tmp_path / "x.json")


# --------------------------------------------------------------------------- #
# CLI wiring. The reshaped `route` is one command: no plan on a non-TTY emits the recommendation; a
# `--plan-path` records the plan and edits the report. (Report editing is covered end-to-end in
# test_cli_route_agentic.py; here we exercise the record/recommend wiring against a bare inventory.)
# --------------------------------------------------------------------------- #


def _write_empty_report(output_dir: Path) -> None:
    """A minimal report matching the inventory's pipelines so the edit step has something to rewrite."""
    work = output_dir / ".work"
    work.mkdir(parents=True, exist_ok=True)
    report = {
        "pipelines": [
            {"name": "parent", "tasks": [{"name": "call_child", "task_key": "call_child", "type": "CopyActivity"}]},
            {"name": "child", "tasks": [{"name": "copy_orders", "task_key": "copy_orders", "type": "CopyActivity"}]},
            {"name": "solo", "tasks": [{"name": "load", "task_key": "load", "type": "NotebookActivity"}]},
        ]
    }
    (work / "translation_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")


def test_cli_route_without_plan_emits_components(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    import io

    _write_inventory(tmp_path, _inventory())
    monkeypatch.setattr("sys.stdin", io.StringIO(""))  # not a TTY -> dry-run recommendation
    code = adapter_cli_main(["route", "--output-dir", str(tmp_path)])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [c["component_id"] for c in payload["components"]] == ["component-1", "component-2"]
    assert "default_plan" in payload


def test_cli_route_with_plan_records_and_reports_the_edit(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _write_inventory(tmp_path, _inventory())
    _write_empty_report(tmp_path)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(_authored_plan()), encoding="utf-8")
    code = adapter_cli_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True and payload["components"] == 2
    assert "edit" in payload
    assert (tmp_path / "metadata" / PLAN_FILENAME).exists()


def test_cli_route_validation_failure_returns_1(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _write_inventory(tmp_path, _inventory())
    _write_empty_report(tmp_path)
    raw = _authored_plan()
    raw["components"].pop()  # partial plan: component-2 undecided
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(raw), encoding="utf-8")
    code = adapter_cli_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)])
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False and payload["violations"]


def test_fixture_isolation() -> None:
    a = _authored_plan()
    b = _authored_plan()
    a["components"][0]["decision"] = "agentic"
    assert b["components"][0]["decision"] == "deterministic"
    assert copy.deepcopy(a) == a
    # CONCEPT_RUN_WORKFLOW is imported for symmetry with the discovery fixtures; touch it so linters
    # see the dependency used.
    assert isinstance(CONCEPT_RUN_WORKFLOW, str)
