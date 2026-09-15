"""Tests for agent-authored discover insights: models, validation, and the atomic merge.

The inventory fixtures are built by hand through the source-agnostic emitter
(:func:`flowx.discovery_inventory.build_source_inventory`) -- no ADF, no Airflow -- so the
insights engine is proven against the standardised inventory shape, control-edge lineage and
all, exactly as it will see it in production.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from flowx.adapter.__main__ import main as adapter_cli_main
from flowx.discovery_insights import (
    INSIGHTS_KEY,
    enrich_inventory,
    inventory_fingerprint,
    load_insights,
    merge_into_inventory,
    validate_insights,
)
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.models.discovery import CONCEPT_NOTEBOOK, CONCEPT_RUN_WORKFLOW, SourceGraph, SourceNode
from flowx.models.insights import (
    Insights,
    LineageEdgeRef,
    PipelineInsight,
    PipelineRelationship,
    RecommendedPattern,
    SystemRecommendation,
)
from flowx.models.ir import ControlEdge, Lineage

# --------------------------------------------------------------------------- #
# Fixtures: a two-pipeline factory where "parent" invokes "child" via a proven
# control edge, plus a standalone "sibling" for inferred-coupling tests.
# --------------------------------------------------------------------------- #


def _node(task_key: str, native_type: str, concept: str = CONCEPT_NOTEBOOK) -> SourceNode:
    return SourceNode(
        source_id=task_key,
        task_key=task_key,
        concept=concept,
        source="unit",
        name=task_key,
        native_type=native_type,
        properties={STRATEGY_PROPERTY: "deterministic"},
        raw={"name": task_key, "type": native_type},
    )


def _inventory() -> dict[str, Any]:
    parent = SourceGraph(
        name="parent",
        source="unit",
        tasks=[_node("call_child", "ExecutePipeline", CONCEPT_RUN_WORKFLOW)],
        lineage=Lineage(
            control_edges=[ControlEdge(source_workflow="parent", target_workflow="child", via_task_key="call_child")]
        ),
    )
    child = SourceGraph(name="child", source="unit", tasks=[_node("load", "Notebook")])
    sibling = SourceGraph(name="sibling", source="unit", tasks=[_node("export", "Notebook")])
    return build_source_inventory([parent, child, sibling], source="unit", source_dir="/tmp/src")


def _valid_insights() -> dict[str, Any]:
    return {
        "overview": "A parent orchestrates a child extractor; a sibling exports downstream.",
        "system_recommendation": {
            "headline": "Collapse the extraction factory onto a managed connector",
            "recommended_patterns": [
                {
                    "pattern": "Lakeflow Connect SQL Server connector",
                    "fit": "Replaces the child extractor",
                    "simplification_pattern": True,
                },
                {
                    "pattern": "Parameterised Lakeflow Job",
                    "fit": "Like-for-like orchestration",
                    "simplification_pattern": False,
                },
            ],
            "cascade": ["child extractor -> managed connector pipeline"],
            "decision_driver": "Is the Lakeflow Connect connector GA for this source?",
        },
        "pipeline_insights": [
            {
                "pipeline": "child",
                "intent": "Extract a table into the lake",
                "databricks_pattern": "Managed ingestion",
                "recommended_patterns": [
                    {"pattern": "Lakeflow Connect", "fit": "Managed CDC ingestion", "simplification_pattern": True}
                ],
                "conversion_notes": ["Point the connector at the same source"],
                "risk_if_ignored": "Bespoke extractor code carries forward",
            }
        ],
        "pipeline_relationships": [
            {
                "from_pipeline": "parent",
                "to_pipeline": "child",
                "lineage_edge": {"edge_type": "control", "edge_identity": "call_child"},
                "relationship_summary": "parent runs child",
            },
            {
                "from_pipeline": "child",
                "to_pipeline": "sibling",
                "lineage_edge": {
                    "edge_type": "inferred",
                    "edge_identity": "shared table sales.curated",
                    "evidence": "Both notebooks read/write sales.curated in their code",
                    "confidence": "medium",
                },
            },
        ],
    }


def _write_inventory(output_dir: Path, inventory: dict[str, Any]) -> Path:
    """Write inventory.json exactly as the discover phase does (json.dumps(indent=2), no newline)."""
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    path = metadata / "inventory.json"
    path.write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    return path


# --------------------------------------------------------------------------- #
# Models.
# --------------------------------------------------------------------------- #


def test_models_construct_and_document_the_contract() -> None:
    insights = Insights(
        overview="o",
        system_recommendation=SystemRecommendation(
            headline="h",
            recommended_patterns=[RecommendedPattern(pattern="p", fit="f", simplification_pattern=True)],
        ),
        pipeline_insights=[PipelineInsight(pipeline="child", intent="i")],
        pipeline_relationships=[
            PipelineRelationship(
                from_pipeline="parent",
                to_pipeline="child",
                lineage_edge=LineageEdgeRef(edge_type="control", edge_identity="call_child"),
            )
        ],
    )
    assert insights.pipeline_insights[0].pipeline == "child"
    assert insights.pipeline_relationships[0].lineage_edge.edge_type == "control"


# --------------------------------------------------------------------------- #
# Validation: success.
# --------------------------------------------------------------------------- #


def test_valid_insights_pass_validation() -> None:
    assert validate_insights(_valid_insights(), _inventory()) == []


# --------------------------------------------------------------------------- #
# Validation: failure modes.
# --------------------------------------------------------------------------- #


def test_non_dict_payload_is_a_violation() -> None:
    violations = validate_insights([1, 2, 3], _inventory())
    assert violations == ["insights must be a JSON object, got list"]


def test_unknown_pipeline_reference_in_insight() -> None:
    raw = _valid_insights()
    raw["pipeline_insights"][0]["pipeline"] = "ghost"
    violations = validate_insights(raw, _inventory())
    assert any("pipeline 'ghost' not in inventory" in v for v in violations)


def test_unknown_pipeline_reference_in_relationship_endpoint() -> None:
    raw = _valid_insights()
    raw["pipeline_relationships"][0]["to_pipeline"] = "ghost"
    violations = validate_insights(raw, _inventory())
    assert any("to_pipeline 'ghost' not in inventory" in v for v in violations)


def test_control_edge_not_matching_inventory_lineage() -> None:
    raw = _valid_insights()
    # Wrong via_task_key -- no such control edge from parent to child.
    raw["pipeline_relationships"][0]["lineage_edge"]["edge_identity"] = "not_a_real_task"
    violations = validate_insights(raw, _inventory())
    assert any("does not resolve to a lineage edge from 'parent' to 'child'" in v for v in violations)


def test_control_edge_with_wrong_endpoints_does_not_resolve() -> None:
    raw = _valid_insights()
    # The via_task_key is real, but between parent->child, not child->sibling.
    raw["pipeline_relationships"][0]["from_pipeline"] = "child"
    raw["pipeline_relationships"][0]["to_pipeline"] = "sibling"
    violations = validate_insights(raw, _inventory())
    assert any("call_child' does not resolve" in v for v in violations)


def test_control_edge_may_not_carry_evidence_or_confidence() -> None:
    raw = _valid_insights()
    raw["pipeline_relationships"][0]["lineage_edge"]["evidence"] = "nope"
    raw["pipeline_relationships"][0]["lineage_edge"]["confidence"] = "high"
    violations = validate_insights(raw, _inventory())
    assert any("'evidence' is only valid on an 'inferred' edge" in v for v in violations)
    assert any("'confidence' is only valid on an 'inferred' edge" in v for v in violations)


def test_inferred_edge_requires_evidence_and_confidence() -> None:
    raw = _valid_insights()
    edge = raw["pipeline_relationships"][1]["lineage_edge"]
    del edge["evidence"]
    edge["confidence"] = "certain"
    violations = validate_insights(raw, _inventory())
    assert any("requires a non-empty 'evidence' string" in v for v in violations)
    assert any("requires 'confidence' in" in v for v in violations)


def test_unknown_edge_type_is_rejected() -> None:
    raw = _valid_insights()
    raw["pipeline_relationships"][0]["lineage_edge"]["edge_type"] = "data"
    violations = validate_insights(raw, _inventory())
    assert any("edge_type must be 'control' or 'inferred'" in v for v in violations)


def test_unknown_top_level_key_including_library_owned_fields() -> None:
    raw = _valid_insights()
    raw["schema_version"] = "1"
    raw["nonsense"] = True
    violations = validate_insights(raw, _inventory())
    assert any("unknown top-level key: 'schema_version' (set by the library, not the author)" in v for v in violations)
    assert any("unknown top-level key: 'nonsense'" in v for v in violations)


def test_recommended_patterns_cap_and_shape() -> None:
    raw = _valid_insights()
    raw["pipeline_insights"][0]["recommended_patterns"] = [
        {"pattern": f"p{i}", "fit": "f", "simplification_pattern": False} for i in range(5)
    ]
    violations = validate_insights(raw, _inventory())
    assert any("at most 4 are allowed" in v for v in violations)


def test_recommended_pattern_simplification_flag_must_be_boolean() -> None:
    raw = _valid_insights()
    raw["pipeline_insights"][0]["recommended_patterns"][0]["simplification_pattern"] = "yes"
    violations = validate_insights(raw, _inventory())
    assert any("'simplification_pattern' must be a boolean" in v for v in violations)


def test_empty_recommended_patterns_list_is_rejected() -> None:
    raw = _valid_insights()
    raw["pipeline_insights"][0]["recommended_patterns"] = []
    violations = validate_insights(raw, _inventory())
    assert any("must contain 1-4 patterns when present" in v for v in violations)


def test_system_recommendation_requires_headline_and_patterns() -> None:
    raw = _valid_insights()
    raw["system_recommendation"] = {"cascade": ["x"]}
    violations = validate_insights(raw, _inventory())
    assert any("'headline' must be a non-empty string" in v for v in violations)
    assert any("missing required field 'recommended_patterns'" in v for v in violations)


# --------------------------------------------------------------------------- #
# Merge: fingerprint, additive key, byte-compat, atomicity, idempotency.
# --------------------------------------------------------------------------- #


def test_merge_adds_single_additive_block_with_fingerprint_and_schema_version() -> None:
    inventory = _inventory()
    merged = merge_into_inventory(inventory, _valid_insights())
    # Original keys are untouched and one additive key is appended, last.
    assert list(merged) == ["source", "source_dir", "pipelines", "summary", "insights"]
    block = merged[INSIGHTS_KEY]
    assert block["schema_version"] == "1"
    assert block["inventory_sha256"] == inventory_fingerprint(inventory)
    assert "overview" in block and "pipeline_relationships" in block
    # merge does not mutate the input.
    assert INSIGHTS_KEY not in inventory


def test_enrich_writes_additive_block_and_leaves_existing_keys_byte_identical(tmp_path: Path) -> None:
    inventory = _inventory()
    path = _write_inventory(tmp_path, inventory)
    original_bytes = path.read_bytes()

    result = enrich_inventory(tmp_path, insights=_valid_insights())
    assert result["ok"] is True
    assert result["pipeline_insights"] == 1
    assert result["relationships"] == 2

    enriched = json.loads(path.read_text(encoding="utf-8"))
    assert INSIGHTS_KEY in enriched
    # Every existing key is byte-identical: strip the additive block and re-serialise.
    stripped = {k: v for k, v in enriched.items() if k != INSIGHTS_KEY}
    assert json.dumps(stripped, indent=2).encode("utf-8") == original_bytes


def test_enrich_leaves_inventory_untouched_on_validation_failure(tmp_path: Path) -> None:
    path = _write_inventory(tmp_path, _inventory())
    original_bytes = path.read_bytes()

    raw = _valid_insights()
    raw["pipeline_insights"][0]["pipeline"] = "ghost"
    result = enrich_inventory(tmp_path, insights=raw)

    assert result["ok"] is False
    assert any("ghost" in v for v in result["violations"])
    assert path.read_bytes() == original_bytes  # untouched


def test_enrich_is_idempotent_and_replaces_prior_block(tmp_path: Path) -> None:
    path = _write_inventory(tmp_path, _inventory())

    first = enrich_inventory(tmp_path, insights=_valid_insights())
    after_first = path.read_bytes()
    # Re-running with the same authored insights rewrites byte-identical content.
    second = enrich_inventory(tmp_path, insights=_valid_insights())
    assert path.read_bytes() == after_first
    assert first["inventory_sha256"] == second["inventory_sha256"]

    # Enriching with different insights replaces (not stacks) the block; fingerprint unchanged
    # because the deterministic base is the same.
    changed = _valid_insights()
    changed["overview"] = "A different narrative"
    third = enrich_inventory(tmp_path, insights=changed)
    enriched = json.loads(path.read_text(encoding="utf-8"))
    assert enriched[INSIGHTS_KEY]["overview"] == "A different narrative"
    assert third["inventory_sha256"] == first["inventory_sha256"]
    # Still exactly one insights block.
    assert list(enriched).count(INSIGHTS_KEY) == 1


def test_fingerprint_ignores_any_prior_insights_block() -> None:
    inventory = _inventory()
    base_fingerprint = inventory_fingerprint(inventory)
    enriched = merge_into_inventory(inventory, _valid_insights())
    # Fingerprinting the already-enriched inventory yields the same digest (insights excluded).
    assert inventory_fingerprint(enriched) == base_fingerprint


def test_enrich_raises_when_inventory_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        enrich_inventory(tmp_path, insights=_valid_insights())


def test_load_insights_requires_exactly_one_source(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        load_insights()
    with pytest.raises(ValueError):
        load_insights(insights={}, insights_path=tmp_path / "x.json")
    path = tmp_path / "insights.json"
    path.write_text(json.dumps({"overview": "hi"}), encoding="utf-8")
    assert load_insights(insights_path=path) == {"overview": "hi"}


# --------------------------------------------------------------------------- #
# CLI wiring.
# --------------------------------------------------------------------------- #


def test_cli_enrich_success(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _write_inventory(tmp_path, _inventory())
    insights_path = tmp_path / "insights.json"
    insights_path.write_text(json.dumps(_valid_insights()), encoding="utf-8")

    code = adapter_cli_main(["enrich", "--output-dir", str(tmp_path), "--insights-path", str(insights_path)])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True and payload["relationships"] == 2


def test_cli_enrich_validation_failure_returns_1_and_emits_violations(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_inventory(tmp_path, _inventory())
    raw = _valid_insights()
    raw["pipeline_relationships"][0]["lineage_edge"]["edge_identity"] = "nope"
    insights_path = tmp_path / "insights.json"
    insights_path.write_text(json.dumps(raw), encoding="utf-8")

    code = adapter_cli_main(["enrich", "--output-dir", str(tmp_path), "--insights-path", str(insights_path)])
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False and payload["violations"]


# --------------------------------------------------------------------------- #
# MCP wiring.
# --------------------------------------------------------------------------- #


def test_mcp_enrich_inline_insights(monkeypatch, tmp_path: Path) -> None:
    server = pytest.importorskip("flowx.mcp.server")
    runner = pytest.importorskip("flowx.mcp.runner")

    captured: dict[str, Any] = {}

    class _Result:
        ok = True
        returncode = 0

        def __init__(self, stdout: str) -> None:
            self.stdout = stdout
            self.stderr = ""

        def as_dict(self) -> dict[str, Any]:
            return {"returncode": 0, "stdout": self.stdout, "stderr": ""}

    def fake_run_adapter(args: list[Any]) -> Any:
        captured["args"] = [str(a) for a in args]
        # The inline dict is staged to a temp file that the CLI would read; here just echo success.
        return _Result(json.dumps({"ok": True, "violations": [], "pipeline_insights": 1, "relationships": 2}))

    monkeypatch.setattr(runner, "run_adapter", fake_run_adapter)

    out = server._cmd_enrich({"output_dir": str(tmp_path), "insights": _valid_insights()})
    assert out["ok"] is True
    assert out["result"]["relationships"] == 2
    # The handler forwarded a real --insights-path (the staged temp file) to the CLI.
    assert captured["args"][0] == "enrich"
    assert "--insights-path" in captured["args"]


def test_mcp_enrich_requires_exactly_one_source(tmp_path: Path) -> None:
    server = pytest.importorskip("flowx.mcp.server")
    both = server._cmd_enrich({"output_dir": str(tmp_path), "insights": {}, "insights_path": "x.json"})
    neither = server._cmd_enrich({"output_dir": str(tmp_path)})
    assert both["ok"] is False and "exactly one" in both["error"]
    assert neither["ok"] is False and "exactly one" in neither["error"]


# Ensure the deep-copied fixtures never share mutable state between tests.
def test_fixture_isolation() -> None:
    a = _valid_insights()
    b = _valid_insights()
    a["pipeline_insights"][0]["pipeline"] = "mutated"
    assert b["pipeline_insights"][0]["pipeline"] == "child"
    assert copy.deepcopy(a) == a
