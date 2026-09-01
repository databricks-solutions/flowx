"""Tests for agentic insights models, validation, and enrichment (discover phase)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from flowx.adapter.__main__ import main as adapter_cli_main
from flowx.mcp import runner as mcp_runner
from flowx.mcp.server import _cmd_enrich
from flowx.models.adf_ast import (
    Insights,
    LineageEdgeRef,
    PipelineInsight,
    PipelineRelationship,
    RecommendedPattern,
    SystemRecommendation,
)
from flowx.parser.adf_loader import (
    _inventory_to_dict,
    build_inventory,
    load_adf_definitions,
)
from flowx.parser.pipeline_insights import (
    enrich_inventory,
    load_insights,
    merge_into_inventory,
    validate_insights,
)


def test_insights_dataclasses_construct_with_defaults():
    edge = LineageEdgeRef(edge_type="control", edge_identity="Run Ingestion Pipeline")
    rel = PipelineRelationship(from_pipeline="factory_a", to_pipeline="factory_b", lineage_edge=edge)
    insight = PipelineInsight(pipeline="factory_a")
    doc = Insights(
        overview="whole factory",
        pipeline_insights=[insight],
        pipeline_relationships=[rel],
    )
    assert doc.pipeline_insights[0].pipeline == "factory_a"
    assert doc.pipeline_relationships[0].lineage_edge.edge_type == "control"
    assert doc.pipeline_relationships[0].lineage_edge.edge_identity == "Run Ingestion Pipeline"
    # optional fields default cleanly
    assert insight.recommended_patterns == []
    assert insight.conversion_notes == []
    assert insight.risk_if_ignored is None
    assert rel.relationship_summary is None


def test_recommended_pattern_dataclass_defaults():
    pat = RecommendedPattern(
        pattern="Lakeflow Connect SQL Server connector",
        fit="Managed CDC ingestion replaces the bespoke watermark Copy",
        simplification_pattern=True,
    )
    assert pat.pattern == "Lakeflow Connect SQL Server connector"
    assert pat.simplification_pattern is True


def test_system_recommendation_dataclass_defaults():
    sr = SystemRecommendation(headline="Managed ingestion collapses the extraction factory")
    assert sr.headline.startswith("Managed ingestion")
    assert sr.recommended_patterns == []
    assert sr.cascade == []
    assert sr.decision_driver is None


def _inventory() -> dict:
    """A minimal inventory dict in discover's serialized shape."""
    return {
        "source_dir": "/tmp/adf",
        "pipelines": [
            {"name": "factory_a", "activities": []},
            {"name": "factory_b", "activities": []},
        ],
        "summary": {"pipeline_count": 2},
        "lineage": {
            "control_edges": [
                {
                    "caller_pipeline": "factory_a",
                    "callee_pipeline": "factory_b",
                    "activity_name": "Run Ingestion Pipeline",
                    "wait_on_completion": True,
                }
            ],
            "data_edges": [
                {
                    "dataset_name": "ds_orders",
                    "identity": "curated.orders",
                    "producer_pipeline": "factory_a",
                    "producer_activity": "Write Orders",
                    "consumer_pipeline": "factory_b",
                    "consumer_activity": "Read Orders",
                    "match_kind": "identity",
                    "match_key": "curated.orders",
                }
            ],
        },
    }


def _good_insights() -> dict:
    return {
        "overview": "Two-stage ingestion then transform.",
        "pipeline_insights": [
            {
                "pipeline": "factory_a",
                "intent": "Ingest",
                "databricks_pattern": "Autoloader",
                "recommended_patterns": [
                    {
                        "pattern": "Lakeflow Connect SQL Server connector",
                        "fit": "Managed CDC ingestion replaces the bespoke watermark Copy",
                        "simplification_pattern": True,
                    },
                    {
                        "pattern": "Auto Loader",
                        "fit": "Incremental file ingestion when a managed connector is unavailable",
                        "simplification_pattern": False,
                    },
                ],
                "risk_if_ignored": "Switch-nested calls read as a leaf in lineage",
            },
            {"pipeline": "factory_b", "intent": "Transform"},
        ],
        "pipeline_relationships": [
            {
                "from_pipeline": "factory_a",
                "to_pipeline": "factory_b",
                "lineage_edge": {
                    "edge_type": "control",
                    "edge_identity": "Run Ingestion Pipeline",
                },
                "relationship_summary": "A invokes B",
                "databricks_pattern": "run_job_task",
                "risk_if_ignored": "ordering lost",
            }
        ],
    }


def test_validator_accepts_good_insights():
    assert validate_insights(_good_insights(), _inventory()) == []


def test_rejects_pipeline_not_in_inventory():
    raw = _good_insights()
    raw["pipeline_insights"][0]["pipeline"] = "ghost_pipeline"
    violations = validate_insights(raw, _inventory())
    assert violations
    assert any("ghost_pipeline" in v for v in violations)


def test_rejects_relationship_endpoint_not_in_inventory():
    raw = _good_insights()
    raw["pipeline_relationships"][0]["to_pipeline"] = "ghost_pipeline"
    violations = validate_insights(raw, _inventory())
    assert any("ghost_pipeline" in v for v in violations)


def test_rejects_unresolvable_control_edge():
    raw = _good_insights()
    raw["pipeline_relationships"][0]["lineage_edge"]["edge_identity"] = "No Such Activity"
    violations = validate_insights(raw, _inventory())
    assert any("No Such Activity" in v for v in violations)


def test_data_edge_binds_on_match_key():
    raw = _good_insights()
    raw["pipeline_relationships"][0]["lineage_edge"] = {
        "edge_type": "data",
        "edge_identity": "curated.orders",
    }
    assert validate_insights(raw, _inventory()) == []
    # a non-matching key is rejected
    raw["pipeline_relationships"][0]["lineage_edge"]["edge_identity"] = "curated.missing"
    assert validate_insights(raw, _inventory())


def test_inferred_edge_with_evidence_and_confidence_validates():
    """An inferred edge needs no deterministic edge to resolve against -- just
    a real endpoint pair, a non-empty evidence string, and a confidence level."""
    raw = _good_insights()
    raw["pipeline_relationships"][0]["lineage_edge"] = {
        "edge_type": "inferred",
        "edge_identity": "curated.orders_enriched",
        "evidence": "factory_a's notebook writes curated.orders_enriched; factory_b's notebook reads it.",
        "confidence": "medium",
    }
    assert validate_insights(raw, _inventory()) == []


def test_inferred_edge_requires_evidence():
    raw = _good_insights()
    raw["pipeline_relationships"][0]["lineage_edge"] = {
        "edge_type": "inferred",
        "edge_identity": "curated.orders_enriched",
        "confidence": "low",
    }
    violations = validate_insights(raw, _inventory())
    assert any("evidence" in v for v in violations)


def test_inferred_edge_requires_valid_confidence():
    raw = _good_insights()
    raw["pipeline_relationships"][0]["lineage_edge"] = {
        "edge_type": "inferred",
        "edge_identity": "curated.orders_enriched",
        "evidence": "shared table observed in both notebooks",
        "confidence": "pretty-sure",
    }
    violations = validate_insights(raw, _inventory())
    assert any("confidence" in v for v in violations)
    # missing confidence entirely is also rejected
    del raw["pipeline_relationships"][0]["lineage_edge"]["confidence"]
    assert any("confidence" in v for v in validate_insights(raw, _inventory()))


def test_inferred_edge_does_not_resolve_against_lineage():
    """An inferred edge_identity is agent-authored, not a real edge key, so it must
    NOT be validated against the deterministic lineage sets."""
    raw = _good_insights()
    raw["pipeline_relationships"][0]["lineage_edge"] = {
        "edge_type": "inferred",
        # deliberately not a real control activity_name or data match_key
        "edge_identity": "not-a-real-lineage-key",
        "evidence": "coupling inferred from shared notebook output path",
        "confidence": "high",
    }
    assert validate_insights(raw, _inventory()) == []


def test_annotation_edge_rejects_evidence_confidence():
    """evidence/confidence are inferred-only; a control/data edge carrying them is rejected."""
    raw = _good_insights()
    raw["pipeline_relationships"][0]["lineage_edge"]["evidence"] = "should not be here"
    raw["pipeline_relationships"][0]["lineage_edge"]["confidence"] = "high"
    violations = validate_insights(raw, _inventory())
    assert any("evidence" in v for v in violations)
    assert any("confidence" in v for v in violations)


def test_control_edge_resolves_on_full_triple_not_just_activity_name():
    """A control edge_identity that is a real activity_name but belongs to a
    DIFFERENT (caller, callee) pair must be rejected.

    ADF names the ExecutePipeline activity after the callee, so one activity_name
    is shared by every caller of that callee; resolving on the bare name (a global
    set) would wrongly accept a relationship whose from/to point at another pair.
    """
    inventory = {
        "pipelines": [
            {"name": "orchestrator_a", "activities": []},
            {"name": "orchestrator_b", "activities": []},
            {"name": "shared_callee", "activities": []},
        ],
        "summary": {"pipeline_count": 3},
        "lineage": {
            "control_edges": [
                {
                    "caller_pipeline": "orchestrator_a",
                    "callee_pipeline": "shared_callee",
                    "activity_name": "Run Shared",
                    "wait_on_completion": True,
                },
                {
                    "caller_pipeline": "orchestrator_b",
                    "callee_pipeline": "shared_callee",
                    "activity_name": "Run Shared",  # same name, different caller
                    "wait_on_completion": True,
                },
            ],
            "data_edges": [],
        },
    }
    # Real edge: orchestrator_a -> shared_callee with "Run Shared" resolves.
    good = {
        "pipeline_insights": [],
        "pipeline_relationships": [
            {
                "from_pipeline": "orchestrator_a",
                "to_pipeline": "shared_callee",
                "lineage_edge": {"edge_type": "control", "edge_identity": "Run Shared"},
            }
        ],
    }
    assert validate_insights(good, inventory) == []

    # Wrong pair: no edge orchestrator_a -> orchestrator_b exists, even though the
    # activity_name "Run Shared" is a real name elsewhere. Must be rejected.
    bad = json.loads(json.dumps(good))
    bad["pipeline_relationships"][0]["to_pipeline"] = "orchestrator_b"
    violations = validate_insights(bad, inventory)
    assert violations, "a valid activity_name on the wrong (from,to) pair must not resolve"
    assert any("orchestrator_b" in v for v in violations)

    # Right pair, wrong identity: the edge orchestrator_a -> shared_callee is real,
    # but "Nope" is not its activity_name. Must be rejected.
    wrong_id = json.loads(json.dumps(good))
    wrong_id["pipeline_relationships"][0]["lineage_edge"]["edge_identity"] = "Nope"
    assert validate_insights(wrong_id, inventory)

    # Reversed direction: the real edge is caller=orchestrator_a -> callee=shared_callee.
    # Swapping from/to is NOT a real edge and must be rejected -- pins the non-reversed
    # from->caller / to->callee mapping so a future refactor cannot silently flip it.
    reversed_rel = json.loads(json.dumps(good))
    reversed_rel["pipeline_relationships"][0]["from_pipeline"] = "shared_callee"
    reversed_rel["pipeline_relationships"][0]["to_pipeline"] = "orchestrator_a"
    assert validate_insights(reversed_rel, inventory), "reversed-direction edge must not resolve"


def test_data_edge_resolves_on_full_triple_not_just_match_key():
    """Two producers write the same match_key. A relationship must resolve only to
    the producer/consumer pair that actually exists, not to any edge with that key."""
    inventory = {
        "pipelines": [
            {"name": "producer_p", "activities": []},
            {"name": "producer_q", "activities": []},
            {"name": "consumer_c", "activities": []},
        ],
        "summary": {"pipeline_count": 3},
        "lineage": {
            "control_edges": [],
            "data_edges": [
                {
                    "dataset_name": "ds",
                    "identity": "curated.shared",
                    "producer_pipeline": "producer_p",
                    "producer_activity": "Write",
                    "consumer_pipeline": "consumer_c",
                    "consumer_activity": "Read",
                    "match_kind": "identity",
                    "match_key": "curated.shared",
                },
                {
                    "dataset_name": "ds",
                    "identity": "curated.shared",
                    "producer_pipeline": "producer_q",  # same key, different producer
                    "producer_activity": "Write",
                    "consumer_pipeline": "consumer_c",
                    "consumer_activity": "Read",
                    "match_kind": "identity",
                    "match_key": "curated.shared",
                },
            ],
        },
    }
    good = {
        "pipeline_insights": [],
        "pipeline_relationships": [
            {
                "from_pipeline": "producer_q",
                "to_pipeline": "consumer_c",
                "lineage_edge": {"edge_type": "data", "edge_identity": "curated.shared"},
            }
        ],
    }
    assert validate_insights(good, inventory) == []

    # producer_p -> producer_q is NOT a real edge, though both know curated.shared.
    bad = json.loads(json.dumps(good))
    bad["pipeline_relationships"][0]["from_pipeline"] = "producer_p"
    bad["pipeline_relationships"][0]["to_pipeline"] = "producer_q"
    assert validate_insights(bad, inventory)


def test_rejects_missing_required_field():
    # PipelineInsight missing 'pipeline'
    raw = {"pipeline_insights": [{"intent": "x"}], "pipeline_relationships": []}
    assert any("pipeline" in v for v in validate_insights(raw, _inventory()))
    # PipelineRelationship missing 'lineage_edge'
    raw2 = {
        "pipeline_insights": [],
        "pipeline_relationships": [{"from_pipeline": "factory_a", "to_pipeline": "factory_b"}],
    }
    assert any("lineage_edge" in v for v in validate_insights(raw2, _inventory()))


def test_rejects_unknown_field():
    raw = _good_insights()
    raw["pipeline_insights"][0]["bogus_key"] = "x"
    assert any("bogus_key" in v for v in validate_insights(raw, _inventory()))


def test_rejects_unknown_top_level_key():
    raw = _good_insights()
    raw["surprise"] = 1
    assert any("surprise" in v for v in validate_insights(raw, _inventory()))


# --- recommended_patterns -------------------------------------------------


def _set_patterns(raw: dict, patterns: object) -> dict:
    """Set factory_a's recommended_patterns to *patterns* and return the dict."""
    raw["pipeline_insights"][0]["recommended_patterns"] = patterns
    return raw


def test_recommended_patterns_optional_when_omitted():
    raw = _good_insights()
    del raw["pipeline_insights"][0]["recommended_patterns"]
    assert validate_insights(raw, _inventory()) == []


def test_recommended_patterns_accepts_one_to_four():
    one = [{"pattern": "Lakeflow Jobs", "fit": "orchestration", "simplification_pattern": True}]
    assert validate_insights(_set_patterns(_good_insights(), one), _inventory()) == []
    four = [{"pattern": f"Pattern {n}", "fit": f"reason {n}", "simplification_pattern": n % 2 == 0} for n in range(4)]
    assert validate_insights(_set_patterns(_good_insights(), four), _inventory()) == []


def test_recommended_patterns_rejects_more_than_four():
    five = [{"pattern": f"Pattern {n}", "fit": f"reason {n}", "simplification_pattern": True} for n in range(5)]
    violations = validate_insights(_set_patterns(_good_insights(), five), _inventory())
    assert any("recommended_patterns" in v for v in violations)


def test_recommended_patterns_rejects_empty_list():
    violations = validate_insights(_set_patterns(_good_insights(), []), _inventory())
    assert any("recommended_patterns" in v for v in violations)


def test_recommended_patterns_rejects_non_list():
    violations = validate_insights(_set_patterns(_good_insights(), "Lakeflow Jobs"), _inventory())
    assert any("recommended_patterns" in v and "list" in v for v in violations)


def test_recommended_patterns_rejects_non_dict_item():
    violations = validate_insights(_set_patterns(_good_insights(), ["Lakeflow Jobs"]), _inventory())
    assert any("recommended_patterns[0]" in v for v in violations)


def test_recommended_patterns_requires_pattern_and_fit():
    missing_pattern = [{"fit": "x", "simplification_pattern": True}]
    assert any(
        "pattern" in v for v in validate_insights(_set_patterns(_good_insights(), missing_pattern), _inventory())
    )
    missing_fit = [{"pattern": "Lakeflow Jobs", "simplification_pattern": True}]
    assert any("fit" in v for v in validate_insights(_set_patterns(_good_insights(), missing_fit), _inventory()))
    blank_pattern = [{"pattern": "   ", "fit": "x", "simplification_pattern": True}]
    assert any("pattern" in v for v in validate_insights(_set_patterns(_good_insights(), blank_pattern), _inventory()))


def test_recommended_patterns_simplification_pattern_must_be_bool():
    bad = [{"pattern": "Lakeflow Jobs", "fit": "x", "simplification_pattern": "yes"}]
    violations = validate_insights(_set_patterns(_good_insights(), bad), _inventory())
    assert any("simplification_pattern" in v for v in violations)
    # missing entirely is also rejected (the field is required)
    missing = [{"pattern": "Lakeflow Jobs", "fit": "x"}]
    assert any(
        "simplification_pattern" in v for v in validate_insights(_set_patterns(_good_insights(), missing), _inventory())
    )


def test_recommended_patterns_effort_field_removed():
    """`effort` was dropped from the schema; it must now be rejected as an unknown field."""
    bad = [{"pattern": "Lakeflow Jobs", "fit": "x", "simplification_pattern": True, "effort": "moderate"}]
    violations = validate_insights(_set_patterns(_good_insights(), bad), _inventory())
    assert any("effort" in v for v in violations)


def test_recommended_patterns_rejects_unknown_item_field():
    bad = [{"pattern": "Lakeflow Jobs", "fit": "x", "simplification_pattern": True, "bogus": 1}]
    violations = validate_insights(_set_patterns(_good_insights(), bad), _inventory())
    assert any("bogus" in v for v in violations)


# --- system_recommendation ------------------------------------------------


def _good_system_recommendation() -> dict:
    return {
        "headline": "Managed ingestion collapses the extraction factory",
        "recommended_patterns": [
            {
                "pattern": "Lakeflow Connect for the whole SQL Server extraction family",
                "fit": "One managed connector replaces the fan-out orchestrator, the clones, and the watermark CSV",
                "simplification_pattern": True,
            },
            {
                "pattern": "For-each orchestrator + collapsed parameterized jobs",
                "fit": "Fallback when the connector is not approved for this source",
                "simplification_pattern": False,
            },
        ],
        "cascade": [
            "clone extractors -> managed connector pipelines",
            "version-watermark CSV -> gone",
        ],
        "decision_driver": "Is the Lakeflow Connect SQL Server connector GA/approved for this source?",
    }


def test_system_recommendation_optional_when_omitted():
    raw = _good_insights()
    assert "system_recommendation" not in raw
    assert validate_insights(raw, _inventory()) == []


def test_system_recommendation_accepts_good():
    raw = _good_insights()
    raw["system_recommendation"] = _good_system_recommendation()
    assert validate_insights(raw, _inventory()) == []


def test_system_recommendation_must_be_object():
    raw = _good_insights()
    raw["system_recommendation"] = "nope"
    assert any("system_recommendation" in v for v in validate_insights(raw, _inventory()))


def test_system_recommendation_requires_headline():
    raw = _good_insights()
    sr = _good_system_recommendation()
    del sr["headline"]
    raw["system_recommendation"] = sr
    assert any("headline" in v for v in validate_insights(raw, _inventory()))


def test_system_recommendation_requires_recommended_patterns():
    raw = _good_insights()
    sr = _good_system_recommendation()
    del sr["recommended_patterns"]
    raw["system_recommendation"] = sr
    assert any("recommended_patterns" in v for v in validate_insights(raw, _inventory()))


def test_system_recommendation_reuses_pattern_validation():
    """The branch patterns go through the same validator, scoped under system_recommendation."""
    raw = _good_insights()
    sr = _good_system_recommendation()
    sr["recommended_patterns"][0]["simplification_pattern"] = "yes"  # must be a bool
    raw["system_recommendation"] = sr
    violations = validate_insights(raw, _inventory())
    assert any("simplification_pattern" in v and "system_recommendation" in v for v in violations)


def test_system_recommendation_rejects_unknown_field():
    raw = _good_insights()
    sr = _good_system_recommendation()
    sr["bogus"] = 1
    raw["system_recommendation"] = sr
    assert any("bogus" in v for v in validate_insights(raw, _inventory()))


def test_system_recommendation_cascade_must_be_list_of_strings():
    raw = _good_insights()
    sr = _good_system_recommendation()
    sr["cascade"] = "not a list"
    raw["system_recommendation"] = sr
    assert any("cascade" in v for v in validate_insights(raw, _inventory()))


def test_system_recommendation_survives_enrich_round_trip(tmp_path: Path):
    _write_inventory(tmp_path, _inventory())
    raw = _good_insights()
    raw["system_recommendation"] = _good_system_recommendation()
    result = enrich_inventory(tmp_path, insights=raw)
    assert result["ok"] is True
    on_disk = json.loads((tmp_path / "metadata" / "inventory.json").read_text())
    assert on_disk["insights"]["system_recommendation"]["headline"].startswith("Managed ingestion")


def _write_inventory(tmp_path: Path, inventory: dict) -> Path:
    """Write inventory.json exactly as discover does (indent=2, no trailing newline)."""
    metadata = tmp_path / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    path = metadata / "inventory.json"
    path.write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    return path


def test_load_insights_requires_exactly_one_source():
    with pytest.raises(ValueError):
        load_insights()
    with pytest.raises(ValueError):
        load_insights(insights={"a": 1}, insights_path=Path("/x"))


def test_load_insights_from_inline_dict():
    assert load_insights(insights={"overview": "x"}) == {"overview": "x"}


def test_load_insights_from_path(tmp_path: Path):
    p = tmp_path / "ins.json"
    p.write_text(json.dumps({"overview": "y"}), encoding="utf-8")
    assert load_insights(insights_path=p) == {"overview": "y"}


def test_merge_into_inventory_adds_one_key_without_mutating():
    inv = {"pipelines": [], "summary": {}, "lineage": {}}
    raw = {"overview": "z"}
    merged = merge_into_inventory(inv, raw)
    assert merged["insights"] == {"overview": "z"}
    assert "insights" not in inv  # input not mutated
    assert set(merged) == {"pipelines", "summary", "lineage", "insights"}


def test_enrich_success_counts_and_writes(tmp_path: Path):
    _write_inventory(tmp_path, _inventory())
    result = enrich_inventory(tmp_path, insights=_good_insights())
    assert result["ok"] is True
    assert result["violations"] == []
    assert result["pipeline_insights"] == 2
    assert result["relationships"] == 1
    on_disk = json.loads((tmp_path / "metadata" / "inventory.json").read_text())
    assert on_disk["insights"]["overview"] == "Two-stage ingestion then transform."


def test_two_pass_deterministic_keys_byte_identical(tmp_path: Path):
    path = _write_inventory(tmp_path, _inventory())
    before = path.read_text(encoding="utf-8")
    enrich_inventory(tmp_path, insights=_good_insights())
    after = json.loads(path.read_text(encoding="utf-8"))
    # every key except the added 'insights' is byte-identical to the pre-enrich file
    after_without_insights = {k: v for k, v in after.items() if k != "insights"}
    assert json.dumps(after_without_insights, indent=2) == before


def test_enrich_is_idempotent(tmp_path: Path):
    path = _write_inventory(tmp_path, _inventory())
    enrich_inventory(tmp_path, insights=_good_insights())
    first = path.read_text(encoding="utf-8")
    enrich_inventory(tmp_path, insights=_good_insights())
    second = path.read_text(encoding="utf-8")
    assert first == second


def test_validation_failure_does_not_write(tmp_path: Path):
    path = _write_inventory(tmp_path, _inventory())
    before = path.read_text(encoding="utf-8")
    bad = _good_insights()
    bad["pipeline_insights"][0]["pipeline"] = "ghost_pipeline"
    result = enrich_inventory(tmp_path, insights=bad)
    assert result["ok"] is False
    assert result["violations"]
    assert path.read_text(encoding="utf-8") == before  # file untouched


def test_enrich_missing_inventory_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        enrich_inventory(tmp_path, insights=_good_insights())


def _real_inventory(fixtures_dir) -> dict:
    """Build a real inventory dict (with lineage) from the shipped fixtures."""
    definitions = load_adf_definitions(fixtures_dir)
    inventory = build_inventory(definitions)
    return _inventory_to_dict(inventory, str(fixtures_dir))


def test_control_edge_binding_matches_and_rejects(fixtures_dir):
    inventory = _real_inventory(fixtures_dir)
    good = {
        "pipeline_insights": [],
        "pipeline_relationships": [
            {
                "from_pipeline": "pipeline_execute_pipeline_nested",
                "to_pipeline": "pipeline_copy_sql_to_delta",
                "lineage_edge": {
                    "edge_type": "control",
                    "edge_identity": "Run Ingestion Pipeline",
                },
            }
        ],
    }
    assert validate_insights(good, inventory) == []

    bad = json.loads(json.dumps(good))
    bad["pipeline_relationships"][0]["lineage_edge"]["edge_identity"] = "No Such Activity"
    assert validate_insights(bad, inventory)


def test_real_inventory_enrich_round_trip(fixtures_dir, tmp_path: Path):
    inventory = _real_inventory(fixtures_dir)
    metadata = tmp_path / "metadata"
    metadata.mkdir(parents=True)
    (metadata / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    result = enrich_inventory(
        tmp_path,
        insights={
            "overview": "orchestrated ingest/transform/cleanup",
            "pipeline_insights": [{"pipeline": "pipeline_execute_pipeline_nested", "intent": "orchestrate"}],
            "pipeline_relationships": [],
        },
    )
    assert result["ok"] is True
    on_disk = json.loads((metadata / "inventory.json").read_text())
    assert on_disk["insights"]["pipeline_insights"][0]["pipeline"] == "pipeline_execute_pipeline_nested"


def test_adapter_enrich_success(tmp_path: Path):
    _write_inventory(tmp_path, _inventory())
    ins = tmp_path / "insights.json"
    ins.write_text(json.dumps(_good_insights()), encoding="utf-8")
    code = adapter_cli_main(["enrich", "--output-dir", str(tmp_path), "--insights-path", str(ins)])
    assert code == 0
    on_disk = json.loads((tmp_path / "metadata" / "inventory.json").read_text())
    assert "insights" in on_disk


def test_adapter_enrich_validation_failure_returns_1(tmp_path: Path):
    path = _write_inventory(tmp_path, _inventory())
    before = path.read_text(encoding="utf-8")
    bad = _good_insights()
    bad["pipeline_insights"][0]["pipeline"] = "ghost_pipeline"
    ins = tmp_path / "insights.json"
    ins.write_text(json.dumps(bad), encoding="utf-8")
    code = adapter_cli_main(["enrich", "--output-dir", str(tmp_path), "--insights-path", str(ins)])
    assert code == 1
    assert path.read_text(encoding="utf-8") == before  # untouched


def test_adapter_enrich_missing_inventory_returns_1(tmp_path: Path):
    ins = tmp_path / "insights.json"
    ins.write_text(json.dumps(_good_insights()), encoding="utf-8")
    code = adapter_cli_main(["enrich", "--output-dir", str(tmp_path), "--insights-path", str(ins)])
    assert code == 1


def test_adapter_enrich_inline_json_string(tmp_path: Path):
    _write_inventory(tmp_path, _inventory())
    code = adapter_cli_main(["enrich", "--output-dir", str(tmp_path), "--insights", json.dumps(_good_insights())])
    assert code == 0
    assert "insights" in json.loads((tmp_path / "metadata" / "inventory.json").read_text())


def test_materialize_json_round_trips(tmp_path: Path):
    path = mcp_runner.materialize_json({"overview": "x"})
    try:
        assert json.loads(Path(path).read_text()) == {"overview": "x"}
    finally:
        mcp_runner.cleanup_materialized(path)
    assert not Path(path).exists()


def test_cmd_enrich_requires_a_payload():
    result = _cmd_enrich({"output_dir": "./flowx_output"})
    assert result["ok"] is False
    assert "insights" in result["error"]


def test_cmd_enrich_inline_dict_success(tmp_path: Path):
    _write_inventory(tmp_path, _inventory())
    result = _cmd_enrich({"output_dir": str(tmp_path), "insights": _good_insights()})
    assert result["ok"] is True
    assert "insights" in json.loads((tmp_path / "metadata" / "inventory.json").read_text())


def test_validate_insights_rejects_non_dict():
    """validate_insights must reject non-dict inputs with an actionable violation."""
    violations_none = validate_insights(None, _inventory())  # type: ignore[arg-type]
    assert violations_none, "expected violations for None input"
    assert any("JSON object" in v or "NoneType" in v for v in violations_none)

    violations_list = validate_insights([], _inventory())  # type: ignore[arg-type]
    assert violations_list, "expected violations for list input"
    assert any("JSON object" in v or "list" in v for v in violations_list)


def test_adapter_enrich_rejects_neither_source(tmp_path: Path):
    """enrich must return 1 when neither --insights nor --insights-path is given."""
    _write_inventory(tmp_path, _inventory())
    code = adapter_cli_main(["enrich", "--output-dir", str(tmp_path)])
    assert code == 1


def test_adapter_enrich_rejects_both_sources(tmp_path: Path):
    """enrich must return 1 when both --insights and --insights-path are given."""
    _write_inventory(tmp_path, _inventory())
    ins_file = tmp_path / "insights.json"
    ins_file.write_text(json.dumps(_good_insights()), encoding="utf-8")
    code = adapter_cli_main(
        [
            "enrich",
            "--output-dir",
            str(tmp_path),
            "--insights",
            json.dumps(_good_insights()),
            "--insights-path",
            str(ins_file),
        ]
    )
    assert code == 1


def test_adapter_enrich_rejects_malformed_inline_json(tmp_path: Path):
    """enrich must return 1 and not write when --insights is not valid JSON."""
    path = _write_inventory(tmp_path, _inventory())
    before = path.read_text(encoding="utf-8")
    code = adapter_cli_main(["enrich", "--output-dir", str(tmp_path), "--insights", "{not valid json"])
    assert code == 1
    assert path.read_text(encoding="utf-8") == before  # inventory untouched


def test_cmd_enrich_failure_surfaces_structured_violations(tmp_path: Path):
    """On a validation failure, _cmd_enrich must return ok:false with a non-empty violations list."""
    _write_inventory(tmp_path, _inventory())
    bad = _good_insights()
    bad["pipeline_insights"][0]["pipeline"] = "ghost_pipeline"
    result = _cmd_enrich({"output_dir": str(tmp_path), "insights": bad})
    assert result["ok"] is False
    violations = result.get("violations")
    assert violations, "expected a non-empty violations list on failure"
    assert any("ghost_pipeline" in v for v in violations)


def test_cmd_enrich_success_has_no_violations(tmp_path: Path):
    """On a successful enrich, _cmd_enrich must not carry stray violations."""
    _write_inventory(tmp_path, _inventory())
    result = _cmd_enrich({"output_dir": str(tmp_path), "insights": _good_insights()})
    assert result["ok"] is True
    assert not result.get("violations")


def test_cleanup_materialized_handles_all_prefixes(tmp_path: Path):
    import tempfile as _tempfile
    from pathlib import Path as _Path

    # mkdtemp dirs for all three prefixes are removed
    for prefix in ("flowx-adf-", "flowx-vol-", "flowx-ws-"):
        d = _tempfile.mkdtemp(prefix=prefix)
        assert _Path(d).is_dir()
        mcp_runner.cleanup_materialized(d)
        assert not _Path(d).exists()

    # single ARM-template file inside a flowx-adf- dir removes the parent dir
    base = _tempfile.mkdtemp(prefix="flowx-adf-")
    arm = _Path(base) / "arm_template.json"
    arm.write_text("{}", encoding="utf-8")
    mcp_runner.cleanup_materialized(str(arm))
    assert not _Path(base).exists()

    # materialize_json file is unlinked WITHOUT removing the system temp root
    f = mcp_runner.materialize_json({"a": 1})
    temp_root = _Path(_tempfile.gettempdir())
    mcp_runner.cleanup_materialized(f)
    assert not _Path(f).exists()
    assert temp_root.is_dir()  # temp root itself untouched
