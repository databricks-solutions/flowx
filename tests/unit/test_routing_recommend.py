"""Tests for the per-component conversion recommendation (routing, #77).

Each component surfaces BOTH options as first-class peers: a deterministic option (engine-capability
assessment + motif/coverage evidence + uncovered gaps) and an agentic option (recommended Databricks
patterns from the insights block, with any simplification pattern surfaced prominently). The
``recommended`` field is a library-computed starting suggestion -- deterministic only when the whole
component is engine-capable.
"""

from __future__ import annotations

from typing import Any

from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.models.ir import ControlEdge, Lineage
from flowx.models.motifs import MOTIF_METADATA_DRIVEN_BULK_COPY, DetectedMotif
from flowx.routing import build_recommendation, recommend_component


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


def _inventory(
    graphs: list[SourceGraph],
    *,
    motifs: dict[str, list[DetectedMotif]] | None = None,
    insights: dict[str, Any] | None = None,
) -> dict[str, Any]:
    inventory = build_source_inventory(graphs, source="unit", source_dir="/tmp/src", motifs_by_pipeline=motifs)
    if insights is not None:
        inventory["insights"] = insights
    return inventory


def test_all_deterministic_component_recommends_deterministic() -> None:
    graphs = [SourceGraph(name="a", source="unit", tasks=[_node("load", "Notebook"), _node("copy", "Copy")])]
    recommended, options = recommend_component(["a"], _inventory(graphs))
    assert recommended == "deterministic"
    assert options["deterministic"]["capable"] is True
    assert options["deterministic"]["uncovered"] == []
    assert options["deterministic"]["activity_counts"] == {"deterministic": 2, "agentic": 0, "unsupported": 0}


def test_uncovered_agentic_activity_recommends_agentic_and_cites_the_gap() -> None:
    graphs = [
        SourceGraph(
            name="a",
            source="unit",
            tasks=[_node("load", "Notebook"), _node("flow", "ExecuteDataFlow", strategy="agentic")],
        )
    ]
    recommended, options = recommend_component(["a"], _inventory(graphs))
    assert recommended == "agentic"
    assert options["deterministic"]["capable"] is False
    assert options["deterministic"]["uncovered"] == [
        {"pipeline": "a", "activity": "flow", "type": "ExecuteDataFlow", "strategy": "agentic"}
    ]
    assert options["deterministic"]["activity_counts"] == {"deterministic": 1, "agentic": 1, "unsupported": 0}


def test_motif_covered_agentic_activity_is_engine_capable() -> None:
    # An agentic-strategy activity that a detected motif claims is covered -> the component stays
    # engine-capable and the recommendation is deterministic.
    graphs = [
        SourceGraph(
            name="a",
            source="unit",
            tasks=[_node("lookup", "Lookup"), _node("each", "ForEach", strategy="agentic")],
        )
    ]
    motif = DetectedMotif(
        definition=MOTIF_METADATA_DRIVEN_BULK_COPY,
        matched_activities=["lookup", "each"],
        source_type_hint="database",
    )
    recommended, options = recommend_component(["a"], _inventory(graphs, motifs={"a": [motif]}))
    assert recommended == "deterministic"
    assert options["deterministic"]["capable"] is True
    assert options["deterministic"]["uncovered"] == []
    assert options["deterministic"]["motifs"] == ["metadata_driven_bulk_copy"]


def test_motif_coverage_does_not_leak_across_pipelines_in_a_component() -> None:
    # A component spans A -> B. Pipeline A has a motif claiming an activity named 'shared'; pipeline B
    # has an UNRELATED activity also named 'shared' with no motif. Motif coverage must be keyed by
    # (pipeline, activity), so B's 'shared' stays an uncovered gap rather than being masked by A's.
    graphs = [
        SourceGraph(
            name="A",
            source="unit",
            tasks=[_node("call_b", "ExecutePipeline"), _node("shared", "Copy", strategy="agentic")],
            lineage=Lineage(
                control_edges=[ControlEdge(source_workflow="A", target_workflow="B", via_task_key="call_b")]
            ),
        ),
        SourceGraph(name="B", source="unit", tasks=[_node("shared", "Copy", strategy="agentic")]),
    ]
    motif = DetectedMotif(
        definition=MOTIF_METADATA_DRIVEN_BULK_COPY, matched_activities=["shared"], source_type_hint="database"
    )
    recommended, options = recommend_component(["A", "B"], _inventory(graphs, motifs={"A": [motif]}))
    assert recommended == "agentic"
    assert options["deterministic"]["capable"] is False
    # A's 'shared' is motif-covered; only B's unrelated 'shared' is the uncovered gap.
    assert options["deterministic"]["uncovered"] == [
        {"pipeline": "B", "activity": "shared", "type": "Copy", "strategy": "agentic"}
    ]


def test_missing_strategy_counts_as_unsupported_gap() -> None:
    node = _node("mystery", "Custom")
    node.properties.pop(STRATEGY_PROPERTY)  # no strategy recorded at all
    graphs = [SourceGraph(name="a", source="unit", tasks=[node])]
    recommended, options = recommend_component(["a"], _inventory(graphs))
    assert recommended == "agentic"
    assert options["deterministic"]["activity_counts"] == {"deterministic": 0, "agentic": 0, "unsupported": 1}
    assert options["deterministic"]["uncovered"][0]["activity"] == "mystery"


def test_agentic_option_surfaces_insight_patterns_with_simplification_prominent() -> None:
    graphs = [SourceGraph(name="a", source="unit", tasks=[_node("load", "Notebook")])]
    insights = {
        "pipeline_insights": [
            {
                "pipeline": "a",
                "recommended_patterns": [
                    {
                        "pattern": "Lakeflow Connect SQL Server connector",
                        "fit": "Replaces the bespoke extractor",
                        "simplification_pattern": True,
                    },
                    {"pattern": "Parameterised Lakeflow Job", "fit": "Like-for-like", "simplification_pattern": False},
                ],
            }
        ]
    }
    _, options = recommend_component(["a"], _inventory(graphs, insights=insights))
    agentic = options["agentic"]
    assert agentic["has_simplification"] is True
    assert agentic["recommended_patterns"] == [
        {
            "pipeline": "a",
            "pattern": "Lakeflow Connect SQL Server connector",
            "fit": "Replaces the bespoke extractor",
            "simplification_pattern": True,
        },
        {
            "pipeline": "a",
            "pattern": "Parameterised Lakeflow Job",
            "fit": "Like-for-like",
            "simplification_pattern": False,
        },
    ]


def test_agentic_option_empty_when_no_insights() -> None:
    graphs = [SourceGraph(name="a", source="unit", tasks=[_node("load", "Notebook")])]
    _, options = recommend_component(["a"], _inventory(graphs))
    assert options["agentic"] == {
        "recommended_patterns": [],
        "has_simplification": False,
        "release_disclosures": [],
    }


# --------------------------------------------------------------------------- #
# Agentic option: neutral GA/Preview release-state disclosure (no warnings).
# --------------------------------------------------------------------------- #


def _insights_one_pattern(pattern: dict[str, Any]) -> dict[str, Any]:
    """An insights block with a single recommended pattern on pipeline 'a'."""
    return {"pipeline_insights": [{"pipeline": "a", "recommended_patterns": [pattern]}]}


def test_agentic_option_ga_and_unknown_are_silent() -> None:
    # 'ga' and 'unknown' both contribute NO disclosure entry -- 'unknown' is treated exactly like 'ga'.
    for state in ("ga", "unknown"):
        graphs = [SourceGraph(name="a", source="unit", tasks=[_node("load", "Notebook")])]
        insights = _insights_one_pattern(
            {
                "pattern": "Lakeflow Job",
                "fit": "Native orchestration",
                "simplification_pattern": False,
                "release_state": state,
            }
        )
        _, options = recommend_component(["a"], _inventory(graphs, insights=insights))
        assert options["agentic"]["release_disclosures"] == [], f"{state!r} must be silent"


def test_agentic_option_public_preview_is_disclosed_as_production_ready() -> None:
    graphs = [SourceGraph(name="a", source="unit", tasks=[_node("load", "Notebook")])]
    insights = _insights_one_pattern(
        {
            "pattern": "Lakeflow Connect",
            "fit": "Managed ingestion",
            "simplification_pattern": True,
            "release_state": "public_preview",
            "release_state_source": "https://docs.databricks.com/ingestion/lakeflow-connect/",
        }
    )
    _, options = recommend_component(["a"], _inventory(graphs, insights=insights))
    disclosures = options["agentic"]["release_disclosures"]
    assert len(disclosures) == 1
    disclosure = disclosures[0]
    assert disclosure["release_state"] == "public_preview"
    assert disclosure["label"] == "Public Preview (production-ready)"
    assert disclosure["pipeline"] == "a"
    # A neutral disclosure -- no warning/severity framing.
    assert "severity" not in disclosure
    # The cited source rides along into the human-readable disclosure.
    assert "docs.databricks.com" in disclosure["message"]


def test_agentic_option_private_preview_and_beta_are_plain_labels() -> None:
    for state, expected_label in (("private_preview", "Private Preview"), ("beta", "Beta")):
        graphs = [SourceGraph(name="a", source="unit", tasks=[_node("load", "Notebook")])]
        insights = _insights_one_pattern(
            {
                "pattern": "Some capability",
                "fit": "A capability",
                "simplification_pattern": False,
                "release_state": state,
                "release_state_source": "https://docs.databricks.com/some-feature/",
            }
        )
        _, options = recommend_component(["a"], _inventory(graphs, insights=insights))
        disclosures = options["agentic"]["release_disclosures"]
        assert len(disclosures) == 1
        assert disclosures[0]["label"] == expected_label
        assert disclosures[0]["release_state"] == state
        # Plain factual label -- no warning/severity field.
        assert "severity" not in disclosures[0]


def test_agentic_option_discloses_each_non_silent_pattern() -> None:
    # A component with a public-preview pattern and a private-preview pattern discloses BOTH as plain
    # neutral labels (public preview noted production-ready); a ga/unknown pattern would add nothing.
    graphs = [
        SourceGraph(
            name="parent",
            source="unit",
            tasks=[_node("call_child", "ExecutePipeline")],
            lineage=Lineage(
                control_edges=[
                    ControlEdge(source_workflow="parent", target_workflow="child", via_task_key="call_child")
                ]
            ),
        ),
        SourceGraph(name="child", source="unit", tasks=[_node("load", "Notebook")]),
    ]
    insights = {
        "pipeline_insights": [
            {
                "pipeline": "parent",
                "recommended_patterns": [
                    {
                        "pattern": "Public thing",
                        "fit": "x",
                        "simplification_pattern": False,
                        "release_state": "public_preview",
                        "release_state_source": "https://docs.databricks.com/a/",
                    }
                ],
            },
            {
                "pipeline": "child",
                "recommended_patterns": [
                    {
                        "pattern": "Gated thing",
                        "fit": "y",
                        "simplification_pattern": False,
                        "release_state": "private_preview",
                        "release_state_source": "https://docs.databricks.com/b/",
                    }
                ],
            },
        ]
    }
    _, options = recommend_component(["child", "parent"], _inventory(graphs, insights=insights))
    disclosures = options["agentic"]["release_disclosures"]
    labels = {d["release_state"]: d["label"] for d in disclosures}
    assert labels == {"public_preview": "Public Preview (production-ready)", "private_preview": "Private Preview"}


def test_build_recommendation_emits_both_options_and_a_default_plan() -> None:
    graphs = [
        SourceGraph(
            name="parent",
            source="unit",
            tasks=[_node("call_child", "ExecutePipeline")],
            lineage=Lineage(
                control_edges=[
                    ControlEdge(source_workflow="parent", target_workflow="child", via_task_key="call_child")
                ]
            ),
        ),
        SourceGraph(name="child", source="unit", tasks=[_node("flow", "ExecuteDataFlow", strategy="agentic")]),
        SourceGraph(name="solo", source="unit", tasks=[_node("load", "Notebook")]),
    ]
    recommendation = build_recommendation(_inventory(graphs))
    ids = [component["component_id"] for component in recommendation["components"]]
    assert ids == ["component-1", "component-2"]
    first = recommendation["components"][0]
    assert first["members"] == ["child", "parent"]
    assert set(first["options"]) == {"deterministic", "agentic"}
    assert first["recommended"] == "agentic"  # child's ExecuteDataFlow is an uncovered gap
    assert recommendation["components"][1]["recommended"] == "deterministic"  # solo notebook
    # The default plan proposes decision == recommended for every component, ready to record as-is.
    assert recommendation["default_plan"]["components"] == [
        {"component_id": "component-1", "members": ["child", "parent"], "decision": "agentic"},
        {"component_id": "component-2", "members": ["solo"], "decision": "deterministic"},
    ]


def test_recommendation_is_pure_and_idempotent() -> None:
    graphs = [SourceGraph(name="a", source="unit", tasks=[_node("load", "Notebook")])]
    inventory = _inventory(graphs)
    assert build_recommendation(inventory) == build_recommendation(inventory)
