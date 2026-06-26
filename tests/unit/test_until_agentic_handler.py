"""#1: Until (incl. nested) must surface as an agentic gap carrying the full ARM JSON."""

from __future__ import annotations

from flowx.models.adf_ast import AdfDefinitions
from flowx.models.ir import PlaceholderActivity
from flowx.parser.adf_loader import _parse_pipeline_json
from flowx.translator.engine import translate_pipeline

_DEFS = AdfDefinitions(pipelines=[], datasets={}, linked_services={}, triggers=[])

_PIPELINE = {
    "name": "p",
    "properties": {
        "activities": [
            {
                "name": "Gate",
                "type": "IfCondition",
                "typeProperties": {
                    "expression": {"value": "@equals(1, 1)", "type": "Expression"},
                    "ifTrueActivities": [
                        {
                            "name": "Poll Until Ready",
                            "type": "Until",
                            "typeProperties": {
                                "expression": {"value": "@equals(variables('s'), 'done')", "type": "Expression"},
                                "timeout": "0.01:00:00",
                                "activities": [
                                    {"name": "Wait A Bit", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 5}}
                                ],
                            },
                        }
                    ],
                },
            }
        ]
    },
}


def test_nested_until_gap_carries_full_arm_json():
    adf = _parse_pipeline_json(_PIPELINE, fallback_name="p")
    report = translate_pipeline(adf, _DEFS)
    until_gaps = [g for g in report.gaps if g.activity_type == "Until"]
    assert len(until_gaps) == 1, "nested Until must be reported as a gap"
    raw = until_gaps[0].raw_definition
    assert raw is not None and raw.get("type") == "Until"
    # full ARM JSON, not just typeProperties: name + nested loop body present
    assert raw.get("name") == "Poll Until Ready"
    assert raw["typeProperties"]["activities"][0]["name"] == "Wait A Bit"
    assert until_gaps[0].recommended_skill == "adf-to-databricks:adf-pipeline-converter"


def test_until_placeholder_ir_node_carries_arm_json():
    adf = _parse_pipeline_json(_PIPELINE, fallback_name="p")
    report = translate_pipeline(adf, _DEFS)

    def _find(tasks):
        for t in tasks:
            if isinstance(t, PlaceholderActivity) and t.original_type == "Until":
                return t
            for attr in ("inner_activities", "if_true_activities", "if_false_activities"):
                found = _find(getattr(t, attr, []) or [])
                if found:
                    return found
        return None

    ph = _find(report.pipeline.tasks)
    assert ph is not None and ph.raw_definition is not None
    assert ph.raw_definition.get("type") == "Until"
    assert ph.agentic_skill == "adf-to-databricks:adf-pipeline-converter"
