from __future__ import annotations

import ast

import flowx.discovery_inventory as discovery_inventory
from flowx.discovery_inventory import INVENTORY_VISIBLE_PROPERTY, STRATEGY_PROPERTY, build_source_inventory
from flowx.models.discovery import CONCEPT_GROUP, CONCEPT_NOTEBOOK, ContainerNode, SourceGraph, SourceNode
from flowx.models.ir import ControlEdge, Lineage


def _node(task_key: str, strategy: str) -> SourceNode:
    return SourceNode(
        source_id=task_key,
        task_key=task_key,
        concept=CONCEPT_NOTEBOOK,
        source="unit",
        name=task_key,
        native_type="Notebook",
        properties={STRATEGY_PROPERTY: strategy},
        raw={"task_key": task_key},
    )


def test_emitter_has_no_source_specific_imports() -> None:
    module_path = (discovery_inventory.__file__ or "").rstrip("c")
    tree = ast.parse(open(module_path, encoding="utf-8").read())
    imports = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None]
    assert not any(name.startswith("flowx.sources") for name in imports)


def test_inventory_shape_counts_and_structural_containers() -> None:
    structural_group = ContainerNode(
        source_id="group:etl",
        task_key="etl",
        concept=CONCEPT_GROUP,
        source="unit",
        name="etl",
        native_type="TaskGroup",
        properties={INVENTORY_VISIBLE_PROPERTY: False, "structural_only": True},
        branches={"group": [_node("extract", "deterministic"), _node("load", "agentic")]},
    )
    graph = SourceGraph(name="workflow", source="unit", tasks=[structural_group])

    inventory = build_source_inventory([graph], source="unit", source_dir="/source")

    assert set(inventory) == {"source", "source_dir", "pipelines", "summary"}
    assert [item["name"] for item in inventory["pipelines"][0]["activities"]] == ["extract", "load"]
    assert inventory["summary"] == {
        "pipeline_count": 1,
        "activity_count": 2,
        "deterministic_count": 1,
        "agentic_count": 1,
        "unsupported_count": 0,
        "coverage_pct": 100.0,
    }


def test_inventory_emits_lineage_and_additive_source_fields() -> None:
    graph = SourceGraph(
        name="workflow",
        source="unit",
        tasks=[_node("task", "deterministic")],
        lineage=Lineage(
            control_edges=[
                ControlEdge(
                    source_workflow="workflow",
                    target_workflow="child",
                    via_task_key="task",
                    wait_for_completion=False,
                )
            ]
        ),
    )

    entry = build_source_inventory([graph], source="unit", source_dir="/source")["pipelines"][0]

    assert entry["activities"][0] == {
        "name": "task",
        "type": "Notebook",
        "strategy": "deterministic",
        "original_type": "Notebook",
        "dependencies": [],
        "raw": {"task_key": "task"},
    }
    assert entry["lineage"]["control_edges"][0]["target_workflow"] == "child"
