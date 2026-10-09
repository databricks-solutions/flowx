"""Agentic routing is ADF-only in Phase 1, and ``route`` enforces it.

Before this guard only ``fill-agentic combine`` checked the source, so ``route`` with an agentic
decision on an Airflow inventory recorded the plan and rewrote the DAG's deterministic tasks into
placeholder gaps, outside the Airflow per-gap resolver.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from flowx import routing
from flowx.adapter.__main__ import main as adapter_cli_main
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.route_agentic import GAPS_FILENAME, REPORT_FILENAME, WORK_DIRNAME


def _node(task_key: str, strategy: str) -> SourceNode:
    return SourceNode(
        source_id=task_key,
        task_key=task_key,
        concept=CONCEPT_NOTEBOOK,
        source="airflow",
        name=task_key,
        native_type="PythonOperator",
        properties={STRATEGY_PROPERTY: strategy},
    )


def _inventory(source: str | None) -> dict[str, Any]:
    """One fully deterministic DAG and one DAG with an agentic gap."""
    graphs = [
        SourceGraph(
            name="chain_dag", source="airflow", tasks=[_node("a", "deterministic"), _node("b", "deterministic")]
        ),
        SourceGraph(name="gap_dag", source="airflow", tasks=[_node("c", "agentic")]),
    ]
    inventory = build_source_inventory(graphs, source=source or "adf", source_dir="/tmp/dags")
    if source is None:
        del inventory["source"]
    return inventory


def _setup(output_dir: Path, inventory: dict[str, Any]) -> bytes:
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    (metadata / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    work = output_dir / WORK_DIRNAME
    work.mkdir(parents=True, exist_ok=True)
    report = {
        "pipelines": [
            {"name": "chain_dag", "tasks": [{"name": "a", "task_key": "a", "type": "NotebookActivity"}]},
            {"name": "gap_dag", "tasks": [{"name": "c", "task_key": "c", "type": "PlaceholderActivity"}]},
        ]
    }
    report_bytes = json.dumps(report, indent=2).encode("utf-8")
    (work / REPORT_FILENAME).write_bytes(report_bytes)
    (work / GAPS_FILENAME).write_text("[]", encoding="utf-8")
    return report_bytes


def _plan(decision_for_chain: str) -> dict[str, Any]:
    return {
        "components": [
            {"component_id": "component-1", "members": ["chain_dag"], "decision": decision_for_chain},
            {"component_id": "component-2", "members": ["gap_dag"], "decision": "deterministic"},
        ]
    }


def test_route_refuses_an_agentic_decision_on_an_airflow_inventory(tmp_path: Path) -> None:
    report_bytes = _setup(tmp_path, _inventory("airflow"))
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(_plan("agentic")), encoding="utf-8")

    code = adapter_cli_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)])

    assert code == 1
    assert not (tmp_path / "metadata" / "conversion_plan.json").exists()
    assert (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes() == report_bytes


def test_airflow_inventory_is_recommended_and_recorded_deterministic_only(tmp_path: Path) -> None:
    inventory = _inventory("airflow")

    recommendation = routing.build_recommendation(inventory)

    assert {component["recommended"] for component in recommendation["components"]} == {"deterministic"}
    assert any("ADF-only" in finding for finding in recommendation["findings"])
    _setup(tmp_path, inventory)
    components = [
        {
            "component_id": component["component_id"],
            "members": component["members"],
            "decision": component["recommended"],
        }
        for component in recommendation["components"]
    ]
    recorded = routing.record_plan(tmp_path, plan={"components": components})
    assert recorded["ok"] is True


def test_an_adf_inventory_accepts_agentic() -> None:
    assert routing.validate_plan(_plan("agentic"), _inventory("adf")) == []


def test_an_inventory_without_a_source_refuses_agentic_and_says_re_run_discover() -> None:
    inventory = _inventory(None)

    assert routing.validate_plan(_plan("agentic"), inventory) == [
        "components[0]: the inventory records no source, so it cannot be routed agentic; re-run discover"
    ]
    assert {component["recommended"] for component in routing.build_recommendation(inventory)["components"]} == {
        "deterministic"
    }
