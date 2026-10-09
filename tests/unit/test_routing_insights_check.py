"""Route, fill-agentic and package refuse when agentic_insights.json and inventory.json disagree.

Enrich writes ``metadata/agentic_insights.json`` and then ``inventory.json``. A run stopped between
the two, or a hand edit, leaves them out of step; the readers then refuse and say how to recover. The
file's own ``agentic_insights_sha256`` is checked too. Package runs the check whenever enrich ran,
plan or no plan; a run without enrich is unaffected.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from flowx import routing
from flowx.adapter.__main__ import main as adapter_cli_main
from flowx.bundler.dab_writer import main as package_main
from flowx.discovery_insights import enrich_inventory
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.discovery_serde import canonical_sha256
from flowx.ir_serde import pipeline_to_dict
from flowx.models.conversion_plan import ConversionPlan
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.models.ir import NotebookActivity, Pipeline
from flowx.route_agentic import REPORT_FILENAME, WORK_DIRNAME, apply_agentic_output


def _setup(output_dir: Path) -> None:
    """An enriched one-pipeline factory with its convert report."""
    node = SourceNode(
        source_id="load",
        task_key="load",
        concept=CONCEPT_NOTEBOOK,
        source="adf",
        name="load",
        native_type="DatabricksNotebook",
        properties={STRATEGY_PROPERTY: "agentic"},
    )
    inventory = build_source_inventory(
        [SourceGraph(name="solo", source="adf", tasks=[node])], source="adf", source_dir="/x"
    )
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    (metadata / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    assert enrich_inventory(output_dir, insights={"overview": "Solo loads one table."})["ok"] is True
    work = output_dir / WORK_DIRNAME
    work.mkdir(parents=True, exist_ok=True)
    pipeline = Pipeline(
        name="solo",
        tasks=[NotebookActivity(name="load", task_key="load", notebook_path="/Workspace/Shared/load")],
        tags={"source": "adf"},
    )
    (work / REPORT_FILENAME).write_text(json.dumps({"pipelines": [pipeline_to_dict(pipeline)]}), encoding="utf-8")
    (work / "gaps.json").write_text("[]", encoding="utf-8")


def _insights_path(output_dir: Path) -> Path:
    return output_dir / "metadata" / "agentic_insights.json"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _insights_ahead_of_the_inventory(output_dir: Path) -> None:
    """The crash window: enrich replaced agentic_insights.json but stopped before inventory.json."""
    document = _read(_insights_path(output_dir))
    content = {key: value for key, value in document.items() if key != "agentic_insights_sha256"}
    content["overview"] = "A later enrich."
    _insights_path(output_dir).write_text(
        json.dumps({**content, "agentic_insights_sha256": canonical_sha256(content)}), encoding="utf-8"
    )


def _insights_missing(output_dir: Path) -> None:
    _insights_path(output_dir).unlink()


def _insights_edited_in_both(output_dir: Path) -> None:
    """Both files agree, but the document no longer matches its own hash."""
    document = _read(_insights_path(output_dir))
    document["overview"] = "Edited by hand."
    _insights_path(output_dir).write_text(json.dumps(document), encoding="utf-8")
    inventory = _read(output_dir / "metadata" / "inventory.json")
    inventory["insights"] = document
    (output_dir / "metadata" / "inventory.json").write_text(json.dumps(inventory), encoding="utf-8")


_BREAKS = {
    "insights-ahead": (_insights_ahead_of_the_inventory, "disagree"),
    "insights-missing": (_insights_missing, "is missing"),
    "self-hash-broken": (_insights_edited_in_both, "does not match its recorded agentic_insights_sha256"),
}


def test_route_binds_the_plan_to_the_real_agentic_insights_hash(tmp_path: Path) -> None:
    _setup(tmp_path)
    plan = {"components": [{"component_id": "component-1", "members": ["solo"], "decision": "deterministic"}]}

    assert routing.record_plan(tmp_path, plan=plan)["ok"] is True

    recorded = ConversionPlan.load(tmp_path)
    assert recorded is not None
    assert recorded.agentic_insights_sha256 is not None
    assert recorded.agentic_insights_sha256 == _read(_insights_path(tmp_path))["agentic_insights_sha256"]


def test_an_enriched_or_unenriched_run_with_files_in_step_passes(tmp_path: Path) -> None:
    _setup(tmp_path)
    inventory = _read(tmp_path / "metadata" / "inventory.json")
    assert routing.insights_file_violations(tmp_path, inventory) == []
    del inventory["insights"]
    _insights_missing(tmp_path)
    assert routing.insights_file_violations(tmp_path, inventory) == []


@pytest.mark.parametrize("case", sorted(_BREAKS))
def test_route_refuses_and_says_how_to_recover(tmp_path: Path, case: str, capsys: pytest.CaptureFixture[str]) -> None:
    _setup(tmp_path)
    breaker, expected = _BREAKS[case]
    breaker(tmp_path)

    assert adapter_cli_main(["route", "--output-dir", str(tmp_path)]) == 1

    (violation,) = json.loads(capsys.readouterr().out)["violations"]
    assert expected in violation
    assert "run enrich again with the same insights" in violation and ".enrich.lock" in violation
    assert "running discover again also clears conversion_plan.json and agentic_conversion.json" in violation
    assert not (tmp_path / "metadata" / "conversion_plan.json").exists()


@pytest.mark.parametrize("case", sorted(_BREAKS))
def test_package_refuses_even_without_a_plan(tmp_path: Path, case: str, capsys: pytest.CaptureFixture[str]) -> None:
    _setup(tmp_path)
    breaker, expected = _BREAKS[case]
    breaker(tmp_path)

    code = package_main(["--output-dir", str(tmp_path), "--no-download-workspace-files", "--keep-intermediates"])

    assert code == 1
    error = capsys.readouterr().err
    assert expected in error and "run enrich again" in error
    assert not (tmp_path / "databricks.yml").exists()


def test_fill_agentic_refuses_when_the_files_disagree(tmp_path: Path) -> None:
    _setup(tmp_path)
    plan = {"components": [{"component_id": "component-1", "members": ["solo"], "decision": "agentic"}]}
    assert adapter_cli_main(["route", "--output-dir", str(tmp_path), "--plan-path", _write(tmp_path, plan)]) == 0
    _insights_ahead_of_the_inventory(tmp_path)

    result = apply_agentic_output(tmp_path, ["solo"], [{"name": "solo", "tags": {"source": "adf"}, "tasks": []}])

    assert result["ok"] is False and "disagree" in result["error"]


def _write(output_dir: Path, plan: dict[str, Any]) -> str:
    path = output_dir / "plan.json"
    path.write_text(json.dumps(plan), encoding="utf-8")
    return str(path)
