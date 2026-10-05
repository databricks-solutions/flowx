"""Package refuses to ship a translation report whose routing plan has gone stale.

A recorded ``metadata/conversion_plan.json`` is bound to the inventory fingerprint it was recorded
against. When discover runs again, or a combine fill was applied under a different plan, the report
no longer reflects the user's decision, so package fails closed before writing any bundle file.
When the plan still matches, the route audit records hashes of what was packaged.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from flowx import routing
from flowx.bundler.dab_writer import main as package_main
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.ir_serde import pipeline_to_dict
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.models.ir import NotebookActivity, Pipeline
from flowx.route_agentic import COMBINE_PROVENANCE_KEY, REPORT_FILENAME, WORK_DIRNAME, apply_plan_to_report


def _inventory(strategy: str = "deterministic") -> dict[str, Any]:
    node = SourceNode(
        source_id="load",
        task_key="load",
        concept=CONCEPT_NOTEBOOK,
        source="adf",
        name="load",
        native_type="DatabricksNotebook",
        properties={STRATEGY_PROPERTY: strategy},
    )
    return build_source_inventory([SourceGraph(name="solo", source="adf", tasks=[node])], source="adf", source_dir="/x")


def _setup(output_dir: Path, inventory: dict[str, Any]) -> Path:
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    (metadata / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    work = output_dir / WORK_DIRNAME
    work.mkdir(parents=True, exist_ok=True)
    pipeline = Pipeline(
        name="solo",
        tasks=[NotebookActivity(name="load", task_key="load", notebook_path="/Workspace/Shared/load")],
        tags={"source": "adf"},
    )
    report_path = work / REPORT_FILENAME
    report_path.write_text(json.dumps({"pipelines": [pipeline_to_dict(pipeline)]}, indent=2), encoding="utf-8")
    return report_path


def _package(output_dir: Path) -> int:
    return package_main(["--output-dir", str(output_dir), "--no-download-workspace-files", "--keep-intermediates"])


def _record(output_dir: Path, decision: str) -> None:
    plan = {"components": [{"component_id": "component-1", "members": ["solo"], "decision": decision}]}
    assert routing.record_plan(output_dir, plan=plan)["ok"] is True


def test_package_refuses_a_plan_recorded_against_a_different_inventory(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _setup(tmp_path, _inventory())
    _record(tmp_path, "deterministic")
    # Discover runs again and the inventory changes after route recorded the plan.
    (tmp_path / "metadata" / "inventory.json").write_text(json.dumps(_inventory("agentic")), encoding="utf-8")

    assert _package(tmp_path) == 1
    assert "re-run route against the current inventory" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()
    assert not (tmp_path / "metadata" / "route_audit.json").exists()


def test_package_refuses_a_combine_that_does_not_match_the_plan(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report_path = _setup(tmp_path, _inventory())
    _record(tmp_path, "deterministic")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    current = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text())["inventory_sha256"]
    report[COMBINE_PROVENANCE_KEY] = [{"component_id": "component-1", "inventory_sha256": current, "members": ["solo"]}]
    report_path.write_text(json.dumps(report), encoding="utf-8")

    assert _package(tmp_path) == 1
    assert "does not match an agentic component in the recorded plan" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()


def test_package_with_a_current_plan_writes_an_audit_with_hashes(tmp_path: Path) -> None:
    report_path = _setup(tmp_path, _inventory("agentic"))
    baseline_sha256 = hashlib.sha256(report_path.read_bytes()).hexdigest()
    _record(tmp_path, "agentic")
    plan = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))
    apply_plan_to_report(tmp_path, plan)

    assert _package(tmp_path) in (0, 1)  # 1 only signals bundle-invariant warnings, not the preflight
    assert (tmp_path / "databricks.yml").exists()

    audit = json.loads((tmp_path / "metadata" / "route_audit.json").read_text(encoding="utf-8"))
    assert audit["inventory_sha256"] == audit["recorded_against_inventory_sha256"] == plan["inventory_sha256"]
    assert (
        audit["conversion_plan_sha256"]
        == hashlib.sha256((tmp_path / "metadata" / "conversion_plan.json").read_bytes()).hexdigest()
    )
    assert audit["baseline_report_sha256"] == baseline_sha256
    assert audit["translation_report_sha256"] == hashlib.sha256(report_path.read_bytes()).hexdigest()
