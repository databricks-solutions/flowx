"""Package refuses to ship a translation report whose routing plan has gone stale.

A recorded ``metadata/conversion_plan.json`` is bound to the inventory fingerprint it was recorded
against, and route stamps a routing record onto the report it edits. When discover runs again, or
the report's routing record no longer matches the plan, the report no longer reflects the user's
decision, so package fails closed before writing any bundle file. When the plan still matches, the
route audit records hashes and outcomes of what was packaged.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from flowx import routing
from flowx.adapter.__main__ import main as adapter_main
from flowx.bundler.dab_writer import main as package_main
from flowx.discovery_insights import enrich_inventory
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.ir_serde import pipeline_to_dict
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.models.ir import NotebookActivity, Pipeline
from flowx.route_agentic import REPORT_FILENAME, WORK_DIRNAME, apply_plan_to_report, routing_record


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


def test_package_refuses_a_routing_record_that_does_not_match_the_plan(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report_path = _setup(tmp_path, _inventory("agentic"))
    _record(tmp_path, "agentic")
    plan = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))
    apply_plan_to_report(tmp_path, plan)
    # The decision is recorded again as deterministic without re-converting, so the report still
    # carries the agentic edit.
    _record(tmp_path, "deterministic")

    assert _package(tmp_path) == 1
    error = capsys.readouterr().err
    assert "was routed under a different conversion plan" in error
    assert "was routed 'agentic' but the plan decides 'deterministic'" in error
    assert not (tmp_path / "databricks.yml").exists()
    assert routing_record(json.loads(report_path.read_text(encoding="utf-8"))) is not None


def test_package_refuses_an_agentic_plan_when_the_report_has_no_routing_record(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _setup(tmp_path, _inventory("agentic"))
    _record(tmp_path, "agentic")  # recorded, but never applied to the report

    assert _package(tmp_path) == 1
    assert "carries no routing record; re-run convert, then route" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()


def test_package_with_a_deterministic_plan_and_no_record_passes(tmp_path: Path) -> None:
    report_path = _setup(tmp_path, _inventory())
    before = report_path.read_bytes()
    _record(tmp_path, "deterministic")
    plan = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))
    apply_plan_to_report(tmp_path, plan)
    assert report_path.read_bytes() == before

    assert _package(tmp_path) == 0
    audit = json.loads((tmp_path / "metadata" / "route_audit.json").read_text(encoding="utf-8"))
    assert audit["components"] == [
        {
            "component_id": "component-1",
            "members": ["solo"],
            "decision": "deterministic",
            "outcome": "deterministic",
            "combine_sha256": None,
        }
    ]
    assert audit["baseline_report_sha256"] is None


def _authored_pipeline() -> dict[str, Any]:
    pipeline = Pipeline(
        name="solo_agentic",
        tasks=[NotebookActivity(name="load", task_key="load", notebook_path="/Workspace/Shared/agentic_load")],
        tags={"source": "adf"},
    )
    return pipeline_to_dict(pipeline)


def test_route_combine_modify_then_package_keeps_the_routing_record(tmp_path: Path) -> None:
    report_path = _setup(tmp_path, _inventory("agentic"))
    baseline_sha256 = hashlib.sha256(report_path.read_bytes()).hexdigest()
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps({"components": [{"component_id": "component-1", "members": ["solo"], "decision": "agentic"}]}),
        encoding="utf-8",
    )
    authored_path = tmp_path / "authored.json"
    authored_path.write_text(json.dumps([_authored_pipeline()]), encoding="utf-8")

    assert adapter_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)]) == 0
    assert (
        adapter_main(
            [
                "fill-agentic",
                "combine",
                "--output-dir",
                str(tmp_path),
                "--members",
                "solo",
                "--pipelines-path",
                str(authored_path),
            ]
        )
        == 0
    )
    assert adapter_main(["modify", str(report_path), "--output-dir", str(tmp_path)]) == 0
    stamped_path = tmp_path / WORK_DIRNAME / "translation_report.stamped.json"
    assert routing_record(json.loads(stamped_path.read_text(encoding="utf-8"))) is not None

    assert _package(tmp_path) == 0
    audit = json.loads((tmp_path / "metadata" / "route_audit.json").read_text(encoding="utf-8"))
    component_audit = audit["components"][0]
    assert component_audit["component_id"] == "component-1"
    assert component_audit["members"] == ["solo"]
    assert component_audit["decision"] == "agentic"
    assert component_audit["outcome"] == "agentic-applied"
    assert "combine_sha256" in component_audit
    assert "fingerprint" in component_audit
    assert audit["baseline_report_sha256"] == baseline_sha256
    assert audit["translation_report_sha256"] == hashlib.sha256(stamped_path.read_bytes()).hexdigest()


def test_rerouting_the_same_decisions_after_enrich_refreshes_the_stamped_report(tmp_path: Path) -> None:
    report_path = _setup(tmp_path, _inventory("agentic"))
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps({"components": [{"component_id": "component-1", "members": ["solo"], "decision": "agentic"}]}),
        encoding="utf-8",
    )
    authored_path = tmp_path / "authored.json"
    authored_path.write_text(json.dumps([_authored_pipeline()]), encoding="utf-8")
    route = ["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)]

    assert adapter_main(route) == 0
    combine = ["fill-agentic", "combine", "--output-dir", str(tmp_path), "--members", "solo"]
    assert adapter_main([*combine, "--pipelines-path", str(authored_path)]) == 0
    assert adapter_main(["modify", str(report_path), "--output-dir", str(tmp_path)]) == 0
    assert enrich_inventory(tmp_path, insights={"overview": "Solo loads one table."})["ok"] is True
    assert _package(tmp_path) == 1  # the plan is now stale against the new insights

    assert adapter_main(route) == 0
    assert _package(tmp_path) == 0
    audit = json.loads((tmp_path / "metadata" / "route_audit.json").read_text(encoding="utf-8"))
    assert audit["components"][0]["outcome"] == "agentic-applied"


def test_route_agentic_then_deterministic_switches_back_to_deterministic(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Re-routing from agentic to deterministic is allowed; the record is cleared (spec change)."""
    report_path = _setup(tmp_path, _inventory("agentic"))
    plan_path = tmp_path / "plan.json"

    def _route(decision: str) -> int:
        plan = {"components": [{"component_id": "component-1", "members": ["solo"], "decision": decision}]}
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        return adapter_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)])

    assert _route("agentic") == 0
    capsys.readouterr()

    assert _route("deterministic") == 0
    capsys.readouterr()
    deterministic_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert "_routing_record" not in deterministic_report


def test_package_with_a_current_plan_writes_an_audit_with_hashes(tmp_path: Path) -> None:
    report_path = _setup(tmp_path, _inventory("agentic"))
    baseline_sha256 = hashlib.sha256(report_path.read_bytes()).hexdigest()
    _record(tmp_path, "agentic")
    plan = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))
    apply_plan_to_report(tmp_path, plan)
    record = routing_record(json.loads(report_path.read_text(encoding="utf-8")))
    assert record is not None

    assert _package(tmp_path) in (0, 1)  # 1 only signals bundle-invariant warnings, not the preflight
    assert (tmp_path / "databricks.yml").exists()

    audit = json.loads((tmp_path / "metadata" / "route_audit.json").read_text(encoding="utf-8"))
    assert audit["inventory_sha256"] == audit["recorded_against_inventory_sha256"] == plan["inventory_sha256"]
    assert (
        audit["conversion_plan_sha256"]
        == hashlib.sha256((tmp_path / "metadata" / "conversion_plan.json").read_bytes()).hexdigest()
    )
    assert audit["baseline_report_sha256"] == baseline_sha256
    assert audit["baseline_gaps_sha256"] == record["baseline_gaps_sha256"]
    assert audit["translation_report_sha256"] == hashlib.sha256(report_path.read_bytes()).hexdigest()
    (component,) = audit["components"]
    recorded = record["components"][component["component_id"]]
    assert component["decision"] == "agentic"
    assert component["outcome"] == recorded["outcome"] == "agentic-not-viable"
    assert component["fingerprint"] == recorded["fingerprint"]
    assert component["combine_sha256"] is None


def test_package_refuses_tampered_baseline(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Package refuses when the baseline hash in the record doesn't match the actual file."""
    _setup(tmp_path, _inventory("agentic"))
    _record(tmp_path, "agentic")
    plan = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))
    apply_plan_to_report(tmp_path, plan)

    baseline_path = tmp_path / "metadata" / ".work" / "route_baseline" / "translation_report.json"
    if not baseline_path.exists():
        baseline_path = tmp_path / ".work" / "route_baseline" / "translation_report.json"
    baseline_path.write_text("tampered", encoding="utf-8")

    assert _package(tmp_path) == 1
    assert "baseline is missing or changed" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()


def test_package_refuses_stale_unstamped_report(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Package refuses when unstamped report doesn't match fresh rebuild (suggests re-run route)."""
    report_path = _setup(tmp_path, _inventory("agentic"))
    _record(tmp_path, "agentic")
    plan = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))
    apply_plan_to_report(tmp_path, plan)

    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["pipelines"][0]["name"] = "tampered"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    assert _package(tmp_path) == 1
    error = capsys.readouterr().err
    assert "does not match" in error or "re-run" in error.lower()
    assert not (tmp_path / "databricks.yml").exists()


def test_package_refuses_edited_combine_store(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Package refuses when agentic_combines.json pipelines were edited after the combine."""
    _setup(tmp_path, _inventory("agentic"))
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps({"components": [{"component_id": "component-1", "members": ["solo"], "decision": "agentic"}]}),
        encoding="utf-8",
    )
    authored_path = tmp_path / "authored.json"
    authored_path.write_text(json.dumps([_authored_pipeline()]), encoding="utf-8")

    from flowx.adapter.__main__ import main as adapter_main

    assert adapter_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)]) == 0
    assert (
        adapter_main(
            [
                "fill-agentic",
                "combine",
                "--output-dir",
                str(tmp_path),
                "--members",
                "solo",
                "--pipelines-path",
                str(authored_path),
            ]
        )
        == 0
    )

    combines_path = tmp_path / "metadata" / "agentic_combines.json"
    combines = json.loads(combines_path.read_text(encoding="utf-8"))
    combines["component-1"]["pipelines"][0]["name"] = "tampered_name"
    combines_path.write_text(json.dumps(combines, indent=2), encoding="utf-8")

    assert _package(tmp_path) == 1
    error = capsys.readouterr().err
    assert "combine" in error.lower() or "re-run" in error.lower()
    assert not (tmp_path / "databricks.yml").exists()


def test_package_refuses_stale_stamped_report(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Package refuses a stamped report whose record no longer matches the rebuild."""
    report_path = _setup(tmp_path, _inventory("agentic"))
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps({"components": [{"component_id": "component-1", "members": ["solo"], "decision": "agentic"}]}),
        encoding="utf-8",
    )
    authored_path = tmp_path / "authored.json"
    authored_path.write_text(json.dumps([_authored_pipeline()]), encoding="utf-8")

    from flowx.adapter.__main__ import main as adapter_main

    assert adapter_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)]) == 0
    assert (
        adapter_main(
            [
                "fill-agentic",
                "combine",
                "--output-dir",
                str(tmp_path),
                "--members",
                "solo",
                "--pipelines-path",
                str(authored_path),
            ]
        )
        == 0
    )
    assert adapter_main(["modify", str(report_path), "--output-dir", str(tmp_path)]) == 0

    stamped_path = tmp_path / ".work" / "translation_report.stamped.json"
    stamped_report = json.loads(stamped_path.read_text(encoding="utf-8"))
    record = stamped_report["_routing_record"]
    record["components"]["component-1"]["outcome"] = "agentic-not-viable"
    stamped_path.write_text(json.dumps(stamped_report, indent=2), encoding="utf-8")

    assert _package(tmp_path) == 1
    assert "out of date; re-run modify" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()
