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
from flowx.discovery_serde import canonical_sha256
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
    assert "carries no routing record; re-run route" in capsys.readouterr().err
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
            "output_sha256": None,
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
    assert "output_sha256" in component_audit
    assert "fingerprint" in component_audit
    assert audit["baseline_report_sha256"] == baseline_sha256
    assert audit["translation_report_sha256"] == hashlib.sha256(stamped_path.read_bytes()).hexdigest()


def test_route_after_modify_leaves_the_stamped_report_and_package_asks_to_re_run_modify(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report_path = _setup(tmp_path, _inventory("agentic"))
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps({"components": [{"component_id": "component-1", "members": ["solo"], "decision": "agentic"}]}),
        encoding="utf-8",
    )
    authored_path = tmp_path / "authored.json"
    authored_path.write_text(json.dumps([_authored_pipeline()]), encoding="utf-8")
    route = ["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)]
    stamped_path = tmp_path / WORK_DIRNAME / "translation_report.stamped.json"

    assert adapter_main(route) == 0
    combine = ["fill-agentic", "--output-dir", str(tmp_path), "--members", "solo"]
    assert adapter_main([*combine, "--pipelines-path", str(authored_path)]) == 0
    assert adapter_main(["modify", str(report_path), "--output-dir", str(tmp_path)]) == 0
    assert enrich_inventory(tmp_path, insights={"overview": "Solo loads one table."})["ok"] is True
    assert _package(tmp_path) == 1  # the plan is now stale against the new insights
    stamped_before = stamped_path.read_bytes()
    capsys.readouterr()

    assert adapter_main(route) == 0
    assert stamped_path.read_bytes() == stamped_before
    assert _package(tmp_path) == 1
    assert "re-run modify" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()

    assert adapter_main(["modify", str(report_path), "--output-dir", str(tmp_path)]) == 0
    assert _package(tmp_path) == 0
    audit = json.loads((tmp_path / "metadata" / "route_audit.json").read_text(encoding="utf-8"))
    assert audit["components"][0]["outcome"] == "agentic-applied"


def test_route_agentic_then_deterministic_switches_back_to_deterministic(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Re-routing from agentic to deterministic writes convert's report and gaps back byte for byte."""
    report_path = _setup(tmp_path, _inventory("agentic"))
    converted = report_path.read_bytes()
    plan_path = tmp_path / "plan.json"

    def _route(decision: str) -> int:
        plan = {"components": [{"component_id": "component-1", "members": ["solo"], "decision": decision}]}
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        return adapter_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)])

    assert _route("agentic") == 0
    capsys.readouterr()

    assert _route("deterministic") == 0
    capsys.readouterr()
    assert report_path.read_bytes() == converted
    baseline_gaps = tmp_path / WORK_DIRNAME / "route_baseline" / "gaps.json"
    assert (tmp_path / WORK_DIRNAME / "gaps.json").read_bytes() == baseline_gaps.read_bytes()
    assert _package(tmp_path) == 0


def test_package_refuses_an_agentic_component_with_no_agent_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report_path = _setup(tmp_path, _inventory("agentic"))
    _record(tmp_path, "agentic")
    plan = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))
    apply_plan_to_report(tmp_path, plan)
    assert routing_record(json.loads(report_path.read_text(encoding="utf-8"))) is not None

    assert _package(tmp_path) == 1

    error = capsys.readouterr().err
    assert "component 'component-1' (solo) is routed agentic but has no agent output yet" in error
    assert not (tmp_path / "databricks.yml").exists()
    assert not (tmp_path / "solo" / "databricks.yml").exists()
    _fill_solo(tmp_path)
    assert _package(tmp_path) == 0


def test_package_with_a_current_plan_writes_an_audit_with_hashes(tmp_path: Path) -> None:
    report_path = _setup(tmp_path, _inventory("agentic"))
    baseline_sha256 = hashlib.sha256(report_path.read_bytes()).hexdigest()
    _record(tmp_path, "agentic")
    plan = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text(encoding="utf-8"))
    apply_plan_to_report(tmp_path, plan)
    _fill_solo(tmp_path)
    record = routing_record(json.loads(report_path.read_text(encoding="utf-8")))
    assert record is not None

    assert _package(tmp_path) == 0
    assert (tmp_path / "databricks.yml").exists()

    audit = json.loads((tmp_path / "metadata" / "route_audit.json").read_text(encoding="utf-8"))
    assert audit["inventory_sha256"] == audit["recorded_against_inventory_sha256"] == plan["inventory_sha256"]
    assert audit["conversion_plan_sha256"] == record["conversion_plan_sha256"] == canonical_sha256(plan)
    assert audit["baseline_report_sha256"] == baseline_sha256
    assert audit["baseline_gaps_sha256"] == record["baseline_gaps_sha256"]
    assert audit["translation_report_sha256"] == hashlib.sha256(report_path.read_bytes()).hexdigest()
    assert "gaps_introduced" not in audit and "gaps_count" not in audit
    (component,) = audit["components"]
    recorded = record["components"][component["component_id"]]
    assert component["decision"] == "agentic"
    assert component["outcome"] == recorded["outcome"] == "agentic-applied"
    assert component["fingerprint"] == recorded["fingerprint"]
    assert component["output_sha256"] == recorded["output_sha256"] is not None


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
    assert "translation_report.json does not match a fresh rebuild; re-run route" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()


def test_package_refuses_edited_combine_store(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Package refuses when agentic_conversion.json pipelines were edited after the combine."""
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

    combines_path = tmp_path / "metadata" / "agentic_conversion.json"
    combines = json.loads(combines_path.read_text(encoding="utf-8"))
    combines["components"]["component-1"]["pipelines"][0]["name"] = "tampered_name"
    combines_path.write_text(json.dumps(combines, indent=2), encoding="utf-8")

    assert _package(tmp_path) == 1
    assert (
        "metadata/agentic_conversion.json has been edited (component-1); re-run fill-agentic for that component"
        in capsys.readouterr().err
    )
    assert not (tmp_path / "databricks.yml").exists()

    combine = ["fill-agentic", "--output-dir", str(tmp_path), "--members", "solo"]
    assert adapter_main([*combine, "--pipelines-path", str(authored_path)]) == 0
    assert _package(tmp_path) == 0


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


def test_package_replays_an_explicit_live_report_even_when_a_stamped_copy_exists(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report_path = _setup(tmp_path, _inventory("agentic"))
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps({"components": [{"component_id": "component-1", "members": ["solo"], "decision": "agentic"}]}),
        encoding="utf-8",
    )
    assert adapter_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)]) == 0
    assert adapter_main(["modify", str(report_path), "--output-dir", str(tmp_path)]) == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["pipelines"][0]["tasks"] = []
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    capsys.readouterr()

    exit_code = package_main(
        ["--output-dir", str(tmp_path), "--report", str(report_path), "--no-download-workspace-files"]
    )

    assert exit_code == 1
    assert "translation_report.json does not match a fresh rebuild; re-run route" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()


def _route_solo_agentic_and_modify(output_dir: Path, report_path: Path, *modify_args: str) -> list[str]:
    """Route 'solo' agentic through the CLI, then run modify; returns the route command for re-runs."""
    plan_path = output_dir / "plan.json"
    plan_path.write_text(
        json.dumps({"components": [{"component_id": "component-1", "members": ["solo"], "decision": "agentic"}]}),
        encoding="utf-8",
    )
    route = ["route", "--output-dir", str(output_dir), "--plan-path", str(plan_path)]
    assert adapter_main(route) == 0
    _fill_solo(output_dir)
    assert adapter_main(["modify", str(report_path), "--output-dir", str(output_dir), *modify_args]) == 0
    return route


def _fill_solo(output_dir: Path) -> None:
    """Fill the routed-agentic 'solo' component with the authored pipeline through the CLI."""
    authored_path = output_dir / "authored.json"
    authored_path.write_text(json.dumps([_authored_pipeline()]), encoding="utf-8")
    fill = ["fill-agentic", "--output-dir", str(output_dir), "--members", "solo", "--pipelines-path"]
    assert adapter_main([*fill, str(authored_path)]) == 0


def test_package_asks_to_re_run_modify_after_a_re_convert_and_re_route(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report_path = _setup(tmp_path, _inventory("agentic"))
    route = _route_solo_agentic_and_modify(tmp_path, report_path)
    _setup(tmp_path, _inventory("agentic"))
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["pipelines"][0]["tasks"][0]["notebook_path"] = "/Workspace/Shared/reconverted"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    assert adapter_main(route) == 0
    capsys.readouterr()

    assert _package(tmp_path) == 1
    assert "the configured report is out of date; re-run modify" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()

    assert adapter_main(["modify", str(report_path), "--output-dir", str(tmp_path)]) == 0
    assert _package(tmp_path) == 0


def test_package_checks_a_modify_out_copy_by_its_routing_record(tmp_path: Path) -> None:
    report_path = _setup(tmp_path, _inventory("agentic"))
    configured_path = tmp_path / "configured.json"
    _route_solo_agentic_and_modify(tmp_path, report_path, "--out", str(configured_path))

    exit_code = package_main(
        ["--output-dir", str(tmp_path), "--report", str(configured_path), "--no-download-workspace-files"]
    )

    assert exit_code == 0
    assert (tmp_path / "databricks.yml").exists()


def test_package_refuses_a_routed_report_whose_plan_is_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report_path = _setup(tmp_path, _inventory("agentic"))
    _route_solo_agentic_and_modify(tmp_path, report_path)
    (tmp_path / "metadata" / "conversion_plan.json").unlink()
    capsys.readouterr()

    assert _package(tmp_path) == 1

    assert "carries a routing record) but metadata/conversion_plan.json is missing" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()


def test_package_revalidates_a_recorded_plan_that_lost_its_component(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _setup(tmp_path, _inventory())
    _record(tmp_path, "deterministic")
    plan_path = tmp_path / "metadata" / "conversion_plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["components"] = []
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    assert _package(tmp_path) == 1

    assert "conversion_plan.json is not a valid decision" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()


def test_package_refuses_a_plan_with_a_pending_decision(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _setup(tmp_path, _inventory())
    plan = {"components": [{"component_id": "component-1", "members": ["solo"], "decision": None}]}
    assert routing.record_plan(tmp_path, plan=plan)["ok"] is True

    assert _package(tmp_path) == 1

    assert "components ['component-1'] have no decision yet" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()


def test_package_refuses_a_configured_report_of_an_unfilled_agentic_component(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report_path = _setup(tmp_path, _inventory("agentic"))
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps({"components": [{"component_id": "component-1", "members": ["solo"], "decision": "agentic"}]}),
        encoding="utf-8",
    )
    assert adapter_main(["route", "--output-dir", str(tmp_path), "--plan-path", str(plan_path)]) == 0
    assert adapter_main(["modify", str(report_path), "--output-dir", str(tmp_path)]) == 0
    capsys.readouterr()

    assert _package(tmp_path) == 1

    assert "is routed agentic but has no agent output yet" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()
