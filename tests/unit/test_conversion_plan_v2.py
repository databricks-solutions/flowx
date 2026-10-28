"""The typed conversion plan (schema 3): the library's load, validate and record path.

The plan is bound to what it was decided on: the deterministic inventory fingerprint, the saved
source graphs hash the inventory records (H0) and the saved agentic insights hash (H1). Phase 1
decides whole components, and a plan holds no per-node assignments. A recorded plan must be complete
and well formed to load.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from flowx import routing
from flowx.bundler.dab_writer import main as package_main
from flowx.discovery_insights import inventory_fingerprint
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.discovery_serde import canonical_sha256
from flowx.ir_serde import pipeline_to_dict
from flowx.models.conversion_plan import SCHEMA_VERSION, ComponentPlan, ConversionPlan
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.models.ir import NotebookActivity, Pipeline
from flowx.route_agentic import REPORT_FILENAME, WORK_DIRNAME, apply_plan


def _insights(label: str, inventory: dict[str, Any]) -> dict[str, Any]:
    """A self-hashed agentic insights document for ``inventory``; a different ``label`` gives a different hash."""
    content = {"schema_version": "1", "inventory_sha256": inventory_fingerprint(inventory), "overview": label}
    return {**content, "agentic_insights_sha256": canonical_sha256(content)}


def _inventory(insights_label: str | None = "insights-1") -> dict[str, Any]:
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
        [SourceGraph(name="solo", source="adf", tasks=[node])],
        source="adf",
        source_dir="/src",
        source_graphs_sha256="graphs-1",
    )
    if insights_label is not None:
        inventory["insights"] = _insights(insights_label, inventory)
    return inventory


def _write_inventory(output_dir: Path, inventory: dict[str, Any]) -> None:
    """Write the inventory and, when it is enriched, the agentic_insights.json it was built with."""
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    (metadata / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    if "insights" in inventory:
        (metadata / "agentic_insights.json").write_text(json.dumps(inventory["insights"], indent=2), encoding="utf-8")


def _setup(output_dir: Path, inventory: dict[str, Any]) -> None:
    _write_inventory(output_dir, inventory)
    work = output_dir / WORK_DIRNAME
    work.mkdir(parents=True, exist_ok=True)
    pipeline = Pipeline(
        name="solo",
        tasks=[NotebookActivity(name="load", task_key="load", notebook_path="/Workspace/Shared/load")],
        tags={"source": "adf"},
    )
    (work / REPORT_FILENAME).write_text(json.dumps({"pipelines": [pipeline_to_dict(pipeline)]}), encoding="utf-8")


def _decide(decision: str, **extra: Any) -> dict[str, Any]:
    return {"components": [{"component_id": "component-1", "members": ["solo"], "decision": decision, **extra}]}


def test_plan_round_trips(tmp_path: Path) -> None:
    plan = ConversionPlan(
        inventory_sha256="inv",
        source_graphs_sha256="h0",
        agentic_insights_sha256="h1",
        components=[ComponentPlan(component_id="component-1", members=["solo"], decision="agentic")],
    )

    plan.write(tmp_path)

    assert ConversionPlan.load(tmp_path) == plan
    recorded = json.loads((tmp_path / "metadata" / "conversion_plan.json").read_text())
    assert recorded["schema_version"] == "3"
    assert "assignments" not in recorded["components"][0]


def test_a_plan_from_another_schema_version_is_refused(tmp_path: Path) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "conversion_plan.json").write_text(json.dumps({"schema_version": "1", "components": []}))

    with pytest.raises(ValueError, match="Re-run route"):
        ConversionPlan.load(tmp_path)


def test_recorded_plan_is_bound_to_the_source_graphs_and_insights(tmp_path: Path) -> None:
    _setup(tmp_path, _inventory())

    result = routing.record_plan(tmp_path, plan=_decide("agentic"))
    recorded = ConversionPlan.load(tmp_path)

    assert result["ok"] is True
    assert recorded is not None
    assert recorded.schema_version == SCHEMA_VERSION
    assert recorded.source_graphs_sha256 == "graphs-1"
    assert recorded.agentic_insights_sha256 == _inventory()["insights"]["agentic_insights_sha256"]
    assert result["source_graphs_sha256"] == "graphs-1"


def test_a_plan_has_no_per_node_assignments() -> None:
    """Phase 1 decides whole components, so an 'assignments' list is refused like any other unknown field."""
    violations = routing.validate_plan(_decide("agentic", assignments=[]), _inventory())

    assert violations == ["components[0]: unknown field 'assignments'"]


def test_an_edited_copy_of_the_recorded_plan_cannot_change_the_bound_hashes(tmp_path: Path) -> None:
    _setup(tmp_path, _inventory())
    plan = {**_decide("agentic"), "source_graphs_sha256": "x", "agentic_insights_sha256": "y"}

    assert routing.validate_plan(plan, _inventory()) == []
    assert routing.record_plan(tmp_path, plan=plan)["ok"] is True

    recorded = ConversionPlan.load(tmp_path)
    assert recorded is not None
    assert recorded.source_graphs_sha256 == "graphs-1"
    assert recorded.agentic_insights_sha256 == _inventory()["insights"]["agentic_insights_sha256"]


def test_an_unknown_key_in_an_authored_plan_is_still_refused() -> None:
    violations = routing.validate_plan({**_decide("agentic"), "decisions": {}}, _inventory())

    assert violations == ["unknown top-level key: 'decisions'"]


@pytest.mark.parametrize(
    "components",
    [None, {"component-1": {}}, [{"members": ["solo"], "decision": "agentic"}], ["component-1"]],
    ids=["missing", "an-object", "no-component-id", "not-an-object"],
)
def test_a_malformed_recorded_plan_is_refused_on_load(tmp_path: Path, components: Any) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    document: dict[str, Any] = {"schema_version": SCHEMA_VERSION}
    if components is not None:
        document["components"] = components
    (metadata / "conversion_plan.json").write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="malformed"):
        ConversionPlan.load(tmp_path)


def test_package_refuses_a_plan_when_the_insights_changed_after_route(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Only the H1 binding catches this: the inventory fingerprint ignores the insights block."""
    _setup(tmp_path, _inventory("insights-1"))
    assert routing.record_plan(tmp_path, plan=_decide("deterministic"))["ok"] is True
    _write_inventory(tmp_path, _inventory("insights-2"))

    code = package_main(["--output-dir", str(tmp_path), "--no-download-workspace-files", "--keep-intermediates"])

    assert code == 1
    assert "different agentic insights" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()


def test_apply_plan_refuses_a_stale_plan(tmp_path: Path) -> None:
    _setup(tmp_path, _inventory())
    assert routing.record_plan(tmp_path, plan=_decide("agentic"))["ok"] is True
    recorded = ConversionPlan.load(tmp_path)
    assert recorded is not None
    report_bytes = (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes()
    _write_inventory(tmp_path, _inventory("insights-2"))

    with pytest.raises(ValueError, match="different agentic insights"):
        apply_plan(tmp_path, recorded)
    assert (tmp_path / WORK_DIRNAME / REPORT_FILENAME).read_bytes() == report_bytes


def _recorded_plan(**component_fields: Any) -> dict[str, Any]:
    component = {"component_id": "component-1", "members": ["solo"], "decision": "agentic", **component_fields}
    return {
        "schema_version": SCHEMA_VERSION,
        "inventory_sha256": "inv",
        "source_graphs_sha256": None,
        "agentic_insights_sha256": None,
        "components": [component],
    }


def _write_recorded_plan(output_dir: Path, document: dict[str, Any]) -> None:
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    (metadata / "conversion_plan.json").write_text(json.dumps(document), encoding="utf-8")


def test_the_well_formed_recorded_plan_the_load_tests_start_from_loads(tmp_path: Path) -> None:
    _write_recorded_plan(tmp_path, _recorded_plan())

    assert ConversionPlan.load(tmp_path) is not None


@pytest.mark.parametrize(
    ("document", "key"),
    [
        ({**_recorded_plan(), "conversaton": []}, "conversaton"),
        (_recorded_plan(desicion="deterministic"), "desicion"),
        ({**_recorded_plan(), "suggested_groupings": [{"grouping_id": "grouping-1", "acepted": True}]}, "acepted"),
        ({**_recorded_plan(), "conversation": [{"question": "q", "answer": "a", "asked_by": "agent"}]}, "asked_by"),
        (_recorded_plan(assignments=[]), "assignments"),
    ],
    ids=["top-level", "component", "grouping", "conversation", "assignments"],
)
def test_a_recorded_plan_with_an_unknown_key_is_refused_on_load(
    tmp_path: Path, document: dict[str, Any], key: str
) -> None:
    """Route refuses these keys, so a reader must not drop them: package and the fill load through here."""
    _write_recorded_plan(tmp_path, document)

    with pytest.raises(ValueError, match=f"unknown key '{key}'"):
        ConversionPlan.load(tmp_path)


@pytest.mark.parametrize("key", ["inventory_sha256", "source_graphs_sha256", "agentic_insights_sha256"])
def test_a_recorded_plan_missing_a_binding_hash_is_refused_on_load(tmp_path: Path, key: str) -> None:
    document = _recorded_plan()
    del document[key]
    _write_recorded_plan(tmp_path, document)

    with pytest.raises(ValueError, match=f"needs '{key}'"):
        ConversionPlan.load(tmp_path)


def test_a_recorded_plan_with_no_inventory_fingerprint_is_refused_on_load(tmp_path: Path) -> None:
    _write_recorded_plan(tmp_path, {**_recorded_plan(), "inventory_sha256": None})

    with pytest.raises(ValueError, match="needs 'inventory_sha256'"):
        ConversionPlan.load(tmp_path)


def test_package_refuses_a_plan_given_an_unknown_key_after_route(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _setup(tmp_path, _inventory())
    assert routing.record_plan(tmp_path, plan=_decide("deterministic"))["ok"] is True
    plan_path = tmp_path / "metadata" / "conversion_plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["conversaton"] = [{"question": "Why migrate?", "answer": "Cost"}]
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    code = package_main(["--output-dir", str(tmp_path), "--no-download-workspace-files", "--keep-intermediates"])

    assert code == 1
    assert "unknown key 'conversaton'" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()


@pytest.mark.parametrize(
    "edit",
    [
        lambda plan: plan["components"][0].update(recommended="deterministic"),
        lambda plan: plan["components"][0]["options"]["deterministic"].update(capable=True),
        lambda plan: plan.update(findings=["nothing to see"]),
    ],
    ids=["recommended", "options", "findings"],
)
def test_package_refuses_a_plan_whose_library_owned_facts_were_edited(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], edit: Any
) -> None:
    """Route recomputes these from the inventory; a recorded copy that no longer matches was edited by hand."""
    _setup(tmp_path, _inventory())
    assert routing.record_plan(tmp_path, plan=_decide("deterministic"))["ok"] is True
    plan_path = tmp_path / "metadata" / "conversion_plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    edit(plan)
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    code = package_main(["--output-dir", str(tmp_path), "--no-download-workspace-files", "--keep-intermediates"])

    assert code == 1
    assert "library-owned" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()


@pytest.mark.parametrize(
    "document",
    [
        _recorded_plan(options=["deterministic"]),
        {**_recorded_plan(), "suggested_groupings": [{"grouping_id": "grouping-1", "members": 5}]},
        {**_recorded_plan(), "suggested_groupings": [{"grouping_id": "grouping-1", "components": "component-1"}]},
        {**_recorded_plan(), "suggested_groupings": [{"grouping_id": "grouping-1", "basis": 5}]},
        {**_recorded_plan(), "findings": 5},
        {**_recorded_plan(), "findings": [None]},
    ],
    ids=[
        "options-not-an-object",
        "grouping-members-not-a-list",
        "grouping-components-not-a-list",
        "grouping-basis-not-a-list",
        "findings-not-a-list",
        "finding-not-a-string",
    ],
)
def test_a_malformed_nested_field_is_refused_on_load(tmp_path: Path, document: dict[str, Any]) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "conversion_plan.json").write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="malformed"):
        ConversionPlan.load(tmp_path)


@pytest.mark.parametrize("accepted", ["true", 1, None], ids=["string", "number", "null"])
def test_a_grouping_accepted_that_is_not_a_boolean_is_refused_on_load(tmp_path: Path, accepted: object) -> None:
    document = {**_recorded_plan(), "suggested_groupings": [{"grouping_id": "grouping-1", "accepted": accepted}]}
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "conversion_plan.json").write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match=r"suggested_groupings\[0\] 'accepted' must be true or false"):
        ConversionPlan.load(tmp_path)


def test_package_fails_closed_on_a_malformed_nested_plan_field(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _setup(tmp_path, _inventory())
    assert routing.record_plan(tmp_path, plan=_decide("deterministic"))["ok"] is True
    plan_path = tmp_path / "metadata" / "conversion_plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["components"][0]["options"] = ["deterministic"]
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    code = package_main(["--output-dir", str(tmp_path), "--no-download-workspace-files", "--keep-intermediates"])

    assert code == 1
    assert "conversion_plan.json is unreadable" in capsys.readouterr().err
    assert not (tmp_path / "databricks.yml").exists()
