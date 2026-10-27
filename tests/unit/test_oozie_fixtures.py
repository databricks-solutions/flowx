"""Fixtures are valid Oozie (official XSDs), every rule has a fixture, and every fixture triggers its rules."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

etree = pytest.importorskip("lxml.etree")

from flowx.sources.oozie.loader import discover_apps, load_app  # noqa: E402
from flowx.sources.oozie.rules import RULES  # noqa: E402

_ROOT = Path(__file__).resolve().parents[1] / "resources" / "oozie"
_MANIFEST = yaml.safe_load((_ROOT / "fixtures.yaml").read_text())["fixtures"]
_PROVENANCE = {"xsd-derived", "apache-example", "oracle-backed", "hand-derived", "customer"}
_COVERED_BY_UNIT_TESTS = {"PROP-UNRESOLVED", "SPARK-AUTOSCALE", "SPARK-MAVEN", "SPARK-DEFERRED"}
_WORKFLOW_SCHEMAS = {
    "uri:oozie:workflow:1.0": "oozie-workflow-1.0.xsd",
    "uri:oozie:workflow:0.5": "oozie-workflow-0.5.xsd",
}
_ACTION_SCHEMAS = {
    "uri:oozie:spark-action:1.0": "spark-action-1.0.xsd",
    "uri:oozie:shell-action:1.0": "shell-action-1.0.xsd",
    "uri:oozie:sqoop-action:1.0": "sqoop-action-1.0.xsd",
    "uri:oozie:sla:0.2": "oozie-sla-0.2.xsd",
}


def _schema(workflow_namespace: str):
    """Builds one schema that imports the workflow XSD and every action XSD.

    The workflow XSD lets an action hold any element from another namespace, but as a *strict* wildcard: the
    validator must know that namespace's schema, or it rejects the element. Importing the action schemas
    alongside the workflow one is how Oozie itself validates, and it checks the action bodies in the same pass.
    """
    imports = {workflow_namespace: _WORKFLOW_SCHEMAS[workflow_namespace], **_ACTION_SCHEMAS}
    wrapper = (
        '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">'
        + "".join(
            f'<xs:import namespace="{namespace}" schemaLocation="{(_ROOT / "xsd" / file_name).as_uri()}"/>'
            for namespace, file_name in imports.items()
        )
        + "</xs:schema>"
    )
    return etree.XMLSchema(etree.fromstring(wrapper.encode()))


@pytest.mark.parametrize("fixture", sorted(_MANIFEST))
def test_fixture_is_valid_oozie(fixture: str):
    document = etree.parse(str(_ROOT / "apps" / fixture / "workflow.xml"))
    schema = _schema(etree.QName(document.getroot()).namespace)
    assert schema.validate(document), schema.error_log


def test_schema_rejects_a_misordered_spark_action():
    """Guards the validation itself: an action body in the wrong element order must fail."""
    document = etree.fromstring(
        b'<workflow-app name="w" xmlns="uri:oozie:workflow:1.0"><start to="s"/>'
        b'<action name="s"><spark xmlns="uri:oozie:spark-action:1.0">'
        b"<name>s</name><master>yarn</master><jar>x.jar</jar></spark>"
        b'<ok to="end"/><error to="end"/></action><end name="end"/></workflow-app>'
    )
    assert not _schema("uri:oozie:workflow:1.0").validate(document)


def test_manifest_and_rules_agree():
    assert set(_MANIFEST) == {path.name for path in (_ROOT / "apps").iterdir() if path.is_dir()}
    declared = {rule for entry in _MANIFEST.values() for rule in entry["rules"]}
    assert declared <= set(RULES), declared - set(RULES)
    assert set(RULES) - declared - _COVERED_BY_UNIT_TESTS == set()
    assert all(entry["provenance"] in _PROVENANCE for entry in _MANIFEST.values())


@pytest.mark.parametrize("fixture", sorted(_MANIFEST))
def test_fixture_triggers_its_declared_rules(fixture: str):
    (app,) = [app for app in discover_apps(_ROOT / "apps") if app.name == fixture]
    pipeline = load_app(app)
    assert pipeline.reconciliation_status != "failed", pipeline.not_translatable
    applied = {entry["rule"] for entry in pipeline.audit["transformations"]}
    assert set(_MANIFEST[fixture]["rules"]) <= applied, set(_MANIFEST[fixture]["rules"]) - applied
