"""Loading Oozie app directories into Pipeline IR: parameters, audit counts, findings, failure isolation."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowx.models.ir import PlaceholderActivity, SparkJarActivity
from flowx.sources.oozie.loader import api, load_pipelines

_WORKFLOW = """
<workflow-app name="daily" xmlns="uri:oozie:workflow:1.0">
  <credentials><credential name="hcat" type="hcat"/></credentials>
  <start to="load"/>
  <action name="load">
    <spark xmlns="uri:oozie:spark-action:1.0">
      <class>com.ek.Load</class><jar>lib/etl.jar</jar>
      <arg>${inputDir}</arg><arg>${runDate}</arg><arg>${missing}</arg>
    </spark>
    <ok to="check"/><error to="fail"/>
  </action>
  <decision name="check">
    <switch><case to="crunch">${hadoop:counters('load')['x'] gt 0}</case><default to="end"/></switch>
  </decision>
  <action name="crunch"><pig><script>c.pig</script></pig><ok to="end"/><error to="fail"/></action>
  <kill name="fail"><message>failed</message></kill>
  <end name="end"/>
</workflow-app>
"""


def test_loads_a_pipeline_with_parameters_audit_and_findings(tmp_path: Path):
    _write_app(tmp_path, "daily")
    (pipeline,) = load_pipelines(tmp_path)
    assert pipeline.name == "daily" and pipeline.tags == {"source": "oozie"}
    assert {parameter["name"]: parameter["default"] for parameter in pipeline.parameters or []} == {
        "runDate": "2024-01-01",
        "missing": None,
    }
    load = next(task for task in pipeline.tasks if task.task_key == "load")
    assert isinstance(load, SparkJarActivity)
    assert load.parameters == ["/data/raw", "{{job.parameters.runDate}}", "{{job.parameters.missing}}"]
    assert isinstance(next(task for task in pipeline.tasks if task.task_key == "crunch"), PlaceholderActivity)
    audit = pipeline.audit
    assert audit["audited_activity_count"] == 3  # load, check, crunch
    assert (audit["deterministic_count"], audit["agentic_count"]) == (1, 2)
    assert set(audit["agentic_task_keys"]) == {"check", "crunch"}
    assert audit["source_file"].endswith("daily/workflow.xml")
    assert audit["oozie"]["unresolved_properties"] == ["missing"]
    assert audit["oozie"]["credentials_dropped"] == ["hcat"]
    assert "counters" in audit["oozie"]["agentic_reasons"]["check"]
    assert audit["oozie"]["node_types"]["check"] == "decision"
    assert set(audit["agentic_task_keys"]) <= set(audit["audited_task_keys"])
    assert audit["oozie"]["kill_messages"] == {"fail": "failed"}
    assert pipeline.reconciliation_status == "verified_with_gaps"
    assert all(finding["severity"] in ("gap", "failed") for finding in pipeline.not_translatable)
    rules = {entry["rule"] for entry in audit["transformations"]}
    assert {"PROP-JOB-PARAMETER", "PROP-STRIPPED", "PROP-UNRESOLVED", "PROP-DERIVED-INLINE"} <= rules


def test_a_broken_app_fails_alone(tmp_path: Path):
    _write_app(tmp_path, "good")
    _write_app(tmp_path, "broken", workflow="<workflow-app name='b'><start to='nowhere'/></workflow-app>")
    pipelines = {pipeline.name: pipeline for pipeline in load_pipelines(tmp_path)}
    assert pipelines["good"].reconciliation_status == "verified_with_gaps"
    broken = pipelines["broken"]
    assert broken.reconciliation_status == "failed" and broken.tasks == []
    assert broken.audit["failed_count"] == 1
    assert broken.not_translatable[0]["severity"] == "failed" and "nowhere" in broken.not_translatable[0]["message"]


def test_pipeline_filter_and_exclusion(tmp_path: Path):
    _write_app(tmp_path, "one")
    _write_app(tmp_path, "two")
    assert [pipeline.name for pipeline in load_pipelines(tmp_path, pipeline="two")] == ["two"]
    excluded = {pipeline.name: pipeline for pipeline in load_pipelines(tmp_path, exclude=frozenset({"one"}))}["one"]
    assert (excluded.migration_status, excluded.reconciliation_status) == ("excluded", "excluded")
    audit = excluded.audit
    assert (audit["excluded_count"], audit["deterministic_count"], audit["agentic_count"], audit["failed_count"]) == (
        3,
        0,
        0,
        0,
    )


_TWO_CASE = """
<workflow-app name="t" xmlns="uri:oozie:workflow:1.0">
  <start to="check"/>
  <decision name="check">
    <switch>
      <case to="a">${wf:conf('x') eq 'y'}</case>
      <case to="a">${hadoop:counters('a')['x'] gt 0}</case>
      <default to="end"/>
    </switch>
  </decision>
  <action name="a"><pig><script>c.pig</script></pig><ok to="end"/><error to="fail"/></action>
  <kill name="fail"><message>f</message></kill>
  <end name="end"/>
</workflow-app>
"""

_SCOPED = """
<workflow-app name="s" xmlns="uri:oozie:workflow:1.0">
  <start to="a"/>
  <action name="a">
    <spark xmlns="uri:oozie:spark-action:1.0">
      <configuration><property><name>x</name><value>1</value></property></configuration>
      <class>C</class><jar>lib/etl.jar</jar><arg>${x}</arg>
    </spark>
    <ok to="b"/><error to="fail"/>
  </action>
  <action name="b">
    <spark xmlns="uri:oozie:spark-action:1.0"><class>C</class><jar>lib/etl.jar</jar><arg>${x}</arg></spark>
    <ok to="end"/><error to="fail"/>
  </action>
  <kill name="fail"><message>f</message></kill>
  <end name="end"/>
</workflow-app>
"""


def test_a_later_agentic_case_makes_the_whole_decision_one_agentic_item(tmp_path: Path):
    _write_app(tmp_path, "t", workflow=_TWO_CASE)
    (pipeline,) = load_pipelines(tmp_path)
    audit = pipeline.audit
    assert set(audit["agentic_task_keys"]) <= set(audit["audited_task_keys"])
    assert audit["audited_task_keys"].count("check") == 1
    assert "check" in audit["agentic_task_keys"]
    assert "counters" in audit["oozie"]["agentic_reasons"]["check"]


def test_an_action_configuration_overrides_a_property_for_that_action_only(tmp_path: Path):
    _write_app(tmp_path, "s", workflow=_SCOPED)
    (tmp_path / "s" / "job.properties").write_text("x=2\n")
    (pipeline,) = load_pipelines(tmp_path)
    tasks = {task.task_key: task for task in pipeline.tasks}
    assert tasks["a"].parameters == ["1"]
    assert tasks["b"].parameters == ["{{job.parameters.x}}"]
    assert {parameter["name"]: parameter["default"] for parameter in pipeline.parameters or []} == {"x": "2"}


def test_an_unexpected_error_fails_only_that_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _write_app(tmp_path, "good")
    _write_app(tmp_path, "bad")
    real = api.lower_workflow

    def flaky(workflow, **kwargs):
        if workflow.name == "daily" and flaky.calls:
            raise KeyError("boom")
        flaky.calls += 1
        return real(workflow, **kwargs)

    flaky.calls = 0
    monkeypatch.setattr(api, "lower_workflow", flaky)
    pipelines = {pipeline.name: pipeline for pipeline in load_pipelines(tmp_path)}
    assert pipelines["bad"].reconciliation_status == "verified_with_gaps"
    assert pipelines["good"].reconciliation_status == "failed"
    assert "unexpected error: KeyError" in pipelines["good"].not_translatable[0]["message"]


def _write_app(root: Path, name: str, workflow: str = _WORKFLOW) -> None:
    directory = root / name
    (directory / "lib").mkdir(parents=True)
    (directory / "lib" / "etl.jar").write_bytes(b"jar")
    (directory / "workflow.xml").write_text(workflow)
    (directory / "job.properties").write_text(
        "nameNode=hdfs://ns1\ninputDir=${nameNode}/data/raw\nrunDate=2024-01-01\n"
    )


_JOB_XML_WORKFLOW = """
<workflow-app name="daily" xmlns="uri:oozie:workflow:1.0">
  <global>
    <job-xml>conf/global-site.xml</job-xml>
    <configuration><property><name>mapred.job.queue.name</name><value>etl</value></property></configuration>
  </global>
  <start to="load"/>
  <action name="load">
    <spark xmlns="uri:oozie:spark-action:1.0">
      <job-xml>conf/spark-site.xml</job-xml>
      <class>com.ek.Load</class><jar>lib/etl.jar</jar>
    </spark>
    <ok to="crunch"/><error to="fail"/>
  </action>
  <action name="crunch"><pig><job-xml>conf/gone.xml</job-xml><script>c.pig</script></pig>
    <ok to="end"/><error to="fail"/></action>
  <kill name="fail"><message>failed</message></kill>
  <end name="end"/>
</workflow-app>
"""


def test_job_xml_and_global_configuration_are_reported_not_dropped(tmp_path: Path):
    _write_app(tmp_path, "daily", _JOB_XML_WORKFLOW)
    (tmp_path / "daily" / "conf").mkdir()
    (tmp_path / "daily" / "conf" / "spark-site.xml").write_text("<configuration/>")
    (pipeline,) = load_pipelines(tmp_path)
    job_xml = [finding for finding in pipeline.not_translatable if finding["code"] == "job_xml_not_applied"]
    assert sorted((finding["details"]["node"], finding["details"]["job_xml"]) for finding in job_xml) == [
        ("<global>", "conf/global-site.xml"),
        ("crunch", "conf/gone.xml"),
        ("load", "conf/spark-site.xml"),
    ]
    assert all(finding["severity"] == "gap" for finding in job_xml)
    assert pipeline.audit["oozie"]["missing_files"] == [
        {"action": "<global>", "reference": "conf/global-site.xml"},
        {"action": "crunch", "reference": "conf/gone.xml"},
    ]
    (global_configuration,) = [
        finding for finding in pipeline.not_translatable if finding["code"] == "global_configuration_not_merged"
    ]
    assert "only to resolve" in global_configuration["message"]
    assert global_configuration["details"] == {"properties": ["mapred.job.queue.name"]}
    assert pipeline.reconciliation_status == "verified_with_gaps"


def test_no_global_section_means_no_global_findings(tmp_path: Path):
    _write_app(tmp_path, "daily")
    (pipeline,) = load_pipelines(tmp_path)
    codes = {finding["code"] for finding in pipeline.not_translatable}
    assert not codes & {"job_xml_not_applied", "global_configuration_not_merged"}
