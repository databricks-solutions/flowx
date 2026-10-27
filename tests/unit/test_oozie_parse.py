"""Parsing Oozie workflow.xml into the typed AST, across schema versions and action namespaces."""

from __future__ import annotations

import pytest

from flowx.sources.oozie.loader.ast import (
    ActionNode,
    DecisionNode,
    ForkNode,
    GenericAction,
    JoinNode,
    KillNode,
    OozieParseError,
    SparkAction,
)
from flowx.sources.oozie.loader.parse import parse_workflow

_SPARK_WORKFLOW = """
<workflow-app name="daily-${env}" xmlns="uri:oozie:workflow:1.0">
  <parameters>
    <property><name>env</name><value>prod</value></property>
    <property><name>runDate</name></property>
  </parameters>
  <global>
    <job-xml>global.xml</job-xml>
    <configuration><property><name>mapred.job.queue.name</name><value>etl</value></property></configuration>
  </global>
  <credentials><credential name="hive_creds" type="hive2"/></credentials>
  <start to="load"/>
  <action name="load" retry-max="3" retry-interval="5" cred="hive_creds">
    <spark xmlns="uri:oozie:spark-action:1.0">
      <resource-manager>${jobTracker}</resource-manager>
      <name-node>${nameNode}</name-node>
      <prepare><delete path="${nameNode}/tmp/out"/><mkdir path="${nameNode}/tmp/out"/></prepare>
      <configuration><property><name>spark.yarn.queue</name><value>etl</value></property></configuration>
      <master>yarn</master>
      <mode>cluster</mode>
      <name>Load bookings</name>
      <class>com.ek.Load</class>
      <jar>${nameNode}/apps/daily/lib/etl.jar</jar>
      <spark-opts>--num-executors 4 --conf spark.sql.shuffle.partitions=64</spark-opts>
      <arg>--date</arg>
      <arg>${runDate}</arg>
      <file>lookup.csv#lookup</file>
    </spark>
    <ok to="branch"/>
    <error to="fail"/>
  </action>
  <decision name="branch">
    <switch>
      <case to="fan_out">${wf:conf('mode') eq 'full'}</case>
      <default to="end"/>
    </switch>
  </decision>
  <fork name="fan_out"><path start="pig_step"/><path start="end_join"/></fork>
  <action name="pig_step">
    <pig><script>x.pig</script></pig>
    <ok to="end_join"/>
    <error to="fail"/>
  </action>
  <join name="end_join" to="end"/>
  <kill name="fail"><message>Failed: [${wf:errorMessage(wf:lastErrorNode())}]</message></kill>
  <end name="end"/>
</workflow-app>
"""


def test_parses_workflow_header_parameters_global_and_credentials():
    workflow = parse_workflow(_SPARK_WORKFLOW)
    assert workflow.name == "daily-${env}"
    assert workflow.schema_version == "1.0"
    assert workflow.parameters == {"env": "prod", "runDate": None}
    assert workflow.global_job_xml == ["global.xml"]
    assert workflow.global_configuration == {"mapred.job.queue.name": "etl"}
    assert workflow.credentials == ["hive_creds"]
    assert workflow.start.to == "load"


def test_parses_spark_action_fully():
    load = parse_workflow(_SPARK_WORKFLOW).nodes["load"]
    assert isinstance(load, ActionNode)
    assert (load.action_type, load.ok_to, load.error_to) == ("spark", "branch", "fail")
    assert (load.retry_max, load.retry_interval_minutes) == (3, 5)
    payload = load.payload
    assert isinstance(payload, SparkAction)
    assert (payload.master, payload.mode, payload.spark_name) == ("yarn", "cluster", "Load bookings")
    assert payload.main_class == "com.ek.Load"
    assert payload.jar == "${nameNode}/apps/daily/lib/etl.jar"
    assert payload.spark_opts == "--num-executors 4 --conf spark.sql.shuffle.partitions=64"
    assert payload.arguments == ["--date", "${runDate}"]
    assert payload.files == ["lookup.csv#lookup"]
    assert [(step.operation, step.path) for step in payload.prepare] == [
        ("delete", "${nameNode}/tmp/out"),
        ("mkdir", "${nameNode}/tmp/out"),
    ]
    assert payload.configuration == {"spark.yarn.queue": "etl"}


def test_parses_control_nodes_and_generic_actions():
    nodes = parse_workflow(_SPARK_WORKFLOW).nodes
    decision = nodes["branch"]
    assert isinstance(decision, DecisionNode)
    assert [(case.predicate, case.to) for case in decision.cases] == [("${wf:conf('mode') eq 'full'}", "fan_out")]
    assert decision.default_to == "end"
    assert isinstance(nodes["fan_out"], ForkNode) and nodes["fan_out"].paths == ["pig_step", "end_join"]
    assert isinstance(nodes["end_join"], JoinNode) and nodes["end_join"].to == "end"
    assert isinstance(nodes["fail"], KillNode) and nodes["fail"].message.startswith("Failed:")
    pig = nodes["pig_step"]
    assert isinstance(pig, ActionNode) and isinstance(pig.payload, GenericAction)
    assert pig.action_type == "pig" and "<script>x.pig</script>" in pig.payload.raw_xml


@pytest.mark.parametrize("namespace", ["", ' xmlns="uri:oozie:workflow:0.2"', ' xmlns="uri:oozie:workflow:0.5"'])
def test_namespace_variants_parse_the_same(namespace: str):
    workflow = parse_workflow(f'<workflow-app name="w"{namespace}><start to="end"/><end name="end"/></workflow-app>')
    assert workflow.start.to == "end" and set(workflow.nodes) == {"end"}


@pytest.mark.parametrize(
    "xml_text, message",
    [
        ("<workflow-app", "not well-formed"),
        ("<coordinator-app/>", "workflow-app"),
        ('<workflow-app name="w"><end name="end"/></workflow-app>', "<start>"),
        ('<workflow-app name="w"><start to="end"/><end name="end"/><end name="end"/></workflow-app>', "duplicate"),
        (
            '<workflow-app name="w"><start to="a"/><action name="a"><pig/><ok to="end"/></action>'
            '<end name="end"/></workflow-app>',
            "<error>",
        ),
    ],
)
def test_rejects_malformed_workflows(xml_text: str, message: str):
    with pytest.raises(OozieParseError, match=message):
        parse_workflow(xml_text)


def _named_workflow(decision_name: str, action_name: str) -> str:
    return (
        f'<workflow-app name="w"><start to="{decision_name}"/>'
        f'<decision name="{decision_name}"><switch><default to="{action_name}"/></switch></decision>'
        f'<action name="{action_name}"><pig/><ok to="end"/><error to="end"/></action>'
        '<end name="end"/></workflow-app>'
    )


@pytest.mark.parametrize(
    "decision_name, action_name",
    [
        ("pick&#10;import os", "load"),
        ("pick", "load&#10;raise SystemExit"),
        ("pick", "load&quot;)"),
        ("pick", "load'x"),
        ("pick", "Load.Data"),
        ("pick", "9load"),
    ],
)
def test_node_names_outside_the_xsd_identifier_are_rejected(decision_name: str, action_name: str):
    with pytest.raises(OozieParseError, match="invalid node name"):
        parse_workflow(_named_workflow(decision_name, action_name))


def test_a_node_without_a_name_is_rejected():
    with pytest.raises(OozieParseError, match="invalid node name ''"):
        parse_workflow('<workflow-app name="w"><start to="end"/><end name="end"/><kill/></workflow-app>')


def test_hyphenated_and_underscored_node_names_are_accepted():
    workflow = parse_workflow(_named_workflow("_pick-branch", "load-data"))
    assert set(workflow.nodes) == {"_pick-branch", "load-data", "end"}
