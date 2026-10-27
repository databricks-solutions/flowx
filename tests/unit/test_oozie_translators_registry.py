"""Dispatching Oozie actions to translators, with every gap turned into a counted agentic placeholder."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from flowx.models.ir import NotebookActivity, PlaceholderActivity
from flowx.sources.oozie import rules, translators
from flowx.sources.oozie.loader import graph as graph_module
from flowx.sources.oozie.loader.discovery import OozieApp
from flowx.sources.oozie.loader.el_render import ElEnvironment
from flowx.sources.oozie.loader.graph import TranslatedAction
from flowx.sources.oozie.loader.parse import parse_workflow
from flowx.sources.oozie.loader.properties import PropertyScope
from flowx.sources.oozie.loader.report import AppReport, expand_properties
from flowx.sources.oozie.translators import ActionContext, AgenticAction, translate_action

_GRAPH_RULE_NOTE = re.compile(r'self\.note\("([A-Z]+(?:-[A-Z]+)+)"')


def _context(tmp_path: Path) -> ActionContext:
    app = OozieApp(name="app", directory=tmp_path, workflow_path=tmp_path / "workflow.xml")
    environment = ElEnvironment(properties=PropertyScope(layers=({},)), workflow_name="w", action_task_keys={})
    return ActionContext(task_key="step", app=app, environment=environment, report=AppReport())


def _action(body: str):
    workflow = parse_workflow(
        f'<workflow-app name="w"><start to="step"/><action name="step">{body}<ok to="end"/><error to="end"/></action>'
        '<end name="end"/></workflow-app>'
    )
    return workflow.nodes["step"]


@pytest.mark.parametrize(
    "body, reason_fragment",
    [
        ("<pig><script>x.pig</script></pig>", "Pig"),
        ('<sqoop xmlns="uri:oozie:sqoop-action:1.0"><command>import --table t</command></sqoop>', "Sqoop"),
        ("<map-reduce/>", "MapReduce"),
        ('<ssh xmlns="uri:oozie:ssh-action:0.2"><host>h</host><command>ls</command></ssh>', "SSH"),
        ('<shell xmlns="uri:oozie:shell-action:1.0"><exec>run.sh</exec></shell>', "planned for M2"),
        ('<hive2 xmlns="uri:oozie:hive2-action:1.0"><script>q.hql</script></hive2>', "planned for M2"),
        ('<my-custom xmlns="uri:company:custom:1.0"/>', "custom action"),
    ],
)
def test_unsupported_actions_become_placeholders(tmp_path: Path, body: str, reason_fragment: str):
    context = _context(tmp_path)
    translated = translate_action(_action(body), context)
    placeholder = translated.main
    assert isinstance(placeholder, PlaceholderActivity)
    assert placeholder.original_type.startswith("oozie:")
    assert reason_fragment in (placeholder.comment or "")
    assert placeholder.raw_definition and "<" in placeholder.raw_definition["xml"]
    assert context.report.agentic_reasons["step"] == placeholder.comment
    assert "step" in context.environment.agentic_actions
    assert any(entry["rule"] == "AGENTIC-ACTION" for entry in context.report.transformations)


def test_translator_raising_agentic_action_falls_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def broken(action, context):
        raise AgenticAction("spark-opts can't be parsed", hints={"spark_opts": "--conf 'x"})

    monkeypatch.setitem(translators.TRANSLATORS, "pig", broken)
    translated = translate_action(_action("<pig><script>x</script></pig>"), _context(tmp_path))
    assert isinstance(translated.main, PlaceholderActivity)
    assert translated.main.raw_definition["hints"] == {"spark_opts": "--conf 'x"}


def test_translator_exception_becomes_placeholder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def exploding(action, context):
        raise ValueError("odd XML in spark-opts")

    monkeypatch.setitem(translators.TRANSLATORS, "pig", exploding)
    context = _context(tmp_path)
    translated = translate_action(_action("<pig><script>x</script></pig>"), context)
    assert isinstance(translated.main, PlaceholderActivity)
    reason = "translator for <pig> failed: ValueError: odd XML in spark-opts"
    assert translated.main.comment == reason
    assert context.report.agentic_reasons["step"] == reason
    assert translated.main.raw_definition["hints"] == {"error": repr(ValueError("odd XML in spark-opts"))}
    assert [finding["code"] for finding in context.report.findings] == ["agentic_action"]


def test_fallback_rolls_back_what_the_failed_translator_recorded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def partial(action, context):
        context.report.note("SPARK-JAR", action.name, "half done")
        context.report.record_hdfs_paths(action.name, "jar", "hdfs://ns1/apps/etl.jar")
        raise AgenticAction("cannot finish")

    monkeypatch.setitem(translators.TRANSLATORS, "spark", partial)
    context = _context(tmp_path)
    translate_action(_action('<spark xmlns="uri:oozie:spark-action:1.0"><jar>x.jar</jar></spark>'), context)
    assert context.report.hdfs_paths == []
    assert [entry["rule"] for entry in context.report.transformations] == ["AGENTIC-ACTION"]
    assert [finding["code"] for finding in context.report.findings] == ["agentic_action"]


@pytest.mark.parametrize(
    "path",
    [
        "hdfs://ns1/data/raw/x",
        "HDFS://ns1/data/raw/x",
        "webhdfs://nn:50070/data",
        "swebhdfs://nn:50470/data",
        "viewfs://cluster/data",
        "maprfs:///mapr/cluster/data",
        "${nameNode}/apps/lib/etl.jar",
        "${namenode}/apps/lib/etl.jar",
    ],
)
def test_hdfs_family_references_are_recorded(path: str):
    report = AppReport()
    report.record_hdfs_paths("load", "arg", path)
    assert report.hdfs_paths == [{"action": "load", "field": "arg", "path": path}]


def test_registered_translator_is_used(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setitem(
        translators.TRANSLATORS,
        "pig",
        lambda action, context: TranslatedAction(
            main=NotebookActivity(name="s", task_key=context.task_key, notebook_path="/n")
        ),
    )
    assert isinstance(translate_action(_action("<pig/>"), _context(tmp_path)).main, NotebookActivity)


def test_report_records_hdfs_paths_and_serialises():
    report = AppReport()
    report.record_hdfs_paths("load", "jar", "${nameNode}/apps/lib/etl.jar")
    report.record_hdfs_paths("load", "arg", "hdfs://ns1/data/raw/x")
    report.record_hdfs_paths("load", "arg", "--date")
    assert report.hdfs_paths == [
        {"action": "load", "field": "jar", "path": "${nameNode}/apps/lib/etl.jar"},
        {"action": "load", "field": "arg", "path": "hdfs://ns1/data/raw/x"},
    ]
    assert set(report.to_dict()) >= {
        "hdfs_paths",
        "missing_files",
        "dropped_spark_conf",
        "kill_messages",
        "credentials_dropped",
    }


def test_hdfs_path_expansion_follows_properties_and_survives_cycles():
    scope = PropertyScope(
        layers=({"nameNode": "hdfs://ns1", "inputDir": "${nameNode}/data/raw", "a": "${b}", "b": "${a}"},)
    )
    assert (
        expand_properties("${inputDir}/x ${missing} ${wf:id()}", scope)
        == "${nameNode}/data/raw/x ${missing} ${wf:id()}"
    )
    assert expand_properties("${a}", scope) == "${a}"
    report = AppReport()
    report.record_hdfs_paths("load", "arg", "${inputDir}", scope)
    report.record_hdfs_paths("load", "arg", "${a}", scope)
    assert report.hdfs_paths == [{"action": "load", "field": "arg", "path": "${nameNode}/data/raw"}]


def test_every_rule_has_a_description():
    assert rules.RULES and all(isinstance(text, str) and text for text in rules.RULES.values())


def test_every_rule_id_noted_by_graph_is_registered():
    source = Path(graph_module.__file__).read_text(encoding="utf-8")
    used = set(_GRAPH_RULE_NOTE.findall(source))
    assert used
    assert used <= set(rules.RULES)
