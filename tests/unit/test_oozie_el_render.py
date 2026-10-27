"""Rendering Oozie EL as Databricks dynamic references (tier 1), Python (tier 2), or agentic (tier 3)."""

from __future__ import annotations

import pytest

from flowx.sources.oozie.loader.el_render import (
    PYTHON_HELPERS,
    AgenticCondition,
    BridgedCondition,
    ElEnvironment,
    NativeCondition,
    PropertyCycleError,
    render_python,
    render_reference,
    translate_predicate,
)
from flowx.sources.oozie.loader.properties import PropertyScope


def _environment(**properties: str) -> ElEnvironment:
    return ElEnvironment(
        properties=PropertyScope(layers=(dict(properties),)),
        workflow_name="daily",
        action_task_keys={"load": "load", "shell-step": "shell_step"},
        agentic_actions={"shell-step"},
    )


@pytest.mark.parametrize(
    "text, expected",
    [
        ("plain", "plain"),
        ("${runDate}", "{{job.parameters.runDate}}"),
        ("${wf:conf('runDate')}", "{{job.parameters.runDate}}"),
        ("${nameNode}/data/raw", "/data/raw"),
        ("${outDir}/part", "/data/out/{{job.parameters.runDate}}/part"),
        ("${wf:id()}", "{{job.run_id}}"),
        ("${wf:name()}", "daily"),
        ("${wf:user()}", "${workspace.current_user.userName}"),
        ("${wf:actionData('load')['rows']}", "{{tasks.load.values.rows}}"),
        ("${oozie.wf.application.path}/lib", "${workspace.file_path}/lib"),
        ("${true}", "true"),
    ],
)
def test_tier_one_references(text: str, expected: str):
    environment = _environment(runDate="2024-01-01", outDir="${nameNode}/data/out/${runDate}", nameNode="hdfs://ns1")
    assert render_reference(text, environment) == expected


def test_leaf_and_unresolved_properties_become_job_parameters():
    environment = _environment(runDate="2024-01-01")
    render_reference("${runDate}-${missing.value}", environment)
    assert environment.job_parameters == {"runDate": "2024-01-01", "missing_value": None}
    assert environment.unresolved_properties == {"missing.value"}


@pytest.mark.parametrize(
    "text",
    [
        "${concat(a, b)}",
        "${fs:exists('/x')}",
        "${wf:errorCode('load')}",
        "${wf:actionData('shell-step')['k']}",
        "${hadoop:counters('load')['x']}",
        "${a eq b}",
    ],
)
def test_beyond_tier_one_renders_none(text: str):
    assert render_reference(text, _environment(a="1", b="2")) is None


def test_property_cycles_are_reported():
    with pytest.raises(PropertyCycleError, match="a -> b -> a"):
        render_reference("${a}", _environment(a="${b}", b="${a}"))


@pytest.mark.parametrize(
    "predicate, expected",
    [
        ("${mode eq 'full'}", NativeCondition(op="EQUAL_TO", left="{{job.parameters.mode}}", right="full")),
        ("${mode != 'full'}", NativeCondition(op="NOT_EQUAL", left="{{job.parameters.mode}}", right="full")),
        (
            "${wf:actionData('load')['rows'] gt 0}",
            NativeCondition(op="GREATER_THAN", left="{{tasks.load.values.rows}}", right="0"),
        ),
        ("${true}", NativeCondition(op="EQUAL_TO", left="true", right="true")),
        ("${false}", NativeCondition(op="EQUAL_TO", left="true", right="false")),
    ],
)
def test_native_predicates(predicate: str, expected: NativeCondition):
    assert translate_predicate(predicate, _environment(mode="full", skipLoad="false")) == expected


@pytest.mark.parametrize(
    "predicate",
    [
        "${mode eq 'full' and fs:exists('/data/_SUCCESS')}",
        "${empty(mode)}",
        "${mode lt other}",
        "${concat(mode, '_x') eq 'full_x'}",
        "${not skipLoad}",
        "${count eq 5}",
        "${skipLoad}",
        "${skipLoad eq true}",
    ],
)
def test_bridged_predicates(predicate: str):
    translation = translate_predicate(predicate, _environment(mode="full", other="z", skipLoad="false"))
    assert isinstance(translation, BridgedCondition)
    assert translation.imports == [PYTHON_HELPERS]
    compile(translation.code, "<bridge>", "eval")


@pytest.mark.parametrize(
    "predicate, reason_fragment",
    [
        ("${hadoop:counters('load')['FileSystemCounters']['HDFS_BYTES_READ'] gt 0}", "counters"),
        ("${wf:lastErrorNode() eq 'load'}", "lastErrorNode"),
        ("${wf:errorCode('load') eq 'JA018'}", "error codes"),
        ("${wf:actionData('shell-step')['k'] eq 'v'}", "agentic"),
        ("${my:custom('x')}", "custom EL function"),
        ("${a eq}", "parse"),
    ],
)
def test_agentic_predicates(predicate: str, reason_fragment: str):
    translation = translate_predicate(predicate, _environment())
    assert isinstance(translation, AgenticCondition) and reason_fragment in translation.reason


def test_python_rendering_evaluates_with_el_semantics():
    environment = _environment(count="10", mode="full")
    expression = render_python("${count gt 5 and concat(mode, '!') eq 'full!'}", environment)
    assert expression is not None
    namespace: dict = {}
    exec(PYTHON_HELPERS, namespace)
    widgets = {"p_count": "10", "p_mode": "full"}
    namespace["dbutils"] = type("D", (), {"widgets": type("W", (), {"get": staticmethod(widgets.__getitem__)})})
    assert eval(expression.code, namespace) is True
    assert expression.widget_bindings == {"p_count": "{{job.parameters.count}}", "p_mode": "{{job.parameters.mode}}"}


def test_derived_properties_are_collected():
    environment = _environment(runDate="2024-01-01", outDir="${nameNode}/data/out/${runDate}", nameNode="hdfs://ns1")
    render_reference("${outDir}/part", environment)
    assert "outDir" in environment.derived_properties


def test_replace_all_converts_dollar_groups():
    namespace: dict = {}
    exec(PYTHON_HELPERS, namespace)
    assert namespace["_el_replace_all"]("a1b2", "([0-9])", "<$1>") == "a<1>b<2>"


_INJECTION_KEYS = ['x") + __import__("os").getcwd() + ("', "a}} b", "a\nb", "abc\n"]


@pytest.mark.parametrize("key", _INJECTION_KEYS)
def test_action_data_keys_must_be_plain(key: str):
    text = "${wf:actionData('load')['" + key + "']}"
    assert render_reference(text, _environment()) is None
    assert isinstance(translate_predicate(text, _environment()), AgenticCondition)


def _run(code: str, widgets: dict[str, str]):
    namespace: dict = {}
    exec(PYTHON_HELPERS, namespace)
    namespace["dbutils"] = type("D", (), {"widgets": type("W", (), {"get": staticmethod(widgets.__getitem__)})})
    return eval(code, namespace)


def test_bridged_predicate_runs_end_to_end():
    translation = translate_predicate("${skipLoad eq true}", _environment(skipLoad="false"))
    assert isinstance(translation, BridgedCondition)
    assert _run(translation.code, {"p_skipLoad": "TRUE"}) == "true"
    assert _run(translation.code, {"p_skipLoad": "false"}) == "false"


def test_property_parameter_names_do_not_collide():
    environment = _environment(**{"a.b": "1", "a_b": "2"})
    assert render_reference("${a.b}-${a_b}", environment) == "{{job.parameters.a_b}}-{{job.parameters.a_b_2}}"
    assert environment.job_parameters == {"a_b": "1", "a_b_2": "2"}


def test_widget_names_do_not_collide():
    environment = _environment()
    environment.action_task_keys.update({"a_b": "a_b", "a": "a"})
    expression = render_python("${wf:actionData('a_b')['c']}${wf:actionData('a')['b_c']}", environment)
    assert expression is not None
    assert len(expression.widget_bindings) == 2
    assert set(expression.widget_bindings.values()) == {"{{tasks.a_b.values.c}}", "{{tasks.a.values.b_c}}"}


def test_malformed_referenced_property_is_agentic_but_cycles_propagate():
    translation = translate_predicate("${x eq 'a'}", _environment(x="${a eq}"))
    assert isinstance(translation, AgenticCondition)
    with pytest.raises(PropertyCycleError):
        translate_predicate("${a eq 'x'}", _environment(a="${b}", b="${a}"))


def test_non_finite_number_literal_is_unsupported():
    assert render_python("${1e999}", _environment()) is None
    assert render_reference("${1e999}", _environment()) is None
