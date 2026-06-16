"""Regression tests for #2 parsing fixes: web-activity body expressions and
@utcNow pipeline-parameter defaults."""

from __future__ import annotations

from flowx.models.adf_ast import AdfActivity, AdfDefinitions, AdfParameter, AdfPipeline
from flowx.models.ir import TranslationContext
from flowx.preparer.code_generator import generate_web_activity_notebook
from flowx.translator.activity_translators import web_activity
from flowx.translator.engine import translate_pipeline

_DEFS = AdfDefinitions(pipelines=[], datasets={}, linked_services={}, triggers=[])


def _web(type_properties: dict, context: TranslationContext):
    activity = AdfActivity(name="Notify", type="WebActivity", type_properties=type_properties)
    return web_activity.translate(activity, {"name": "Notify", "task_key": "notify"}, context, _DEFS)


def test_nested_concat_variables_body_is_lowered_to_python():
    ctx = TranslationContext().with_variable("batchId", "_init_batchId")
    ir = _web(
        {
            "method": "POST",
            "url": "https://example.com/hook",
            "body": {"text": {"value": "@concat('batch ', variables('batchId'))", "type": "Expression"}},
        },
        ctx,
    )
    assert ir.body_code is not None
    nb = generate_web_activity_notebook(ir)
    assert "@concat" not in nb and "@variables" not in nb
    assert "dbutils.widgets.get('batchId')" in nb
    assert "'batch ' +" in nb  # concatenation, not raw token


def test_bare_variables_body_reads_from_widget_and_binds_it():
    ctx = TranslationContext().with_variable("statusMessage", "set_msg")
    ir = _web(
        {
            "method": "POST",
            "url": "https://example.com/hook",
            "body": {"text": {"value": "@variables('statusMessage')", "type": "Expression"}},
        },
        ctx,
    )
    nb = generate_web_activity_notebook(ir)
    assert 'dbutils.widgets.get("statusMessage")' in nb
    # the dab ref is threaded so the preparer can bind it in base_parameters
    assert ir.body_required_parameters.get("statusMessage") == "{{tasks.set_msg.values.statusMessage}}"


def test_literal_body_unchanged():
    ir = _web(
        {"method": "POST", "url": "https://x", "body": {"status": "completed"}},
        TranslationContext(),
    )
    assert ir.body_code is None  # pure literal -> generator renders directly
    nb = generate_web_activity_notebook(ir)
    assert "completed" in nb


def test_utcnow_parameter_default_resolves_to_dab_ref():
    pipeline = AdfPipeline(
        name="p",
        activities=[AdfActivity(name="W", type="Wait", type_properties={"waitTimeInSeconds": 1})],
        parameters={"runDate": AdfParameter(type="String", default_value="@utcNow('yyyy-MM-dd')")},
    )
    report = translate_pipeline(pipeline, _DEFS)
    run_date = next(p for p in report.pipeline.parameters if p["name"] == "runDate")
    assert run_date["default"] == "{{job.start_time.iso_date}}"
