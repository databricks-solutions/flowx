"""Load Oozie application directories into flowx Pipeline IR (one pipeline per app).

Each app is translated on its own: a parse or graph error marks that app ``failed`` with a finding and the
others carry on. Counts in ``audit`` follow the Airflow source-audit contract the package gate checks, so
Oozie reports package the same way.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path

from flowx.models.ir import Pipeline
from flowx.sources.oozie.loader.ast import ActionNode, DecisionNode, OozieWorkflow
from flowx.sources.oozie.loader.discovery import OozieApp, discover_apps, resolve_app_file
from flowx.sources.oozie.loader.el_render import ElEnvironment, PropertyCycleError, translate_predicate
from flowx.sources.oozie.loader.graph import OozieGraphError, TranslatedAction, assign_task_keys, lower_workflow
from flowx.sources.oozie.loader.parse import OozieParseError, parse_workflow
from flowx.sources.oozie.loader.properties import STRIPPED_PROPERTIES, PropertyScope, parse_properties
from flowx.sources.oozie.loader.report import AppReport
from flowx.sources.oozie.translators import ActionContext, translate_action

logger = logging.getLogger(__name__)


def load_app(app: OozieApp) -> Pipeline:
    """Translates one app directory into a Pipeline; never raises for problems in the app itself."""
    try:
        return _translate_app(app)
    except (OozieParseError, OozieGraphError, PropertyCycleError, UnicodeDecodeError, OSError) as error:
        return _failed_pipeline(app, str(error))
    except Exception as error:
        logger.exception("Unexpected error loading Oozie app %r", app.name)
        return _failed_pipeline(app, f"unexpected error: {type(error).__name__}: {error}")


def _translate_app(app: OozieApp) -> Pipeline:
    workflow = parse_workflow(app.workflow_path.read_text(encoding="utf-8"))
    scope = _property_scope(app, workflow)
    report = AppReport(credentials_dropped=list(workflow.credentials))
    keys = assign_task_keys(workflow)
    environment = ElEnvironment(properties=scope, workflow_name=app.name, action_task_keys=keys.actions)
    _report_unmerged_global_settings(app, workflow, report)

    def translate_one_action(action: ActionNode, task_key: str) -> TranslatedAction:
        own_configuration = getattr(action.payload, "configuration", None) or {}
        action_environment = replace(
            environment,
            properties=scope.with_layer(own_configuration),
            inline_properties=frozenset(own_configuration),
        )
        context = ActionContext(task_key=task_key, app=app, environment=action_environment, report=report)
        translated = translate_action(action, context)
        for reference in getattr(action.payload, "job_xml", None) or []:
            _report_job_xml(app, report, action.name, reference)
        return translated

    lowering = lower_workflow(
        workflow,
        task_keys=keys,
        translate_action=translate_one_action,
        translate_predicate=lambda predicate: translate_predicate(predicate, environment),
    )

    _note_property_rules(environment, scope, report)
    report.kill_messages = dict(lowering.kill_messages)

    emitted = {task.task_key for task in lowering.tasks}
    audited_keys = [keys.actions[name] for name, node in workflow.nodes.items() if isinstance(node, ActionNode)]
    agentic_decision_keys: set[str] = set()
    for name, node in workflow.nodes.items():
        if not isinstance(node, DecisionNode) or not node.cases:
            continue
        condition_keys = keys.decisions[name]
        first_key = condition_keys[0]
        audited_keys.append(first_key)
        report.node_types[first_key] = "decision"
        agentic_condition_keys = set(condition_keys) & lowering.agentic_task_keys
        if agentic_condition_keys:
            agentic_decision_keys.add(first_key)
            report.agentic_reasons[first_key] = "; ".join(
                finding["message"]
                for finding in lowering.findings
                if finding.get("details", {}).get("task_key") in agentic_condition_keys
            )
    audited_keys = [key for key in audited_keys if key in emitted]
    agentic_keys = sorted(key for key in {*report.agentic_reasons, *agentic_decision_keys} if key in audited_keys)

    findings = [*report.findings, *lowering.findings]
    oozie_section = {
        **report.to_dict(),
        "unresolved_properties": sorted(environment.unresolved_properties),
        "warnings": lowering.warnings,
    }
    return Pipeline(
        name=app.name,  # app names are directory-derived; the bundler normalises them into job keys
        description="; ".join(f"Oozie kill {name!r}: {message}" for name, message in lowering.kill_messages.items())
        or None,
        parameters=[{"name": name, "default": default} for name, default in environment.job_parameters.items()],
        tags={"source": "oozie"},
        tasks=lowering.tasks,
        not_translatable=findings,
        reconciliation_status="verified_with_gaps" if agentic_keys or findings else "verified",
        audit={
            "source_file": str(app.workflow_path),
            "audited_activity_count": len(audited_keys),
            "deterministic_count": len(audited_keys) - len(agentic_keys),
            "agentic_count": len(agentic_keys),
            "failed_count": 0,
            "excluded_count": 0,
            "transformations": [*lowering.transformations, *report.transformations],
            "audited_task_keys": audited_keys,
            "agentic_task_keys": agentic_keys,
            "oozie": oozie_section,
        },
    )


def load_pipelines(
    source_dir: Path,
    *,
    pipeline: str | None = None,
    exclude: frozenset[str] = frozenset(),
    output_dir: Path | None = None,
) -> list[Pipeline]:
    """Loads every app under *source_dir* (or only *pipeline*), marking names in *exclude* as excluded.

    Apps under *output_dir* (flowx's own copies) are not loaded.
    """
    pipelines: list[Pipeline] = []
    for app in discover_apps(source_dir, output_dir=output_dir):
        if pipeline is not None and app.name != pipeline:
            continue
        loaded = load_app(app)
        if app.name in exclude:
            loaded.migration_status = "excluded"
            loaded.reconciliation_status = "excluded"
            count = max(int(loaded.audit.get("audited_activity_count", 0)), int(loaded.audit.get("failed_count", 0)))
            loaded.audit.update(deterministic_count=0, agentic_count=0, failed_count=0, excluded_count=count)
        pipelines.append(loaded)
    return pipelines


def _property_scope(app: OozieApp, workflow: OozieWorkflow) -> PropertyScope:
    job_properties = parse_properties(app.properties_path.read_text(encoding="utf-8")) if app.properties_path else {}
    defaults = {name: value for name, value in workflow.parameters.items() if value is not None}
    return PropertyScope(layers=(workflow.global_configuration, job_properties, defaults))


def _report_job_xml(app: OozieApp, report: AppReport, owner: str, reference: str) -> None:
    """Flags a ``<job-xml>`` file: M1 doesn't merge its settings into the action, so nothing in it is applied."""
    report.gap(
        "job_xml_not_applied",
        f"{owner}: <job-xml> {reference!r} is not applied; its settings aren't merged into the action in M1",
        {"node": owner, "job_xml": reference},
    )
    if resolve_app_file(app, reference) is None:
        report.missing_files.append({"action": owner, "reference": reference})


def _report_unmerged_global_settings(app: OozieApp, workflow: OozieWorkflow, report: AppReport) -> None:
    """Flags the parts of ``<global>`` that M1 doesn't carry onto each action, so nothing is dropped silently."""
    for reference in workflow.global_job_xml:
        _report_job_xml(app, report, "<global>", reference)
    if workflow.global_configuration:
        report.gap(
            "global_configuration_not_merged",
            "<global> <configuration> is used only to resolve ${...} properties in M1; "
            "it isn't merged into each action's settings",
            {"properties": sorted(workflow.global_configuration)},
        )


def _note_property_rules(environment: ElEnvironment, scope: PropertyScope, report: AppReport) -> None:
    for name in environment.job_parameters:
        report.note("PROP-JOB-PARAMETER", name)
    for name in sorted(environment.unresolved_properties):
        report.note("PROP-UNRESOLVED", name)
    defined = {name for layer in scope.layers for name in layer}
    for name in sorted(defined & STRIPPED_PROPERTIES):
        report.note("PROP-STRIPPED", name)
    for name in sorted(environment.derived_properties):
        report.note("PROP-DERIVED-INLINE", name)


def _failed_pipeline(app: OozieApp, message: str) -> Pipeline:
    return Pipeline(
        name=app.name,
        tags={"source": "oozie"},
        reconciliation_status="failed",
        not_translatable=[{"code": "oozie_app_failed", "severity": "failed", "message": message, "details": {}}],
        audit={
            "source_file": str(app.workflow_path),
            "audited_activity_count": 0,
            "deterministic_count": 0,
            "agentic_count": 0,
            "failed_count": 1,
            "excluded_count": 0,
            "transformations": [],
            "audited_task_keys": [],
            "agentic_task_keys": [],
            "oozie": {},
        },
    )
