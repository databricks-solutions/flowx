"""Map the ADF AST onto the shared, source-faithful discovery AST.

This is the ADF half of the discovery contract (issue #62): it turns the typed
ADF AST (:mod:`flowx.models.adf_ast`) into the shared
:class:`~flowx.models.discovery.SourceGraph` model that both ADF and Airflow
align to. The mapping is deliberately **1:1 and lossless**:

* every ADF activity becomes exactly one discovery node -- no motif collapse,
  no merging;
* the activity's own type string is kept verbatim as ``native_type``;
* the verbatim source dict is preserved on every node (``raw``) and on the graph;
* dependency edges keep **all** of their outcome conditions (a ``dependsOn`` with
  ``["Succeeded", "Skipped"]`` stays a two-condition edge);
* anything without a shared typed home -- the ADF folder, retry policy detail,
  the target-side translation strategy -- rides in the ``properties`` /
  ``extensions`` seams rather than being dropped or forced into a field.

Populating a node's ``data_reads`` / ``data_writes`` from ADF dataset resolvers,
deriving control edges, and Switch-aware lineage walking are intentionally **not**
done here -- they belong to the follow-up slice (#62b). Nodes therefore carry
empty read/write lists for now.

The control-flow container shape follows :class:`~flowx.models.discovery.ContainerNode`:
an ``IfCondition`` becomes ``{"true": [...], "false": [...]}``, a ``ForEach`` /
``Until`` becomes ``{"body": [...]}``, and a ``Switch`` becomes
``{"<case value>": [...], "default": [...]}``. Branch insertion order matches the
order ADF declares the branches so a downstream flatten reproduces source order.
"""

from __future__ import annotations

from typing import Any

from flowx.discovery_inventory import STRATEGY_PROPERTY
from flowx.models.adf_ast import AdfActivity, AdfDefinitions, AdfParameter, AdfPipeline, AdfTrigger, AdfVariable
from flowx.models.discovery import (
    CONCEPT_BRANCH,
    CONCEPT_COPY_DATA,
    CONCEPT_GAP,
    CONCEPT_LOOP,
    CONCEPT_NOTEBOOK,
    CONCEPT_QUERY,
    CONCEPT_RUN_WORKFLOW,
    CONCEPT_SCRIPT,
    CONCEPT_SET_VARIABLE,
    CONCEPT_SWITCH,
    CONCEPT_WAIT,
    SOURCE_ADF,
    ContainerNode,
    GapNode,
    ParameterSpec,
    PolicySpec,
    ScheduleSpec,
    SourceDependency,
    SourceGraph,
    SourceNode,
)
from flowx.sources.adf.loader import classify_activity

# Neutral trigger category for each ADF trigger type. Anything unrecognised maps
# to ``""`` (unknown) with the ADF type preserved verbatim in the schedule
# extensions, so no trigger information is lost even for a type not listed here.
_TRIGGER_KIND: dict[str, str] = {
    "ScheduleTrigger": "schedule",
    "TumblingWindowTrigger": "interval",
    "BlobEventsTrigger": "file_arrival",
    "CustomEventsTrigger": "event",
}

# Neutral concept for each ADF activity type. The discovery concept vocabulary is
# intentionally non-exhaustive: ADF types with no shared concept (WebActivity,
# Delete, Filter, and the agentic-only types) map to CONCEPT_GAP, which here means
# "no shared concept applies" -- independent of the translation strategy, which is
# recorded separately under properties[STRATEGY_PROPERTY].
_CONCEPT_BY_TYPE: dict[str, str] = {
    "Copy": CONCEPT_COPY_DATA,
    "DatabricksNotebook": CONCEPT_NOTEBOOK,
    "DatabricksSparkJar": CONCEPT_SCRIPT,
    "DatabricksSparkPython": CONCEPT_SCRIPT,
    "DatabricksJob": CONCEPT_RUN_WORKFLOW,
    "ExecutePipeline": CONCEPT_RUN_WORKFLOW,
    "ForEach": CONCEPT_LOOP,
    "Until": CONCEPT_LOOP,
    "IfCondition": CONCEPT_BRANCH,
    "Switch": CONCEPT_SWITCH,
    "SetVariable": CONCEPT_SET_VARIABLE,
    "AppendVariable": CONCEPT_SET_VARIABLE,
    "Wait": CONCEPT_WAIT,
    "Lookup": CONCEPT_QUERY,
}


def adf_definitions_to_source_graphs(definitions: AdfDefinitions) -> list[SourceGraph]:
    """Map every pipeline in *definitions* to a shared :class:`SourceGraph`.

    Pipeline order is preserved so downstream consumers see the same ordering the
    loader produced. ADF triggers are mapped into :class:`ScheduleSpec` and attached
    to each pipeline they reference (see :func:`_attach_schedules`), so schedule /
    trigger information is preserved rather than dropped.
    """
    graphs = [adf_pipeline_to_source_graph(pipeline) for pipeline in definitions.pipelines]
    _attach_schedules(graphs, definitions.triggers)
    return graphs


def _attach_schedules(graphs: list[SourceGraph], triggers: list[AdfTrigger]) -> None:
    """Populate each graph's ``schedule`` from the triggers that reference it.

    A trigger can drive several pipelines and a pipeline can be driven by several
    triggers. The first trigger to reference a pipeline becomes its typed
    :attr:`SourceGraph.schedule`; any further triggers for the same pipeline are
    preserved verbatim under ``schedule.extensions["additional_triggers"]`` so
    nothing is lost. Pipeline references are matched case-insensitively, mirroring
    ADF's case-insensitive identifier semantics.
    """
    graphs_by_name = {graph.name: graph for graph in graphs}
    graphs_by_lower = {graph.name.lower(): graph for graph in graphs}

    for trigger in triggers:
        schedule = _trigger_to_schedule(trigger)
        for pipeline_name in _trigger_pipeline_names(trigger):
            graph = graphs_by_name.get(pipeline_name) or graphs_by_lower.get(pipeline_name.lower())
            if graph is None:
                continue
            if graph.schedule is None:
                graph.schedule = schedule
            else:
                additional = graph.schedule.extensions.setdefault("additional_triggers", [])
                additional.append(schedule.extensions.get("properties"))


def _trigger_to_schedule(trigger: AdfTrigger) -> ScheduleSpec:
    """Map a single ADF trigger to a source-faithful :class:`ScheduleSpec`.

    The recurrence payload is kept as-given: schedule triggers nest it under
    ``typeProperties.recurrence`` while tumbling-window / event triggers put their
    detail directly in ``typeProperties``, so whichever is present becomes the
    verbatim ``expression``. The full trigger ``properties`` block also rides in
    ``extensions`` so the mapping is lossless even for trigger detail with no typed
    home yet (pipeline parameters, runtime state, annotations).
    """
    type_properties = trigger.properties.get("typeProperties") or {}
    recurrence = type_properties.get("recurrence") if isinstance(type_properties, dict) else None
    if isinstance(recurrence, dict):
        expression: Any = recurrence
        timezone = recurrence.get("timeZone")
    else:
        expression = type_properties or None
        timezone = type_properties.get("timeZone") if isinstance(type_properties, dict) else None

    return ScheduleSpec(
        kind=_TRIGGER_KIND.get(trigger.type, ""),
        expression=expression,
        timezone=timezone,
        extensions={
            "trigger_name": trigger.name,
            "trigger_type": trigger.type,
            "properties": trigger.properties,
        },
    )


def _trigger_pipeline_names(trigger: AdfTrigger) -> list[str]:
    """Collect the names of the pipelines a trigger references, in order."""
    names: list[str] = []
    for reference in trigger.pipelines or []:
        pipeline_reference = reference.get("pipelineReference") if isinstance(reference, dict) else None
        name = pipeline_reference.get("referenceName") if isinstance(pipeline_reference, dict) else None
        if name:
            names.append(name)
    return names


def adf_pipeline_to_source_graph(pipeline: AdfPipeline) -> SourceGraph:
    """Map a single ADF pipeline to a source-faithful :class:`SourceGraph`."""
    properties: dict[str, str] = {}
    if pipeline.folder:
        properties["folder"] = pipeline.folder

    return SourceGraph(
        name=pipeline.name,
        source=SOURCE_ADF,
        parameters=_parameters_to_specs(pipeline.parameters),
        variables=_variables_to_specs(pipeline.variables),
        tags=list(pipeline.annotations) if pipeline.annotations else [],
        tasks=[_activity_to_node(activity) for activity in pipeline.activities],
        properties=properties,
        raw=pipeline.raw,
    )


def _activity_to_node(activity: AdfActivity) -> SourceNode:
    """Map one ADF activity to a discovery node (1:1, source-faithful).

    Fields are passed explicitly to each node class (rather than unpacking a
    shared dict) so the mapping stays type-checked end to end.
    """
    strategy = classify_activity(activity.type)
    concept = _CONCEPT_BY_TYPE.get(activity.type, CONCEPT_GAP)
    dependencies = [
        SourceDependency(upstream=dependency.activity, conditions=list(dependency.dependency_conditions))
        for dependency in (activity.depends_on or [])
    ]
    policy = _policy_to_spec(activity)
    properties: dict[str, Any] = {STRATEGY_PROPERTY: strategy.value}

    branches = _control_flow_branches(activity)
    if branches is not None:
        return ContainerNode(
            source_id=activity.name,
            task_key=activity.name,
            concept=concept,
            source=SOURCE_ADF,
            name=activity.name,
            native_type=activity.type,
            dependencies=dependencies,
            policy=policy,
            properties=properties,
            raw=activity.raw,
            branches=branches,
        )
    if strategy.value == "unsupported":
        return GapNode(
            source_id=activity.name,
            task_key=activity.name,
            source=SOURCE_ADF,
            name=activity.name,
            native_type=activity.type,
            dependencies=dependencies,
            policy=policy,
            properties=properties,
            raw=activity.raw,
            reason=f"unsupported ADF activity type {activity.type!r}",
        )
    return SourceNode(
        source_id=activity.name,
        task_key=activity.name,
        concept=concept,
        source=SOURCE_ADF,
        name=activity.name,
        native_type=activity.type,
        dependencies=dependencies,
        policy=policy,
        properties=properties,
        raw=activity.raw,
    )


def _control_flow_branches(activity: AdfActivity) -> dict[str, list[SourceNode]] | None:
    """Return the labelled branches of a control-flow activity, or ``None`` if it is a leaf.

    Keyed on the activity **type**, not on whether children happen to be present:
    a control-flow activity always maps to a :class:`ContainerNode`, and every
    branch it declares is always represented -- an empty branch is present-but-empty,
    never omitted. So an empty ``IfCondition`` still yields ``{"true": [], "false": []}``
    and a one-sided ``If`` keeps its empty ``false`` branch, rather than collapsing to
    a plain node and losing the control-flow structure. Returning ``None`` (not an
    empty dict) is what tells the caller the activity is a leaf.

    Branch order matches ADF's own declaration order (true before false, cases
    before default) so a downstream flatten walks children in source order.
    """
    if activity.type == "IfCondition":
        return {
            "true": [_activity_to_node(child) for child in (activity.if_true_activities or [])],
            "false": [_activity_to_node(child) for child in (activity.if_false_activities or [])],
        }
    if activity.type in ("ForEach", "Until"):
        return {"body": [_activity_to_node(child) for child in (activity.activities or [])]}
    if activity.type == "Switch":
        branches: dict[str, list[SourceNode]] = {}
        for case_value, case_activities in (activity.switch_cases or {}).items():
            branches[case_value] = [_activity_to_node(child) for child in case_activities]
        branches["default"] = [_activity_to_node(child) for child in (activity.switch_default_activities or [])]
        return branches
    return None


def _policy_to_spec(activity: AdfActivity) -> PolicySpec | None:
    """Map an ADF retry/timeout policy to a :class:`PolicySpec`, if present.

    Retry count and interval are already numeric in ADF and map to typed fields.
    The ADF timeout is an ISO-8601 duration *string* -- normalising it to seconds
    is a target concern, so it rides verbatim in ``extensions`` alongside the
    ``secureInput`` / ``secureOutput`` flags rather than being guessed at here.
    """
    policy = activity.policy
    if policy is None:
        return None
    extensions: dict[str, object] = {}
    if policy.timeout is not None:
        extensions["timeout"] = policy.timeout
    if policy.secure_input:
        extensions["secure_input"] = True
    if policy.secure_output:
        extensions["secure_output"] = True
    return PolicySpec(
        max_retries=policy.retry,
        retry_interval_seconds=policy.retry_interval_in_seconds,
        extensions=extensions,
    )


def _parameters_to_specs(parameters: dict[str, AdfParameter] | None) -> dict[str, ParameterSpec]:
    """Map ADF pipeline parameters to shared parameter specs."""
    if not parameters:
        return {}
    return {
        name: ParameterSpec(type=parameter.type, default=parameter.default_value)
        for name, parameter in parameters.items()
    }


def _variables_to_specs(variables: dict[str, AdfVariable] | None) -> dict[str, ParameterSpec]:
    """Map ADF pipeline variables to shared parameter specs (same shape)."""
    if not variables:
        return {}
    return {
        name: ParameterSpec(type=variable.type, default=variable.default_value) for name, variable in variables.items()
    }
