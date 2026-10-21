"""Map the ADF AST onto the source-neutral discovery graph contract (``SourceGraph``).

This is the ADF half of the discovery contract (issue #62): it turns the typed
ADF AST (:mod:`flowx.models.adf_ast`) into the shared
:class:`~flowx.models.discovery.SourceGraph` model that both ADF and Airflow
align to. The mapping is deliberately **1:1 and lossless**:

* every ADF activity becomes exactly one discovery node -- no motif collapse,
  no merging;
* the activity's own type string is kept verbatim as ``native_type``;
* the source dict is preserved on every node (``raw``) and on the graph, verbatim
  apart from the fields an ADF REST GET adds without meaning (see
  :func:`_pipeline_without_get_defaults`), so a live export and a Git or ARM export
  of the same factory give the same graphs and hash;
* dependency edges keep **all** of their outcome conditions (a ``dependsOn`` with
  ``["Succeeded", "Skipped"]`` stays a two-condition edge);
* anything without a shared typed home -- the ADF folder, retry policy detail,
  the target-side translation strategy -- rides in the ``properties`` /
  ``extensions`` seams rather than being dropped or forced into a field.

Data lineage (#62b) is populated here now: every node's ``data_reads`` /
``data_writes`` are resolved from the ADF dataset references via
:func:`~flowx.sources.adf.dataset_lineage.activity_data_assets` (the two-tier
identity / signature model), and every ``ExecutePipeline`` node records the child
pipeline it invokes under the neutral
:data:`~flowx.discovery_lineage.INVOKES_WORKFLOW_PROPERTY` marker. Because
:func:`_activity_to_node` recurses into every control-flow branch, this population
is Switch-aware for free -- a Copy or ExecutePipeline nested inside a Switch case
(or default), a ForEach / Until body, or either If branch carries its assets and
marker like any top-level node. The graph's :attr:`SourceGraph.lineage` block is
then derived by the source-neutral :func:`~flowx.discovery_lineage.build_graph_lineage`.

The control-flow container shape follows :class:`~flowx.models.discovery.ContainerNode`:
an ``IfCondition`` becomes ``{"true": [...], "false": [...]}``, a ``ForEach`` /
``Until`` becomes ``{"body": [...]}``, and a ``Switch`` becomes
``{"<case value>": [...], "default": [...]}``. Branch insertion order matches the
order ADF declares the branches so a downstream flatten reproduces source order.
Every declared case is its own branch, as conversion converts every case. A case whose
value is literally ``"default"``, or repeats an earlier case's value, is keyed with a
``case:`` prefix (``"case:default"``, ``"case:full"``, adding more prefixes until the label
is free) so it never collides with the Switch's own default branch or another case.
"""

from __future__ import annotations

import logging
from collections.abc import Collection
from typing import Any

from flowx.discovery_inventory import STRATEGY_PROPERTY
from flowx.discovery_lineage import (
    INVOKES_WAIT_PROPERTY,
    INVOKES_WORKFLOW_PROPERTY,
    build_graph_lineage,
    walk_nodes,
)
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
from flowx.sources.adf.dataset_lineage import activity_data_assets
from flowx.sources.adf.loader import classify_activity, parse_activity

logger = logging.getLogger(__name__)

# Switch node property listing case values declared more than once: each entry is
# ``{"value": ..., "branches": [...]}`` naming every branch label that carries the value.
# Every case is kept, so this only flags a probably malformed Switch.
DUPLICATE_SWITCH_CASES_PROPERTY = "duplicate_switch_cases"

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


def adf_definitions_to_source_graphs(
    definitions: AdfDefinitions, *, known_workflows: Collection[str] | None = None
) -> list[SourceGraph]:
    """Map every pipeline in *definitions* to a shared :class:`SourceGraph`.

    Pipeline order is preserved so downstream consumers see the same ordering the
    loader produced. ADF triggers are mapped into :class:`ScheduleSpec` and attached
    to each pipeline they reference (see :func:`_attach_schedules`), so schedule /
    trigger information is preserved rather than dropped. Each graph's
    :attr:`SourceGraph.lineage` block is derived (control + data edges) once its
    nodes' reads / writes and invocation markers are populated -- data edges are
    joined within a single graph, matching the per-workflow scope of the shared
    :class:`~flowx.models.lineage.Lineage` model. An ``ExecutePipeline`` edge is resolved
    only when its child pipeline is one of the pipelines in this export.

    ``definitions`` is threaded down to the activity mapper so a node's data assets
    resolve against the factory's datasets and linked services. ``known_workflows``
    names every pipeline in the export; pass it when *definitions* holds only some
    of them (such as under ``--pipeline``), so a child that was exported but not
    selected still counts as resolved. It defaults to the pipelines in *definitions*.
    A child is matched case-insensitively, as ADF matches pipeline names, and a
    matched child is recorded under its exported spelling; an unmatched one keeps
    the spelling written at the call site.
    """
    graphs = [adf_pipeline_to_source_graph(pipeline, definitions) for pipeline in definitions.pipelines]
    _attach_schedules(graphs, definitions.triggers)
    exported_names = known_workflows if known_workflows is not None else [graph.name for graph in graphs]
    exported_spelling = {name.lower(): name for name in exported_names}
    exported_pipeline_names = set(exported_spelling.values())
    for graph in graphs:
        _use_exported_child_spelling(graph, exported_spelling)
        graph.lineage = build_graph_lineage(graph, known_workflows=exported_pipeline_names)
    return graphs


def _use_exported_child_spelling(graph: SourceGraph, exported_spelling: dict[str, str]) -> None:
    """Rewrite each ``ExecutePipeline`` child name to the exported pipeline it names, ignoring case."""
    for node in walk_nodes(graph.tasks):
        callee = node.properties.get(INVOKES_WORKFLOW_PROPERTY)
        if isinstance(callee, str) and callee.lower() in exported_spelling:
            node.properties[INVOKES_WORKFLOW_PROPERTY] = exported_spelling[callee.lower()]


def _attach_schedules(graphs: list[SourceGraph], triggers: list[AdfTrigger]) -> None:
    """Populate each graph's ``schedule`` from the triggers that reference it.

    A trigger can drive several pipelines and a pipeline can be driven by several
    triggers. The first trigger to reference a pipeline becomes its typed
    :attr:`SourceGraph.schedule`; any further triggers for the same pipeline are
    kept as written under ``schedule.extensions["additional_triggers"]``, except
    for an empty ``annotations`` list (see :data:`_TRIGGER_GET_DEFAULTS`), so
    nothing meaningful is lost. Pipeline references are matched case-insensitively, mirroring
    ADF's case-insensitive identifier semantics.

    A **fresh** :class:`ScheduleSpec` is built for each pipeline assignment rather
    than sharing one instance across every pipeline a trigger references: a shared
    instance would let a later trigger's mutation (appending to
    ``additional_triggers``) leak onto every other pipeline that trigger touched.
    """
    graphs_by_name = {graph.name: graph for graph in graphs}
    graphs_by_lower = {graph.name.lower(): graph for graph in graphs}

    for trigger in triggers:
        for pipeline_name in _trigger_pipeline_names(trigger):
            graph = graphs_by_name.get(pipeline_name) or graphs_by_lower.get(pipeline_name.lower())
            if graph is None:
                continue
            if graph.schedule is None:
                # Per-pipeline instance: no shared mutable state across pipelines.
                graph.schedule = _trigger_to_schedule(trigger)
            else:
                additional = graph.schedule.extensions.setdefault("additional_triggers", [])
                additional.append(
                    {
                        "trigger_name": trigger.name,
                        "trigger_type": trigger.type,
                        "properties": _without_defaults(trigger.properties, _TRIGGER_GET_DEFAULTS),
                    }
                )


def _trigger_to_schedule(trigger: AdfTrigger) -> ScheduleSpec:
    """Map a single ADF trigger to a source-faithful :class:`ScheduleSpec`.

    The recurrence payload is kept as-given: schedule triggers nest it under
    ``typeProperties.recurrence`` while tumbling-window / event triggers put their
    detail directly in ``typeProperties``, so whichever is present becomes the
    verbatim ``expression``. The trigger ``properties`` block also rides in
    ``extensions``, kept as written except for an empty ``annotations`` list (see
    :data:`_TRIGGER_GET_DEFAULTS`), so trigger detail with no typed home yet
    (pipeline parameters, runtime state, non-empty annotations) is not lost.
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
            "properties": _without_defaults(trigger.properties, _TRIGGER_GET_DEFAULTS),
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


# Fields an ADF REST GET adds that carry no meaning, the same set main's ADF corpus
# allows from a live export: the pipeline's publish time, and these keys only when
# they hold the empty value shown. Everything else is kept as written.
_READ_ONLY_PIPELINE_FIELDS = frozenset({"lastPublishTime"})
_PIPELINE_GET_DEFAULTS: dict[str, Any] = {"annotations": [], "parameters": {}, "variables": {}}
_ACTIVITY_GET_DEFAULTS: dict[str, Any] = {"dependsOn": [], "userProperties": []}
_TRIGGER_GET_DEFAULTS: dict[str, Any] = {"annotations": []}
_NESTED_ACTIVITY_LISTS = ("ifTrueActivities", "ifFalseActivities", "activities", "defaultActivities")
_ABSENT = object()


def _without_defaults(value: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    """*value* without each key in *defaults* whose value equals the default given there."""
    return {key: item for key, item in value.items() if defaults.get(key, _ABSENT) != item}


def _activity_without_get_defaults(activity: Any) -> Any:
    """An activity's raw dict without empty ``dependsOn`` / ``userProperties``, nested activities included."""
    if not isinstance(activity, dict):
        return activity
    cleaned = _without_defaults(activity, _ACTIVITY_GET_DEFAULTS)
    type_properties = cleaned.get("typeProperties")
    if isinstance(type_properties, dict):
        type_properties = dict(type_properties)
        for key in _NESTED_ACTIVITY_LISTS:
            if isinstance(type_properties.get(key), list):
                type_properties[key] = [_activity_without_get_defaults(child) for child in type_properties[key]]
        if isinstance(type_properties.get("cases"), list):
            type_properties["cases"] = [
                {**case, "activities": [_activity_without_get_defaults(child) for child in case["activities"]]}
                if isinstance(case, dict) and isinstance(case.get("activities"), list)
                else case
                for case in type_properties["cases"]
            ]
        cleaned["typeProperties"] = type_properties
    return cleaned


def _pipeline_without_get_defaults(raw: Any) -> Any:
    """A pipeline's raw dict without its publish time and its empty GET defaults, activities cleaned too."""
    if not isinstance(raw, dict) or not isinstance(raw.get("properties"), dict):
        return raw
    properties = {
        key: item
        for key, item in _without_defaults(raw["properties"], _PIPELINE_GET_DEFAULTS).items()
        if key not in _READ_ONLY_PIPELINE_FIELDS
    }
    if isinstance(properties.get("activities"), list):
        properties["activities"] = [_activity_without_get_defaults(child) for child in properties["activities"]]
    return {**raw, "properties": properties}


def adf_pipeline_to_source_graph(pipeline: AdfPipeline, definitions: AdfDefinitions | None = None) -> SourceGraph:
    """Map a single ADF pipeline to a source-faithful :class:`SourceGraph`.

    ``definitions`` supplies the datasets and linked services the activity mapper
    needs to resolve each node's data assets to a physical identity. It is optional
    so a caller mapping a pipeline in isolation still works; without it, dataset
    references simply stay unresolved (identity ``None``) and fall back to their
    neutral signature. It does **not** attach the graph-level lineage block --
    :func:`adf_definitions_to_source_graphs` owns that, once every graph is built.
    """
    resolved_definitions = definitions if definitions is not None else AdfDefinitions(pipelines=[])
    properties: dict[str, str] = {}
    if pipeline.folder:
        properties["folder"] = pipeline.folder

    graph = SourceGraph(
        name=pipeline.name,
        source=SOURCE_ADF,
        parameters=_parameters_to_specs(pipeline.parameters),
        variables=_variables_to_specs(pipeline.variables),
        tags=list(pipeline.annotations) if pipeline.annotations else [],
        tasks=[_activity_to_node(activity, resolved_definitions) for activity in pipeline.activities],
        properties=properties,
        raw=_pipeline_without_get_defaults(pipeline.raw),
    )
    _warn_duplicate_switch_cases(graph)
    return graph


def _warn_duplicate_switch_cases(graph: SourceGraph) -> None:
    """Log each Switch case value that is declared more than once, naming the branches that carry it."""
    for node in walk_nodes(graph.tasks):
        for duplicate in node.properties.get(DUPLICATE_SWITCH_CASES_PROPERTY) or []:
            logger.warning(
                "Switch %r in pipeline %r declares case %r more than once; every case is kept, as branches %s",
                node.name,
                graph.name,
                duplicate["value"],
                duplicate["branches"],
            )


def _activity_to_node(activity: AdfActivity, definitions: AdfDefinitions) -> SourceNode:
    """Map one ADF activity to a discovery node (1:1, source-faithful).

    Fields are passed explicitly to each node class (rather than unpacking a
    shared dict) so the mapping stays type-checked end to end. Data reads / writes
    are resolved from the activity's dataset references, and an ``ExecutePipeline``
    records the child pipeline it invokes under the neutral control-edge marker;
    both apply to nested nodes too because the branch children below route back
    through this function.
    """
    strategy = classify_activity(activity.type)
    concept = _CONCEPT_BY_TYPE.get(activity.type, CONCEPT_GAP)
    dependencies = [
        SourceDependency(upstream=dependency.activity, conditions=list(dependency.dependency_conditions))
        for dependency in (activity.depends_on or [])
    ]
    policy = _policy_to_spec(activity)
    properties: dict[str, Any] = {STRATEGY_PROPERTY: strategy.value}
    _record_invocation(activity, properties)
    data_reads, data_writes = activity_data_assets(activity, definitions)

    branches = _control_flow_branches(activity, definitions)
    if branches is not None:
        _record_duplicate_switch_cases(activity, properties)
        return ContainerNode(
            source_id=activity.name,
            task_key=activity.name,
            concept=concept,
            source=SOURCE_ADF,
            name=activity.name,
            native_type=activity.type,
            dependencies=dependencies,
            policy=policy,
            data_reads=data_reads,
            data_writes=data_writes,
            properties=properties,
            raw=_activity_without_get_defaults(activity.raw),
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
            data_reads=data_reads,
            data_writes=data_writes,
            properties=properties,
            raw=_activity_without_get_defaults(activity.raw),
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
        data_reads=data_reads,
        data_writes=data_writes,
        properties=properties,
        raw=_activity_without_get_defaults(activity.raw),
    )


def _record_invocation(activity: AdfActivity, properties: dict[str, Any]) -> None:
    """Stash the child pipeline an ``ExecutePipeline`` invokes under neutral keys.

    The callee reference name and ``waitOnCompletion`` flag are read from the same
    ADF ``typeProperties`` the convert-time translator reads, then recorded under
    the source-neutral :data:`INVOKES_WORKFLOW_PROPERTY` / :data:`INVOKES_WAIT_PROPERTY`
    keys so the source-agnostic control-edge derivation can find them without
    knowing anything about ADF. An empty / missing callee, or a reference that is
    neither a reference object nor a name (``null``, say), still records the marker
    with an empty target, so the edge is reported as unresolved rather than dropped.
    Only a real boolean wait flag is recorded; ``null``, a string or a number leaves
    the wait unset (unknown), as the shared control-edge reader expects.
    """
    if activity.type != "ExecutePipeline":
        return
    type_properties = activity.type_properties or {}
    reference = type_properties.get("pipeline")
    if isinstance(reference, dict):
        reference = reference.get("referenceName")
    callee = reference if isinstance(reference, str) else ""
    properties[INVOKES_WORKFLOW_PROPERTY] = callee
    wait = type_properties.get("waitOnCompletion", True)
    if isinstance(wait, bool):
        properties[INVOKES_WAIT_PROPERTY] = wait


def _record_duplicate_switch_cases(activity: AdfActivity, properties: dict[str, Any]) -> None:
    """Record, and warn about, Switch case values that more than one case declares.

    Nothing is dropped: the parsed AST, the graph and conversion all keep every
    case. A repeated value is still worth flagging, so this lists the branch labels
    that carry it (:func:`_warn_duplicate_switch_cases` logs it once the pipeline
    name is known).
    """
    if activity.type != "Switch":
        return
    labels_by_value: dict[str, list[str]] = {}
    for value, label, _ in _switch_case_labels(activity):
        labels_by_value.setdefault(value, []).append(label)
    duplicates = [{"value": value, "branches": labels} for value, labels in labels_by_value.items() if len(labels) > 1]
    if not duplicates:
        return
    properties[DUPLICATE_SWITCH_CASES_PROPERTY] = duplicates


def _switch_case_labels(activity: AdfActivity) -> list[tuple[str, str, dict[str, Any]]]:
    """Pair each declared Switch case with its value and a branch label no other branch uses.

    A case keeps its own value as its label unless that label is taken (by the
    default branch or an earlier case with the same value); then it gets ``case:``
    prefixes until the label is neither taken nor any case's declared value.
    """
    declared_cases = (activity.type_properties or {}).get("cases") or []
    declared_values = {str(case.get("value", "")) for case in declared_cases}
    labels_in_use = {"default"}
    labelled_cases: list[tuple[str, str, dict[str, Any]]] = []
    for case in declared_cases:
        value = str(case.get("value", ""))
        label = value
        if label in labels_in_use:
            label = f"case:{label}"
            while label in labels_in_use or label in declared_values:
                label = f"case:{label}"
        labels_in_use.add(label)
        labelled_cases.append((value, label, case))
    return labelled_cases


def _control_flow_branches(activity: AdfActivity, definitions: AdfDefinitions) -> dict[str, list[SourceNode]] | None:
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
            "true": [_activity_to_node(child, definitions) for child in (activity.if_true_activities or [])],
            "false": [_activity_to_node(child, definitions) for child in (activity.if_false_activities or [])],
        }
    if activity.type in ("ForEach", "Until"):
        return {"body": [_activity_to_node(child, definitions) for child in (activity.activities or [])]}
    if activity.type == "Switch":
        branches: dict[str, list[SourceNode]] = {}
        # Built from the raw cases: the parsed AST merges cases that share a value and skips empty
        # ones, while each declared case stays its own branch here.
        for _, label, case in _switch_case_labels(activity):
            branches[label] = [
                _activity_to_node(parse_activity(child), definitions) for child in case.get("activities") or []
            ]
        branches["default"] = [
            _activity_to_node(child, definitions) for child in (activity.switch_default_activities or [])
        ]
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
