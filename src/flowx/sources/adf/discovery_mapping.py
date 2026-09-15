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
from flowx.models.adf_ast import AdfActivity, AdfDefinitions, AdfParameter, AdfPipeline, AdfVariable
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
    SourceDependency,
    SourceGraph,
    SourceNode,
)
from flowx.sources.adf.loader import classify_activity

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
    loader produced.
    """
    return [adf_pipeline_to_source_graph(pipeline) for pipeline in definitions.pipelines]


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

    branches = _activity_branches(activity)
    if branches:
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


def _activity_branches(activity: AdfActivity) -> dict[str, list[SourceNode]]:
    """Build the labelled child branches of a control-flow activity.

    Empty for a leaf activity. Branch order matches ADF's own declaration order so
    a downstream flatten walks children in source order.
    """
    branches: dict[str, list[SourceNode]] = {}
    if activity.if_true_activities:
        branches["true"] = [_activity_to_node(child) for child in activity.if_true_activities]
    if activity.if_false_activities:
        branches["false"] = [_activity_to_node(child) for child in activity.if_false_activities]
    if activity.activities:
        branches["body"] = [_activity_to_node(child) for child in activity.activities]
    if activity.switch_cases:
        for case_value, case_activities in activity.switch_cases.items():
            branches[case_value] = [_activity_to_node(child) for child in case_activities]
    if activity.switch_default_activities:
        branches["default"] = [_activity_to_node(child) for child in activity.switch_default_activities]
    return branches


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
