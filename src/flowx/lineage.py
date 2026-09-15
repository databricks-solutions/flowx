"""Source-neutral lineage derivation over the flowx Pipeline IR.

These functions turn an already-translated :class:`~flowx.models.ir.Pipeline`
into its :class:`~flowx.models.ir.Lineage` block. They operate on IR primitives
only -- ``Activity`` subclasses, ``DataAsset``, ``task_key`` -- and import nothing
from ``sources/adf`` or ``sources/airflow`` so both front-ends share one code
path once they populate ``data_reads`` / ``data_writes`` / ``motif_id``.

The tier-matching, self-edge drop, and dedup rules are factored into two
primitive-level cores -- :func:`control_edges_from_calls` and
:func:`data_edges_from_endpoints` -- that take only ``task_key`` strings and
:class:`DataAsset` values, never a ``Pipeline``. The IR entry points
(:func:`build_control_edges` / :func:`build_data_edges`) gather those primitives
from a pipeline and delegate, and the source-neutral discovery-AST derivation in
:mod:`flowx.discovery_lineage` gathers the same primitives from a
:class:`~flowx.models.discovery.SourceGraph` and delegates too, so both phases
join edges through exactly one implementation.

Everything here is pure: the functions read the pipeline and return new edge
lists / a new :class:`Lineage`; nothing is mutated. :func:`with_lineage` attaches
a block by returning a *new* ``Pipeline`` rather than mutating the input, unlike
the in-place dependency rewrite in ``motifs/collapser.py``.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Iterator

from flowx.models.ir import (
    Activity,
    ControlEdge,
    DataAsset,
    DataEdge,
    ExecutePipelineActivity,
    ForEachActivity,
    IfConditionActivity,
    Lineage,
    MotifActivity,
    MotifAnnotation,
    Pipeline,
    RunJobActivity,
    SwitchActivity,
)


def walk_activities(activities: list[Activity]) -> Iterator[Activity]:
    """Yield every activity in *activities*, descending into control-flow containers.

    Recurses into ForEach inner activities, both If-condition branches, and every
    Switch case plus its default branch, so a nested ExecutePipeline or a data
    asset buried inside a Switch case is still reached. Motif ``original_activities``
    are intentionally not traversed: they are the pre-collapse originals kept for
    reference, not live graph members.

    Args:
        activities: Top-level (or already-nested) activity list to walk.

    Yields:
        Each activity, container nodes included, in depth-first order.
    """
    for activity in activities:
        yield activity
        match activity:
            case ForEachActivity():
                yield from walk_activities(activity.inner_activities)
            case IfConditionActivity():
                yield from walk_activities(activity.if_true_activities)
                yield from walk_activities(activity.if_false_activities)
            case SwitchActivity():
                for case_branch in activity.cases:
                    yield from walk_activities(case_branch.activities)
                yield from walk_activities(activity.default_activities)


def control_edges_from_calls(
    source_workflow: str,
    calls: Iterable[tuple[str, bool | None, str]],
) -> list[ControlEdge]:
    """Assemble deduplicated control edges from raw invocation primitives.

    The shared core behind :func:`build_control_edges` (IR) and the discovery-AST
    control-edge derivation: it owns the self-edge drop, the dedup, and the
    unresolved-callee recording so those rules live in exactly one place and both
    phases behave identically.

    Args:
        source_workflow: Name of the calling workflow (pipeline / DAG).
        calls: One ``(target_workflow, wait_for_completion, via_task_key)`` triple
            per call site, in the order they should be considered. ``target_workflow``
            may be empty when the callee could not be resolved from a partial export.

    Returns:
        Deduplicated control edges in first-seen order. A call whose target equals
        ``source_workflow`` is dropped (no self-edge); an empty target is kept with
        ``resolved=False`` rather than dropped.
    """
    edges: list[ControlEdge] = []
    seen: set[tuple[str, str, str]] = set()
    for target, wait, via_task_key in calls:
        target_name = target or ""
        if target_name and target_name == source_workflow:
            continue
        key = (source_workflow, target_name, via_task_key)
        if key in seen:
            continue
        seen.add(key)
        edges.append(
            ControlEdge(
                source_workflow=source_workflow,
                target_workflow=target_name,
                via_task_key=via_task_key,
                wait_for_completion=wait,
                resolved=bool(target_name),
            )
        )
    return edges


def build_control_edges(pipeline: Pipeline) -> list[ControlEdge]:
    """Derive cross-workflow invocation edges for a pipeline.

    Emits one :class:`ControlEdge` per invoking activity -- an
    ``ExecutePipelineActivity`` (ADF) or a ``RunJobActivity`` (Airflow) -- found
    anywhere in the pipeline, including inside ForEach / If / Switch containers
    (fan-out is preserved: each call site is its own edge). Edges whose callee
    equals the caller are dropped (no self-edges), and identical edges are
    collapsed (no duplicates). An unresolved callee is recorded with
    ``resolved=False`` rather than dropped.

    Args:
        pipeline: The translated pipeline IR.

    Returns:
        Deduplicated list of control edges, in first-seen order.
    """

    def _calls() -> Iterator[tuple[str, bool | None, str]]:
        for activity in walk_activities(pipeline.tasks):
            match activity:
                case ExecutePipelineActivity():
                    yield activity.pipeline_name or "", activity.wait_on_completion, activity.task_key
                case RunJobActivity():
                    yield activity.job_name or "", None, activity.task_key

    return control_edges_from_calls(pipeline.name, _calls())


def _match_assets(producer: DataAsset, consumer: DataAsset) -> tuple[str, str, str | None] | None:
    """Decide whether a written asset hands off to a read asset, and how.

    Two tiers, per #36:

    - **identity** -- when both sides resolved to a physical identity, they match
      only if those identities are equal. Two differently-resolved identities do
      *not* fall through to a signature match; that is what manufactured the
      spurious edges #36 removed.
    - **signature** -- when at least one identity is unresolved, fall back to the
      neutral descriptor and match on equal, non-empty signatures.

    Returns:
        ``(match_kind, match_key, identity)`` when the pair matches, else ``None``.
    """
    if producer.identity is not None and consumer.identity is not None:
        if producer.identity == consumer.identity:
            return "identity", producer.identity, producer.identity
        return None
    if producer.signature and producer.signature == consumer.signature:
        return "signature", producer.signature, producer.identity or consumer.identity
    return None


def data_edges_from_endpoints(
    producers: Iterable[tuple[str, DataAsset]],
    consumers: Iterable[tuple[str, DataAsset]],
) -> list[DataEdge]:
    """Join producer endpoints to consumer endpoints via the two-tier match.

    The shared core behind :func:`build_data_edges` (IR) and the discovery-AST
    data-edge derivation: it owns the :func:`_match_assets` tier logic, the
    no-self-edge rule, and the dedup, so both phases join identically.

    Args:
        producers: ``(task_key, written asset)`` pairs, in first-seen order.
        consumers: ``(task_key, read asset)`` pairs, in first-seen order.

    Returns:
        Deduplicated data edges in first-seen order. A producer never hands off to
        a consumer sharing its ``task_key`` (no self-edge), and identical
        ``(producer, consumer, match_kind, match_key)`` edges are collapsed.
    """
    producer_list = list(producers)
    consumer_list = list(consumers)

    edges: list[DataEdge] = []
    seen: set[tuple[str, str, str, str]] = set()
    for producer_key, producer_asset in producer_list:
        for consumer_key, consumer_asset in consumer_list:
            if producer_key == consumer_key:
                continue
            matched = _match_assets(producer_asset, consumer_asset)
            if matched is None:
                continue
            match_kind, match_key, identity = matched
            dedupe_key = (producer_key, consumer_key, match_kind, match_key)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            edges.append(
                DataEdge(
                    source_task_key=producer_key,
                    target_task_key=consumer_key,
                    match_kind=match_kind,
                    match_key=match_key,
                    identity=identity,
                    asset_type=producer_asset.asset_type or consumer_asset.asset_type,
                )
            )
    return edges


def build_data_edges(pipeline: Pipeline) -> list[DataEdge]:
    """Derive proven producer -> consumer data hand-offs for a pipeline.

    A producer is any activity with a ``data_writes`` asset; a consumer any
    activity with a ``data_reads`` asset, gathered across the whole pipeline
    (ForEach / If / Switch bodies included). Each producer asset is joined against
    each consumer asset via :func:`_match_assets`, tagging the edge as an
    ``identity`` or ``signature`` match. An activity never hands off to itself
    (no self-edges), and identical edges are collapsed (no duplicates).

    Args:
        pipeline: The translated pipeline IR.

    Returns:
        Deduplicated list of data edges, in first-seen order.
    """
    activities = list(walk_activities(pipeline.tasks))
    producers = [(activity.task_key, asset) for activity in activities for asset in activity.data_writes]
    consumers = [(activity.task_key, asset) for activity in activities for asset in activity.data_reads]
    return data_edges_from_endpoints(producers, consumers)


def build_motif_annotations(pipeline: Pipeline) -> list[MotifAnnotation]:
    """Derive motif annotations from the collapsed motif activities in a pipeline.

    One annotation per :class:`MotifActivity`, listing the task keys it spans
    (the motif task itself plus any member activities that carry the same
    ``motif_id`` tag). Deduplicated by ``motif_id`` in first-seen order.

    Args:
        pipeline: The translated pipeline IR.

    Returns:
        List of motif annotations.
    """
    annotations: list[MotifAnnotation] = []
    seen: set[str] = set()
    activities = list(walk_activities(pipeline.tasks))
    for activity in activities:
        if not isinstance(activity, MotifActivity):
            continue
        if activity.motif_id in seen:
            continue
        seen.add(activity.motif_id)
        members = [activity.task_key]
        members.extend(
            other.task_key for other in activities if other is not activity and other.motif_id == activity.motif_id
        )
        annotations.append(
            MotifAnnotation(
                motif_id=activity.motif_id,
                member_task_keys=members,
                display_name=activity.display_name,
                databricks_replacement=activity.databricks_replacement,
                notes=list(activity.confidence_notes),
            )
        )
    return annotations


def build_lineage(pipeline: Pipeline) -> Lineage:
    """Compose the full source-neutral lineage block for a pipeline.

    Args:
        pipeline: The translated pipeline IR.

    Returns:
        A :class:`Lineage` with control edges, data edges, and motif annotations.
    """
    return Lineage(
        control_edges=build_control_edges(pipeline),
        data_edges=build_data_edges(pipeline),
        motifs=build_motif_annotations(pipeline),
    )


def with_lineage(pipeline: Pipeline, lineage: Lineage) -> Pipeline:
    """Return a *new* pipeline carrying *lineage*, leaving the input untouched.

    Args:
        pipeline: The pipeline to copy.
        lineage: The lineage block to attach.

    Returns:
        A shallow copy of *pipeline* with ``lineage`` set.
    """
    return dataclasses.replace(pipeline, lineage=lineage)
