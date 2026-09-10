"""Source-neutral lineage derivation over the flowx Pipeline IR.

These functions turn an already-translated :class:`~flowx.models.ir.Pipeline`
into its :class:`~flowx.models.ir.Lineage` block. They operate on IR primitives
only -- ``Activity`` subclasses, ``DataAsset``, ``task_key`` -- and import nothing
from ``sources/adf`` or ``sources/airflow`` so both front-ends share one code
path once they populate ``data_reads`` / ``data_writes`` / ``motif_id``.

Everything here is pure: the functions read the pipeline and return new edge
lists / a new :class:`Lineage`; nothing is mutated. :func:`with_lineage` attaches
a block by returning a *new* ``Pipeline`` rather than mutating the input, unlike
the in-place dependency rewrite in ``motifs/collapser.py``.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator

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
    edges: list[ControlEdge] = []
    seen: set[tuple[str, str, str]] = set()
    for activity in walk_activities(pipeline.tasks):
        target: str | None
        wait: bool | None
        match activity:
            case ExecutePipelineActivity():
                target = activity.pipeline_name
                wait = activity.wait_on_completion
            case RunJobActivity():
                target = activity.job_name
                wait = None
            case _:
                continue
        target_name = target or ""
        if target_name and target_name == pipeline.name:
            continue
        key = (pipeline.name, target_name, activity.task_key)
        if key in seen:
            continue
        seen.add(key)
        edges.append(
            ControlEdge(
                source_workflow=pipeline.name,
                target_workflow=target_name,
                via_task_key=activity.task_key,
                wait_for_completion=wait,
                resolved=bool(target_name),
            )
        )
    return edges


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

    edges: list[DataEdge] = []
    seen: set[tuple[str, str, str, str]] = set()
    for producer_key, producer_asset in producers:
        for consumer_key, consumer_asset in consumers:
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
