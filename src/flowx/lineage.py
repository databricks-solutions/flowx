"""Source-neutral lineage-edge cores.

Primitive-level building blocks for deriving lineage edges. They take only
``task_key`` strings and :class:`~flowx.models.ir.DataAsset` values -- never a
``Pipeline`` or any ``Activity`` -- so the tier-matching, self-edge drop, and
dedup rules live in exactly one place. The discovery-AST derivation in
:mod:`flowx.discovery_lineage` gathers those primitives from a
:class:`~flowx.models.discovery.SourceGraph` and delegates here; the IR-facing
derivation that gathers them from a :class:`~flowx.models.ir.Pipeline` is added
separately in the convert->package lineage work so it stacks on this standard.

They import nothing from ``sources/adf`` or ``sources/airflow``. Everything here
is pure: the functions read their inputs and return new edge lists; nothing is
mutated.
"""

from __future__ import annotations

from collections.abc import Iterable

from flowx.models.ir import ControlEdge, DataAsset, DataEdge


def control_edges_from_calls(
    source_workflow: str,
    calls: Iterable[tuple[str, bool | None, str]],
) -> list[ControlEdge]:
    """Assemble deduplicated control edges from raw invocation primitives.

    The shared core behind the discovery-AST control-edge derivation (and the
    IR-facing derivation added in the convert->package work): it owns the
    self-edge drop, the dedup, and the unresolved-callee recording so those rules
    live in exactly one place and every phase behaves identically.

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

    The shared core behind the discovery-AST data-edge derivation (and the
    IR-facing derivation added in the convert->package work): it owns the
    :func:`_match_assets` tier logic, the no-self-edge rule, and the dedup, so
    every phase joins identically.

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
