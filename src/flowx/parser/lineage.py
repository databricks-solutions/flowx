"""Deterministic lineage extraction from parsed ADF definitions.

Builds two graphs with no LLM involvement:
  * control lineage -- ``ExecutePipeline`` caller -> callee call edges;
  * data lineage    -- dataset producer -> consumer edges joined on resolved physical identity.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Literal

from flowx.models.adf_ast import (
    AdfActivity,
    AdfDatasetReference,
    AdfDefinitions,
    ControlEdge,
    DataEdge,
    Lineage,
)
from flowx.parser.dataset_resolvers import resolve_dataset_identity

# Recognized ADF runtime references inside a path expression. Each is a value only
# knowable at runtime; for a *structural* signature we collapse them all to one slot
# token so that, e.g., a writer's ``pipeline().parameters.entityID`` and a reader's
# ``item().entityID`` (the same value passed down a ForEach) produce the same shape.
_PARAM_REF_RE = re.compile(
    r"pipeline\(\)\.parameters\.\w+"
    r"|item\(\)(?:\.\w+)*"
    r"|variables\('[^']*'\)"
    r"|dataset\(\)\.\w+"
    r"|activity\('[^']*'\)\.[\w.]+"
)
_QUOTED_LITERAL_RE = re.compile(r"'([^']*)'")


def _walk_activities(activities: list[AdfActivity]) -> Iterator[AdfActivity]:
    """Yield every activity, descending into container children (ForEach/If/Until)."""
    for activity in activities:
        yield activity
        for child in (activity.if_true_activities, activity.if_false_activities, activity.activities):
            if child:
                yield from _walk_activities(child)


def read_execute_pipeline_ref(activity: AdfActivity) -> tuple[str, bool]:
    """Return ``(callee_reference_name, wait_on_completion)`` for an ExecutePipeline.

    Reads the same ADF fields the convert-time translator reads, so parser and
    translator agree on the raw values. ``callee_reference_name`` is the raw
    string (empty when absent); expression resolution is the caller's concern.
    """
    props = activity.type_properties or {}
    ref = props.get("pipeline", {})
    if isinstance(ref, dict):
        name = ref.get("referenceName", "") or ""
    else:
        name = str(ref)
    wait = bool(props.get("waitOnCompletion", True))
    return name, wait


def _build_control_edges(definitions: AdfDefinitions) -> list[ControlEdge]:
    edges: list[ControlEdge] = []
    for pipeline in definitions.pipelines:
        for activity in _walk_activities(pipeline.activities):
            if activity.type != "ExecutePipeline":
                continue
            callee_raw, wait = read_execute_pipeline_ref(activity)
            if not callee_raw:
                continue
            resolved = definitions.get_pipeline(callee_raw)
            callee_name = resolved.name if resolved is not None else callee_raw
            edges.append(
                ControlEdge(
                    caller_pipeline=pipeline.name,
                    callee_pipeline=callee_name,
                    activity_name=activity.name,
                    wait_on_completion=wait,
                )
            )
    return edges


@dataclass(slots=True, kw_only=True)
class _DatasetEndpoint:
    pipeline: str
    activity: str
    dataset_name: str
    identity: str | None
    path_signature: str | None


def _normalize_path_expression(expr: Any) -> str | None:
    """Reduce a (possibly parameterized) ADF path expression to a structural signature.

    Keeps the literal path segments and collapses every runtime reference
    (``pipeline().parameters.X``, ``item().X``, ``variables(...)``, ``dataset().X``,
    ``activity(...).output...``) to a single ``<P>`` slot. The result captures the
    path *shape* (literal skeleton + slot count) without guessing the runtime value.
    Returns ``None`` when there is no literal segment to anchor on (a signature of
    only slots is too weak to be a meaningful join key -- never guess).

    Known limitation: literal fragments are concatenated without positional
    information, so two expressions that use the *same* literal segments in a
    *different* order around their slots (e.g. ``@concat('/data/',p.x,'/rpt/')``
    vs ``@concat(p.y,'/data/','/rpt/')``) collapse to the same signature. This is
    rare in practice and the expression tier is deliberately the lower-confidence
    match (see ``_build_data_edges``); the agentic enrichment pass weighs it.
    """
    if isinstance(expr, dict):
        expr = expr.get("value", "")
    if not isinstance(expr, str) or not expr.strip():
        return None
    text = expr.strip()
    if "@" not in text:
        # A bare literal value (no ADF expression) — the whole string is the literal
        # path/filename, with no runtime slots.
        literal = re.sub(r"/+", "/", text).strip("/")
        return f"{literal}|slots=0" if literal else None
    marked = _PARAM_REF_RE.sub("<P>", text)
    literal = re.sub(r"/+", "/", "".join(_QUOTED_LITERAL_RE.findall(marked))).strip("/")
    if not literal:
        return None
    return f"{literal}|slots={marked.count('<P>')}"


def _path_signature(parameters: dict[str, Any] | None) -> str | None:
    """Structural signature of a dataset reference's parameterized folderPath/fileName.

    Requires a resolvable **folderPath** literal anchor. The file name alone is too
    weak a discriminator: many unrelated activities write ``.csv``/``.json`` files to
    opaque parameterized folders, so a signature built only from a file extension
    (``FP[None]/FN[.csv|slots=1]``) would join them all — re-creating the very
    explosion the identity-only join was introduced to avoid. Anchoring on the literal
    folder segment keeps the match specific to a real, named location.

    ``None`` when the folder path has no literal segment to anchor on.
    """
    if not parameters:
        return None
    folder_sig = _normalize_path_expression(parameters.get("folderPath"))
    if folder_sig is None:
        return None
    file_sig = _normalize_path_expression(parameters.get("fileName"))
    return f"FP[{folder_sig}]/FN[{file_sig}]"


def _typeprops_dataset_ref(candidate: object) -> AdfDatasetReference | None:
    """Build an AdfDatasetReference from a typeProperties source/sink/dataset slot.

    Carries the reference's ``parameters`` (Lookup/Delete/GetMetadata put the
    dataset call-site params here) so the path signature can be computed.
    """
    if isinstance(candidate, dict):
        name = candidate.get("referenceName")
        if isinstance(name, str) and name:
            params = candidate.get("parameters")
            return AdfDatasetReference(
                reference_name=name,
                parameters=params if isinstance(params, dict) else None,
            )
    return None


def _activity_dataset_refs(activity: AdfActivity, *, produced: bool) -> Iterator[AdfDatasetReference]:
    """Yield dataset references an activity writes (produced) or reads (not produced).

    A dataset named in both an activity-level slot (``inputs``/``outputs``) and its
    ``typeProperties`` (``source``/``sink``/``dataset``) is yielded once, so a single
    activity does not create duplicate identical edges for the same dataset.
    """
    props = activity.type_properties or {}
    candidates: list[AdfDatasetReference] = []
    if produced:
        candidates.extend(activity.outputs or [])
        sink_ref = _typeprops_dataset_ref(props.get("sink"))
        if sink_ref is not None:
            candidates.append(sink_ref)
    else:
        candidates.extend(activity.inputs or [])
        for key in ("source", "dataset"):
            read_ref = _typeprops_dataset_ref(props.get(key))
            if read_ref is not None:
                candidates.append(read_ref)

    seen: set[str] = set()
    for ref in candidates:
        if ref.reference_name in seen:
            continue
        seen.add(ref.reference_name)
        yield ref


def _build_data_edges(definitions: AdfDefinitions) -> list[DataEdge]:
    # TODO: the producer x consumer join below is O(producers x consumers). Fine for
    # today's factories; if one ever has thousands of same-signature endpoints, bucket
    # producers/consumers by (identity or path_signature) and join within buckets.
    producers: list[_DatasetEndpoint] = []
    consumers: list[_DatasetEndpoint] = []
    for pipeline in definitions.pipelines:
        for activity in _walk_activities(pipeline.activities):
            for produced, bucket in ((True, producers), (False, consumers)):
                for ref in _activity_dataset_refs(activity, produced=produced):
                    bucket.append(
                        _DatasetEndpoint(
                            pipeline=pipeline.name,
                            activity=activity.name,
                            dataset_name=ref.reference_name,
                            identity=resolve_dataset_identity(ref, definitions),
                            path_signature=_path_signature(ref.parameters),
                        )
                    )

    # Two deterministic join tiers, in confidence order. We deliberately do NOT fall back
    # to the ADF dataset NAME: a single parameterized dataset is commonly reused across many
    # activities that each point it at a DIFFERENT physical file, so a name join manufactures
    # false hand-offs.
    #
    #   1. identity   -- both ends resolve to the SAME physical asset (schema.table / path).
    #                    High confidence; a literal, provable hand-off.
    #   2. expression -- both ends build the SAME normalized path signature from parameterized
    #                    expressions (same literal path skeleton + slot shape). The runtime
    #                    value is unknown, but the structural match is a real coupling signal
    #                    (e.g. a watermark file written and read back per loop key). Lower
    #                    confidence; anchored on a literal path segment so it does not
    #                    over-match unrelated datasets that merely share a name.
    #
    # An endpoint that qualifies for identity is matched there and NOT re-matched by
    # expression, so each (producer, consumer) pair yields at most one edge.
    edges: list[DataEdge] = []
    for prod in producers:
        for cons in consumers:
            if prod.pipeline == cons.pipeline and prod.activity == cons.activity:
                continue
            if prod.identity is not None and prod.identity == cons.identity:
                edges.append(_data_edge(prod, cons, match_kind="identity", match_key=prod.identity))
            elif (
                prod.identity is None
                and cons.identity is None
                and prod.path_signature is not None
                and prod.path_signature == cons.path_signature
            ):
                edges.append(_data_edge(prod, cons, match_kind="expression", match_key=prod.path_signature))
    return edges


def _data_edge(
    prod: _DatasetEndpoint,
    cons: _DatasetEndpoint,
    *,
    match_kind: Literal["identity", "expression"],
    match_key: str | None,
) -> DataEdge:
    return DataEdge(
        dataset_name=prod.dataset_name,
        identity=prod.identity,
        producer_pipeline=prod.pipeline,
        producer_activity=prod.activity,
        consumer_pipeline=cons.pipeline,
        consumer_activity=cons.activity,
        match_kind=match_kind,
        match_key=match_key,
    )


def build_lineage(definitions: AdfDefinitions) -> Lineage:
    """Assemble deterministic control and data lineage graphs."""
    return Lineage(
        control_edges=_build_control_edges(definitions),
        data_edges=_build_data_edges(definitions),
    )
