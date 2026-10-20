"""Source-neutral lineage types shared by the discovery graph and the Databricks IR.

Data assets, the control and data edges between them, and motif annotations are
the same in both phases, so they are defined once here. :mod:`flowx.models.ir`
re-exports them under their old names.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True, kw_only=True)
class DataAsset:
    """A general, best-effort description of something a task reads or writes.

    Deliberately source-neutral: the same shape describes an ADF dataset, an
    Airflow dataset/hook target, an XCom / TaskFlow return value, or any other
    front-end's data reference, so the lineage substrate never has to know which
    source produced it.

    Population is **best-effort and open**, not physical-only. Each source fills
    in what it can -- fully, partially, or not at all -- and an empty
    ``data_reads`` / ``data_writes`` is valid and expected in places. The asset
    need not be a physically-resolved table or file: a logical dataset or a pure
    value hand-off (e.g. an Airflow XCom) is an equally valid asset a source MAY
    record.

    Two-tier identity (from #36): ``identity`` is the resolved physical location
    (``schema.table`` or a concrete storage path) and is the strong join key.
    It is ``None`` when it cannot be resolved deterministically -- never a guess.
    ``signature`` is always present and carries the neutral fallback descriptor
    (a dataset name, a normalised reference, a value key, or an expression) so
    two assets can still be compared when neither side resolved to a physical
    identity.

    Attributes:
        signature: Neutral, always-present descriptor used as the weak join key
            (e.g. dataset name, value/XCom key, or normalised expression).
        identity: Resolved physical identity used as the strong join key, or
            ``None`` when it could not be resolved deterministically (or the
            asset is non-physical). Never guessed.
        asset_type: Open, neutral kind of the asset -- physical kinds like
            ``"table"`` / ``"file"`` / ``"volume"`` as well as non-physical /
            logical ones like ``"value"`` (an XCom / TaskFlow return) or
            ``"logical"`` (a named logical dataset). The vocabulary is not
            closed; ``None`` when unknown. The signature tier refuses a match
            when both sides set ``asset_type`` and the two differ, compared
            exactly, so the writer and reader of one asset must spell it the
            same way.
        properties: Free-form extra attributes carried through verbatim.
    """

    signature: str
    identity: str | None = None
    asset_type: str | None = None
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class ControlEdge:
    """A control-flow invocation between two workflows.

    Emitted for cross-workflow calls -- an ADF ``ExecutePipeline`` invoking a
    child pipeline, an Airflow ``RunJobActivity`` triggering another job -- so
    the field names stay neutral (``source_workflow`` / ``target_workflow``)
    rather than baking in either source's vocabulary.

    Attributes:
        source_workflow: Name of the calling workflow (the pipeline/DAG the
            invoking activity lives in).
        target_workflow: Name of the invoked workflow (child pipeline / job).
        via_task_key: Task key of the activity that performs the invocation.
        wait_for_completion: Whether the caller blocks on the callee, when the
            source expresses it; ``None`` when the source has no such notion.
        resolved: ``False`` when the callee could not be resolved from a partial
            export (recorded, not dropped); ``True`` otherwise.
    """

    source_workflow: str
    target_workflow: str
    via_task_key: str
    wait_for_completion: bool | None = None
    resolved: bool = True


@dataclass(slots=True, kw_only=True)
class DataEdge:
    """A proven data hand-off between a producing and a consuming task.

    A producer writes a :class:`DataAsset` that a consumer reads; the pairing
    matches assets, not run order (see
    :func:`~flowx.lineage.data_edges_from_endpoints`). The two tiers from #36 are
    recorded on the edge itself: ``match_kind`` says whether the two assets were
    joined on resolved physical ``identity`` or on their neutral ``signature``,
    and ``match_key`` is the value they matched on.

    Attributes:
        source_task_key: Task key of the producer (writes the asset).
        target_task_key: Task key of the consumer (reads the asset).
        match_kind: ``"identity"`` when joined on resolved physical identity,
            ``"signature"`` when joined on the neutral fallback descriptor.
        match_key: The value the two assets matched on.
        identity: Resolved physical identity of the hand-off, or ``None`` when
            the match was signature-only / the identity was unresolvable.
        asset_type: Neutral kind of the handed-off asset, when known.
    """

    source_task_key: str
    target_task_key: str
    match_kind: str
    match_key: str
    identity: str | None = None
    asset_type: str | None = None


@dataclass(slots=True, kw_only=True)
class MotifAnnotation:
    """Describes a detected motif spanning one or more activities.

    Source-neutral record tying a motif id to the tasks that belong to it, so
    the lineage block can report motifs without depending on how any particular
    source detects them.  ``member_task_keys`` lists the tasks the motif spans.

    Attributes:
        motif_id: Identifier of the matched motif definition.
        member_task_keys: Task keys of the activities the motif spans.
        display_name: Human-readable motif name, if any.
        databricks_replacement: Target Databricks construct the motif maps to.
        notes: Detector notes explaining the match rationale.
        source_type_hint: Optional source-specific hint the detector attached
            (for example the ADF copy source type), or ``None``.
    """

    motif_id: str
    member_task_keys: list[str] = field(default_factory=list)
    display_name: str | None = None
    databricks_replacement: str | None = None
    notes: list[str] = field(default_factory=list)
    source_type_hint: str | None = None


@dataclass(slots=True, kw_only=True)
class Lineage:
    """Source-neutral lineage block attached to a translated pipeline.

    Edge lists are always concrete lists (empty, never ``None``) so serialised
    reports produce stable golden diffs.

    Attributes:
        control_edges: Cross-workflow invocation edges.
        data_edges: Proven producer -> consumer data hand-offs.
        motifs: Detected motif annotations.
    """

    control_edges: list[ControlEdge] = field(default_factory=list)
    data_edges: list[DataEdge] = field(default_factory=list)
    motifs: list[MotifAnnotation] = field(default_factory=list)
