"""Source-neutral lineage types shared by the discovery graph and the Databricks IR.

Data assets, the control and data edges between them, and motif annotations are
the same in both phases, so they are defined once here. :mod:`flowx.models.ir`
re-exports them so code that works with the IR can import them from there.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True, kw_only=True)
class DataAsset:
    """Something a task reads or writes, described on a best-effort basis.

    The same shape covers a dataset reference, a table or file path, or a value passed
    between tasks, so the lineage code never needs to know the source. An asset need not
    be a physical table or file, and a task with no assets at all is valid.

    Two assets are joined on ``identity`` when both have one, otherwise on
    ``signature``. When both sides set ``asset_type`` and the two differ, they never
    match, so the writer and reader of one asset must spell the type the same way.

    Attributes:
        signature: Always-present descriptor (dataset name, value key or normalised
            expression), the weaker join key.
        identity: Resolved physical location (``schema.table`` or a storage path),
            the stronger join key. ``None`` when it cannot be resolved; never guessed.
        asset_type: Open kind of asset, such as ``"table"``, ``"file"``, ``"volume"``,
            ``"value"`` or ``"logical"``; ``None`` when unknown.
        properties: Free-form extra attributes, kept as given.
    """

    signature: str
    identity: str | None = None
    asset_type: str | None = None
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class ControlEdge:
    """One workflow invoking another.

    Attributes:
        source_workflow: Name of the calling workflow.
        target_workflow: Name of the invoked workflow.
        via_task_key: Task key of the task that makes the call.
        wait_for_completion: Whether the caller waits for the callee; ``None`` when
            the source has no such notion.
        resolved: ``False`` when the callee is not in the export (kept, not dropped).
    """

    source_workflow: str
    target_workflow: str
    via_task_key: str
    wait_for_completion: bool | None = None
    resolved: bool = True


@dataclass(slots=True, kw_only=True)
class DataEdge:
    """A producing task and a consuming task that handle the same asset.

    The pairing matches assets, not run order (see
    :func:`~flowx.lineage.data_edges_from_endpoints`).

    Attributes:
        source_task_key: Task key of the producer.
        target_task_key: Task key of the consumer.
        match_kind: ``"identity"`` or ``"signature"``, the join key that matched.
        match_key: The value the two assets matched on.
        identity: Physical identity of the asset, or ``None`` for a signature match.
        asset_type: Kind of the asset, when known.
    """

    source_task_key: str
    target_task_key: str
    match_kind: str
    match_key: str
    identity: str | None = None
    asset_type: str | None = None


@dataclass(slots=True, kw_only=True)
class MotifAnnotation:
    """A detected motif and the tasks it spans.

    Attributes:
        motif_id: Identifier of the matched motif definition.
        member_task_keys: Task keys of the tasks the motif spans.
        display_name: Human-readable motif name, if any.
        databricks_replacement: Databricks construct the motif maps to.
        notes: Why the detector matched.
        source_type_hint: Source-specific hint from the detector, or ``None``.
    """

    motif_id: str
    member_task_keys: list[str] = field(default_factory=list)
    display_name: str | None = None
    databricks_replacement: str | None = None
    notes: list[str] = field(default_factory=list)
    source_type_hint: str | None = None


@dataclass(slots=True, kw_only=True)
class Lineage:
    """Lineage attached to a discovery graph or a translated pipeline.

    The lists are always concrete (empty, never ``None``) so serialised output is stable.

    Attributes:
        control_edges: Cross-workflow invocation edges.
        data_edges: Proven producer -> consumer data hand-offs.
        motifs: Detected motif annotations.
    """

    control_edges: list[ControlEdge] = field(default_factory=list)
    data_edges: list[DataEdge] = field(default_factory=list)
    motifs: list[MotifAnnotation] = field(default_factory=list)
