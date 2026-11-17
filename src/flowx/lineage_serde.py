"""JSON serialisation for the lineage substrate (:mod:`flowx.models.lineage`).

Data assets, control and data edges, and motif annotations are written the same way
in the translation report (:mod:`flowx.ir_serde`) and in the discovery graph
(:mod:`flowx.discovery_serde`). Both import from here, so the discovery side does not
depend on the IR's serialiser and the two always read and write one shape.
"""

from __future__ import annotations

from typing import Any

from flowx.models.lineage import ControlEdge, DataAsset, DataEdge, Lineage, MotifAnnotation


def data_asset_to_dict(asset: DataAsset) -> dict[str, Any]:
    """Serialise a :class:`DataAsset` to a JSON-friendly dictionary.

    ``signature`` always appears; the optional ``identity`` / ``asset_type`` /
    ``properties`` are emitted only when set so reports stay compact.
    """
    result: dict[str, Any] = {"signature": asset.signature}
    if asset.identity is not None:
        result["identity"] = asset.identity
    if asset.asset_type is not None:
        result["asset_type"] = asset.asset_type
    if asset.properties:
        result["properties"] = dict(asset.properties)
    return result


def data_asset_from_dict(raw: dict[str, Any]) -> DataAsset:
    """Rehydrate a :class:`DataAsset` from the dict :func:`data_asset_to_dict` emits.

    The canonical inverse of :func:`data_asset_to_dict`, so every consumer of the
    serialised DataAsset shape (the lineage substrate and the discovery graph)
    reads it the same way.
    """
    return DataAsset(
        signature=raw.get("signature", ""),
        identity=raw.get("identity"),
        asset_type=raw.get("asset_type"),
        properties=dict(raw.get("properties") or {}),
    )


def lineage_to_dict(lineage: Lineage) -> dict[str, Any]:
    """Serialise a :class:`Lineage` block to a JSON-friendly dictionary.

    Edge lists are always emitted (empty, never ``None``) for stable diffs.
    """
    return {
        "control_edges": [
            {
                "source_workflow": edge.source_workflow,
                "target_workflow": edge.target_workflow,
                "via_task_key": edge.via_task_key,
                "wait_for_completion": edge.wait_for_completion,
                "resolved": edge.resolved,
            }
            for edge in lineage.control_edges
        ],
        "data_edges": [
            {
                "source_task_key": edge.source_task_key,
                "target_task_key": edge.target_task_key,
                "match_kind": edge.match_kind,
                "match_key": edge.match_key,
                "identity": edge.identity,
                "asset_type": edge.asset_type,
            }
            for edge in lineage.data_edges
        ],
        "motifs": [_motif_annotation_to_dict(motif) for motif in lineage.motifs],
    }


def lineage_from_dict(raw: dict[str, Any]) -> Lineage:
    """Rehydrate a :class:`Lineage` block from the dict :func:`lineage_to_dict` emits.

    The one inverse shared by the translation report and the discovery graph, so a
    lineage block reads back the same way wherever it was written. A control edge is
    resolved only when it was saved with ``resolved: true``; a missing or non-boolean
    flag reads back as unresolved, never as resolved. A ``wait_for_completion`` that
    is not a boolean reads back as unset (``None``).
    """
    return Lineage(
        control_edges=[
            ControlEdge(
                source_workflow=edge.get("source_workflow", ""),
                target_workflow=edge.get("target_workflow", ""),
                via_task_key=edge.get("via_task_key", ""),
                wait_for_completion=wait if isinstance(wait := edge.get("wait_for_completion"), bool) else None,
                resolved=edge.get("resolved") is True,
            )
            for edge in raw.get("control_edges") or []
        ],
        data_edges=[
            DataEdge(
                source_task_key=edge.get("source_task_key", ""),
                target_task_key=edge.get("target_task_key", ""),
                match_kind=edge.get("match_kind", ""),
                match_key=edge.get("match_key", ""),
                identity=edge.get("identity"),
                asset_type=edge.get("asset_type"),
            )
            for edge in raw.get("data_edges") or []
        ],
        motifs=[
            MotifAnnotation(
                motif_id=motif.get("motif_id", ""),
                member_task_keys=list(motif.get("member_task_keys") or []),
                display_name=motif.get("display_name"),
                databricks_replacement=motif.get("databricks_replacement"),
                notes=list(motif.get("notes") or []),
                source_type_hint=motif.get("source_type_hint"),
            )
            for motif in raw.get("motifs") or []
        ],
    )


def _motif_annotation_to_dict(motif: MotifAnnotation) -> dict[str, Any]:
    """Serialise one motif annotation; ``source_type_hint`` is written only when set.

    Leaving the key out when it is ``None`` keeps every existing lineage block
    byte-identical, since only discovery-time detections carry a hint.
    """
    result: dict[str, Any] = {
        "motif_id": motif.motif_id,
        "member_task_keys": list(motif.member_task_keys),
        "display_name": motif.display_name,
        "databricks_replacement": motif.databricks_replacement,
        "notes": list(motif.notes),
    }
    if motif.source_type_hint is not None:
        result["source_type_hint"] = motif.source_type_hint
    return result
