"""Unit tests for the IR side of the source-neutral lineage substrate (#61).

Covers the ``ir_serde`` round-trip for the lineage IR types plus the ``Activity``
base fields, including a MotifActivity round-trip that proves the R1 ``motif_id``
collision is handled. The edge rules themselves are tested on the shared cores in
``test_discovery_lineage.py``.
"""

from __future__ import annotations

import json

import pytest

from flowx.bundler.dab_writer import pipeline_dict_to_ir
from flowx.ir_serde import lineage_from_dict, pipeline_to_dict
from flowx.models.ir import (
    ControlEdge,
    DataAsset,
    DataEdge,
    Lineage,
    MotifActivity,
    MotifAnnotation,
    NotebookActivity,
    Pipeline,
    WaitActivity,
)


def _notebook(task_key: str, *, reads=None, writes=None, motif_id=None) -> NotebookActivity:
    return NotebookActivity(
        name=task_key,
        task_key=task_key,
        notebook_path=f"/Shared/{task_key}",
        data_reads=list(reads or []),
        data_writes=list(writes or []),
        motif_id=motif_id,
    )


# --------------------------------------------------------------------------- #
# ir_serde round-trips
# --------------------------------------------------------------------------- #


def test_serde_round_trip_new_activity_fields_and_lineage_block():
    """data_reads/data_writes/motif_id and the lineage block survive JSON round-trip."""
    pipeline = Pipeline(
        name="p",
        tasks=[
            _notebook(
                "writer",
                writes=[DataAsset(signature="ds_out", identity="curated.orders", asset_type="table")],
                motif_id="auto_loader",
            ),
            _notebook(
                "reader",
                reads=[
                    DataAsset(
                        signature="ds_in",
                        identity="curated.orders",
                        asset_type="table",
                        properties={"format": "delta"},
                    )
                ],
            ),
            WaitActivity(name="pause", task_key="pause", wait_time_seconds=5),
        ],
        lineage=Lineage(
            control_edges=[
                ControlEdge(
                    source_workflow="p",
                    target_workflow="child",
                    via_task_key="writer",
                    wait_for_completion=True,
                    resolved=True,
                )
            ],
            data_edges=[
                DataEdge(
                    source_task_key="writer",
                    target_task_key="reader",
                    match_kind="identity",
                    match_key="curated.orders",
                    identity="curated.orders",
                    asset_type="table",
                )
            ],
            motifs=[
                MotifAnnotation(
                    motif_id="auto_loader",
                    member_task_keys=["writer"],
                    display_name="Auto Loader",
                    databricks_replacement="auto_loader",
                    notes=["note"],
                    source_type_hint="AzureBlobFSReadSettings",
                )
            ],
        ),
    )

    reloaded, _ = pipeline_dict_to_ir(json.loads(json.dumps(pipeline_to_dict(pipeline))))

    writer = reloaded.tasks[0]
    reader = reloaded.tasks[1]
    assert writer.motif_id == "auto_loader"
    assert writer.data_writes == [DataAsset(signature="ds_out", identity="curated.orders", asset_type="table")]
    assert reader.data_reads == [
        DataAsset(signature="ds_in", identity="curated.orders", asset_type="table", properties={"format": "delta"})
    ]
    # A task without lineage fields rehydrates to empty lists / None, never missing.
    assert reloaded.tasks[2].data_reads == []
    assert reloaded.tasks[2].data_writes == []
    assert reloaded.tasks[2].motif_id is None

    assert reloaded.lineage == pipeline.lineage


def test_serde_round_trip_motif_activity_no_kwarg_collision_r1():
    """A MotifActivity round-trips without the R1 'multiple values for motif_id' TypeError."""
    pipeline = Pipeline(
        name="p",
        tasks=[
            MotifActivity(
                name="motif",
                task_key="motif_auto_loader",
                motif_id="auto_loader",
                display_name="Auto Loader",
                databricks_replacement="auto_loader",
                matched_activity_names=["Copy A", "Copy B"],
                data_reads=[DataAsset(signature="src", identity="raw.src")],
            )
        ],
    )

    reloaded, _ = pipeline_dict_to_ir(json.loads(json.dumps(pipeline_to_dict(pipeline))))

    task = reloaded.tasks[0]
    assert isinstance(task, MotifActivity)
    assert task.motif_id == "auto_loader"
    assert task.display_name == "Auto Loader"
    assert task.matched_activity_names == ["Copy A", "Copy B"]
    assert task.data_reads == [DataAsset(signature="src", identity="raw.src")]


def test_a_motif_activity_saved_without_a_motif_id_reads_back_as_unknown():
    """The ``motif_id`` of a MotifActivity is a string, so a missing one reads back as ``"unknown"``, not ``None``."""
    pipeline = Pipeline(
        name="p",
        tasks=[
            MotifActivity(
                name="motif",
                task_key="motif_auto_loader",
                motif_id="auto_loader",
                display_name="Auto Loader",
                databricks_replacement="auto_loader",
                matched_activity_names=["Copy A"],
            )
        ],
    )
    saved = json.loads(json.dumps(pipeline_to_dict(pipeline)))
    del saved["tasks"][0]["motif_id"]

    reloaded, _ = pipeline_dict_to_ir(saved)

    assert reloaded.tasks[0].motif_id == "unknown"


def test_an_empty_motif_id_survives_a_round_trip_and_an_untagged_task_writes_none():
    """Only a missing ``motif_id`` is left out of the saved task; an empty one is kept as written."""
    pipeline = Pipeline(
        name="p",
        tasks=[
            MotifActivity(
                name="motif",
                task_key="motif",
                motif_id="",
                display_name="motif",
                databricks_replacement="notebook",
                matched_activity_names=[],
            ),
            _notebook("n"),
        ],
    )
    saved = json.loads(json.dumps(pipeline_to_dict(pipeline)))

    assert saved["tasks"][0]["motif_id"] == ""
    assert "motif_id" not in saved["tasks"][1]
    reloaded, _ = pipeline_dict_to_ir(saved)
    assert reloaded.tasks[0].motif_id == ""
    assert reloaded.tasks[1].motif_id is None


def test_lineage_block_always_emits_lists_never_null():
    """An attached empty lineage serialises its edge collections as lists, not null."""
    pipeline = Pipeline(name="p", tasks=[_notebook("n")], lineage=Lineage())

    serialised = pipeline_to_dict(pipeline)

    assert serialised["lineage"] == {"control_edges": [], "data_edges": [], "motifs": []}


@pytest.mark.parametrize("written_resolved", [None, "true", "false", 1])
def test_a_control_edge_read_back_without_a_true_resolved_flag_is_unresolved(written_resolved: object) -> None:
    """Only a saved ``resolved: true`` reads back as resolved; a missing or non-boolean flag means unknown."""
    edge: dict = {"source_workflow": "parent", "target_workflow": "child", "via_task_key": "Run"}
    if written_resolved is not None:
        edge["resolved"] = written_resolved

    lineage = lineage_from_dict({"control_edges": [edge]})

    assert [control_edge.resolved for control_edge in lineage.control_edges] == [False]


def test_a_control_edge_saved_as_resolved_still_reads_back_resolved() -> None:
    """A saved ``resolved: true`` round-trips unchanged."""
    lineage = lineage_from_dict(
        {
            "control_edges": [
                {"source_workflow": "parent", "target_workflow": "child", "via_task_key": "Run", "resolved": True}
            ]
        }
    )

    assert [control_edge.resolved for control_edge in lineage.control_edges] == [True]


@pytest.mark.parametrize(("written_wait", "read_wait"), [("true", None), (0, None), (True, True), (False, False)])
def test_a_control_edge_wait_flag_reads_back_only_when_it_is_a_boolean(
    written_wait: object, read_wait: bool | None
) -> None:
    """A saved wait flag that is not a boolean comes back as unknown, never verbatim."""
    lineage = lineage_from_dict(
        {
            "control_edges": [
                {
                    "source_workflow": "parent",
                    "target_workflow": "child",
                    "via_task_key": "Run",
                    "wait_for_completion": written_wait,
                }
            ]
        }
    )

    assert [control_edge.wait_for_completion for control_edge in lineage.control_edges] == [read_wait]


def test_the_pipeline_debug_dict_includes_its_lineage_block() -> None:
    """The debug dict promises every field, so it carries the pipeline's lineage too."""
    from flowx.ir_serde import pipeline_to_debug_dict
    from flowx.models.ir import ControlEdge, Lineage, Pipeline

    edge = ControlEdge(source_workflow="parent", target_workflow="child", via_task_key="Run", resolved=True)
    debug = pipeline_to_debug_dict(Pipeline(name="parent", lineage=Lineage(control_edges=[edge])))

    assert debug["lineage"]["control_edges"][0]["target_workflow"] == "child"
    assert pipeline_to_debug_dict(Pipeline(name="parent"))["lineage"] is None
