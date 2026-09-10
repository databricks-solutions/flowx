"""Unit tests for the source-neutral lineage substrate (#61).

Covers the pure derivation (:mod:`flowx.lineage`) -- control fan-out, nested and
Switch recursion, the identity-vs-signature join tiers, no self-edges, no
duplicate edges -- and the ``ir_serde`` round-trip for the new IR types plus the
new ``Activity`` base fields, including a MotifActivity round-trip that proves the
R1 ``motif_id`` collision is handled.
"""

from __future__ import annotations

import json

from flowx.bundler.dab_writer import pipeline_dict_to_ir
from flowx.ir_serde import pipeline_to_dict
from flowx.lineage import (
    build_control_edges,
    build_data_edges,
    build_lineage,
    build_motif_annotations,
    with_lineage,
)
from flowx.models.ir import (
    ControlEdge,
    DataAsset,
    DataEdge,
    ExecutePipelineActivity,
    ForEachActivity,
    IfConditionActivity,
    Lineage,
    MotifActivity,
    MotifAnnotation,
    NotebookActivity,
    Pipeline,
    RunJobActivity,
    SwitchActivity,
    SwitchCase,
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


def _execute(task_key: str, callee: str, *, wait: bool = True) -> ExecutePipelineActivity:
    return ExecutePipelineActivity(name=task_key, task_key=task_key, pipeline_name=callee, wait_on_completion=wait)


# --------------------------------------------------------------------------- #
# Control-edge derivation
# --------------------------------------------------------------------------- #


def test_control_edges_fan_out_and_nested_switch_recursion():
    """ExecutePipeline calls are found at top level and inside ForEach/If/Switch."""
    pipeline = Pipeline(
        name="parent",
        tasks=[
            _execute("call_a", "child_a"),
            ForEachActivity(
                name="fe",
                task_key="fe",
                items_expression="@x",
                inner_activities=[_execute("call_b", "child_b")],
            ),
            IfConditionActivity(
                name="cond",
                task_key="cond",
                op="equals",
                left="@a",
                right="@b",
                if_true_activities=[_execute("call_c", "child_c")],
                if_false_activities=[_execute("call_d", "child_d")],
            ),
            SwitchActivity(
                name="sw",
                task_key="sw",
                on_expression="@e",
                cases=[SwitchCase(value="one", activities=[_execute("call_e", "child_e")])],
                default_activities=[_execute("call_f", "child_f")],
            ),
        ],
    )

    edges = build_control_edges(pipeline)

    targets = sorted(edge.target_workflow for edge in edges)
    assert targets == ["child_a", "child_b", "child_c", "child_d", "child_e", "child_f"]
    assert all(edge.source_workflow == "parent" for edge in edges)
    # Each call site keeps its own via_task_key (fan-out preserved).
    assert {edge.via_task_key for edge in edges} == {
        "call_a",
        "call_b",
        "call_c",
        "call_d",
        "call_e",
        "call_f",
    }


def test_control_edges_run_job_activity_is_source_neutral():
    """A RunJobActivity (Airflow) produces a control edge just like ExecutePipeline."""
    pipeline = Pipeline(
        name="dag_main",
        tasks=[RunJobActivity(name="run", task_key="run", job_name="downstream_job")],
    )

    edges = build_control_edges(pipeline)

    assert len(edges) == 1
    assert edges[0].source_workflow == "dag_main"
    assert edges[0].target_workflow == "downstream_job"
    assert edges[0].via_task_key == "run"
    assert edges[0].wait_for_completion is None
    assert edges[0].resolved is True


def test_control_edges_unresolved_callee_is_recorded_not_dropped():
    """An empty callee is kept with resolved=False rather than silently dropped."""
    pipeline = Pipeline(name="parent", tasks=[_execute("call", "")])

    edges = build_control_edges(pipeline)

    assert len(edges) == 1
    assert edges[0].target_workflow == ""
    assert edges[0].resolved is False


def test_control_edges_no_self_edge():
    """A pipeline invoking itself produces no edge."""
    pipeline = Pipeline(name="loop", tasks=[_execute("call", "loop")])

    assert build_control_edges(pipeline) == []


def test_control_edges_no_duplicate_from_recursion():
    """A single call site nested in a container is emitted exactly once."""
    pipeline = Pipeline(
        name="parent",
        tasks=[
            ForEachActivity(
                name="fe",
                task_key="fe",
                items_expression="@x",
                inner_activities=[_execute("call", "child")],
            )
        ],
    )

    edges = build_control_edges(pipeline)

    assert len(edges) == 1
    assert edges[0].target_workflow == "child"


# --------------------------------------------------------------------------- #
# Data-edge derivation: identity vs signature tiers
# --------------------------------------------------------------------------- #


def test_data_edges_identity_tier_joins_across_different_signatures():
    """Two assets with the same resolved identity match even when their names differ."""
    pipeline = Pipeline(
        name="p",
        tasks=[
            _notebook("writer", writes=[DataAsset(signature="ds_out", identity="curated.orders")]),
            _notebook("reader", reads=[DataAsset(signature="ds_in_other_name", identity="curated.orders")]),
        ],
    )

    edges = build_data_edges(pipeline)

    assert len(edges) == 1
    assert edges[0].source_task_key == "writer"
    assert edges[0].target_task_key == "reader"
    assert edges[0].match_kind == "identity"
    assert edges[0].match_key == "curated.orders"
    assert edges[0].identity == "curated.orders"


def test_data_edges_signature_tier_when_identity_unresolved():
    """When identity is unresolvable, matching falls back to the neutral signature."""
    pipeline = Pipeline(
        name="p",
        tasks=[
            _notebook("writer", writes=[DataAsset(signature="shared_ds")]),
            _notebook("reader", reads=[DataAsset(signature="shared_ds")]),
        ],
    )

    edges = build_data_edges(pipeline)

    assert len(edges) == 1
    assert edges[0].match_kind == "signature"
    assert edges[0].match_key == "shared_ds"
    assert edges[0].identity is None


def test_data_edges_distinct_identities_do_not_fall_back_to_signature():
    """Two resolved-but-different identities never manufacture a signature edge (#36)."""
    pipeline = Pipeline(
        name="p",
        tasks=[
            _notebook("writer", writes=[DataAsset(signature="shared", identity="a.first")]),
            _notebook("reader", reads=[DataAsset(signature="shared", identity="b.second")]),
        ],
    )

    assert build_data_edges(pipeline) == []


def test_data_edges_fan_out_one_writer_many_readers():
    """One producer handing off to several consumers yields one edge each."""
    pipeline = Pipeline(
        name="p",
        tasks=[
            _notebook("writer", writes=[DataAsset(signature="ds", identity="x.y")]),
            _notebook("reader_one", reads=[DataAsset(signature="ds", identity="x.y")]),
            _notebook("reader_two", reads=[DataAsset(signature="ds", identity="x.y")]),
        ],
    )

    edges = build_data_edges(pipeline)

    assert sorted(edge.target_task_key for edge in edges) == ["reader_one", "reader_two"]
    assert all(edge.source_task_key == "writer" for edge in edges)


def test_data_edges_no_self_edge():
    """An activity that both writes and reads the same asset does not edge to itself."""
    pipeline = Pipeline(
        name="p",
        tasks=[
            _notebook(
                "roundtrip",
                writes=[DataAsset(signature="ds", identity="x.y")],
                reads=[DataAsset(signature="ds", identity="x.y")],
            )
        ],
    )

    assert build_data_edges(pipeline) == []


def test_data_edges_no_duplicate_from_repeated_asset():
    """A producer listing the same asset twice still yields a single edge."""
    pipeline = Pipeline(
        name="p",
        tasks=[
            _notebook(
                "writer",
                writes=[DataAsset(signature="ds", identity="x.y"), DataAsset(signature="ds", identity="x.y")],
            ),
            _notebook("reader", reads=[DataAsset(signature="ds", identity="x.y")]),
        ],
    )

    edges = build_data_edges(pipeline)

    assert len(edges) == 1


def test_data_edges_nested_switch_recursion():
    """A producer buried in a Switch case hands off to a top-level consumer."""
    pipeline = Pipeline(
        name="p",
        tasks=[
            SwitchActivity(
                name="sw",
                task_key="sw",
                on_expression="@e",
                cases=[
                    SwitchCase(
                        value="one",
                        activities=[_notebook("writer", writes=[DataAsset(signature="ds", identity="x.y")])],
                    )
                ],
                default_activities=[],
            ),
            _notebook("reader", reads=[DataAsset(signature="ds", identity="x.y")]),
        ],
    )

    edges = build_data_edges(pipeline)

    assert len(edges) == 1
    assert edges[0].source_task_key == "writer"
    assert edges[0].target_task_key == "reader"


# --------------------------------------------------------------------------- #
# Motif annotations + composition + purity
# --------------------------------------------------------------------------- #


def test_build_motif_annotations_groups_members_by_tag():
    """A MotifActivity plus tagged members become one annotation over their task keys."""
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
                confidence_notes=["matched on file source"],
            ),
            _notebook("member", motif_id="auto_loader"),
            _notebook("unrelated"),
        ],
    )

    annotations = build_motif_annotations(pipeline)

    assert len(annotations) == 1
    assert annotations[0].motif_id == "auto_loader"
    assert annotations[0].member_task_keys == ["motif_auto_loader", "member"]
    assert annotations[0].display_name == "Auto Loader"
    assert annotations[0].databricks_replacement == "auto_loader"


def test_with_lineage_is_pure():
    """with_lineage returns a new pipeline and never mutates the input."""
    pipeline = Pipeline(name="p", tasks=[_notebook("n")])
    lineage = build_lineage(pipeline)

    updated = with_lineage(pipeline, lineage)

    assert pipeline.lineage is None
    assert updated is not pipeline
    assert updated.lineage is lineage


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


def test_lineage_block_always_emits_lists_never_null():
    """An attached empty lineage serialises its edge collections as lists, not null."""
    pipeline = with_lineage(Pipeline(name="p", tasks=[_notebook("n")]), Lineage())

    serialised = pipeline_to_dict(pipeline)

    assert serialised["lineage"] == {"control_edges": [], "data_edges": [], "motifs": []}
