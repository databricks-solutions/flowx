"""Unit tests for deterministic lineage extraction (lineage.py)."""

from __future__ import annotations

from flowx.models.adf_ast import AdfActivity, AdfDataset, AdfDatasetReference, AdfDefinitions, AdfPipeline
from flowx.parser.lineage import build_lineage, read_execute_pipeline_ref
from flowx.sources.adf.loader import load_adf_definitions


def _ep(name: str, callee: str, *, wait: bool = True) -> AdfActivity:
    """An ExecutePipeline activity calling `callee`."""
    return AdfActivity(
        name=name,
        type="ExecutePipeline",
        type_properties={
            "pipeline": {"referenceName": callee, "type": "PipelineReference"},
            "waitOnCompletion": wait,
        },
    )


def _pipeline(name: str, activities: list[AdfActivity]) -> AdfPipeline:
    return AdfPipeline(name=name, activities=activities)


def _foreach(name: str, children: list[AdfActivity]) -> AdfActivity:
    """A ForEach container wrapping `children` (uses the `.activities` slot the walker recurses)."""
    return AdfActivity(
        name=name,
        type="ForEach",
        type_properties={"items": {"value": "@pipeline().parameters.items", "type": "Expression"}},
        activities=children,
    )


class TestControlEdges:
    def test_flat_execute_pipeline_edges_from_shared_fixture(self, fixtures_dir):
        # Reuses the EXISTING shared fixture (not a new one): 3 ExecutePipeline activities.
        defs = load_adf_definitions(fixtures_dir)
        lineage = build_lineage(defs)
        edges = {
            (e.caller_pipeline, e.callee_pipeline, e.wait_on_completion)
            for e in lineage.control_edges
            if e.caller_pipeline == "pipeline_execute_pipeline_nested"
        }
        assert ("pipeline_execute_pipeline_nested", "pipeline_copy_sql_to_delta", True) in edges
        assert ("pipeline_execute_pipeline_nested", "pipeline_notebook_with_params", True) in edges
        assert ("pipeline_execute_pipeline_nested", "pipeline_delete_recursive", False) in edges

    def test_parent_fans_out_to_children(self):
        # Shape of a real parent-orchestrator migration, generic names.
        parent = _pipeline(
            "orchestrator",
            [
                _ep("Run Child A", "child_a"),
                _ep("Run Child B", "child_b", wait=False),
            ],
        )
        child_a = _pipeline("child_a", [])
        child_b = _pipeline("child_b", [])
        defs = AdfDefinitions(pipelines=[parent, child_a, child_b], datasets={}, linked_services={}, triggers=[])
        lineage = build_lineage(defs)
        edges = {(e.caller_pipeline, e.callee_pipeline, e.wait_on_completion) for e in lineage.control_edges}
        assert ("orchestrator", "child_a", True) in edges
        assert ("orchestrator", "child_b", False) in edges

    def test_nested_execute_pipeline_is_found(self):
        # ExecutePipeline nested inside a ForEach must be caught by the recursion.
        parent = _pipeline("orchestrator", [_foreach("For Each Key", [_ep("Run Nested", "child_a")])])
        child_a = _pipeline("child_a", [])
        defs = AdfDefinitions(pipelines=[parent, child_a], datasets={}, linked_services={}, triggers=[])
        lineage = build_lineage(defs)
        assert any(
            e.caller_pipeline == "orchestrator" and e.callee_pipeline == "child_a" for e in lineage.control_edges
        )

    def test_unresolved_callee_is_recorded(self):
        parent = _pipeline("caller", [_ep("Run Ghost", "not_exported")])
        defs = AdfDefinitions(pipelines=[parent], datasets={}, linked_services={}, triggers=[])
        lineage = build_lineage(defs)
        assert any(e.callee_pipeline == "not_exported" for e in lineage.control_edges)


class TestReadExecutePipelineRef:
    def test_reads_reference_and_wait(self):
        act = _ep("Run", "child", wait=False)
        assert read_execute_pipeline_ref(act) == ("child", False)

    def test_defaults_wait_true_and_empty_name(self):
        act = AdfActivity(name="Run", type="ExecutePipeline", type_properties={})
        assert read_execute_pipeline_ref(act) == ("", True)

    def test_non_dict_pipeline_ref_is_stringified(self):
        # ADF normally exports a dict ref, but a bare string must not crash the reader.
        act = AdfActivity(name="Run", type="ExecutePipeline", type_properties={"pipeline": "child"})
        assert read_execute_pipeline_ref(act) == ("child", True)


class TestDataEdges:
    def _delta_dataset(self, name: str, table: str) -> AdfDataset:
        from flowx.models.adf_ast import AdfDataset

        return AdfDataset(
            name=name,
            type="AzureDatabricksDeltaLakeDataset",
            properties={
                "type": "AzureDatabricksDeltaLakeDataset",
                "linkedServiceName": {"referenceName": "ls_x", "type": "LinkedServiceReference"},
                "typeProperties": {"table": table},
            },
        )

    def _copy_writes(self, name: str, out_dataset: str) -> AdfActivity:
        return AdfActivity(
            name=name,
            type="Copy",
            type_properties={
                "source": {"type": "DelimitedTextSource"},
                "sink": {"type": "AzureDatabricksDeltaLakeSink"},
            },
            outputs=[AdfDatasetReference(reference_name=out_dataset)],
        )

    def _lookup_reads(self, name: str, in_dataset: str) -> AdfActivity:
        return AdfActivity(
            name=name,
            type="Lookup",
            type_properties={"dataset": {"referenceName": in_dataset, "type": "DatasetReference"}},
        )

    def test_producer_consumer_join_on_identity(self):
        # Two DIFFERENTLY-NAMED datasets pointing at the same physical table must join.
        producer = _pipeline("writer", [self._copy_writes("Write Orders", "ds_orders_out")])
        consumer = _pipeline("reader", [self._lookup_reads("Read Orders", "ds_orders_in")])
        defs = AdfDefinitions(
            pipelines=[producer, consumer],
            datasets={
                "ds_orders_out": self._delta_dataset("ds_orders_out", "curated.orders"),
                "ds_orders_in": self._delta_dataset("ds_orders_in", "curated.orders"),
            },
            linked_services={},
            triggers=[],
        )
        lineage = build_lineage(defs)
        matches = [e for e in lineage.data_edges if e.producer_pipeline == "writer" and e.consumer_pipeline == "reader"]
        assert len(matches) == 1
        edge = matches[0]
        assert edge.identity == "curated.orders"
        assert edge.producer_activity == "Write Orders"
        assert edge.consumer_activity == "Read Orders"

    def test_no_self_edge(self):
        # A single activity that both writes and reads the same table must not edge to itself.
        both = AdfActivity(
            name="Merge",
            type="Copy",
            type_properties={"source": {"type": "DeltaSource"}, "sink": {"type": "AzureDatabricksDeltaLakeSink"}},
            inputs=[AdfDatasetReference(reference_name="ds_same")],
            outputs=[AdfDatasetReference(reference_name="ds_same")],
        )
        defs = AdfDefinitions(
            pipelines=[_pipeline("p", [both])],
            datasets={"ds_same": self._delta_dataset("ds_same", "curated.orders")},
            linked_services={},
            triggers=[],
        )
        lineage = build_lineage(defs)
        for e in lineage.data_edges:
            assert not (e.producer_pipeline == e.consumer_pipeline and e.producer_activity == e.consumer_activity)

    def test_unresolved_identity_emits_no_edge(self):
        # A dataset with no resolvable table/path => identity None => NO edge.
        # Rationale (validated on a real-world factory): a single parameterized dataset is
        # commonly reused by many activities pointing at DIFFERENT physical files, so a
        # name-based join would manufacture false hand-offs. Only resolved physical
        # identity joins; unresolved coupling is knowable only at runtime ("never guess").
        from flowx.models.adf_ast import AdfDataset

        opaque = AdfDataset(name="ds_opaque", type="Unknown", properties={"type": "Unknown"})
        producer = _pipeline("writer", [self._copy_writes("Write", "ds_opaque")])
        consumer = _pipeline("reader", [self._lookup_reads("Read", "ds_opaque")])
        defs = AdfDefinitions(
            pipelines=[producer, consumer],
            datasets={"ds_opaque": opaque},
            linked_services={},
            triggers=[],
        )
        lineage = build_lineage(defs)
        matches = [e for e in lineage.data_edges if e.producer_pipeline == "writer" and e.consumer_pipeline == "reader"]
        assert matches == []

    def test_same_name_different_literal_paths_do_not_join(self):
        # Two activities reuse ONE parameterized dataset name but write/read DIFFERENT literal
        # files (a.csv vs b.csv). Identity is unresolvable; the path signatures differ on their
        # literal segment, so neither the identity nor the expression tier joins them. This is
        # the dm_dummyDS-style reuse that a naive name-join would wrongly couple.
        from flowx.models.adf_ast import AdfDataset

        param_ds = AdfDataset(
            name="ds_param",
            type="DelimitedText",
            properties={
                "type": "DelimitedText",
                "parameters": {"fileName": {"type": "string"}},
                "typeProperties": {
                    "location": {
                        "type": "AzureBlobFSLocation",
                        "fileName": {"value": "@dataset().fileName", "type": "Expression"},
                    }
                },
            },
        )
        writer = AdfActivity(
            name="WriteLogA",
            type="Copy",
            type_properties={"source": {"type": "DelimitedTextSource"}, "sink": {"type": "DelimitedTextSink"}},
            outputs=[AdfDatasetReference(reference_name="ds_param", parameters={"fileName": "a.csv"})],
        )
        reader = AdfActivity(
            name="ReadLogB",
            type="Copy",
            type_properties={"source": {"type": "DelimitedTextSource"}, "sink": {"type": "DelimitedTextSink"}},
            inputs=[AdfDatasetReference(reference_name="ds_param", parameters={"fileName": "b.csv"})],
        )
        defs = AdfDefinitions(
            pipelines=[_pipeline("p1", [writer]), _pipeline("p2", [reader])],
            datasets={"ds_param": param_ds},
            linked_services={},
            triggers=[],
        )
        lineage = build_lineage(defs)
        assert lineage.data_edges == []

    def _param_ds(self, name: str) -> "AdfDataset":  # noqa: F821
        # A dataset whose physical path is fully parameterized => identity unresolvable.
        from flowx.models.adf_ast import AdfDataset

        return AdfDataset(
            name=name,
            type="DelimitedText",
            properties={
                "type": "DelimitedText",
                "parameters": {"folderPath": {"type": "string"}, "fileName": {"type": "string"}},
                "typeProperties": {
                    "location": {
                        "type": "AzureBlobFSLocation",
                        "folderPath": {"value": "@dataset().folderPath", "type": "Expression"},
                        "fileName": {"value": "@dataset().fileName", "type": "Expression"},
                    }
                },
            },
        )

    def test_expression_signature_join_recovers_watermark_handoff(self):
        # The watermark pattern: a writer builds a path from pipeline().parameters.X and a
        # reader (inside a ForEach) builds the SAME literal path from item().X. Identity is
        # unresolvable on both, but the normalized path signatures match => one expression edge.
        wm_folder = "@concat(pipeline().parameters.root,'/config-params/entity-versions/')"
        wm_folder_reader = "@concat(pipeline().parameters.root,'/config-params/entity-versions/')"
        writer = AdfActivity(
            name="update_last_version",
            type="Copy",
            type_properties={"source": {"type": "DelimitedTextSource"}, "sink": {"type": "DelimitedTextSink"}},
            outputs=[
                AdfDatasetReference(
                    reference_name="ds_dummy",
                    parameters={
                        "folderPath": wm_folder,
                        "fileName": "@concat(pipeline().parameters.entityID,'.csv')",
                    },
                )
            ],
        )
        reader = AdfActivity(
            name="get_WM_Version",
            type="Lookup",
            type_properties={
                "dataset": {
                    "referenceName": "ds_last_version",
                    "type": "DatasetReference",
                    "parameters": {"folderPath": wm_folder_reader, "fileName": "@concat(item().entityID,'.csv')"},
                }
            },
        )
        defs = AdfDefinitions(
            pipelines=[_pipeline("orchestrator", [writer, reader])],
            datasets={"ds_dummy": self._param_ds("ds_dummy"), "ds_last_version": self._param_ds("ds_last_version")},
            linked_services={},
            triggers=[],
        )
        edges = build_lineage(defs).data_edges
        wm = [
            e for e in edges if e.producer_activity == "update_last_version" and e.consumer_activity == "get_WM_Version"
        ]
        assert len(wm) == 1
        assert wm[0].match_kind == "expression"
        assert wm[0].identity is None
        assert wm[0].match_key == "FP[config-params/entity-versions|slots=1]/FN[.csv|slots=1]"

    def test_expression_signature_does_not_over_match_different_paths(self):
        # Two parameterized writes/reads with DIFFERENT literal path anchors must NOT join,
        # even though both are unresolvable and share the same slot shape. This is why the
        # dummy-dataset logging writes do not collide with the watermark reads.
        writer = AdfActivity(
            name="WriteLog",
            type="Copy",
            type_properties={"source": {"type": "DelimitedTextSource"}, "sink": {"type": "DelimitedTextSink"}},
            outputs=[
                AdfDatasetReference(
                    reference_name="ds_dummy",
                    parameters={
                        "folderPath": "@concat(pipeline().parameters.root,'/executions/ExtractionLog/')",
                        "fileName": "@concat(pipeline().parameters.id,'.csv')",
                    },
                )
            ],
        )
        reader = AdfActivity(
            name="ReadWatermark",
            type="Lookup",
            type_properties={
                "dataset": {
                    "referenceName": "ds_wm",
                    "type": "DatasetReference",
                    "parameters": {
                        "folderPath": "@concat(pipeline().parameters.root,'/config-params/entity-versions/')",
                        "fileName": "@concat(item().id,'.csv')",
                    },
                }
            },
        )
        defs = AdfDefinitions(
            pipelines=[_pipeline("p", [writer, reader])],
            datasets={"ds_dummy": self._param_ds("ds_dummy"), "ds_wm": self._param_ds("ds_wm")},
            linked_services={},
            triggers=[],
        )
        assert build_lineage(defs).data_edges == []

    def test_opaque_folder_same_extension_do_not_join(self):
        # Two UNRELATED activities: opaque parameterized folders (no literal anchor) and the
        # same .csv extension. Their file names would sign, but with no folder-literal anchor
        # the signature is suppressed => no edge. Prevents a .csv/.json extension explosion.
        writer = AdfActivity(
            name="WriteAudit",
            type="Copy",
            type_properties={"source": {"type": "DelimitedTextSource"}, "sink": {"type": "DelimitedTextSink"}},
            outputs=[
                AdfDatasetReference(
                    reference_name="ds_audit",
                    parameters={
                        "folderPath": "@pipeline().parameters.auditFolder",
                        "fileName": "@concat(pipeline().parameters.runId,'.csv')",
                    },
                )
            ],
        )
        reader = AdfActivity(
            name="ReadWatermark",
            type="Lookup",
            type_properties={
                "dataset": {
                    "referenceName": "ds_wm",
                    "type": "DatasetReference",
                    "parameters": {
                        "folderPath": "@pipeline().parameters.wmFolder",
                        "fileName": "@concat(item().key,'.csv')",
                    },
                }
            },
        )
        defs = AdfDefinitions(
            pipelines=[_pipeline("p1", [writer]), _pipeline("p2", [reader])],
            datasets={"ds_audit": self._param_ds("ds_audit"), "ds_wm": self._param_ds("ds_wm")},
            linked_services={},
            triggers=[],
        )
        assert build_lineage(defs).data_edges == []

    def test_expression_join_matches_across_item_property_chain(self):
        # A reader referencing item().properties.name (a multi-level chain) must normalize to
        # the same single <P> slot as a writer's pipeline().parameters.X, so the watermark-style
        # match still holds regardless of how deep the runtime reference is.
        writer = AdfActivity(
            name="Write",
            type="Copy",
            type_properties={"source": {"type": "DelimitedTextSource"}, "sink": {"type": "DelimitedTextSink"}},
            outputs=[
                AdfDatasetReference(
                    reference_name="ds_w",
                    parameters={
                        "folderPath": "@concat(pipeline().parameters.root,'/state/versions/')",
                        "fileName": "@concat(pipeline().parameters.id,'.csv')",
                    },
                )
            ],
        )
        reader = AdfActivity(
            name="Read",
            type="Lookup",
            type_properties={
                "dataset": {
                    "referenceName": "ds_r",
                    "type": "DatasetReference",
                    "parameters": {
                        "folderPath": "@concat(pipeline().parameters.root,'/state/versions/')",
                        "fileName": "@concat(item().properties.id,'.csv')",
                    },
                }
            },
        )
        defs = AdfDefinitions(
            pipelines=[_pipeline("p", [writer, reader])],
            datasets={"ds_w": self._param_ds("ds_w"), "ds_r": self._param_ds("ds_r")},
            linked_services={},
            triggers=[],
        )
        edges = [e for e in build_lineage(defs).data_edges if e.producer_activity == "Write"]
        assert len(edges) == 1
        assert edges[0].match_kind == "expression"
        assert edges[0].match_key == "FP[state/versions|slots=1]/FN[.csv|slots=1]"

    def test_identity_match_takes_precedence_over_expression(self):
        # When both ends resolve to a physical identity, the edge is match_kind='identity'
        # (the higher-confidence tier), not 'expression'.
        producer = _pipeline("writer", [self._copy_writes("Write Orders", "ds_orders_out")])
        consumer = _pipeline("reader", [self._lookup_reads("Read Orders", "ds_orders_in")])
        defs = AdfDefinitions(
            pipelines=[producer, consumer],
            datasets={
                "ds_orders_out": self._delta_dataset("ds_orders_out", "curated.orders"),
                "ds_orders_in": self._delta_dataset("ds_orders_in", "curated.orders"),
            },
            linked_services={},
            triggers=[],
        )
        matches = [e for e in build_lineage(defs).data_edges if e.producer_pipeline == "writer"]
        assert len(matches) == 1
        assert matches[0].match_kind == "identity"
        assert matches[0].match_key == "curated.orders"

    def _copy_sink_typeprops(self, name: str, sink_dataset: str) -> AdfActivity:
        # Producer whose sink dataset is carried in typeProperties.sink.referenceName
        # (not activity.outputs) — exercises _typeprops_dataset_ref's producer path.
        return AdfActivity(
            name=name,
            type="Copy",
            type_properties={
                "source": {"type": "DelimitedTextSource"},
                "sink": {"referenceName": sink_dataset, "type": "DatasetReference"},
            },
        )

    def _copy_source_typeprops(self, name: str, source_dataset: str) -> AdfActivity:
        # Consumer whose source dataset is carried in typeProperties.source.referenceName
        # (not activity.inputs) — exercises _typeprops_dataset_ref's consumer path.
        return AdfActivity(
            name=name,
            type="Copy",
            type_properties={
                "source": {"referenceName": source_dataset, "type": "DatasetReference"},
                "sink": {"type": "AzureDatabricksDeltaLakeSink"},
            },
        )

    def test_no_duplicate_edge_when_ref_in_both_slots(self):
        # A producer naming the same dataset in BOTH activity.outputs AND typeProperties.sink
        # must yield a single endpoint, not two — so exactly one edge to the consumer.
        producer_act = AdfActivity(
            name="Write Both",
            type="Copy",
            type_properties={
                "source": {"type": "DelimitedTextSource"},
                "sink": {"referenceName": "ds_out", "type": "DatasetReference"},
            },
            outputs=[AdfDatasetReference(reference_name="ds_out")],
        )
        producer = _pipeline("writer", [producer_act])
        consumer = _pipeline("reader", [self._lookup_reads("Read", "ds_in")])
        defs = AdfDefinitions(
            pipelines=[producer, consumer],
            datasets={
                "ds_out": self._delta_dataset("ds_out", "curated.orders"),
                "ds_in": self._delta_dataset("ds_in", "curated.orders"),
            },
            linked_services={},
            triggers=[],
        )
        lineage = build_lineage(defs)
        matches = [e for e in lineage.data_edges if e.producer_pipeline == "writer" and e.consumer_pipeline == "reader"]
        assert len(matches) == 1

    def test_join_via_copy_sink_and_source_typeprops(self):
        # Producer writes via typeProperties.sink; consumer reads via typeProperties.source.
        # Two differently-named datasets resolving to the same table must still join.
        producer = _pipeline("writer", [self._copy_sink_typeprops("Write Sink", "ds_sink_out")])
        consumer = _pipeline("reader", [self._copy_source_typeprops("Read Source", "ds_source_in")])
        defs = AdfDefinitions(
            pipelines=[producer, consumer],
            datasets={
                "ds_sink_out": self._delta_dataset("ds_sink_out", "curated.orders"),
                "ds_source_in": self._delta_dataset("ds_source_in", "curated.orders"),
            },
            linked_services={},
            triggers=[],
        )
        lineage = build_lineage(defs)
        matches = [e for e in lineage.data_edges if e.producer_pipeline == "writer" and e.consumer_pipeline == "reader"]
        assert len(matches) == 1
        edge = matches[0]
        assert edge.identity == "curated.orders"
        assert edge.producer_activity == "Write Sink"
        assert edge.consumer_activity == "Read Source"
