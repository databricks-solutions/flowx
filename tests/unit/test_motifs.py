"""Tests for motif detection and collapsing."""

from __future__ import annotations

from flowx.models.adf_ast import (
    AdfActivity,
    AdfDefinitions,
    AdfDependency,
    AdfPipeline,
)
from flowx.models.ir import (
    Activity,
    Dependency,
    MotifActivity,
    Pipeline,
)
from flowx.models.motifs import MOTIF_METADATA_DRIVEN_BULK_COPY
from flowx.motifs.collapser import collapse_motifs
from flowx.motifs.detector import detect_motifs

_EMPTY_DEFS = AdfDefinitions(pipelines=[], datasets={}, linked_services={}, triggers=[])


def _adf_activity(
    name: str,
    adf_type: str,
    depends_on: list[str] | None = None,
    type_properties: dict | None = None,
    activities: list[AdfActivity] | None = None,
) -> AdfActivity:
    deps = (
        [AdfDependency(activity=dep, dependency_conditions=["Succeeded"]) for dep in depends_on] if depends_on else None
    )
    return AdfActivity(
        name=name,
        type=adf_type,
        depends_on=deps,
        type_properties=type_properties,
        activities=activities,
    )


def _ir_activity(name: str, depends_on: list[str] | None = None) -> Activity:
    deps = [Dependency(task_key=dep) for dep in depends_on] if depends_on else None
    return Activity(name=name, task_key=name, depends_on=deps)


class TestDetectorMetadataDrivenBulkCopy:
    def test_detects_lookup_foreach_copy_pattern(self):
        copy_child = _adf_activity("CopyTable", "Copy")
        pipeline = AdfPipeline(
            name="test_pipeline",
            activities=[
                _adf_activity(
                    "GetTableList",
                    "Lookup",
                    type_properties={
                        "source": {"sqlReaderQuery": "SELECT table_name FROM config.control_table"},
                    },
                ),
                _adf_activity(
                    "ForEachTable",
                    "ForEach",
                    depends_on=["GetTableList"],
                    activities=[copy_child],
                ),
            ],
        )
        motifs = detect_motifs(pipeline, _EMPTY_DEFS)
        assert len(motifs) == 1
        assert motifs[0].definition.motif_id == "metadata_driven_bulk_copy"
        assert "GetTableList" in motifs[0].matched_activities
        assert "ForEachTable" in motifs[0].matched_activities

    def test_no_match_when_foreach_has_execute_pipeline(self):
        exec_child = _adf_activity("RunChild", "ExecutePipeline")
        pipeline = AdfPipeline(
            name="test_pipeline",
            activities=[
                _adf_activity("GetList", "Lookup"),
                _adf_activity(
                    "Loop",
                    "ForEach",
                    depends_on=["GetList"],
                    activities=[exec_child],
                ),
            ],
        )
        motifs = detect_motifs(pipeline, _EMPTY_DEFS)
        bulk_copy = [m for m in motifs if m.definition.motif_id == "metadata_driven_bulk_copy"]
        assert len(bulk_copy) == 0


class TestDetectorCopyAndNotify:
    def test_detects_copy_then_web_notification(self):
        pipeline = AdfPipeline(
            name="test_pipeline",
            activities=[
                _adf_activity("CopyData", "Copy"),
                _adf_activity(
                    "NotifySuccess",
                    "WebActivity",
                    depends_on=["CopyData"],
                    type_properties={
                        "url": "https://hooks.slack.com/services/T00/B00/xxx",
                        "method": "POST",
                    },
                ),
            ],
        )
        motifs = detect_motifs(pipeline, _EMPTY_DEFS)
        assert len(motifs) == 1
        assert motifs[0].definition.motif_id == "activity_and_notify"


class TestDetectorParentChild:
    def test_detects_lookup_foreach_execute_pipeline(self):
        exec_child = _adf_activity("RunChildPipeline", "ExecutePipeline")
        pipeline = AdfPipeline(
            name="test_pipeline",
            activities=[
                _adf_activity("GetWorkItems", "Lookup"),
                _adf_activity(
                    "ProcessItems",
                    "ForEach",
                    depends_on=["GetWorkItems"],
                    activities=[exec_child],
                ),
            ],
        )
        motifs = detect_motifs(pipeline, _EMPTY_DEFS)
        assert len(motifs) == 1
        assert motifs[0].definition.motif_id == "parent_child_orchestration"


class TestCollapser:
    def test_collapse_replaces_activities_with_motif(self):
        pipeline = Pipeline(
            name="test",
            tasks=[
                _ir_activity("GetTableList"),
                _ir_activity("ForEachTable", depends_on=["GetTableList"]),
                _ir_activity("PostProcessing", depends_on=["ForEachTable"]),
            ],
        )
        motif = MOTIF_METADATA_DRIVEN_BULK_COPY
        from flowx.models.motifs import DetectedMotif

        detected = DetectedMotif(
            definition=motif,
            matched_activities=["GetTableList", "ForEachTable"],
            source_type_hint="database",
            confidence_notes=["Test match"],
        )
        result = collapse_motifs(pipeline, [detected])

        assert len(result.tasks) == 2
        motif_task = result.tasks[0]
        assert isinstance(motif_task, MotifActivity)
        assert motif_task.motif_id == "metadata_driven_bulk_copy"
        assert motif_task.databricks_replacement == "for_each_ingestion"
        assert "GetTableList" in motif_task.matched_activity_names

        post = result.tasks[1]
        assert post.name == "PostProcessing"
        assert post.depends_on is not None
        assert post.depends_on[0].task_key == motif_task.task_key

    def test_collapse_no_motifs_returns_unchanged(self):
        pipeline = Pipeline(
            name="test",
            tasks=[_ir_activity("A"), _ir_activity("B", depends_on=["A"])],
        )
        result = collapse_motifs(pipeline, [])
        assert len(result.tasks) == 2
        assert result.tasks[0].name == "A"

    def test_collapse_preserves_unclaimed_activities(self):
        pipeline = Pipeline(
            name="test",
            tasks=[
                _ir_activity("Unclaimed1"),
                _ir_activity("GetList"),
                _ir_activity("Loop", depends_on=["GetList"]),
                _ir_activity("Unclaimed2", depends_on=["Loop"]),
            ],
        )
        from flowx.models.motifs import DetectedMotif

        detected = DetectedMotif(
            definition=MOTIF_METADATA_DRIVEN_BULK_COPY,
            matched_activities=["GetList", "Loop"],
        )
        result = collapse_motifs(pipeline, [detected])

        names = [t.name for t in result.tasks]
        assert "Unclaimed1" in names
        assert "Unclaimed2" in names
        motif_tasks = [t for t in result.tasks if isinstance(t, MotifActivity)]
        assert len(motif_tasks) == 1

    def test_collapse_handles_activity_name_distinct_from_task_key(self):
        """Regression: motif collapser must compare against sanitised task_keys.

        Activity names with spaces (or other characters that get sanitised
        in task_keys) used to confuse ``_collect_external_dependencies`` and
        ``_rewire_dependencies``: both compared raw activity names against
        ``Dependency.task_key`` (which is sanitised by the translator), so
        internal motif edges leaked through as "external" deps and
        downstream rewires never matched.
        """
        from flowx.models.motifs import DetectedMotif

        pipeline = Pipeline(
            name="test",
            tasks=[
                Activity(name="Setup Probe", task_key="Setup_Probe"),
                Activity(
                    name="Get Table List",
                    task_key="Get_Table_List",
                    depends_on=[Dependency(task_key="Setup_Probe")],
                ),
                Activity(
                    name="For Each Table",
                    task_key="For_Each_Table",
                    depends_on=[Dependency(task_key="Get_Table_List")],
                ),
                Activity(
                    name="Notify Done",
                    task_key="Notify_Done",
                    depends_on=[Dependency(task_key="For_Each_Table")],
                ),
            ],
        )
        detected = DetectedMotif(
            definition=MOTIF_METADATA_DRIVEN_BULK_COPY,
            matched_activities=["Get Table List", "For Each Table"],
            source_type_hint="database",
        )
        result = collapse_motifs(pipeline, [detected])

        motif = next(t for t in result.tasks if isinstance(t, MotifActivity))
        external_keys = {dep.task_key for dep in motif.depends_on or []}
        assert external_keys == {"Setup_Probe"}, (
            "Internal edge GetTableList -> ForEachTable should NOT appear as an external dep"
        )

        notify = next(t for t in result.tasks if t.name == "Notify Done")
        downstream_keys = {dep.task_key for dep in notify.depends_on or []}
        assert downstream_keys == {motif.task_key}, (
            f"Downstream task should be rewired to point at the motif, got {downstream_keys}"
        )
