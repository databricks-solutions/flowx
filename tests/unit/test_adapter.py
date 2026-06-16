"""Unit tests for the flowx agent adapter and the pipeline modifier."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from flowx.adapter import (
    CopyActivityParadigm,
    NonDatabricksTaskCompute,
    TranslationConfiguration,
    TranslationInputRequired,
    TranslationOption,
    TranslationSession,
    UseLakeflowConnectors,
    apply_configuration,
    gather_options,
    validate_answer,
)
from flowx.adapter.__main__ import main as adapter_cli_main
from flowx.adapter.constants import (
    COMPUTE_MODE_CLASSIC_MULTI_NODE,
    COMPUTE_MODE_CLASSIC_SINGLE_NODE,
    COMPUTE_MODE_INHERIT,
    COMPUTE_MODE_SERVERLESS,
    LAKEFLOW_CONNECT_REPLACEMENT,
    OPTION_COPY_ACTIVITY_PARADIGM,
    OPTION_LAKEFLOW_CONNECTOR_TYPE,
    OPTION_METADATA_DRIVEN_ACCESS,
    OPTION_METADATA_DRIVEN_CONSOLIDATE,
    OPTION_METADATA_DRIVEN_LOOKUP_TOOL,
    OPTION_METADATA_DRIVEN_SIZE,
    OPTION_NON_DATABRICKS_TASK_COMPUTE,
    OPTION_USE_LAKEFLOW_CONNECTORS,
)
from flowx.adapter.operations import allowed_values_for, enum_for
from flowx.models.ir import (
    CopyActivity,
    ForEachActivity,
    MotifActivity,
    NotebookActivity,
    Pipeline,
    SparkPythonActivity,
    WaitActivity,
)
from flowx.models.motifs import (
    MOTIF_INCREMENTAL_LOAD_WATERMARK,
    DetectedMotif,
)


def _make_base(name: str = "task", task_key: str | None = None) -> dict[str, Any]:
    """Builds the common Activity kwargs used by these tests."""
    return {
        "name": name,
        "task_key": task_key or name,
        "description": None,
        "timeout_seconds": None,
        "max_retries": None,
        "min_retry_interval_millis": None,
        "depends_on": None,
        "cluster": None,
    }


def _delta_copy(name: str = "copy_to_delta") -> CopyActivity:
    """Builds a Copy activity whose sink resolves to a Delta table."""
    return CopyActivity(
        **_make_base(name),
        source_type="AzureSqlSource",
        sink_type="DeltaSink",
        sink_format="delta",
        sink_properties={"table": "raw.events"},
    )


def _metadata_driven_motif(task_key: str = "motif_metadata_driven_bulk_copy") -> MotifActivity:
    """Builds a metadata-driven bulk-copy motif activity for tests."""
    return MotifActivity(
        **_make_base(task_key, task_key),
        motif_id="metadata_driven_bulk_copy",
        display_name="Metadata-Driven Bulk Copy",
        databricks_replacement="for_each_ingestion",
        matched_activity_names=["GetTables", "ForEachTable", "CopyTable"],
        source_type_hint="database",
    )


def _query_delta_copy(name: str = "copy_query") -> CopyActivity:
    """Builds a Copy activity that reads via a SQL query and writes to Delta.

    The query analysis fields the translator normally stamps are
    included here so the IR is shaped exactly as it would be after
    ``flowx.translator.engine`` runs against this Copy.
    """
    return CopyActivity(
        **_make_base(name),
        source_type="AzureSqlSource",
        sink_type="DeltaSink",
        sink_format="delta",
        sink_properties={"table": "raw.events"},
        source_properties={
            "sqlReaderQuery": "SELECT id, name FROM dbo.events WHERE updated_at > '2024-01-01'",
            "query_parseable_for_lfc": True,
            "query_cursor_column": "updated_at",
            "query_include_columns": ["id", "name"],
        },
    )


def _file_copy(name: str = "copy_files") -> CopyActivity:
    """Builds a Copy activity that does not target Delta."""
    return CopyActivity(
        **_make_base(name),
        source_type="BlobSource",
        sink_type="ParquetSink",
        sink_format="parquet",
    )


class TestConfiguration:
    def test_default_configuration_are_conservative(self):
        prefs = TranslationConfiguration()
        assert prefs.copy_activity_paradigm is CopyActivityParadigm.NOTEBOOK
        assert prefs.non_databricks_task_compute is NonDatabricksTaskCompute.SERVERLESS
        assert prefs.use_lakeflow_connectors is UseLakeflowConnectors.EXISTING

    def test_string_values_coerce_to_enums(self):
        prefs = TranslationConfiguration(
            copy_activity_paradigm="sdp",
            non_databricks_task_compute="classic",
        )
        assert prefs.copy_activity_paradigm is CopyActivityParadigm.SDP
        assert prefs.non_databricks_task_compute is NonDatabricksTaskCompute.CLASSIC

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError, match="not a valid CopyActivityParadigm"):
            TranslationConfiguration(copy_activity_paradigm="bogus")

    def test_per_task_override_takes_precedence(self):
        base = TranslationConfiguration(
            copy_activity_paradigm="notebook",
            per_task={"copy_a": {"copy_activity_paradigm": "sdp"}},
        )
        scoped = base.effective_for("copy_a")
        assert scoped.copy_activity_paradigm is CopyActivityParadigm.SDP
        other = base.effective_for("copy_b")
        assert other.copy_activity_paradigm is CopyActivityParadigm.NOTEBOOK

    def test_effective_for_returns_self_when_no_override(self):
        prefs = TranslationConfiguration()
        assert prefs.effective_for("missing") is prefs

    def test_enum_for_and_allowed_values_for(self):
        assert enum_for("copy_activity_paradigm") is CopyActivityParadigm
        assert enum_for("unknown") is None
        assert set(allowed_values_for("copy_activity_paradigm")) == {"notebook", "sdp"}
        assert allowed_values_for("unknown") == ()


class TestGatherOptions:
    def test_no_options_for_empty_pipeline(self):
        pipeline = Pipeline(name="empty", tasks=[])
        pending = gather_options(pipeline)
        assert pending.pipeline_name == "empty"
        assert pending.options == []

    def test_copy_paradigm_option_only_when_delta_sink_present(self):
        delta_pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        option_ids = {q.option_id for q in gather_options(delta_pipeline).options}
        assert OPTION_COPY_ACTIVITY_PARADIGM in option_ids

        non_delta = Pipeline(name="p", tasks=[_file_copy()])
        option_ids = {q.option_id for q in gather_options(non_delta).options}
        assert OPTION_COPY_ACTIVITY_PARADIGM not in option_ids

    def test_non_databricks_compute_option_when_any_non_db_task(self):
        pipeline = Pipeline(name="p", tasks=[WaitActivity(**_make_base("w"), wait_time_seconds=1)])
        ids = {q.option_id for q in gather_options(pipeline).options}
        assert OPTION_NON_DATABRICKS_TASK_COMPUTE in ids

    def test_lakeflow_connect_option_only_for_db_to_delta(self):
        with_db = Pipeline(name="p", tasks=[_delta_copy()])
        ids = {q.option_id for q in gather_options(with_db).options}
        assert OPTION_USE_LAKEFLOW_CONNECTORS in ids

        without_db = Pipeline(name="p", tasks=[_file_copy()])
        ids = {q.option_id for q in gather_options(without_db).options}
        assert OPTION_USE_LAKEFLOW_CONNECTORS not in ids

    def test_lakeflow_connect_option_surfaces_for_database_motif_without_detected_motifs(self):
        """CLI callers don't have DetectedMotif objects; eligibility should derive from the IR alone."""
        motif_activity = MotifActivity(
            **_make_base("motif_incremental_load_watermark", "motif_incremental_load_watermark"),
            motif_id="incremental_load_watermark",
            display_name="Incremental Load (Watermark)",
            databricks_replacement="auto_loader",
            matched_activity_names=["Lookup1", "Lookup2", "Copy", "UpdateWatermark"],
            source_type_hint="database",
        )
        pipeline = Pipeline(name="p", tasks=[motif_activity])
        pending = gather_options(pipeline)
        ids = {q.option_id for q in pending.options}
        assert OPTION_USE_LAKEFLOW_CONNECTORS in ids
        option = next(q for q in pending.options if q.option_id == OPTION_USE_LAKEFLOW_CONNECTORS)
        assert "motif_incremental_load_watermark" in option.affected_task_keys

    def test_lakeflow_connect_option_surfaces_for_database_motif(self):
        motif_activity = MotifActivity(
            **_make_base("motif_incremental_load_watermark", "motif_incremental_load_watermark"),
            motif_id="incremental_load_watermark",
            display_name="Incremental Load (Watermark)",
            databricks_replacement="auto_loader",
            matched_activity_names=["Lookup1", "Lookup2", "Copy", "UpdateWatermark"],
            source_type_hint="database",
        )
        pipeline = Pipeline(name="p", tasks=[motif_activity])
        motifs = [
            DetectedMotif(
                definition=MOTIF_INCREMENTAL_LOAD_WATERMARK,
                matched_activities=motif_activity.matched_activity_names,
                source_type_hint="database",
                confidence_notes=[],
            )
        ]
        pending = gather_options(pipeline, motifs)
        ids = {q.option_id for q in pending.options}
        assert OPTION_USE_LAKEFLOW_CONNECTORS in ids
        lfc_option = next(q for q in pending.options if q.option_id == OPTION_USE_LAKEFLOW_CONNECTORS)
        assert "motif_incremental_load_watermark" in lfc_option.affected_task_keys

    def test_no_databricks_task_compute_option_for_notebook(self):
        """The serverless-replacement option for Databricks tasks was removed."""
        pipeline = Pipeline(
            name="p",
            tasks=[NotebookActivity(**_make_base("nb"), notebook_path="/Shared/x")],
        )
        ids = {q.option_id for q in gather_options(pipeline).options}
        assert "databricks_task_compute" not in ids

    def test_no_databricks_task_compute_option_for_spark_python(self):
        """The serverless-replacement option for Databricks tasks was removed."""
        pipeline = Pipeline(
            name="p",
            tasks=[SparkPythonActivity(**_make_base("py"), python_file="dbfs:/scripts/etl.py")],
        )
        ids = {q.option_id for q in gather_options(pipeline).options}
        assert "databricks_task_compute" not in ids

    def test_already_answered_filters_pending(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        pending = gather_options(pipeline, answers={OPTION_COPY_ACTIVITY_PARADIGM: "sdp"})
        ids = {q.option_id for q in pending.options}
        assert OPTION_COPY_ACTIVITY_PARADIGM not in ids

    def test_walks_into_for_each_inner_activities(self):
        inner_copy = _delta_copy("inner_copy")
        for_each = ForEachActivity(
            **_make_base("loop"),
            items_expression="@activity('lookup').output.value",
            inner_activities=[inner_copy],
        )
        pipeline = Pipeline(name="p", tasks=[for_each])
        option = next(
            (q for q in gather_options(pipeline).options if q.option_id == OPTION_COPY_ACTIVITY_PARADIGM),
            None,
        )
        assert option is not None
        assert "inner_copy" in option.affected_task_keys

    def test_motif_consolidation_option_emitted_per_detected_motif(self):
        """Each detected motif produces a ``consolidate_motif:<id>`` option."""
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        motifs = [
            DetectedMotif(
                definition=MOTIF_INCREMENTAL_LOAD_WATERMARK,
                matched_activities=["WatermarkLookup", "DeltaCopy"],
                source_type_hint="database",
                confidence_notes=["Detector matched Lookup→Copy→SP chain"],
            )
        ]
        pending = gather_options(pipeline, motifs)
        ids = {q.option_id for q in pending.options}
        assert "consolidate_motif:incremental_load_watermark" in ids
        motif_option = next(q for q in pending.options if q.option_id == "consolidate_motif:incremental_load_watermark")
        assert motif_option.default == "keep"
        assert {opt.value for opt in motif_option.options} == {"keep", "consolidate"}
        assert "WatermarkLookup" in motif_option.affected_task_keys
        # Confidence note must surface in the rationale so the agent can quote it
        assert "Detector matched Lookup→Copy→SP chain" in motif_option.rationale

    def test_motif_consolidation_option_filtered_by_answer(self):
        """Once answered the per-motif option must drop out of pending."""
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        motifs = [
            DetectedMotif(
                definition=MOTIF_INCREMENTAL_LOAD_WATERMARK,
                matched_activities=["WatermarkLookup"],
                source_type_hint=None,
                confidence_notes=[],
            )
        ]
        pending = gather_options(
            pipeline,
            motifs,
            answers={"consolidate_motif:incremental_load_watermark": "consolidate"},
        )
        ids = {q.option_id for q in pending.options}
        assert "consolidate_motif:incremental_load_watermark" not in ids

    def test_motif_consolidation_validate_answer_accepts_keep_or_consolidate(self):
        assert validate_answer("consolidate_motif:rest_api_pagination", "keep") == "keep"
        assert validate_answer("consolidate_motif:rest_api_pagination", "consolidate") == "consolidate"

    def test_motif_consolidation_validate_answer_rejects_unknown_value(self):
        with pytest.raises(ValueError, match="Invalid answer"):
            validate_answer("consolidate_motif:rest_api_pagination", "merge")


class TestValidateAnswer:
    def test_accepts_allowed_value(self):
        assert validate_answer("copy_activity_paradigm", "sdp") == "sdp"

    def test_rejects_unknown_option(self):
        with pytest.raises(ValueError, match="Unknown option_id"):
            validate_answer("not_a_option", "x")

    def test_rejects_invalid_value(self):
        with pytest.raises(ValueError, match="Invalid answer"):
            validate_answer("copy_activity_paradigm", "yaml")


class TestApplyConfiguration:
    def test_serverless_default_leaves_activities_on_serverless_compute(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy(), WaitActivity(**_make_base("w"), wait_time_seconds=1)])
        modified = apply_configuration(pipeline, TranslationConfiguration())
        copy_task = modified.tasks[0]
        wait_task = modified.tasks[1]
        assert copy_task.compute_mode == COMPUTE_MODE_SERVERLESS
        assert wait_task.compute_mode == COMPUTE_MODE_SERVERLESS

    def test_classic_compute_routes_copy_to_multi_node_cluster(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy(), WaitActivity(**_make_base("w"), wait_time_seconds=1)])
        prefs = TranslationConfiguration(non_databricks_task_compute="classic")
        modified = apply_configuration(pipeline, prefs)
        assert modified.tasks[0].compute_mode == COMPUTE_MODE_CLASSIC_MULTI_NODE
        assert modified.tasks[1].compute_mode == COMPUTE_MODE_CLASSIC_SINGLE_NODE

    def test_databricks_task_always_inherits_linked_service_cluster(self):
        """DatabricksNotebook activities always inherit the source linked-service cluster
        binding; the serverless replacement option was removed."""
        pipeline = Pipeline(name="p", tasks=[NotebookActivity(**_make_base("nb"), notebook_path="/Shared/x")])
        modified = apply_configuration(pipeline, TranslationConfiguration())
        assert modified.tasks[0].compute_mode == COMPUTE_MODE_INHERIT

    def test_spark_python_always_inherits_linked_service_cluster(self):
        """DatabricksSparkPython activities always inherit the source linked-service cluster
        binding; the serverless replacement option was removed."""
        pipeline = Pipeline(
            name="p", tasks=[SparkPythonActivity(**_make_base("py"), python_file="dbfs:/scripts/etl.py")]
        )
        modified = apply_configuration(pipeline, TranslationConfiguration())
        assert modified.tasks[0].compute_mode == COMPUTE_MODE_INHERIT

    def test_copy_paradigm_sdp_stamps_target_format(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        prefs = TranslationConfiguration(copy_activity_paradigm="sdp")
        modified = apply_configuration(pipeline, prefs)
        assert modified.tasks[0].target_format == "sdp"

    def test_copy_paradigm_does_not_apply_to_non_delta_copy(self):
        pipeline = Pipeline(name="p", tasks=[_file_copy()])
        prefs = TranslationConfiguration(copy_activity_paradigm="sdp")
        modified = apply_configuration(pipeline, prefs)
        assert modified.tasks[0].target_format == "notebook"

    def test_lakeflow_connect_flag_set_for_eligible_copy(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        prefs = TranslationConfiguration(use_lakeflow_connectors="lakeflow_connect")
        modified = apply_configuration(pipeline, prefs)
        assert modified.tasks[0].use_lakeflow_connector is True

    def test_lakeflow_connect_skipped_for_non_database_copy(self):
        pipeline = Pipeline(name="p", tasks=[_file_copy()])
        prefs = TranslationConfiguration(use_lakeflow_connectors="lakeflow_connect")
        modified = apply_configuration(pipeline, prefs)
        assert modified.tasks[0].use_lakeflow_connector is False

    def test_motif_replacement_swapped_for_lakeflow_connect_when_database(self):
        motif = MotifActivity(
            **_make_base("motif_incremental_load_watermark", "motif_incremental_load_watermark"),
            motif_id="incremental_load_watermark",
            display_name="Incremental Load (Watermark)",
            databricks_replacement="auto_loader",
            matched_activity_names=["L1", "L2", "C", "U"],
            source_type_hint="database",
        )
        pipeline = Pipeline(name="p", tasks=[motif])
        prefs = TranslationConfiguration(use_lakeflow_connectors="lakeflow_connect")
        modified = apply_configuration(pipeline, prefs)
        assert modified.tasks[0].databricks_replacement == LAKEFLOW_CONNECT_REPLACEMENT

    def test_motif_replacement_unchanged_for_file_source(self):
        motif = MotifActivity(
            **_make_base("motif_file_landing"),
            motif_id="file_landing_zone_processing",
            display_name="File Landing Zone",
            databricks_replacement="auto_loader_file_notification",
            matched_activity_names=["GetMeta", "ForEach", "Copy"],
            source_type_hint="files",
        )
        pipeline = Pipeline(name="p", tasks=[motif])
        prefs = TranslationConfiguration(use_lakeflow_connectors="lakeflow_connect")
        modified = apply_configuration(pipeline, prefs)
        assert modified.tasks[0].databricks_replacement == "auto_loader_file_notification"

    def test_per_task_override_wins(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy("c1"), _delta_copy("c2")])
        prefs = TranslationConfiguration(
            copy_activity_paradigm="notebook",
            per_task={"c1": {"copy_activity_paradigm": "sdp"}},
        )
        modified = apply_configuration(pipeline, prefs)
        assert modified.tasks[0].target_format == "sdp"
        assert modified.tasks[1].target_format == "notebook"

    def test_configuration_attached_to_pipeline(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        prefs = TranslationConfiguration(copy_activity_paradigm="sdp")
        modified = apply_configuration(pipeline, prefs)
        assert modified.translation_configuration is prefs

    def test_apply_configuration_does_not_mutate_input(self):
        original = Pipeline(name="p", tasks=[_delta_copy()])
        apply_configuration(original, TranslationConfiguration(copy_activity_paradigm="sdp"))
        assert original.tasks[0].target_format is None
        assert original.translation_configuration is None

    def test_recurses_into_for_each_inner_activities(self):
        inner = _delta_copy("inner")
        for_each = ForEachActivity(
            **_make_base("loop"),
            items_expression="@activity('l').output.value",
            inner_activities=[inner],
        )
        pipeline = Pipeline(name="p", tasks=[for_each])
        prefs = TranslationConfiguration(copy_activity_paradigm="sdp")
        modified = apply_configuration(pipeline, prefs)
        inner_after = modified.tasks[0].inner_activities[0]
        assert inner_after.target_format == "sdp"


class TestTranslationSession:
    def test_pending_returns_only_outstanding_options(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        session = TranslationSession(pipeline=pipeline)
        first = session.pending()
        assert len(first.options) > 0
        session.answer(OPTION_COPY_ACTIVITY_PARADIGM, "sdp")
        ids_after = {q.option_id for q in session.pending().options}
        assert OPTION_COPY_ACTIVITY_PARADIGM not in ids_after

    def test_run_raises_when_options_outstanding(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        session = TranslationSession(pipeline=pipeline)
        with pytest.raises(TranslationInputRequired) as info:
            session.run()
        assert info.value.pending.pipeline_name == "p"
        assert any(q.option_id == OPTION_COPY_ACTIVITY_PARADIGM for q in info.value.pending.options)

    def test_run_returns_modified_pipeline_when_complete(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        session = TranslationSession(pipeline=pipeline)
        pending = session.pending()
        answers = {q.option_id: q.default for q in pending.options}
        session.answer_many(answers)
        modified = session.run()
        assert modified.translation_configuration is not None

    def test_answer_validates(self):
        session = TranslationSession(pipeline=Pipeline(name="p", tasks=[_delta_copy()]))
        with pytest.raises(ValueError):
            session.answer(OPTION_COPY_ACTIVITY_PARADIGM, "yaml")

    def test_answer_many_is_atomic(self):
        session = TranslationSession(pipeline=Pipeline(name="p", tasks=[_delta_copy()]))
        with pytest.raises(ValueError):
            session.answer_many({OPTION_COPY_ACTIVITY_PARADIGM: "sdp", "bogus": "x"})
        assert OPTION_COPY_ACTIVITY_PARADIGM not in session._answers

    def test_find_option_returns_pending_option(self):
        session = TranslationSession(pipeline=Pipeline(name="p", tasks=[_delta_copy()]))
        found = session.find_option(OPTION_COPY_ACTIVITY_PARADIGM)
        assert isinstance(found, TranslationOption)
        session.answer(OPTION_COPY_ACTIVITY_PARADIGM, "sdp")
        assert session.find_option(OPTION_COPY_ACTIVITY_PARADIGM) is None


class TestSerializationRoundtrip:
    def test_configuration_survive_json_roundtrip(self):
        from flowx.bundler.dab_writer import pipeline_dict_to_ir
        from flowx.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(
            name="p", tasks=[_delta_copy(), NotebookActivity(**_make_base("nb"), notebook_path="/Shared/x")]
        )
        prefs = TranslationConfiguration(
            copy_activity_paradigm="sdp",
            non_databricks_task_compute="classic",
            use_lakeflow_connectors="lakeflow_connect",
        )
        stamped = apply_configuration(pipeline, prefs)
        roundtripped, _ = pipeline_dict_to_ir(json.loads(json.dumps(_pipeline_to_dict(stamped), default=str)))
        assert roundtripped.translation_configuration.copy_activity_paradigm is CopyActivityParadigm.SDP
        assert roundtripped.tasks[0].target_format == "sdp"
        assert roundtripped.tasks[0].use_lakeflow_connector is True
        assert roundtripped.tasks[0].compute_mode == COMPUTE_MODE_CLASSIC_MULTI_NODE
        # NotebookActivity always inherits the linked-service cluster binding now
        # that the serverless replacement option has been removed.
        assert roundtripped.tasks[1].compute_mode == COMPUTE_MODE_INHERIT


class TestMigrationInputSession:
    def test_discover_session_lists_expected_options(self):
        from flowx.adapter import MigrationInputSession

        session = MigrationInputSession(phase="discover")
        ids = [q.option_id for q in session.pending().options]
        assert ids == ["adf_source_path", "adf_resource_url", "output_dir"]

    def test_convert_session_lists_expected_options(self):
        from flowx.adapter import MigrationInputSession

        session = MigrationInputSession(phase="convert")
        ids = [q.option_id for q in session.pending().options]
        assert "inventory_path" in ids
        assert "adf_source_path" in ids

    def test_package_session_lists_expected_options(self):
        from flowx.adapter import MigrationInputSession

        session = MigrationInputSession(phase="package")
        ids = {q.option_id for q in session.pending().options}
        assert {"translation_report_path", "output_bundle_path", "catalog", "schema"} <= ids

    def test_unknown_phase_raises(self):
        from flowx.adapter import MigrationInputSession, UnknownMigrationPhaseError

        with pytest.raises(UnknownMigrationPhaseError):
            MigrationInputSession(phase="bogus")

    def test_answer_records_value_and_drops_from_pending(self):
        from flowx.adapter import MigrationInputSession

        session = MigrationInputSession(phase="discover")
        session.answer("adf_source_path", "/Volumes/main/default/adf")
        ids = [q.option_id for q in session.pending().options]
        assert "adf_source_path" not in ids

    def test_answer_rejects_unknown_option(self):
        from flowx.adapter import MigrationInputSession

        session = MigrationInputSession(phase="discover")
        with pytest.raises(ValueError, match="Unknown input option"):
            session.answer("not_a_field", "x")

    def test_collected_merges_answers_with_defaults(self):
        from flowx.adapter import MigrationInputSession

        session = MigrationInputSession(phase="package")
        session.answer("translation_report_path", "/tmp/report.json")
        collected = session.collected()
        assert collected["translation_report_path"] == "/tmp/report.json"
        assert collected["catalog"] == "main"
        assert collected["schema"] == "default"

    def test_collected_omits_required_when_missing(self):
        from flowx.adapter import MigrationInputSession

        session = MigrationInputSession(phase="discover")
        collected = session.collected()
        assert "adf_source_path" not in collected
        assert collected["output_dir"] == "./flowx_output"


class TestWorkspacePathsCli:
    def test_workspace_paths_detects_notebook_paths(self, tmp_path: Path):
        from flowx.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(
            name="p",
            tasks=[
                NotebookActivity(**_make_base("nb_a"), notebook_path="/Shared/team/a"),
                NotebookActivity(**_make_base("nb_b"), notebook_path="/Shared/team/b"),
            ],
        )
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        out = tmp_path / "ws.json"
        exit_code = adapter_cli_main(["workspace-paths", str(report_path), "--out", str(out)])
        assert exit_code == 0
        payload = json.loads(out.read_text())
        assert payload["paths"] == ["/Shared/team/a", "/Shared/team/b"]
        assert payload["needs_auth"] is True
        assert payload["suggested_hosts"] == []

    def test_workspace_paths_reports_no_auth_when_paths_empty(self, tmp_path: Path):
        from flowx.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        out = tmp_path / "ws.json"
        adapter_cli_main(["workspace-paths", str(report_path), "--out", str(out)])
        payload = json.loads(out.read_text())
        assert payload["paths"] == []
        assert payload["needs_auth"] is False

    def test_workspace_paths_suggests_host_from_databricks_linked_service(self, tmp_path: Path):
        from flowx.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(
            name="p",
            tasks=[NotebookActivity(**_make_base("nb"), notebook_path="/Shared/team/x")],
        )
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        source_dir = tmp_path / "source"
        (source_dir / "linked_services").mkdir(parents=True)
        (source_dir / "linked_services" / "LS_AzureDatabricks.json").write_text(
            json.dumps(
                {
                    "name": "LS_AzureDatabricks",
                    "properties": {"type": "AzureDatabricks", "domain": "https://adb-1234.5.azuredatabricks.net"},
                }
            )
        )
        (source_dir / "linked_services" / "LS_Other.json").write_text(
            json.dumps({"name": "LS_Other", "properties": {"type": "AzureSqlDatabase"}})
        )
        out = tmp_path / "ws.json"
        adapter_cli_main(["workspace-paths", str(report_path), "--source-dir", str(source_dir), "--out", str(out)])
        payload = json.loads(out.read_text())
        assert payload["suggested_hosts"] == ["https://adb-1234.5.azuredatabricks.net"]


class TestInputsCli:
    def test_inputs_emits_discover_options(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        exit_code = adapter_cli_main(["inputs", "discover"])
        assert exit_code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["phase"] == "discover"
        ids = [q["option_id"] for q in payload["options"]]
        assert ids == ["adf_source_path", "adf_resource_url", "output_dir"]

    def test_inputs_writes_to_file(self, tmp_path: Path):
        out = tmp_path / "options.json"
        exit_code = adapter_cli_main(["inputs", "package", "--out", str(out)])
        assert exit_code == 0
        payload = json.loads(out.read_text())
        assert payload["phase"] == "package"
        ids = {q["option_id"] for q in payload["options"]}
        assert "output_bundle_path" in ids


class TestCli:
    def test_inspect_emits_pending_options(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        from flowx.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        exit_code = adapter_cli_main(["inspect", str(report_path)])
        assert exit_code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["pipelines"][0]["pipeline_name"] == "p"
        option_ids = {q["option_id"] for q in payload["pipelines"][0]["options"]}
        assert OPTION_COPY_ACTIVITY_PARADIGM in option_ids

    def test_modify_stamps_configuration(self, tmp_path: Path):
        from flowx.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        out_path = tmp_path / "modified.json"
        exit_code = adapter_cli_main(
            [
                "modify",
                str(report_path),
                "--answer",
                "copy_activity_paradigm=sdp",
                "--answer",
                "non_databricks_task_compute=classic",
                "--answer",
                "use_lakeflow_connectors=lakeflow_connect",
                "--out",
                str(out_path),
                "--config-out",
                str(tmp_path / "configuration.json"),
            ]
        )
        assert exit_code == 0
        modified = json.loads(out_path.read_text())
        # The collected answers are persisted verbatim as configuration.json.
        config = json.loads((tmp_path / "configuration.json").read_text())
        assert config["copy_activity_paradigm"] == "sdp"
        assert modified["translation_configuration"]["copy_activity_paradigm"] == "sdp"
        copy_task = next(task for task in modified["tasks"] if task["task_key"] == "copy_to_delta")
        assert copy_task["target_format"] == "sdp"
        assert copy_task["compute_mode"] == COMPUTE_MODE_CLASSIC_MULTI_NODE
        assert copy_task["use_lakeflow_connector"] is True

    def test_materialize_lookup_from_csv_string(self, tmp_path: Path):
        out = tmp_path / "lookup_values.json"
        csv_source = "schema_name,table_name\ndbo,orders\ndbo,customers\n"
        exit_code = adapter_cli_main(["materialize-lookup", csv_source, "--out", str(out)])
        assert exit_code == 0
        rows = json.loads(out.read_text())
        assert rows == [
            {"schema_name": "dbo", "table_name": "orders"},
            {"schema_name": "dbo", "table_name": "customers"},
        ]

    def test_materialize_lookup_from_csv_file(self, tmp_path: Path):
        csv_path = tmp_path / "lookup.csv"
        csv_path.write_text("table_name\norders\ncustomers\n")
        out = tmp_path / "lookup_values.json"
        exit_code = adapter_cli_main(["materialize-lookup", str(csv_path), "--out", str(out)])
        assert exit_code == 0
        rows = json.loads(out.read_text())
        assert rows == [{"table_name": "orders"}, {"table_name": "customers"}]

    def test_modify_threads_lookup_values_into_metadata_driven_motif(self, tmp_path: Path):
        from flowx.translator.engine import _pipeline_to_dict

        motif = _metadata_driven_motif()
        pipeline = Pipeline(name="p", tasks=[motif])
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        out_path = tmp_path / "modified.json"
        exit_code = adapter_cli_main(
            [
                "modify",
                str(report_path),
                "--answer",
                "metadata_driven_consolidate=consolidate",
                "--answer",
                "metadata_driven_access=yes",
                "--answer",
                "metadata_driven_size=small",
                "--lookup-csv",
                "source_table\norders",
                "--out",
                str(out_path),
                "--config-out",
                str(tmp_path / "configuration.json"),
            ]
        )
        assert exit_code == 0
        modified = json.loads(out_path.read_text())
        motif_task = next(t for t in modified["tasks"] if t["motif_id"] == "metadata_driven_bulk_copy")
        assert motif_task["consolidate_metadata_driven"] is True
        assert motif_task["lookup_values"] == [{"source_table": "orders"}]

    def test_modify_rejects_invalid_answer(self, tmp_path: Path):
        from flowx.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        out_path = tmp_path / "modified.json"
        exit_code = adapter_cli_main(
            ["modify", str(report_path), "--answer", "copy_activity_paradigm=yaml", "--out", str(out_path)]
        )
        assert exit_code == 2

    def test_modify_output_dir_convention_writes_work_and_metadata(self, tmp_path: Path):
        from flowx.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        report_path = tmp_path / ".work" / "translation_report.json"
        report_path.parent.mkdir(parents=True)
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        exit_code = adapter_cli_main(
            ["modify", str(report_path), "--output-dir", str(tmp_path), "--answer", "copy_activity_paradigm=sdp"]
        )
        assert exit_code == 0
        # Stamped IR lands in the transient .work/, configuration.json in metadata/.
        assert (tmp_path / ".work" / "translation_report.stamped.json").exists()
        config = json.loads((tmp_path / "metadata" / "configuration.json").read_text())
        assert config == {"copy_activity_paradigm": "sdp"}

    def test_modify_requires_output_dir_or_out(self, tmp_path: Path):
        from flowx.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        exit_code = adapter_cli_main(["modify", str(report_path), "--answer", "copy_activity_paradigm=sdp"])
        assert exit_code == 2

    def test_inspect_emits_full_schema_with_show_when(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """inspect returns the whole option tree at once; follow-ups carry a show_when condition
        the agent evaluates locally (no per-follow-up round trip)."""
        from flowx.models.ir import CopyActivity, Dependency, WebActivity
        from flowx.translator.engine import _pipeline_to_dict

        copy = CopyActivity(name="Load", task_key="load")
        notify = WebActivity(
            name="Notify",
            task_key="notify",
            url="https://x",
            method="POST",
            depends_on=[Dependency(task_key="load", outcome="Failed")],
        )
        pipeline = Pipeline(name="p", tasks=[copy, notify])
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))

        assert adapter_cli_main(["inspect", str(report_path)]) == 0
        options = {o["option_id"]: o for o in json.loads(capsys.readouterr().out)["pipelines"][0]["options"]}

        # The full chain is present up front, not gated behind an answer.
        assert "notify_destination" in options
        assert "notify_email_recipients" in options
        # The destination question is unconditional; the email follow-up is gated by show_when.
        assert options["notify_destination"]["show_when"] == []
        assert options["notify_email_recipients"]["show_when"] == [{"option_id": "notify_destination", "in": ["email"]}]
        # Free-text follow-up vs. enum option.
        assert options["notify_email_recipients"]["free_text"] is True
        assert [c["value"] for c in options["notify_destination"]["choices"]][0] == "keep"

    def test_inspect_rejects_malformed_answer(self, tmp_path: Path):
        from flowx.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        # Missing '=' -> validation error -> exit 2.
        assert adapter_cli_main(["inspect", str(report_path), "--answer", "no_equals_sign"]) == 2

    def test_record_results_subcommand(self, tmp_path: Path, monkeypatch, capsys: pytest.CaptureFixture[str]):
        import flowx.reporting.results as rr

        md = tmp_path / "metadata"
        md.mkdir()
        (md / "inventory.json").write_text("{}")
        monkeypatch.setattr(rr, "write_results", lambda *a, **k: ("run-xyz", 3))
        rc = adapter_cli_main(["record-results", "--output-dir", str(tmp_path), "--results-table", "c.s.t"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "run-xyz" in out and "3 pipeline" in out

    def test_record_results_requires_inventory(self, tmp_path: Path):
        rc = adapter_cli_main(["record-results", "--output-dir", str(tmp_path), "--results-table", "c.s.t"])
        assert rc == 1  # no metadata/inventory.json

    def test_install_dashboard_subcommand(self, monkeypatch, capsys: pytest.CaptureFixture[str]):
        import flowx.reporting.dashboard as dd

        monkeypatch.setattr(dd, "install_dashboard", lambda *a, **k: ("dash-1", "https://x/sql/dashboardsv3/dash-1"))
        rc = adapter_cli_main(["install-dashboard", "--results-table", "c.s.t"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "dash-1" in out

    def test_install_dashboard_failure_returns_1(self, monkeypatch):
        import flowx.reporting.dashboard as dd

        def _boom(*a, **k):
            raise RuntimeError("no auth")

        monkeypatch.setattr(dd, "install_dashboard", _boom)
        rc = adapter_cli_main(["install-dashboard", "--results-table", "c.s.t"])
        assert rc == 1


class TestBundleOutput:
    def test_classic_copy_compute_emits_two_node_multi_node_cluster(self, tmp_path: Path):
        import yaml

        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        prefs = TranslationConfiguration(non_databricks_task_compute="classic")
        stamped = apply_configuration(pipeline, prefs)
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        job_yml = yaml.safe_load((tmp_path / "resources" / "job.yml").read_text())
        clusters = job_yml["resources"]["jobs"]["job"]["job_clusters"]
        keys = {cluster["job_cluster_key"] for cluster in clusters}
        assert "multi_node_cluster" in keys
        multi_node = next(cluster for cluster in clusters if cluster["job_cluster_key"] == "multi_node_cluster")
        assert multi_node["new_cluster"]["node_type_id"] == "Standard_D8ds_v5"
        assert multi_node["new_cluster"]["num_workers"] == 2

    def test_classic_single_node_cluster_uses_is_single_node_flag(self, tmp_path: Path):
        import yaml

        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[WaitActivity(**_make_base("w"), wait_time_seconds=1)])
        prefs = TranslationConfiguration(non_databricks_task_compute="classic")
        stamped = apply_configuration(pipeline, prefs)
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        job_yml = yaml.safe_load((tmp_path / "resources" / "job.yml").read_text())
        clusters = job_yml["resources"]["jobs"]["job"]["job_clusters"]
        single = next(cluster for cluster in clusters if cluster["job_cluster_key"] == "single_node_cluster")
        new_cluster = single["new_cluster"]
        assert new_cluster["is_single_node"] is True
        assert "num_workers" not in new_cluster
        assert "spark_conf" not in new_cluster
        assert "custom_tags" not in new_cluster

    def test_serverless_default_emits_no_job_clusters(self, tmp_path: Path):
        import yaml

        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        stamped = apply_configuration(pipeline, TranslationConfiguration())
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        job_yml = yaml.safe_load((tmp_path / "resources" / "job.yml").read_text())
        assert "job_clusters" not in job_yml["resources"]["jobs"]["job"]

    def test_sdp_copy_emits_pyspark_pipelines_table_scaffold(self, tmp_path: Path):
        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        stamped = apply_configuration(pipeline, TranslationConfiguration(copy_activity_paradigm="sdp"))
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        notebook_path = tmp_path / "src" / "notebooks" / "copy_a.py"
        body = notebook_path.read_text()
        assert "from pyspark import pipelines as sdp" in body
        assert "@sdp.table" in body
        assert "import dlt" not in body

    def test_lakeflow_connect_emits_pipeline_resource_and_no_notebook(self, tmp_path: Path):
        import yaml

        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        stamped = apply_configuration(pipeline, TranslationConfiguration(use_lakeflow_connectors="lakeflow_connect"))
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        assert not (tmp_path / "src" / "notebooks" / "copy_a.py").exists()
        pipeline_yml = tmp_path / "resources" / "pipelines" / "copy_a_lfc.yml"
        assert pipeline_yml.exists()
        resource = yaml.safe_load(pipeline_yml.read_text())
        lfc = resource["resources"]["pipelines"]["copy_a_lfc"]
        assert lfc["name"] == "copy_a_lfc"
        assert lfc["ingestion_definition"]["connection_name"] == "flowx_copy_a_connection"
        objects = lfc["ingestion_definition"]["objects"]
        assert objects[0]["table"]["destination_table"] == "raw.events"

    def test_lakeflow_connect_job_task_references_pipeline(self, tmp_path: Path):
        import yaml

        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        stamped = apply_configuration(pipeline, TranslationConfiguration(use_lakeflow_connectors="lakeflow_connect"))
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        job_yml = yaml.safe_load((tmp_path / "resources" / "job.yml").read_text())
        task = job_yml["resources"]["jobs"]["job"]["tasks"][0]
        assert "notebook_task" not in task
        assert task["pipeline_task"]["pipeline_id"] == "${resources.pipelines.copy_a_lfc.id}"

    def test_metadata_driven_consolidate_option_surfaces_for_motif(self):
        pipeline = Pipeline(name="p", tasks=[_metadata_driven_motif()])
        ids = {q.option_id for q in gather_options(pipeline).options}
        assert OPTION_METADATA_DRIVEN_CONSOLIDATE in ids

    def test_metadata_driven_followup_options_gated_on_consolidate(self):
        pipeline = Pipeline(name="p", tasks=[_metadata_driven_motif()])
        first_pass = gather_options(pipeline).options
        ids = {q.option_id for q in first_pass}
        assert OPTION_METADATA_DRIVEN_CONSOLIDATE in ids
        assert OPTION_METADATA_DRIVEN_ACCESS not in ids
        assert OPTION_METADATA_DRIVEN_SIZE not in ids
        assert OPTION_METADATA_DRIVEN_LOOKUP_TOOL not in ids

        keep_pass = gather_options(pipeline, answers={OPTION_METADATA_DRIVEN_CONSOLIDATE: "keep"}).options
        keep_ids = {q.option_id for q in keep_pass}
        assert OPTION_METADATA_DRIVEN_ACCESS not in keep_ids

        consolidate_pass = gather_options(pipeline, answers={OPTION_METADATA_DRIVEN_CONSOLIDATE: "consolidate"}).options
        consolidate_ids = {q.option_id for q in consolidate_pass}
        assert OPTION_METADATA_DRIVEN_ACCESS in consolidate_ids
        assert OPTION_METADATA_DRIVEN_SIZE in consolidate_ids
        assert OPTION_METADATA_DRIVEN_LOOKUP_TOOL not in consolidate_ids

    def test_metadata_driven_lookup_tool_option_gated_on_access(self):
        pipeline = Pipeline(name="p", tasks=[_metadata_driven_motif()])
        answers = {
            OPTION_METADATA_DRIVEN_CONSOLIDATE: "consolidate",
            OPTION_METADATA_DRIVEN_ACCESS: "yes",
        }
        pending = gather_options(pipeline, answers=answers).options
        ids = {q.option_id for q in pending}
        assert OPTION_METADATA_DRIVEN_LOOKUP_TOOL in ids

    def test_modifier_consolidates_metadata_driven_when_size_is_small(self):
        pipeline = Pipeline(name="p", tasks=[_metadata_driven_motif()])
        prefs = TranslationConfiguration(
            metadata_driven_consolidate="consolidate",
            metadata_driven_access="yes",
            metadata_driven_size="small",
        )
        modified = apply_configuration(pipeline, prefs)
        assert modified.tasks[0].consolidate_metadata_driven is True

    def test_modifier_does_not_consolidate_when_size_is_large(self):
        pipeline = Pipeline(name="p", tasks=[_metadata_driven_motif()])
        prefs = TranslationConfiguration(
            metadata_driven_consolidate="consolidate",
            metadata_driven_access="yes",
            metadata_driven_size="large",
        )
        modified = apply_configuration(pipeline, prefs)
        assert modified.tasks[0].consolidate_metadata_driven is False

    def test_modifier_does_not_consolidate_when_access_is_no(self):
        pipeline = Pipeline(name="p", tasks=[_metadata_driven_motif()])
        prefs = TranslationConfiguration(
            metadata_driven_consolidate="consolidate",
            metadata_driven_access="no",
            metadata_driven_size="small",
        )
        modified = apply_configuration(pipeline, prefs)
        assert modified.tasks[0].consolidate_metadata_driven is False

    def test_lakeflow_connector_type_option_suppressed_when_only_query_copies(self):
        pipeline = Pipeline(name="p", tasks=[_query_delta_copy("copy_q")])
        ids = {q.option_id for q in gather_options(pipeline).options}
        assert OPTION_USE_LAKEFLOW_CONNECTORS in ids
        assert OPTION_LAKEFLOW_CONNECTOR_TYPE not in ids

    def test_lakeflow_connector_type_option_suppressed_per_copy_eligibility(self):
        """Per-Copy eligibility determines connector type with no overlap.

        Table-based reads can only use CDC (no cursor column) and queries
        with a cursor can only use query-based, so the modifier picks the
        eligible connector per Copy and the prompt is suppressed.
        """
        pipeline = Pipeline(name="p", tasks=[_delta_copy("copy_a")])
        ids = {q.option_id for q in gather_options(pipeline).options}
        assert OPTION_LAKEFLOW_CONNECTOR_TYPE not in ids

    def test_query_copy_routes_to_query_based_connector_regardless_of_configuration(self, tmp_path: Path):
        import yaml

        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_query_delta_copy("copy_q")])
        prefs = TranslationConfiguration(
            use_lakeflow_connectors="lakeflow_connect",
            lakeflow_connector_type="cdc",
        )
        stamped = apply_configuration(pipeline, prefs)
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        resource = yaml.safe_load((tmp_path / "resources" / "pipelines" / "copy_q_lfc.yml").read_text())
        objects = resource["resources"]["pipelines"]["copy_q_lfc"]["ingestion_definition"]["objects"]
        assert "table_configuration" in objects[0]
        table_config = objects[0]["table_configuration"]
        qbc = table_config["query_based_connector_config"]
        assert qbc["cursor"] == "updated_at"
        assert qbc["include_columns"] == ["id", "name"]
        assert table_config["source_table"] == "raw.events"
        assert "table" not in objects[0]

    def test_table_copy_uses_cdc_connector_by_default(self, tmp_path: Path):
        import yaml

        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        prefs = TranslationConfiguration(use_lakeflow_connectors="lakeflow_connect")
        stamped = apply_configuration(pipeline, prefs)
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        resource = yaml.safe_load((tmp_path / "resources" / "pipelines" / "copy_a_lfc.yml").read_text())
        objects = resource["resources"]["pipelines"]["copy_a_lfc"]["ingestion_definition"]["objects"]
        assert "table" in objects[0]
        assert "table_configuration" not in objects[0]

    def test_table_copy_with_query_based_configuration_routes_to_cdc(self, tmp_path: Path):
        """LFC query-based requires a cursor column.  Table-based Copies have none.

        Per the Lakeflow Connect query-based-overview docs, the connector
        requires a cursor column to drive incremental ingestion.  When
        the user prefers query_based but the Copy is table-based (no
        query, no cursor candidate), the modifier honours the
        eligibility rules over the configuration and routes to CDC.
        """
        import yaml

        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        prefs = TranslationConfiguration(
            use_lakeflow_connectors="lakeflow_connect",
            lakeflow_connector_type="query_based",
        )
        stamped = apply_configuration(pipeline, prefs)
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        resource = yaml.safe_load((tmp_path / "resources" / "pipelines" / "copy_a_lfc.yml").read_text())
        objects = resource["resources"]["pipelines"]["copy_a_lfc"]["ingestion_definition"]["objects"]
        assert "table" in objects[0]
        assert "table_configuration" not in objects[0]

    def test_consolidated_metadata_driven_motif_emits_single_pipeline(self, tmp_path: Path):
        import dataclasses

        import yaml

        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        motif = _metadata_driven_motif()
        pipeline = Pipeline(name="job", tasks=[motif])
        prefs = TranslationConfiguration(
            metadata_driven_consolidate="consolidate",
            metadata_driven_access="yes",
            metadata_driven_size="medium",
        )
        stamped = apply_configuration(pipeline, prefs)
        consolidated_motif = dataclasses.replace(
            stamped.tasks[0],
            lookup_values=[
                {"source_schema": "dbo", "source_table": "orders"},
                {"source_schema": "dbo", "source_table": "customers"},
            ],
        )
        stamped = dataclasses.replace(stamped, tasks=[consolidated_motif])
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        resource_path = tmp_path / "resources" / "pipelines" / "motif_metadata_driven_bulk_copy_consolidated.yml"
        assert resource_path.exists()
        resource = yaml.safe_load(resource_path.read_text())
        pipeline_def = resource["resources"]["pipelines"]["motif_metadata_driven_bulk_copy_consolidated"]
        objects = pipeline_def["ingestion_definition"]["objects"]
        assert len(objects) == 2
        assert objects[0]["table"]["source_table"] == "orders"
        assert objects[1]["table"]["source_table"] == "customers"

    def test_table_based_copy_with_query_based_configuration_falls_back_to_cdc(self, tmp_path: Path):
        """Table-based reads have no cursor column, so query-based isn't eligible.

        When the user prefers query_based but the only eligible LFC connector
        for a table-based Copy is CDC, the modifier routes the Copy to CDC
        rather than emitting an unsupported query-based config.
        """
        import yaml

        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        copy = CopyActivity(
            **_make_base("copy_customers"),
            source_type="AzureSqlSource",
            sink_type="DeltaSink",
            sink_format="delta",
            sink_properties={"table": "customers", "schema": "bronze"},
            source_properties={
                "source_schema": "dbo",
                "source_table": "customers",
                "linked_service_name": "LS_AzureSqlDb",
                "connection": {"host": "ghansen-flowx-test-sql.database.windows.net", "port": 1433},
            },
        )
        prefs = TranslationConfiguration(
            use_lakeflow_connectors="lakeflow_connect",
            lakeflow_connector_type="query_based",
        )
        stamped = apply_configuration(Pipeline(name="job", tasks=[copy]), prefs)
        write_bundle(prepare_workflow(stamped), tmp_path)
        resource = yaml.safe_load((tmp_path / "resources" / "pipelines" / "copy_customers_lfc.yml").read_text())
        obj = resource["resources"]["pipelines"]["copy_customers_lfc"]["ingestion_definition"]["objects"][0]
        assert "table" in obj
        assert obj["table"]["destination_table"] == "customers"

    def test_lakeflow_connect_uses_resolved_host_from_linked_service(self, tmp_path: Path):
        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        copy = CopyActivity(
            **_make_base("copy_a"),
            source_type="AzureSqlSource",
            sink_type="DeltaSink",
            sink_format="delta",
            sink_properties={"table": "orders"},
            source_properties={
                "linked_service_name": "LS_AzureSqlDb",
                "connection": {"host": "ghansen-flowx-test-sql.database.windows.net", "port": 1433},
            },
        )
        prefs = TranslationConfiguration(use_lakeflow_connectors="lakeflow_connect")
        stamped = apply_configuration(Pipeline(name="job", tasks=[copy]), prefs)
        write_bundle(prepare_workflow(stamped), tmp_path)
        body = (tmp_path / "src" / "setup" / "create_connections.py").read_text()
        assert "ghansen-flowx-test-sql.database.windows.net" in body
        assert "1433" in body
        assert "PLACEHOLDER_HOST" not in body

    def test_lakeflow_connect_dedupes_connection_across_copies(self, tmp_path: Path):
        import yaml

        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        shared_source = {
            "linked_service_name": "LS_AzureSqlDb",
            "connection": {"host": "host.example.com", "port": 1433},
            "source_schema": "dbo",
        }
        copy_a = CopyActivity(
            **_make_base("copy_a"),
            source_type="AzureSqlSource",
            sink_type="DeltaSink",
            sink_format="delta",
            sink_properties={"table": "customers"},
            source_properties={**shared_source, "source_table": "customers"},
        )
        copy_b = CopyActivity(
            **_make_base("copy_b"),
            source_type="AzureSqlSource",
            sink_type="DeltaSink",
            sink_format="delta",
            sink_properties={"table": "orders"},
            source_properties={**shared_source, "source_table": "orders"},
        )
        prefs = TranslationConfiguration(use_lakeflow_connectors="lakeflow_connect")
        stamped = apply_configuration(Pipeline(name="job", tasks=[copy_a, copy_b]), prefs)
        write_bundle(prepare_workflow(stamped), tmp_path)
        body = (tmp_path / "src" / "setup" / "create_connections.py").read_text()
        assert body.count("CREATE CONNECTION IF NOT EXISTS") == 1
        assert body.count("flowx_LS_AzureSqlDb_connection") >= 1
        for pipeline_file in ("copy_a_lfc.yml", "copy_b_lfc.yml"):
            resource = yaml.safe_load((tmp_path / "resources" / "pipelines" / pipeline_file).read_text())
            key = pipeline_file.replace(".yml", "")
            assert resource["resources"]["pipelines"][key]["ingestion_definition"]["connection_name"] == (
                "flowx_LS_AzureSqlDb_connection"
            )

    def test_lakeflow_connect_emits_connection_setup_notebook(self, tmp_path: Path):
        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        stamped = apply_configuration(pipeline, TranslationConfiguration(use_lakeflow_connectors="lakeflow_connect"))
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        setup_notebook = tmp_path / "src" / "setup" / "create_connections.py"
        assert setup_notebook.exists()
        body = setup_notebook.read_text()
        assert "flowx_copy_a_connection" in body
        assert "SQLSERVER" in body

    def test_existing_default_binds_to_default_cluster(self, tmp_path: Path):
        import yaml

        from flowx.bundler.dab_writer import write_bundle
        from flowx.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(
            name="job",
            tasks=[NotebookActivity(**_make_base("nb"), notebook_path="/Shared/existing")],
        )
        stamped = apply_configuration(pipeline, TranslationConfiguration())
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        job_yml = yaml.safe_load((tmp_path / "resources" / "job.yml").read_text())
        task = job_yml["resources"]["jobs"]["job"]["tasks"][0]
        assert task["job_cluster_key"] == "default_cluster"
