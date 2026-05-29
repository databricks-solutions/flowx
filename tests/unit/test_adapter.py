"""Unit tests for the orchestra agent adapter and the pipeline modifier."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from orchestra.adapter import (
    CopyActivityParadigm,
    DatabricksTaskCompute,
    NonDatabricksTaskCompute,
    TranslationInputRequired,
    TranslationPreferences,
    TranslationQuestion,
    TranslationSession,
    UseLakeflowConnectors,
    apply_preferences,
    gather_questions,
    validate_answer,
)
from orchestra.adapter.__main__ import main as adapter_cli_main
from orchestra.adapter.constants import (
    COMPUTE_MODE_CLASSIC_MULTI_NODE,
    COMPUTE_MODE_CLASSIC_SINGLE_NODE,
    COMPUTE_MODE_INHERIT,
    COMPUTE_MODE_SERVERLESS,
    LAKEFLOW_CONNECT_REPLACEMENT,
    QUESTION_COPY_ACTIVITY_PARADIGM,
    QUESTION_DATABRICKS_TASK_COMPUTE,
    QUESTION_LAKEFLOW_CONNECTOR_TYPE,
    QUESTION_METADATA_DRIVEN_ACCESS,
    QUESTION_METADATA_DRIVEN_CONSOLIDATE,
    QUESTION_METADATA_DRIVEN_LOOKUP_TOOL,
    QUESTION_METADATA_DRIVEN_SIZE,
    QUESTION_NON_DATABRICKS_TASK_COMPUTE,
    QUESTION_USE_LAKEFLOW_CONNECTORS,
)
from orchestra.adapter.operations import allowed_values_for, enum_for
from orchestra.models.ir import (
    CopyActivity,
    ForEachActivity,
    MotifActivity,
    NotebookActivity,
    Pipeline,
    SparkPythonActivity,
    WaitActivity,
)
from orchestra.models.motifs import (
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
    ``orchestra.translator.engine`` runs against this Copy.
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


class TestPreferences:
    def test_default_preferences_are_conservative(self):
        prefs = TranslationPreferences()
        assert prefs.copy_activity_paradigm is CopyActivityParadigm.NOTEBOOK
        assert prefs.non_databricks_task_compute is NonDatabricksTaskCompute.SERVERLESS
        assert prefs.use_lakeflow_connectors is UseLakeflowConnectors.EXISTING
        assert prefs.databricks_task_compute is DatabricksTaskCompute.EXISTING

    def test_string_values_coerce_to_enums(self):
        prefs = TranslationPreferences(
            copy_activity_paradigm="sdp",
            non_databricks_task_compute="classic",
        )
        assert prefs.copy_activity_paradigm is CopyActivityParadigm.SDP
        assert prefs.non_databricks_task_compute is NonDatabricksTaskCompute.CLASSIC

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError, match="not a valid CopyActivityParadigm"):
            TranslationPreferences(copy_activity_paradigm="bogus")

    def test_per_task_override_takes_precedence(self):
        base = TranslationPreferences(
            copy_activity_paradigm="notebook",
            per_task={"copy_a": {"copy_activity_paradigm": "sdp"}},
        )
        scoped = base.effective_for("copy_a")
        assert scoped.copy_activity_paradigm is CopyActivityParadigm.SDP
        other = base.effective_for("copy_b")
        assert other.copy_activity_paradigm is CopyActivityParadigm.NOTEBOOK

    def test_effective_for_returns_self_when_no_override(self):
        prefs = TranslationPreferences()
        assert prefs.effective_for("missing") is prefs

    def test_enum_for_and_allowed_values_for(self):
        assert enum_for("copy_activity_paradigm") is CopyActivityParadigm
        assert enum_for("unknown") is None
        assert set(allowed_values_for("copy_activity_paradigm")) == {"notebook", "sdp"}
        assert allowed_values_for("unknown") == ()


class TestGatherQuestions:
    def test_no_questions_for_empty_pipeline(self):
        pipeline = Pipeline(name="empty", tasks=[])
        pending = gather_questions(pipeline)
        assert pending.pipeline_name == "empty"
        assert pending.questions == []

    def test_copy_paradigm_question_only_when_delta_sink_present(self):
        delta_pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        question_ids = {q.question_id for q in gather_questions(delta_pipeline).questions}
        assert QUESTION_COPY_ACTIVITY_PARADIGM in question_ids

        non_delta = Pipeline(name="p", tasks=[_file_copy()])
        question_ids = {q.question_id for q in gather_questions(non_delta).questions}
        assert QUESTION_COPY_ACTIVITY_PARADIGM not in question_ids

    def test_non_databricks_compute_question_when_any_non_db_task(self):
        pipeline = Pipeline(name="p", tasks=[WaitActivity(**_make_base("w"), wait_time_seconds=1)])
        ids = {q.question_id for q in gather_questions(pipeline).questions}
        assert QUESTION_NON_DATABRICKS_TASK_COMPUTE in ids

    def test_lakeflow_connect_question_only_for_db_to_delta(self):
        with_db = Pipeline(name="p", tasks=[_delta_copy()])
        ids = {q.question_id for q in gather_questions(with_db).questions}
        assert QUESTION_USE_LAKEFLOW_CONNECTORS in ids

        without_db = Pipeline(name="p", tasks=[_file_copy()])
        ids = {q.question_id for q in gather_questions(without_db).questions}
        assert QUESTION_USE_LAKEFLOW_CONNECTORS not in ids

    def test_lakeflow_connect_question_surfaces_for_database_motif_without_detected_motifs(self):
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
        pending = gather_questions(pipeline)
        ids = {q.question_id for q in pending.questions}
        assert QUESTION_USE_LAKEFLOW_CONNECTORS in ids
        question = next(q for q in pending.questions if q.question_id == QUESTION_USE_LAKEFLOW_CONNECTORS)
        assert "motif_incremental_load_watermark" in question.affected_task_keys

    def test_lakeflow_connect_question_surfaces_for_database_motif(self):
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
        pending = gather_questions(pipeline, motifs)
        ids = {q.question_id for q in pending.questions}
        assert QUESTION_USE_LAKEFLOW_CONNECTORS in ids
        lfc_question = next(q for q in pending.questions if q.question_id == QUESTION_USE_LAKEFLOW_CONNECTORS)
        assert "motif_incremental_load_watermark" in lfc_question.affected_task_keys

    def test_databricks_task_compute_question_when_notebook_present(self):
        pipeline = Pipeline(
            name="p",
            tasks=[NotebookActivity(**_make_base("nb"), notebook_path="/Shared/x")],
        )
        ids = {q.question_id for q in gather_questions(pipeline).questions}
        assert QUESTION_DATABRICKS_TASK_COMPUTE in ids

    def test_databricks_task_compute_question_for_spark_python(self):
        pipeline = Pipeline(
            name="p",
            tasks=[SparkPythonActivity(**_make_base("py"), python_file="dbfs:/scripts/etl.py")],
        )
        ids = {q.question_id for q in gather_questions(pipeline).questions}
        assert QUESTION_DATABRICKS_TASK_COMPUTE in ids

    def test_already_answered_filters_pending(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        pending = gather_questions(pipeline, answers={QUESTION_COPY_ACTIVITY_PARADIGM: "sdp"})
        ids = {q.question_id for q in pending.questions}
        assert QUESTION_COPY_ACTIVITY_PARADIGM not in ids

    def test_walks_into_for_each_inner_activities(self):
        inner_copy = _delta_copy("inner_copy")
        for_each = ForEachActivity(
            **_make_base("loop"),
            items_expression="@activity('lookup').output.value",
            inner_activities=[inner_copy],
        )
        pipeline = Pipeline(name="p", tasks=[for_each])
        question = next(
            (q for q in gather_questions(pipeline).questions if q.question_id == QUESTION_COPY_ACTIVITY_PARADIGM),
            None,
        )
        assert question is not None
        assert "inner_copy" in question.affected_task_keys


class TestValidateAnswer:
    def test_accepts_allowed_value(self):
        assert validate_answer("copy_activity_paradigm", "sdp") == "sdp"

    def test_rejects_unknown_question(self):
        with pytest.raises(ValueError, match="Unknown question_id"):
            validate_answer("not_a_question", "x")

    def test_rejects_invalid_value(self):
        with pytest.raises(ValueError, match="Invalid answer"):
            validate_answer("copy_activity_paradigm", "yaml")


class TestApplyPreferences:
    def test_serverless_default_leaves_activities_on_serverless_compute(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy(), WaitActivity(**_make_base("w"), wait_time_seconds=1)])
        modified = apply_preferences(pipeline, TranslationPreferences())
        copy_task = modified.tasks[0]
        wait_task = modified.tasks[1]
        assert copy_task.compute_mode == COMPUTE_MODE_SERVERLESS
        assert wait_task.compute_mode == COMPUTE_MODE_SERVERLESS

    def test_classic_compute_routes_copy_to_multi_node_cluster(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy(), WaitActivity(**_make_base("w"), wait_time_seconds=1)])
        prefs = TranslationPreferences(non_databricks_task_compute="classic")
        modified = apply_preferences(pipeline, prefs)
        assert modified.tasks[0].compute_mode == COMPUTE_MODE_CLASSIC_MULTI_NODE
        assert modified.tasks[1].compute_mode == COMPUTE_MODE_CLASSIC_SINGLE_NODE

    def test_databricks_task_serverless_stamps_serverless(self):
        pipeline = Pipeline(name="p", tasks=[NotebookActivity(**_make_base("nb"), notebook_path="/Shared/x")])
        prefs = TranslationPreferences(databricks_task_compute="serverless")
        modified = apply_preferences(pipeline, prefs)
        assert modified.tasks[0].compute_mode == COMPUTE_MODE_SERVERLESS

    def test_databricks_task_existing_stamps_inherit(self):
        pipeline = Pipeline(name="p", tasks=[NotebookActivity(**_make_base("nb"), notebook_path="/Shared/x")])
        modified = apply_preferences(pipeline, TranslationPreferences())
        assert modified.tasks[0].compute_mode == COMPUTE_MODE_INHERIT

    def test_copy_paradigm_sdp_stamps_target_format(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        prefs = TranslationPreferences(copy_activity_paradigm="sdp")
        modified = apply_preferences(pipeline, prefs)
        assert modified.tasks[0].target_format == "sdp"

    def test_copy_paradigm_does_not_apply_to_non_delta_copy(self):
        pipeline = Pipeline(name="p", tasks=[_file_copy()])
        prefs = TranslationPreferences(copy_activity_paradigm="sdp")
        modified = apply_preferences(pipeline, prefs)
        assert modified.tasks[0].target_format == "notebook"

    def test_lakeflow_connect_flag_set_for_eligible_copy(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        prefs = TranslationPreferences(use_lakeflow_connectors="lakeflow_connect")
        modified = apply_preferences(pipeline, prefs)
        assert modified.tasks[0].use_lakeflow_connector is True

    def test_lakeflow_connect_skipped_for_non_database_copy(self):
        pipeline = Pipeline(name="p", tasks=[_file_copy()])
        prefs = TranslationPreferences(use_lakeflow_connectors="lakeflow_connect")
        modified = apply_preferences(pipeline, prefs)
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
        prefs = TranslationPreferences(use_lakeflow_connectors="lakeflow_connect")
        modified = apply_preferences(pipeline, prefs)
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
        prefs = TranslationPreferences(use_lakeflow_connectors="lakeflow_connect")
        modified = apply_preferences(pipeline, prefs)
        assert modified.tasks[0].databricks_replacement == "auto_loader_file_notification"

    def test_per_task_override_wins(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy("c1"), _delta_copy("c2")])
        prefs = TranslationPreferences(
            copy_activity_paradigm="notebook",
            per_task={"c1": {"copy_activity_paradigm": "sdp"}},
        )
        modified = apply_preferences(pipeline, prefs)
        assert modified.tasks[0].target_format == "sdp"
        assert modified.tasks[1].target_format == "notebook"

    def test_preferences_attached_to_pipeline(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        prefs = TranslationPreferences(copy_activity_paradigm="sdp")
        modified = apply_preferences(pipeline, prefs)
        assert modified.translation_preferences is prefs

    def test_apply_preferences_does_not_mutate_input(self):
        original = Pipeline(name="p", tasks=[_delta_copy()])
        apply_preferences(original, TranslationPreferences(copy_activity_paradigm="sdp"))
        assert original.tasks[0].target_format is None
        assert original.translation_preferences is None

    def test_recurses_into_for_each_inner_activities(self):
        inner = _delta_copy("inner")
        for_each = ForEachActivity(
            **_make_base("loop"),
            items_expression="@activity('l').output.value",
            inner_activities=[inner],
        )
        pipeline = Pipeline(name="p", tasks=[for_each])
        prefs = TranslationPreferences(copy_activity_paradigm="sdp")
        modified = apply_preferences(pipeline, prefs)
        inner_after = modified.tasks[0].inner_activities[0]
        assert inner_after.target_format == "sdp"


class TestTranslationSession:
    def test_pending_returns_only_outstanding_questions(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        session = TranslationSession(pipeline=pipeline)
        first = session.pending()
        assert len(first.questions) > 0
        session.answer(QUESTION_COPY_ACTIVITY_PARADIGM, "sdp")
        ids_after = {q.question_id for q in session.pending().questions}
        assert QUESTION_COPY_ACTIVITY_PARADIGM not in ids_after

    def test_run_raises_when_questions_outstanding(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        session = TranslationSession(pipeline=pipeline)
        with pytest.raises(TranslationInputRequired) as info:
            session.run()
        assert info.value.pending.pipeline_name == "p"
        assert any(q.question_id == QUESTION_COPY_ACTIVITY_PARADIGM for q in info.value.pending.questions)

    def test_run_returns_modified_pipeline_when_complete(self):
        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        session = TranslationSession(pipeline=pipeline)
        pending = session.pending()
        answers = {q.question_id: q.default for q in pending.questions}
        session.answer_many(answers)
        modified = session.run()
        assert modified.translation_preferences is not None

    def test_answer_validates(self):
        session = TranslationSession(pipeline=Pipeline(name="p", tasks=[_delta_copy()]))
        with pytest.raises(ValueError):
            session.answer(QUESTION_COPY_ACTIVITY_PARADIGM, "yaml")

    def test_answer_many_is_atomic(self):
        session = TranslationSession(pipeline=Pipeline(name="p", tasks=[_delta_copy()]))
        with pytest.raises(ValueError):
            session.answer_many({QUESTION_COPY_ACTIVITY_PARADIGM: "sdp", "bogus": "x"})
        assert QUESTION_COPY_ACTIVITY_PARADIGM not in session._answers

    def test_find_question_returns_pending_question(self):
        session = TranslationSession(pipeline=Pipeline(name="p", tasks=[_delta_copy()]))
        found = session.find_question(QUESTION_COPY_ACTIVITY_PARADIGM)
        assert isinstance(found, TranslationQuestion)
        session.answer(QUESTION_COPY_ACTIVITY_PARADIGM, "sdp")
        assert session.find_question(QUESTION_COPY_ACTIVITY_PARADIGM) is None


class TestSerializationRoundtrip:
    def test_preferences_survive_json_roundtrip(self):
        from orchestra.bundler.dab_writer import pipeline_dict_to_ir
        from orchestra.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(
            name="p", tasks=[_delta_copy(), NotebookActivity(**_make_base("nb"), notebook_path="/Shared/x")]
        )
        prefs = TranslationPreferences(
            copy_activity_paradigm="sdp",
            non_databricks_task_compute="classic",
            use_lakeflow_connectors="lakeflow_connect",
            databricks_task_compute="serverless",
        )
        stamped = apply_preferences(pipeline, prefs)
        roundtripped, _ = pipeline_dict_to_ir(json.loads(json.dumps(_pipeline_to_dict(stamped), default=str)))
        assert roundtripped.translation_preferences.copy_activity_paradigm is CopyActivityParadigm.SDP
        assert roundtripped.tasks[0].target_format == "sdp"
        assert roundtripped.tasks[0].use_lakeflow_connector is True
        assert roundtripped.tasks[0].compute_mode == COMPUTE_MODE_CLASSIC_MULTI_NODE
        assert roundtripped.tasks[1].compute_mode == COMPUTE_MODE_SERVERLESS


class TestMigrationInputSession:
    def test_ingest_session_lists_expected_questions(self):
        from orchestra.adapter import MigrationInputSession

        session = MigrationInputSession(phase="ingest")
        ids = [q.question_id for q in session.pending().questions]
        assert ids == ["adf_source_path", "adf_resource_url", "output_dir"]

    def test_translate_session_lists_expected_questions(self):
        from orchestra.adapter import MigrationInputSession

        session = MigrationInputSession(phase="translate")
        ids = [q.question_id for q in session.pending().questions]
        assert "inventory_path" in ids
        assert "adf_source_path" in ids

    def test_prepare_session_lists_expected_questions(self):
        from orchestra.adapter import MigrationInputSession

        session = MigrationInputSession(phase="prepare")
        ids = {q.question_id for q in session.pending().questions}
        assert {"translation_report_path", "output_bundle_path", "catalog", "schema"} <= ids

    def test_unknown_phase_raises(self):
        from orchestra.adapter import MigrationInputSession, UnknownMigrationPhaseError

        with pytest.raises(UnknownMigrationPhaseError):
            MigrationInputSession(phase="bogus")

    def test_answer_records_value_and_drops_from_pending(self):
        from orchestra.adapter import MigrationInputSession

        session = MigrationInputSession(phase="ingest")
        session.answer("adf_source_path", "/Volumes/main/default/adf")
        ids = [q.question_id for q in session.pending().questions]
        assert "adf_source_path" not in ids

    def test_answer_rejects_unknown_question(self):
        from orchestra.adapter import MigrationInputSession

        session = MigrationInputSession(phase="ingest")
        with pytest.raises(ValueError, match="Unknown input question"):
            session.answer("not_a_field", "x")

    def test_collected_merges_answers_with_defaults(self):
        from orchestra.adapter import MigrationInputSession

        session = MigrationInputSession(phase="prepare")
        session.answer("translation_report_path", "/tmp/report.json")
        collected = session.collected()
        assert collected["translation_report_path"] == "/tmp/report.json"
        assert collected["catalog"] == "main"
        assert collected["schema"] == "default"

    def test_collected_omits_required_when_missing(self):
        from orchestra.adapter import MigrationInputSession

        session = MigrationInputSession(phase="ingest")
        collected = session.collected()
        assert "adf_source_path" not in collected
        assert collected["output_dir"] == "./orchestra_output/ingest"


class TestWorkspacePathsCli:
    def test_workspace_paths_detects_notebook_paths(self, tmp_path: Path):
        from orchestra.translator.engine import _pipeline_to_dict

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
        from orchestra.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        out = tmp_path / "ws.json"
        adapter_cli_main(["workspace-paths", str(report_path), "--out", str(out)])
        payload = json.loads(out.read_text())
        assert payload["paths"] == []
        assert payload["needs_auth"] is False

    def test_workspace_paths_suggests_host_from_databricks_linked_service(self, tmp_path: Path):
        from orchestra.translator.engine import _pipeline_to_dict

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
    def test_inputs_emits_ingest_questions(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        exit_code = adapter_cli_main(["inputs", "ingest"])
        assert exit_code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["phase"] == "ingest"
        ids = [q["question_id"] for q in payload["questions"]]
        assert ids == ["adf_source_path", "adf_resource_url", "output_dir"]

    def test_inputs_writes_to_file(self, tmp_path: Path):
        out = tmp_path / "questions.json"
        exit_code = adapter_cli_main(["inputs", "prepare", "--out", str(out)])
        assert exit_code == 0
        payload = json.loads(out.read_text())
        assert payload["phase"] == "prepare"
        ids = {q["question_id"] for q in payload["questions"]}
        assert "output_bundle_path" in ids


class TestCli:
    def test_inspect_emits_pending_questions(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        from orchestra.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        exit_code = adapter_cli_main(["inspect", str(report_path)])
        assert exit_code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["pipelines"][0]["pipeline_name"] == "p"
        question_ids = {q["question_id"] for q in payload["pipelines"][0]["questions"]}
        assert QUESTION_COPY_ACTIVITY_PARADIGM in question_ids

    def test_modify_stamps_preferences(self, tmp_path: Path):
        from orchestra.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        answers_path = tmp_path / "answers.json"
        answers_path.write_text(
            json.dumps(
                {
                    "copy_activity_paradigm": "sdp",
                    "non_databricks_task_compute": "classic",
                    "use_lakeflow_connectors": "lakeflow_connect",
                }
            )
        )
        out_path = tmp_path / "modified.json"
        exit_code = adapter_cli_main(["modify", str(report_path), str(answers_path), "--out", str(out_path)])
        assert exit_code == 0
        modified = json.loads(out_path.read_text())
        assert modified["translation_preferences"]["copy_activity_paradigm"] == "sdp"
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
        from orchestra.translator.engine import _pipeline_to_dict

        motif = _metadata_driven_motif()
        pipeline = Pipeline(name="p", tasks=[motif])
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        answers_path = tmp_path / "answers.json"
        answers_path.write_text(
            json.dumps(
                {
                    "metadata_driven_consolidate": "consolidate",
                    "metadata_driven_access": "yes",
                    "metadata_driven_size": "small",
                }
            )
        )
        lookup_values_path = tmp_path / "lookup_values.json"
        lookup_values_path.write_text(json.dumps([{"source_table": "orders"}]))
        out_path = tmp_path / "modified.json"
        exit_code = adapter_cli_main(
            [
                "modify",
                str(report_path),
                str(answers_path),
                "--lookup-values",
                str(lookup_values_path),
                "--out",
                str(out_path),
            ]
        )
        assert exit_code == 0
        modified = json.loads(out_path.read_text())
        motif_task = next(t for t in modified["tasks"] if t["motif_id"] == "metadata_driven_bulk_copy")
        assert motif_task["consolidate_metadata_driven"] is True
        assert motif_task["lookup_values"] == [{"source_table": "orders"}]

    def test_modify_rejects_invalid_answer(self, tmp_path: Path):
        from orchestra.translator.engine import _pipeline_to_dict

        pipeline = Pipeline(name="p", tasks=[_delta_copy()])
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_pipeline_to_dict(pipeline)))
        answers_path = tmp_path / "answers.json"
        answers_path.write_text(json.dumps({"copy_activity_paradigm": "yaml"}))
        out_path = tmp_path / "modified.json"
        exit_code = adapter_cli_main(["modify", str(report_path), str(answers_path), "--out", str(out_path)])
        assert exit_code == 2


class TestBundleOutput:
    def test_classic_copy_compute_emits_two_node_multi_node_cluster(self, tmp_path: Path):
        import yaml

        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        prefs = TranslationPreferences(non_databricks_task_compute="classic")
        stamped = apply_preferences(pipeline, prefs)
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

        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[WaitActivity(**_make_base("w"), wait_time_seconds=1)])
        prefs = TranslationPreferences(non_databricks_task_compute="classic")
        stamped = apply_preferences(pipeline, prefs)
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

        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        stamped = apply_preferences(pipeline, TranslationPreferences())
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        job_yml = yaml.safe_load((tmp_path / "resources" / "job.yml").read_text())
        assert "job_clusters" not in job_yml["resources"]["jobs"]["job"]

    def test_sdp_copy_emits_pyspark_pipelines_table_scaffold(self, tmp_path: Path):
        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        stamped = apply_preferences(pipeline, TranslationPreferences(copy_activity_paradigm="sdp"))
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        notebook_path = tmp_path / "src" / "notebooks" / "copy_a.py"
        body = notebook_path.read_text()
        assert "from pyspark import pipelines as sdp" in body
        assert "@sdp.table" in body
        assert "import dlt" not in body

    def test_lakeflow_connect_emits_pipeline_resource_and_no_notebook(self, tmp_path: Path):
        import yaml

        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        stamped = apply_preferences(pipeline, TranslationPreferences(use_lakeflow_connectors="lakeflow_connect"))
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        assert not (tmp_path / "src" / "notebooks" / "copy_a.py").exists()
        pipeline_yml = tmp_path / "resources" / "pipelines" / "copy_a_lfc.yml"
        assert pipeline_yml.exists()
        resource = yaml.safe_load(pipeline_yml.read_text())
        lfc = resource["resources"]["pipelines"]["copy_a_lfc"]
        assert lfc["name"] == "copy_a_lfc"
        assert lfc["ingestion_definition"]["connection_name"] == "orchestra_copy_a_connection"
        objects = lfc["ingestion_definition"]["objects"]
        assert objects[0]["table"]["destination_table"] == "raw.events"

    def test_lakeflow_connect_job_task_references_pipeline(self, tmp_path: Path):
        import yaml

        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        stamped = apply_preferences(pipeline, TranslationPreferences(use_lakeflow_connectors="lakeflow_connect"))
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        job_yml = yaml.safe_load((tmp_path / "resources" / "job.yml").read_text())
        task = job_yml["resources"]["jobs"]["job"]["tasks"][0]
        assert "notebook_task" not in task
        assert task["pipeline_task"]["pipeline_id"] == "${resources.pipelines.copy_a_lfc.id}"

    def test_metadata_driven_consolidate_question_surfaces_for_motif(self):
        pipeline = Pipeline(name="p", tasks=[_metadata_driven_motif()])
        ids = {q.question_id for q in gather_questions(pipeline).questions}
        assert QUESTION_METADATA_DRIVEN_CONSOLIDATE in ids

    def test_metadata_driven_followup_questions_gated_on_consolidate(self):
        pipeline = Pipeline(name="p", tasks=[_metadata_driven_motif()])
        first_pass = gather_questions(pipeline).questions
        ids = {q.question_id for q in first_pass}
        assert QUESTION_METADATA_DRIVEN_CONSOLIDATE in ids
        assert QUESTION_METADATA_DRIVEN_ACCESS not in ids
        assert QUESTION_METADATA_DRIVEN_SIZE not in ids
        assert QUESTION_METADATA_DRIVEN_LOOKUP_TOOL not in ids

        keep_pass = gather_questions(pipeline, answers={QUESTION_METADATA_DRIVEN_CONSOLIDATE: "keep"}).questions
        keep_ids = {q.question_id for q in keep_pass}
        assert QUESTION_METADATA_DRIVEN_ACCESS not in keep_ids

        consolidate_pass = gather_questions(
            pipeline, answers={QUESTION_METADATA_DRIVEN_CONSOLIDATE: "consolidate"}
        ).questions
        consolidate_ids = {q.question_id for q in consolidate_pass}
        assert QUESTION_METADATA_DRIVEN_ACCESS in consolidate_ids
        assert QUESTION_METADATA_DRIVEN_SIZE in consolidate_ids
        assert QUESTION_METADATA_DRIVEN_LOOKUP_TOOL not in consolidate_ids

    def test_metadata_driven_lookup_tool_question_gated_on_access(self):
        pipeline = Pipeline(name="p", tasks=[_metadata_driven_motif()])
        answers = {
            QUESTION_METADATA_DRIVEN_CONSOLIDATE: "consolidate",
            QUESTION_METADATA_DRIVEN_ACCESS: "yes",
        }
        pending = gather_questions(pipeline, answers=answers).questions
        ids = {q.question_id for q in pending}
        assert QUESTION_METADATA_DRIVEN_LOOKUP_TOOL in ids

    def test_modifier_consolidates_metadata_driven_when_size_is_small(self):
        pipeline = Pipeline(name="p", tasks=[_metadata_driven_motif()])
        prefs = TranslationPreferences(
            metadata_driven_consolidate="consolidate",
            metadata_driven_access="yes",
            metadata_driven_size="small",
        )
        modified = apply_preferences(pipeline, prefs)
        assert modified.tasks[0].consolidate_metadata_driven is True

    def test_modifier_does_not_consolidate_when_size_is_large(self):
        pipeline = Pipeline(name="p", tasks=[_metadata_driven_motif()])
        prefs = TranslationPreferences(
            metadata_driven_consolidate="consolidate",
            metadata_driven_access="yes",
            metadata_driven_size="large",
        )
        modified = apply_preferences(pipeline, prefs)
        assert modified.tasks[0].consolidate_metadata_driven is False

    def test_modifier_does_not_consolidate_when_access_is_no(self):
        pipeline = Pipeline(name="p", tasks=[_metadata_driven_motif()])
        prefs = TranslationPreferences(
            metadata_driven_consolidate="consolidate",
            metadata_driven_access="no",
            metadata_driven_size="small",
        )
        modified = apply_preferences(pipeline, prefs)
        assert modified.tasks[0].consolidate_metadata_driven is False

    def test_lakeflow_connector_type_question_suppressed_when_only_query_copies(self):
        pipeline = Pipeline(name="p", tasks=[_query_delta_copy("copy_q")])
        ids = {q.question_id for q in gather_questions(pipeline).questions}
        assert QUESTION_USE_LAKEFLOW_CONNECTORS in ids
        assert QUESTION_LAKEFLOW_CONNECTOR_TYPE not in ids

    def test_lakeflow_connector_type_question_suppressed_per_copy_eligibility(self):
        """Per-Copy eligibility determines connector type with no overlap.

        Table-based reads can only use CDC (no cursor column) and queries
        with a cursor can only use query-based, so the modifier picks the
        eligible connector per Copy and the prompt is suppressed.
        """
        pipeline = Pipeline(name="p", tasks=[_delta_copy("copy_a")])
        ids = {q.question_id for q in gather_questions(pipeline).questions}
        assert QUESTION_LAKEFLOW_CONNECTOR_TYPE not in ids

    def test_query_copy_routes_to_query_based_connector_regardless_of_preference(self, tmp_path: Path):
        import yaml

        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_query_delta_copy("copy_q")])
        prefs = TranslationPreferences(
            use_lakeflow_connectors="lakeflow_connect",
            lakeflow_connector_type="cdc",
        )
        stamped = apply_preferences(pipeline, prefs)
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

        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        prefs = TranslationPreferences(use_lakeflow_connectors="lakeflow_connect")
        stamped = apply_preferences(pipeline, prefs)
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        resource = yaml.safe_load((tmp_path / "resources" / "pipelines" / "copy_a_lfc.yml").read_text())
        objects = resource["resources"]["pipelines"]["copy_a_lfc"]["ingestion_definition"]["objects"]
        assert "table" in objects[0]
        assert "table_configuration" not in objects[0]

    def test_table_copy_with_query_based_preference_routes_to_cdc(self, tmp_path: Path):
        """LFC query-based requires a cursor column.  Table-based Copies have none.

        Per the Lakeflow Connect query-based-overview docs, the connector
        requires a cursor column to drive incremental ingestion.  When
        the user prefers query_based but the Copy is table-based (no
        query, no cursor candidate), the modifier honours the
        eligibility rules over the preference and routes to CDC.
        """
        import yaml

        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        prefs = TranslationPreferences(
            use_lakeflow_connectors="lakeflow_connect",
            lakeflow_connector_type="query_based",
        )
        stamped = apply_preferences(pipeline, prefs)
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        resource = yaml.safe_load((tmp_path / "resources" / "pipelines" / "copy_a_lfc.yml").read_text())
        objects = resource["resources"]["pipelines"]["copy_a_lfc"]["ingestion_definition"]["objects"]
        assert "table" in objects[0]
        assert "table_configuration" not in objects[0]

    def test_consolidated_metadata_driven_motif_emits_single_pipeline(self, tmp_path: Path):
        import dataclasses

        import yaml

        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

        motif = _metadata_driven_motif()
        pipeline = Pipeline(name="job", tasks=[motif])
        prefs = TranslationPreferences(
            metadata_driven_consolidate="consolidate",
            metadata_driven_access="yes",
            metadata_driven_size="medium",
        )
        stamped = apply_preferences(pipeline, prefs)
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

    def test_table_based_copy_with_query_based_preference_falls_back_to_cdc(self, tmp_path: Path):
        """Table-based reads have no cursor column, so query-based isn't eligible.

        When the user prefers query_based but the only eligible LFC connector
        for a table-based Copy is CDC, the modifier routes the Copy to CDC
        rather than emitting an unsupported query-based config.
        """
        import yaml

        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

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
                "connection": {"host": "ghansen-orchestra-test-sql.database.windows.net", "port": 1433},
            },
        )
        prefs = TranslationPreferences(
            use_lakeflow_connectors="lakeflow_connect",
            lakeflow_connector_type="query_based",
        )
        stamped = apply_preferences(Pipeline(name="job", tasks=[copy]), prefs)
        write_bundle(prepare_workflow(stamped), tmp_path)
        resource = yaml.safe_load((tmp_path / "resources" / "pipelines" / "copy_customers_lfc.yml").read_text())
        obj = resource["resources"]["pipelines"]["copy_customers_lfc"]["ingestion_definition"]["objects"][0]
        assert "table" in obj
        assert obj["table"]["destination_table"] == "customers"

    def test_lakeflow_connect_uses_resolved_host_from_linked_service(self, tmp_path: Path):
        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

        copy = CopyActivity(
            **_make_base("copy_a"),
            source_type="AzureSqlSource",
            sink_type="DeltaSink",
            sink_format="delta",
            sink_properties={"table": "orders"},
            source_properties={
                "linked_service_name": "LS_AzureSqlDb",
                "connection": {"host": "ghansen-orchestra-test-sql.database.windows.net", "port": 1433},
            },
        )
        prefs = TranslationPreferences(use_lakeflow_connectors="lakeflow_connect")
        stamped = apply_preferences(Pipeline(name="job", tasks=[copy]), prefs)
        write_bundle(prepare_workflow(stamped), tmp_path)
        body = (tmp_path / "src" / "setup" / "create_connections.py").read_text()
        assert "ghansen-orchestra-test-sql.database.windows.net" in body
        assert "1433" in body
        assert "PLACEHOLDER_HOST" not in body

    def test_lakeflow_connect_dedupes_connection_across_copies(self, tmp_path: Path):
        import yaml

        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

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
        prefs = TranslationPreferences(use_lakeflow_connectors="lakeflow_connect")
        stamped = apply_preferences(Pipeline(name="job", tasks=[copy_a, copy_b]), prefs)
        write_bundle(prepare_workflow(stamped), tmp_path)
        body = (tmp_path / "src" / "setup" / "create_connections.py").read_text()
        assert body.count("CREATE CONNECTION IF NOT EXISTS") == 1
        assert body.count("orchestra_LS_AzureSqlDb_connection") >= 1
        for pipeline_file in ("copy_a_lfc.yml", "copy_b_lfc.yml"):
            resource = yaml.safe_load((tmp_path / "resources" / "pipelines" / pipeline_file).read_text())
            key = pipeline_file.replace(".yml", "")
            assert resource["resources"]["pipelines"][key]["ingestion_definition"]["connection_name"] == (
                "orchestra_LS_AzureSqlDb_connection"
            )

    def test_lakeflow_connect_emits_connection_setup_notebook(self, tmp_path: Path):
        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(name="job", tasks=[_delta_copy("copy_a")])
        stamped = apply_preferences(pipeline, TranslationPreferences(use_lakeflow_connectors="lakeflow_connect"))
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        setup_notebook = tmp_path / "src" / "setup" / "create_connections.py"
        assert setup_notebook.exists()
        body = setup_notebook.read_text()
        assert "orchestra_copy_a_connection" in body
        assert "SQLSERVER" in body

    def test_serverless_existing_notebook_skips_default_cluster_bind(self, tmp_path: Path):
        import yaml

        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(
            name="job",
            tasks=[NotebookActivity(**_make_base("nb"), notebook_path="/Shared/existing")],
        )
        stamped = apply_preferences(pipeline, TranslationPreferences(databricks_task_compute="serverless"))
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        job_yml = yaml.safe_load((tmp_path / "resources" / "job.yml").read_text())
        task = job_yml["resources"]["jobs"]["job"]["tasks"][0]
        assert "job_cluster_key" not in task

    def test_existing_default_binds_to_default_cluster(self, tmp_path: Path):
        import yaml

        from orchestra.bundler.dab_writer import write_bundle
        from orchestra.preparer.workflow_preparer import prepare_workflow

        pipeline = Pipeline(
            name="job",
            tasks=[NotebookActivity(**_make_base("nb"), notebook_path="/Shared/existing")],
        )
        stamped = apply_preferences(pipeline, TranslationPreferences())
        workflow = prepare_workflow(stamped)
        write_bundle(workflow, tmp_path)
        job_yml = yaml.safe_load((tmp_path / "resources" / "job.yml").read_text())
        task = job_yml["resources"]["jobs"]["job"]["tasks"][0]
        assert task["job_cluster_key"] == "default_cluster"
