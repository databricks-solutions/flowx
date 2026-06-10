"""Unit tests for the shared helpers used across the preparer and bundler."""

from __future__ import annotations

from orchestra.models.dab import DabNotebook
from orchestra.models.ir import (
    Dependency,
    SetVariableActivity,
)
from orchestra.models.source_types import (
    FILE_SOURCE_TYPES,
    JDBC_SOURCE_TYPES,
    REST_SOURCE_TYPES,
)
from orchestra.preparer.activity_preparers.helpers import (
    build_notebook_activity_task,
    build_notebook_task_artifacts,
    make_jdbc_secrets,
    resolve_param_value,
)
from orchestra.preparer.workflow_preparer import (
    PreparedActivity,
    PreparedArtifacts,
    merge_prepared_artifacts,
)


class TestSourceTypeTaxonomy:
    def test_jdbc_and_rest_are_disjoint(self):
        assert JDBC_SOURCE_TYPES.isdisjoint(REST_SOURCE_TYPES)

    def test_jdbc_and_file_are_disjoint(self):
        assert JDBC_SOURCE_TYPES.isdisjoint(FILE_SOURCE_TYPES)

    def test_http_source_is_file_only_not_rest(self):
        """ADF HttpSource downloads a single file; RestSource is for paginated APIs."""
        assert "HttpSource" in FILE_SOURCE_TYPES
        assert "HttpSource" not in REST_SOURCE_TYPES
        assert "RestSource" in REST_SOURCE_TYPES

    def test_azure_sql_database_source_is_jdbc(self):
        """Regression: ``AzureSqlDatabaseSource`` (the v2 name) was missing once."""
        assert "AzureSqlDatabaseSource" in JDBC_SOURCE_TYPES
        assert "AzureSqlSource" in JDBC_SOURCE_TYPES


class TestResolveParamValue:
    def test_literal_passes_through(self):
        assert resolve_param_value("plain") == "plain"

    def test_dab_ref_resolves(self):
        assert resolve_param_value("@pipeline().RunId") == "{{job.run_id}}"

    def test_interpolated_resolves(self):
        result = resolve_param_value("prefix-@{pipeline().parameters.env}-suffix")
        assert result == "prefix-{{job.parameters.env}}-suffix"

    def test_notebook_code_returns_raw_for_manual_handling(self):
        """Expressions that resolve to Python code return raw text (manual handling)."""
        # Pick a format that intentionally does NOT map to a DAB dynamic
        # value so the resolver falls back to notebook_code.
        raw = "@formatDateTime(utcnow(), 'dd MMM yyyy')"
        result = resolve_param_value(raw)
        assert result == raw

    def test_formatdatetime_utcnow_known_format_resolves_to_dab_ref(self):
        """``formatDateTime(utcnow(), '<iso fmt>')`` short-circuits to a DAB ref."""
        result = resolve_param_value("@formatDateTime(utcnow(), 'yyyy-MM-dd')")
        assert result == "{{job.start_time.iso_date}}"


class TestBuildNotebookTaskArtifacts:
    def test_returns_task_dict_and_one_notebook(self):
        notebook_task, notebooks = build_notebook_task_artifacts(
            notebook_relative_path="notebooks/foo.py",
            notebook_content="# stub",
        )
        assert notebook_task == {"notebook_path": "../src/notebooks/foo.py"}
        assert len(notebooks) == 1
        assert notebooks[0].relative_path == "notebooks/foo.py"
        assert notebooks[0].content == "# stub"

    def test_includes_base_parameters_when_set(self):
        notebook_task, _ = build_notebook_task_artifacts(
            notebook_relative_path="notebooks/foo.py",
            notebook_content="# stub",
            base_parameters={"env": "dev"},
        )
        assert notebook_task["base_parameters"] == {"env": "dev"}

    def test_omits_base_parameters_when_none(self):
        notebook_task, _ = build_notebook_task_artifacts(
            notebook_relative_path="notebooks/foo.py",
            notebook_content="# stub",
            base_parameters=None,
        )
        assert "base_parameters" not in notebook_task


class TestBuildNotebookActivityTask:
    def test_combines_common_fields_and_notebook_task(self):
        activity = SetVariableActivity(
            name="Set Var",
            task_key="set_var",
            depends_on=[Dependency(task_key="upstream")],
            timeout_seconds=600,
            variable_name="x",
            variable_value="hello",
        )
        task, notebooks = build_notebook_activity_task(
            activity,
            notebook_relative_path="notebooks/set_var.py",
            notebook_content="# stub",
            base_parameters={"variable_name": "x"},
        )
        assert task["task_key"] == "set_var"
        assert task["depends_on"] == [{"task_key": "upstream"}]
        assert task["timeout_seconds"] == 600
        assert task["notebook_task"] == {
            "notebook_path": "../src/notebooks/set_var.py",
            "base_parameters": {"variable_name": "x"},
        }
        assert len(notebooks) == 1


class TestMakeJdbcSecrets:
    def test_emits_url_and_password_pair(self):
        secrets = make_jdbc_secrets(
            scope_name="my_pipeline",
            source_type="AzureSqlSource",
            activity_name="LookupConfig",
            role="lookup",
        )
        assert len(secrets) == 2
        assert {s.key for s in secrets} == {"jdbc-url", "jdbc-password"}
        assert all(s.scope == "my_pipeline" for s in secrets)
        assert "AzureSqlSource lookup in activity 'LookupConfig'" in secrets[0].value_source

    def test_role_changes_descriptions_only(self):
        source_secrets = make_jdbc_secrets(
            scope_name="s", source_type="SqlServerSource", activity_name="Copy", role="source"
        )
        sink_secrets = make_jdbc_secrets(
            scope_name="s", source_type="SqlServerSource", activity_name="Copy", role="sink"
        )
        assert {s.key for s in source_secrets} == {s.key for s in sink_secrets}
        assert "source in activity" in source_secrets[0].value_source
        assert "sink in activity" in sink_secrets[0].value_source


class TestPreparedArtifacts:
    def test_default_is_empty(self):
        artifacts = PreparedArtifacts()
        assert artifacts.notebooks == ()
        assert artifacts.secrets == ()
        assert artifacts.setup_tasks == ()
        assert artifacts.inner_workflows == ()

    def test_is_immutable(self):
        artifacts = PreparedArtifacts()
        # frozen dataclass: cannot reassign fields
        try:
            artifacts.notebooks = (DabNotebook(relative_path="x", content=""),)  # type: ignore[misc]
        except Exception:
            return
        raise AssertionError("PreparedArtifacts must be frozen")


class TestMergePreparedArtifacts:
    def test_does_not_mutate_input(self):
        original = PreparedArtifacts()
        prepared = PreparedActivity(
            task={},
            notebooks=[DabNotebook(relative_path="a.py", content="# a")],
        )
        merged = merge_prepared_artifacts(original, prepared)

        assert merged is not original
        assert original.notebooks == ()
        assert len(merged.notebooks) == 1
        assert merged.notebooks[0].relative_path == "a.py"

    def test_extends_each_collection(self):
        from orchestra.models.dab import SecretInstruction, SetupTask

        prepared = PreparedActivity(
            task={"task_key": "t"},
            notebooks=[DabNotebook(relative_path="a.py", content="# a")],
            secrets=[SecretInstruction(scope="s", key="k", value_source="v")],
            setup_tasks=[SetupTask(type="volume", config={})],
        )
        artifacts = merge_prepared_artifacts(PreparedArtifacts(), prepared)
        assert len(artifacts.notebooks) == 1
        assert len(artifacts.secrets) == 1
        assert len(artifacts.setup_tasks) == 1

    def test_chains_correctly_across_multiple_activities(self):
        artifacts = PreparedArtifacts()
        for i in range(3):
            prepared = PreparedActivity(
                task={"task_key": f"t{i}"},
                notebooks=[DabNotebook(relative_path=f"{i}.py", content="")],
            )
            artifacts = merge_prepared_artifacts(artifacts, prepared)
        assert [nb.relative_path for nb in artifacts.notebooks] == ["0.py", "1.py", "2.py"]


class TestExpressionParserHandlerFactories:
    """The ``addDays`` / ``addHours`` / ``addMinutes`` / ``addSeconds`` family
    is generated from a single factory; same for ``getFutureTime`` /
    ``getPastTime``.  Sanity-check each handler still resolves end-to-end."""

    def test_add_unit_handlers_emit_correct_timedelta_keyword(self):
        from orchestra.models.ir import TranslationContext
        from orchestra.parser.expression_parser import resolve_expression

        cases = [
            ("@addDays('2024-01-01', 7)", "timedelta(days=7)"),
            ("@addHours('2024-01-01', 5)", "timedelta(hours=5)"),
            ("@addMinutes('2024-01-01', 30)", "timedelta(minutes=30)"),
            ("@addSeconds('2024-01-01', 90)", "timedelta(seconds=90)"),
        ]
        for expression, fragment in cases:
            result = resolve_expression(expression, TranslationContext())
            assert result is not None
            assert result.kind == "notebook_code"
            assert fragment in result.value

    def test_now_offset_handlers_emit_correct_sign(self):
        from orchestra.models.ir import TranslationContext
        from orchestra.parser.expression_parser import resolve_expression

        future = resolve_expression("@getFutureTime(1, 'Day')", TranslationContext())
        past = resolve_expression("@getPastTime(1, 'Day')", TranslationContext())
        assert future is not None and past is not None
        assert "+ timedelta" in future.value
        assert "- timedelta" in past.value
