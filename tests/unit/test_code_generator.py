"""Unit tests for code_generator.py notebook generators.

Verifies each generator produces valid Python that passes ast.parse().
"""

from __future__ import annotations

import ast
from typing import Any

import pytest

from orchestra.models.ir import (
    AppendVariableActivity,
    CopyActivity,
    DeleteActivity,
    FilterActivity,
    LookupActivity,
    SetVariableActivity,
    WaitActivity,
    WebActivity,
)
from orchestra.preparer.code_generator import (
    generate_append_variable_notebook,
    generate_copy_notebook,
    generate_delete_notebook,
    generate_filter_notebook,
    generate_lookup_notebook,
    generate_set_variable_notebook,
    generate_wait_notebook,
    generate_web_activity_notebook,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_base(name: str = "test", task_key: str = "test") -> dict[str, Any]:
    return {
        "name": name,
        "task_key": task_key,
        "description": None,
        "timeout_seconds": None,
        "max_retries": None,
        "min_retry_interval_millis": None,
        "depends_on": None,
        "cluster": None,
    }


def _assert_valid_python(content: str, label: str = "notebook"):
    """Strip Databricks magic comments and assert the remaining code parses."""
    lines = []
    for line in content.split("\n"):
        stripped = line.lstrip()
        if stripped.startswith("# MAGIC") or stripped.startswith("# COMMAND"):
            continue
        if stripped == "# Databricks notebook source":
            continue
        lines.append(line)
    python_code = "\n".join(lines)
    try:
        ast.parse(python_code)
    except SyntaxError as exc:
        pytest.fail(f"{label} has invalid Python syntax: {exc}\n---\n{python_code}")


# ---------------------------------------------------------------------------
# Copy notebook generator
# ---------------------------------------------------------------------------


class TestGenerateCopyNotebook:
    def test_file_source_auto_loader(self):
        """File-based source generates Auto Loader (cloudFiles) notebook."""
        activity = CopyActivity(
            **_make_base("CopyBlob", "copy_blob"),
            source_type="DelimitedTextSource",
            sink_type="DeltaSink",
            source_properties={"path": "/mnt/raw/data.csv"},
            sink_properties={"table": "bronze.raw_data"},
        )
        content = generate_copy_notebook(activity)
        _assert_valid_python(content, "copy_blob (file source)")
        assert "cloudFiles" in content
        assert "csv" in content.lower() or "cloudFiles.format" in content

    def test_sql_source_jdbc(self):
        """SQL-based source generates JDBC read notebook."""
        activity = CopyActivity(
            **_make_base("CopySql", "copy_sql"),
            source_type="AzureSqlSource",
            sink_type="DeltaSink",
            source_properties={"sqlReaderQuery": "SELECT * FROM orders"},
            sink_properties={"table": "bronze.orders"},
        )
        content = generate_copy_notebook(activity, scope="copy_sql")
        _assert_valid_python(content, "copy_sql (jdbc source)")
        assert "jdbc" in content
        assert "dbutils.secrets.get" in content

    def test_rest_source(self):
        """REST-based source generates HTTP fetch notebook."""
        activity = CopyActivity(
            **_make_base("CopyRest", "copy_rest"),
            source_type="RestSource",
            sink_type="DeltaSink",
            source_properties={"url": "https://api.example.com/data"},
            sink_properties={"table": "bronze.api_data"},
        )
        content = generate_copy_notebook(activity)
        _assert_valid_python(content, "copy_rest (rest source)")
        assert "requests" in content

    def test_expression_query_jdbc(self):
        """JDBC source with an ADF expression query generates parameterized notebook."""
        activity = CopyActivity(
            **_make_base("CopyExpr", "copy_expr"),
            source_type="AzureSqlSource",
            sink_type="DeltaSink",
            source_properties={
                "sqlReaderQuery": {
                    "type": "Expression",
                    "value": "@concat('SELECT * FROM ', item().schema_name, '.', item().table_name)",
                },
            },
            sink_properties={"table": "bronze.dynamic"},
        )
        content = generate_copy_notebook(activity, scope="copy_expr")
        _assert_valid_python(content, "copy_expr (expression query)")
        assert "item" in content
        assert "json" in content

    def test_generic_fallback(self):
        """Unknown source type generates generic Spark read/write."""
        activity = CopyActivity(
            **_make_base("CopyGeneric", "copy_generic"),
            source_type="SomeUnknownSource",
            sink_type="DeltaSink",
        )
        content = generate_copy_notebook(activity)
        _assert_valid_python(content, "copy_generic (unknown source)")
        assert "spark.read" in content

    def test_csv_file_format_inferred(self):
        """DelimitedTextSource infers CSV file format."""
        activity = CopyActivity(
            **_make_base("CopyCsv", "copy_csv"),
            source_type="DelimitedTextSource",
            sink_type="DeltaSink",
            source_properties={},
        )
        content = generate_copy_notebook(activity)
        assert '"csv"' in content


# ---------------------------------------------------------------------------
# Lookup notebook generator
# ---------------------------------------------------------------------------


class TestGenerateLookupNotebook:
    def test_db_source_with_task_values(self):
        """DB source lookup generates JDBC notebook with task value output."""
        activity = LookupActivity(
            **_make_base("LookupConfig", "lookup_config"),
            source_type="AzureSqlSource",
            first_row_only=True,
            source_query="SELECT TOP 1 * FROM config",
        )
        content = generate_lookup_notebook(activity, scope="lookup_config")
        _assert_valid_python(content, "lookup_config (DB source)")
        assert "jdbc" in content
        assert "dbutils.secrets.get" in content
        assert "dbutils.jobs.taskValues.set" in content

    def test_non_db_source_spark_sql(self):
        """Non-DB source lookup uses Spark SQL."""
        activity = LookupActivity(
            **_make_base("LookupSpark", "lookup_spark"),
            source_type="ParquetSource",
            first_row_only=False,
            source_query="SELECT * FROM table_list",
        )
        content = generate_lookup_notebook(activity)
        _assert_valid_python(content, "lookup_spark (Spark SQL)")
        assert "spark.sql" in content
        assert "jdbc" not in content

    def test_query_with_triple_quotes_does_not_break_notebook(self):
        """Regression: a query containing ``\\"\\"\\"`` used to break the embedded string."""
        activity = LookupActivity(
            **_make_base("LookupQQ", "lookup_qq"),
            source_type="AzureSqlSource",
            first_row_only=True,
            source_query='SELECT \'""">\' AS q FROM dual',
        )
        content = generate_lookup_notebook(activity, scope="s")
        _assert_valid_python(content, "lookup_qq (triple-quote in query)")

    def test_dynamic_query_with_widget_passthrough(self):
        """Pre-translated queries (containing dbutils.widgets.get) are spliced as code."""
        activity = LookupActivity(
            **_make_base("LookupDyn", "lookup_dyn"),
            source_type="AzureSqlSource",
            first_row_only=True,
            source_query="\"SELECT * FROM \" + dbutils.widgets.get('table')",
        )
        content = generate_lookup_notebook(activity, scope="s")
        _assert_valid_python(content, "lookup_dyn")
        assert "dbutils.widgets.get('table')" in content

    def test_unparseable_dynamic_query_falls_back_to_literal(self):
        """A pre-translated string that doesn't parse is embedded as a string literal."""
        activity = LookupActivity(
            **_make_base("LookupBad", "lookup_bad"),
            source_type="AzureSqlSource",
            first_row_only=True,
            source_query="dbutils.widgets.get('x' BROKEN",
        )
        content = generate_lookup_notebook(activity, scope="s")
        _assert_valid_python(content, "lookup_bad")
        # The broken expression is embedded as a quoted string literal
        # ("dbutils.widgets.get(..."), not as executable Python.
        assert 'query = "dbutils.widgets.get(' in content

    def test_file_lookup_coerces_expression_dict_path_components(self):
        """C-37 (LSC4-001): folder_path / file_name that arrive as ADF
        expression dicts must not crash ``_assemble_file_lookup_source_path``
        with AttributeError on ``.strip('/')``."""
        activity = LookupActivity(
            **_make_base("Read_Cfg", "read_cfg"),
            source_type="JsonSource",
            first_row_only=False,
            source_properties={
                "dataset_type": "Json",
                "container": "configs",
                "folder_path": {"value": "@pipeline().parameters.folder", "type": "Expression"},
                "file_name": "tables.json",
            },
        )
        # Must not raise.
        content = generate_lookup_notebook(activity)
        _assert_valid_python(content, "read_cfg (expression-dict folder)")

    def test_file_lookup_rewrites_https_to_abfss(self):
        """C-37 (LSC4-003): AzureBlobFS https URLs lower to abfss:// in
        the generated lookup notebook so the read can succeed on a
        Databricks cluster."""
        activity = LookupActivity(
            **_make_base("Read_Cfg", "read_cfg"),
            source_type="JsonSource",
            first_row_only=False,
            source_properties={
                "dataset_type": "Json",
                "container": "configext",
                "folder_path": "lookups",
                "file_name": "tables.json",
                "linked_service_url": "https://datahub01textcfdls.dfs.core.windows.net",
            },
        )
        content = generate_lookup_notebook(activity)
        _assert_valid_python(content, "read_cfg (abfss rewrite)")
        assert "abfss://configext@datahub01textcfdls.dfs.core.windows.net" in content
        assert "https://datahub01textcfdls" not in content

    def test_file_source_lookup_emits_spark_read(self):
        """Change lookup-file-dataset-support (P0): JsonSource + firstRowOnly=False."""
        activity = LookupActivity(
            **_make_base("Read_Configuration", "read_configuration"),
            source_type="JsonSource",
            first_row_only=False,
            source_properties={
                "dataset_type": "Json",
                "container": "configs",
                "folder_path": "lookup",
                "file_name": "tables.json",
                "multiLineJson": True,
            },
        )
        content = generate_lookup_notebook(activity)
        _assert_valid_python(content, "read_configuration (file source)")
        # File-source branch: no spark.sql(''), uses spark.read.format().load().
        assert "spark.sql" not in content
        assert "spark.read.format('json')" in content
        assert '.option("multiline", "true")' in content
        assert "source_path" in content


# ---------------------------------------------------------------------------
# Web activity notebook generator
# ---------------------------------------------------------------------------


class TestGenerateWebActivityNotebook:
    def test_static_headers(self):
        """Web activity with plain string headers."""
        activity = WebActivity(
            **_make_base("GetApi", "get_api"),
            url="https://api.example.com",
            method="GET",
            headers={"Accept": "application/json", "X-Custom": "value"},
        )
        content = generate_web_activity_notebook(activity)
        _assert_valid_python(content, "get_api (static headers)")
        assert "requests" in content
        assert "application/json" in content

    def test_expression_headers(self):
        """Web activity with expression dict headers generates resolved code."""
        activity = WebActivity(
            **_make_base("PostApi", "post_api"),
            url="https://api.example.com/submit",
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": {
                    "type": "Expression",
                    "value": "@concat('Bearer ', pipeline().parameters.Token)",
                },
            },
            body={"key": "value"},
        )
        content = generate_web_activity_notebook(activity)
        _assert_valid_python(content, "post_api (expression headers)")
        assert "requests" in content
        assert "headers" in content

    def test_auth_block_service_principal(self):
        """ServicePrincipal auth generates secret-based Bearer token."""
        activity = WebActivity(
            **_make_base("AuthApi", "auth_api"),
            url="https://api.example.com",
            method="GET",
            authentication={"type": "ServicePrincipal", "resource": "https://api.example.com"},
        )
        content = generate_web_activity_notebook(activity, scope="auth_api")
        _assert_valid_python(content, "auth_api (ServicePrincipal)")
        assert "auth-credential" in content
        assert "Bearer" in content

    def test_auth_block_msi_raises_not_implemented(self):
        """LSC3-002: MSI / ManagedServiceIdentity has no static secret to
        read; the generated notebook must raise NotImplementedError pointing
        at SETUP.md, not emit a fake dbutils.secrets.get('auth-credential')."""
        for auth_type in ("MSI", "ManagedServiceIdentity"):
            activity = WebActivity(
                **_make_base(f"MsiApi_{auth_type}", f"msi_api_{auth_type.lower()}"),
                url="https://api.example.com",
                method="GET",
                authentication={"type": auth_type, "resource": "https://management.azure.com"},
            )
            content = generate_web_activity_notebook(activity, scope=f"msi_api_{auth_type.lower()}")
            _assert_valid_python(content, f"msi_api ({auth_type})")
            assert "auth-credential" not in content
            assert "NotImplementedError" in content
            assert "SETUP.md" in content

    def test_auth_block_basic(self):
        """Basic auth generates username/password secret retrieval."""
        activity = WebActivity(
            **_make_base("BasicApi", "basic_api"),
            url="https://api.example.com",
            method="GET",
            authentication={"type": "Basic"},
        )
        content = generate_web_activity_notebook(activity, scope="basic_api")
        _assert_valid_python(content, "basic_api (Basic auth)")
        assert "auth-username" in content
        assert "base64" in content

    def test_post_with_body(self):
        """POST method includes body block."""
        activity = WebActivity(
            **_make_base("PostData", "post_data"),
            url="https://api.example.com",
            method="POST",
            body={"payload": "data"},
        )
        content = generate_web_activity_notebook(activity)
        _assert_valid_python(content, "post_data (POST)")
        assert "body" in content
        assert "data" in content


# ---------------------------------------------------------------------------
# Delete notebook generator
# ---------------------------------------------------------------------------


class TestGenerateDeleteNotebook:
    def test_delete_generates_valid_notebook(self):
        activity = DeleteActivity(
            **_make_base("DeleteFiles", "delete_files"),
            dataset_name="ds_staging",
            folder_path="/mnt/staging/old",
            recursive=True,
        )
        content = generate_delete_notebook(activity)
        _assert_valid_python(content, "delete_files")
        assert "dbutils.fs.rm" in content
        assert "ds_staging" in content

    def test_delete_no_folder_path(self):
        activity = DeleteActivity(
            **_make_base("DeleteDs", "delete_ds"),
            dataset_name="ds_cleanup",
            recursive=False,
        )
        content = generate_delete_notebook(activity)
        _assert_valid_python(content, "delete_ds (no folder)")


# ---------------------------------------------------------------------------
# Set variable notebook generator
# ---------------------------------------------------------------------------


class TestGenerateSetVariableNotebook:
    def test_literal_value(self):
        """Literal value is read from widget parameter."""
        activity = SetVariableActivity(
            **_make_base("SetLiteral", "set_literal"),
            variable_name="status",
            variable_value="completed",
            value_kind="literal",
        )
        content = generate_set_variable_notebook(activity)
        _assert_valid_python(content, "set_literal")
        assert "dbutils.widgets.get" in content
        assert "dbutils.jobs.taskValues.set" in content

    def test_dab_ref_value(self):
        """DAB ref value is read from widget parameter."""
        activity = SetVariableActivity(
            **_make_base("SetEnv", "set_env"),
            variable_name="env",
            variable_value="{{job.parameters.environment}}",
            value_kind="dab_ref",
        )
        content = generate_set_variable_notebook(activity)
        _assert_valid_python(content, "set_env (dab_ref)")
        assert 'dbutils.widgets.get("value")' in content

    def test_notebook_code_value(self):
        """Notebook code value embeds Python directly, no widget parameter."""
        activity = SetVariableActivity(
            **_make_base("SetDate", "set_date"),
            variable_name="runDate",
            variable_value="datetime.now(timezone.utc).strftime('%Y-%m-%d')",
            value_kind="notebook_code",
            notebook_code="datetime.now(timezone.utc).strftime('%Y-%m-%d')",
            notebook_imports=["from datetime import datetime, timezone"],
        )
        content = generate_set_variable_notebook(activity)
        _assert_valid_python(content, "set_date (notebook_code)")
        assert "strftime" in content
        assert "from datetime import" in content
        # Should NOT reference widgets.get("value") for code values
        assert 'dbutils.widgets.get("value")' not in content


# ---------------------------------------------------------------------------
# Wait notebook generator
# ---------------------------------------------------------------------------


class TestGenerateWaitNotebook:
    def test_wait_generates_valid_notebook(self):
        activity = WaitActivity(
            **_make_base("Wait60", "wait_60"),
            wait_time_seconds=60,
        )
        content = generate_wait_notebook(activity)
        _assert_valid_python(content, "wait_60")
        assert "time.sleep" in content
        assert "60" in content


# ---------------------------------------------------------------------------
# Filter notebook generator
# ---------------------------------------------------------------------------


class TestGenerateFilterNotebook:
    def test_filter_generates_valid_notebook(self):
        activity = FilterActivity(
            **_make_base("FilterItems", "filter_items"),
            items_expression="@variables('myList')",
            condition_expression="@not(empty(item()))",
        )
        content = generate_filter_notebook(activity)
        _assert_valid_python(content, "filter_items")
        assert "dbutils.jobs.taskValues.set" in content
        assert "filter" in content.lower() or "filtered" in content

    def test_resolved_condition_emits_inline_comprehension(self):
        """When the translator pre-resolves the condition, no eval() call appears."""
        activity = FilterActivity(
            **_make_base("FilterActive", "filter_active"),
            items_expression="{{tasks.Lookup.values.result}}",
            condition_expression="@equals(item().status, 'active')",
            condition_code="(item.get('status') == 'active')",
        )
        content = generate_filter_notebook(activity)
        _assert_valid_python(content, "filter_active")
        assert _contains_eval_call(content) is False, "resolved condition path must not contain an eval() call"
        assert "filtered = [item for item in items if (item.get('status') == 'active')]" in content
        assert "json.loads(items_expression)" in content
        assert "NotImplementedError" not in content

    def test_unresolved_condition_falls_back_to_placeholder(self):
        """When condition_code is None, the notebook is a TODO placeholder (no eval call)."""
        activity = FilterActivity(
            **_make_base("FilterMystery", "filter_mystery"),
            items_expression="@variables('myList')",
            condition_expression="@some_function_we_dont_translate(item())",
            condition_code=None,
        )
        content = generate_filter_notebook(activity)
        _assert_valid_python(content, "filter_mystery")
        assert _contains_eval_call(content) is False, "placeholder path must not contain an eval() call"
        assert "NotImplementedError" in content
        assert "Implement filter condition" in content

    def test_no_eval_in_either_branch_with_quotes_in_expressions(self):
        """ADF expressions containing single quotes don't break the notebook syntax."""
        for resolved_code in (None, "(item.get('region') == 'us-west-1')"):
            activity = FilterActivity(
                **_make_base("FilterQ", "filter_q"),
                items_expression="@activity('GetList').output.value",
                condition_expression="@equals(item().region, 'us-west-1')",
                condition_code=resolved_code,
            )
            content = generate_filter_notebook(activity)
            _assert_valid_python(content, "filter_q")
            assert _contains_eval_call(content) is False


def _contains_eval_call(source: str) -> bool:
    """Returns True if the parsed Python source contains a real ``eval(...)`` call."""
    import ast

    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "eval":
            return True
    return False


# ---------------------------------------------------------------------------
# Append variable notebook generator
# ---------------------------------------------------------------------------


class TestGenerateAppendVariableNotebook:
    def test_literal_append(self):
        """Literal value generates widget-based append."""
        activity = AppendVariableActivity(
            **_make_base("AppendLog", "append_log"),
            variable_name="logEntries",
            append_value="step1 done",
            value_kind="literal",
        )
        content = generate_append_variable_notebook(activity)
        _assert_valid_python(content, "append_log (literal)")
        assert "dbutils.jobs.taskValues.set" in content
        assert "append" in content

    def test_notebook_code_append(self):
        """Notebook code generates embedded Python."""
        activity = AppendVariableActivity(
            **_make_base("AppendTS", "append_ts"),
            variable_name="timestamps",
            append_value="datetime.now(timezone.utc).isoformat()",
            value_kind="notebook_code",
            notebook_code="datetime.now(timezone.utc).isoformat()",
            notebook_imports=["from datetime import datetime, timezone"],
        )
        content = generate_append_variable_notebook(activity)
        _assert_valid_python(content, "append_ts (notebook_code)")
        assert "isoformat" in content
        assert "from datetime import" in content
        # Should NOT reference widgets.get("value") for code values
        assert 'dbutils.widgets.get("value")' not in content
