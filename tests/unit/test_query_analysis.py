"""Tests for the Lakeflow Connect query analyzer."""

from __future__ import annotations

import pytest

from orchestra.translator.query_analysis import QueryAnalysis, analyze_copy_query, dialect_for_source_type


class TestParseabilityRejections:
    @pytest.mark.parametrize(
        "query, reason_fragment",
        [
            ("SELECT * FROM dbo.orders JOIN dbo.customers ON orders.cid = customers.id", "JOIN"),
            ("SELECT * FROM dbo.orders INNER JOIN dbo.customers", "JOIN"),
            ("SELECT customer_id, COUNT(*) FROM dbo.orders GROUP BY customer_id", "aggregate"),
            ("SELECT customer_id, COUNT(*) FROM dbo.orders GROUP BY customer_id", "GROUP BY"),
            ("SELECT * FROM dbo.orders UNION SELECT * FROM dbo.orders_archive", "UNION"),
            ("SELECT MAX(updated_at) FROM dbo.orders", "aggregate"),
            ("SELECT id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id) FROM dbo.orders", "window"),
            ("SELECT DISTINCT customer_id FROM dbo.orders", "DISTINCT"),
            ("SELECT * FROM dbo.orders WHERE id IN (SELECT id FROM dbo.active_customers)", "subquery"),
        ],
    )
    def test_disqualifying_constructs(self, query: str, reason_fragment: str):
        analysis = analyze_copy_query(query)
        assert analysis.parseable is False
        assert any(reason_fragment.lower() in reason.lower() for reason in analysis.rejection_reasons)

    def test_empty_query_rejected(self):
        analysis = analyze_copy_query("")
        assert analysis.parseable is False
        assert analysis.rejection_reasons == ["empty query"]

    def test_none_query_rejected(self):
        analysis = analyze_copy_query(None)
        assert analysis.parseable is False

    def test_select_with_expression_rejected(self):
        analysis = analyze_copy_query("SELECT id, UPPER(name) AS upper_name FROM dbo.users")
        assert analysis.parseable is False
        assert any("expression" in reason.lower() or "alias" in reason.lower() for reason in analysis.rejection_reasons)


class TestCursorAndRowFilter:
    def test_range_predicate_yields_cursor(self):
        analysis = analyze_copy_query("SELECT * FROM dbo.orders WHERE order_date >= '2024-01-01'")
        assert analysis.parseable is True
        assert analysis.cursor_column == "order_date"
        assert analysis.row_filter is None

    def test_between_predicate_yields_cursor(self):
        analysis = analyze_copy_query("SELECT * FROM dbo.orders WHERE order_date BETWEEN '2024-01-01' AND '2024-02-01'")
        assert analysis.parseable is True
        assert analysis.cursor_column == "order_date"

    def test_range_plus_static_filter_extracts_row_filter(self):
        analysis = analyze_copy_query(
            "SELECT * FROM dbo.orders WHERE order_date >= '2024-01-01' AND active = 1 AND region = 'US'"
        )
        assert analysis.parseable is True
        assert analysis.cursor_column == "order_date"
        assert analysis.row_filter == "active = 1 AND region = 'US'"

    def test_static_filter_only_has_no_cursor_column(self):
        analysis = analyze_copy_query("SELECT * FROM dbo.orders WHERE active = 1")
        assert analysis.parseable is True
        assert analysis.cursor_column is None
        assert analysis.row_filter == "active = 1"

    def test_no_where_has_no_cursor_no_filter(self):
        analysis = analyze_copy_query("SELECT id, name FROM dbo.users")
        assert analysis.parseable is True
        assert analysis.cursor_column is None
        assert analysis.row_filter is None


class TestIncludeColumns:
    def test_select_star_omits_include_columns(self):
        analysis = analyze_copy_query("SELECT * FROM dbo.orders")
        assert analysis.include_columns is None

    def test_explicit_columns_populated(self):
        analysis = analyze_copy_query("SELECT order_id, customer_id, order_date, amount FROM dbo.orders")
        assert analysis.include_columns == ["order_id", "customer_id", "order_date", "amount"]


class TestRealisticAdfQuery:
    def test_orders_recent_query_with_dateadd(self):
        query = (
            "SELECT order_id, customer_id, order_date, amount FROM dbo.orders "
            "WHERE order_date >= DATEADD(day, -7, GETUTCDATE())"
        )
        analysis = analyze_copy_query(query)
        assert analysis.parseable is True
        assert analysis.cursor_column == "order_date"
        assert analysis.include_columns == ["order_id", "customer_id", "order_date", "amount"]
        assert analysis.row_filter is None

    def test_watermark_incremental_query_has_cursor_and_filter(self):
        query = "SELECT * FROM dbo.orders WHERE updated_at > '2024-01-01' AND tenant_id = 42"
        analysis = analyze_copy_query(query)
        assert analysis.parseable is True
        assert analysis.cursor_column == "updated_at"
        assert analysis.row_filter == "tenant_id = 42"


class TestReturnsAnalysisInstance:
    def test_returns_query_analysis_instance(self):
        analysis = analyze_copy_query("SELECT * FROM dbo.orders WHERE id > 100")
        assert isinstance(analysis, QueryAnalysis)


class TestCrossDialect:
    @pytest.mark.parametrize("dialect", ["tsql", "mysql", "postgres", "oracle", "snowflake", None])
    def test_range_predicate_parses_across_dialects(self, dialect: str | None):
        analysis = analyze_copy_query(
            "SELECT order_id, customer_id FROM orders WHERE order_date >= '2024-01-01'",
            dialect=dialect,
        )
        assert analysis.parseable is True
        assert analysis.cursor_column == "order_date"
        assert analysis.include_columns == ["order_id", "customer_id"]

    def test_mysql_backtick_identifiers(self):
        analysis = analyze_copy_query(
            "SELECT `order_id`, `amount` FROM `orders` WHERE `order_date` > '2024-01-01'",
            dialect="mysql",
        )
        assert analysis.parseable is True
        assert analysis.cursor_column == "order_date"
        assert analysis.include_columns == ["order_id", "amount"]

    def test_postgres_schema_qualified_table_still_parses(self):
        analysis = analyze_copy_query(
            "SELECT * FROM public.orders WHERE order_date >= '2024-01-01'",
            dialect="postgres",
        )
        assert analysis.parseable is True
        assert analysis.cursor_column == "order_date"


class TestQualifiedColumnRejected:
    def test_table_qualified_select_column_is_rejected(self):
        analysis = analyze_copy_query(
            "SELECT o.order_id, o.amount FROM dbo.orders o WHERE o.order_date >= '2024-01-01'"
        )
        assert analysis.parseable is False
        assert any("qualified" in reason.lower() for reason in analysis.rejection_reasons)


class TestDialectMapping:
    @pytest.mark.parametrize(
        "source_type, expected_dialect",
        [
            ("AzureSqlSource", "tsql"),
            ("SqlServerSource", "tsql"),
            ("AzureSqlMISource", "tsql"),
            ("MySqlSource", "mysql"),
            ("AzureMySqlSource", "mysql"),
            ("PostgreSqlSource", "postgres"),
            ("PostgreSqlV2Source", "postgres"),
            ("AzurePostgreSqlSource", "postgres"),
            ("OracleSource", "oracle"),
            ("SnowflakeSource", "snowflake"),
            ("Teradata", "teradata"),
        ],
    )
    def test_known_adf_sources_map_to_dialect(self, source_type: str, expected_dialect: str):
        assert dialect_for_source_type(source_type) == expected_dialect

    @pytest.mark.parametrize("source_type", [None, "", "UnknownSource", "AzureBlobStorage", "Parquet"])
    def test_unknown_sources_return_none(self, source_type: str | None):
        assert dialect_for_source_type(source_type) is None
