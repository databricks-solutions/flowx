"""SQL analysis that drives Lakeflow Connect eligibility for Copy queries.

The Lakeflow Connect query-based connector accepts a structured set of
fields under ``query_based_connector_config`` -- ``cursor``,
``row_filter``, ``include_columns``, ``exclude_columns`` -- not a raw
SQL string.  When orchestra is asked to migrate a Copy activity whose
source carries a ``sqlReaderQuery``, the translator runs the query
through :func:`analyze_copy_query` to decide whether the query
decomposes cleanly into those fields and whether it contains a range
predicate the connector can adopt as its cursor column.

Parsing is delegated to ``sqlglot`` so the analyzer can inspect a real
AST instead of guessing at structure with regex.  ADF Copy queries
target many source databases, so :func:`analyze_copy_query` accepts an
optional ``dialect`` (matching ``sqlglot``'s dialect names: ``tsql``,
``mysql``, ``postgres``, ``oracle``, ``snowflake``, ...).  The
translator picks the dialect from the ADF source type via
:func:`dialect_for_source_type`; when no dialect maps, sqlglot's
generic parser is used.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

_DIALECT_BY_SOURCE_TYPE: Final[tuple[tuple[str, str], ...]] = (
    ("sqlserver", "tsql"),
    ("azuresql", "tsql"),
    ("synapsesql", "tsql"),
    ("sqlmi", "tsql"),
    ("mysql", "mysql"),
    ("azuremysql", "mysql"),
    ("postgre", "postgres"),
    ("azurepostgre", "postgres"),
    ("oracle", "oracle"),
    ("snowflake", "snowflake"),
    ("teradata", "teradata"),
    ("db2", "db2"),
)

_RANGE_COMPARISONS: Final[tuple[type[exp.Expression], ...]] = (exp.GT, exp.GTE, exp.LT, exp.LTE)
_TOP_LEVEL_SET_OPS: Final[tuple[type[exp.Expression], ...]] = (exp.Union, exp.Intersect, exp.Except)


@dataclass(frozen=True, slots=True, kw_only=True)
class QueryAnalysis:
    """Structured result of inspecting a Copy activity's source SQL.

    Attributes:
        parseable: ``True`` when the query elements decompose into the
            Lakeflow Connect query-based connector's structured fields
            (``cursor``, ``row_filter``, ``include_columns``,
            ``exclude_columns``).  ``False`` when the query contains
            constructs the connector cannot represent.
        cursor_column: Name of the column the connector should use as
            its cursor for incremental ingestion.  ``None`` when the
            query has no range predicate.
        row_filter: Static predicate the connector should apply on
            every run (everything from the WHERE clause that does not
            participate in the cursor predicate).  ``None`` when the
            query has no static predicates.
        include_columns: Explicit column list from the SELECT clause,
            or ``None`` when the query selects all columns.
        exclude_columns: Reserved for future use; ADF queries do not
            express exclusions directly so this remains ``None`` for
            now.
        rejection_reasons: Human-readable reasons the analyzer rejected
            the query when ``parseable`` is ``False``.  Empty when the
            query parses cleanly.
    """

    parseable: bool
    cursor_column: str | None = None
    row_filter: str | None = None
    include_columns: list[str] | None = None
    exclude_columns: list[str] | None = None
    rejection_reasons: list[str] = field(default_factory=list)


def dialect_for_source_type(source_type: str | None) -> str | None:
    """Maps an ADF source-side activity type to a ``sqlglot`` dialect name.

    Args:
        source_type: The Copy activity's source ``type`` field as it
            appears in the ADF JSON (e.g. ``"AzureSqlSource"``,
            ``"MySqlSource"``, ``"PostgreSqlV2Source"``).

    Returns:
        A ``sqlglot`` dialect name when the source type contains a
        recognised database token; ``None`` otherwise.  ``None`` falls
        through to sqlglot's generic parser, which is permissive enough
        to handle the simple ``SELECT ... FROM ... WHERE`` queries ADF
        typically emits even when the dialect is unknown.
    """
    if not source_type:
        return None
    lowered = source_type.lower()
    for token, dialect in _DIALECT_BY_SOURCE_TYPE:
        if token in lowered:
            return dialect
    return None


def analyze_copy_query(query: str | None, *, dialect: str | None = None) -> QueryAnalysis:
    """Analyses a Copy activity's source SQL for Lakeflow Connect fit.

    Args:
        query: SQL text the Copy activity will execute against the
            source database.  ``None`` and empty strings short-circuit
            to ``parseable=False`` with a single rejection reason.
        dialect: Optional ``sqlglot`` dialect name describing the
            source database (e.g. ``"tsql"``, ``"mysql"``,
            ``"postgres"``).  Used both to parse the query and to
            render the ``row_filter`` string back out.  ``None`` falls
            through to sqlglot's generic parser.

    Returns:
        A :class:`QueryAnalysis` describing whether the query decomposes
        into the LFC query-based connector fields and what cursor /
        filter / column extraction the connector should configure.
    """
    if not query or not query.strip():
        return QueryAnalysis(parseable=False, rejection_reasons=["empty query"])

    try:
        tree = sqlglot.parse_one(query, dialect=dialect)
    except ParseError as error:
        return QueryAnalysis(parseable=False, rejection_reasons=[f"unparseable SQL: {error}"])

    if isinstance(tree, _TOP_LEVEL_SET_OPS):
        return QueryAnalysis(parseable=False, rejection_reasons=["contains UNION / INTERSECT / EXCEPT"])
    if not isinstance(tree, exp.Select):
        return QueryAnalysis(parseable=False, rejection_reasons=["not a SELECT statement"])

    rejection_reasons = _disqualifying_constructs(tree)
    if rejection_reasons:
        return QueryAnalysis(parseable=False, rejection_reasons=rejection_reasons)

    include_columns, column_rejection = _extract_select_columns(tree)
    if column_rejection:
        return QueryAnalysis(parseable=False, rejection_reasons=[column_rejection])

    where = tree.args.get("where")
    if not isinstance(where, exp.Where):
        return QueryAnalysis(parseable=True, include_columns=include_columns)

    cursor_column, row_filter = _analyze_where(where, dialect)
    return QueryAnalysis(
        parseable=True,
        cursor_column=cursor_column,
        row_filter=row_filter,
        include_columns=include_columns,
    )


def _disqualifying_constructs(tree: exp.Select) -> list[str]:
    """Returns rejection reasons for any unsupported SQL construct in *tree*.

    Args:
        tree: A parsed top-level ``SELECT`` expression.

    Returns:
        List of human-readable rejection reasons.  Empty when the
        query is free of disqualifying constructs.
    """
    reasons: list[str] = []
    if tree.find(exp.Join):
        reasons.append("contains JOIN")
    if tree.args.get("group"):
        reasons.append("contains GROUP BY / HAVING")
    if tree.args.get("having"):
        reasons.append("contains GROUP BY / HAVING")
    if tree.find(exp.AggFunc):
        reasons.append("contains aggregate function")
    if tree.find(exp.Window):
        reasons.append("contains window function (OVER)")
    if tree.args.get("distinct"):
        reasons.append("contains DISTINCT")
    if any(node is not tree for node in tree.find_all(exp.Select)):
        reasons.append("contains subquery")
    return reasons


def _extract_select_columns(tree: exp.Select) -> tuple[list[str] | None, str | None]:
    """Returns the explicit column list from a SELECT clause.

    Args:
        tree: A parsed top-level ``SELECT`` expression.

    Returns:
        Tuple of ``(include_columns, rejection_reason)``.  When the
        query selects all columns with ``*``, ``include_columns`` is
        ``None`` and ``rejection_reason`` is ``None``.  When the column
        list contains expressions, aliases, or qualified names the
        connector cannot accept, ``include_columns`` is ``None`` and
        ``rejection_reason`` carries a short explanation.
    """
    items = tree.expressions
    if not items:
        return None, None
    if len(items) == 1 and isinstance(items[0], exp.Star):
        return None, None
    columns: list[str] = []
    for item in items:
        if isinstance(item, exp.Star):
            return None, "SELECT clause mixes * with explicit columns LFC cannot represent"
        if not isinstance(item, exp.Column):
            return None, "SELECT clause contains expressions or aliases LFC cannot represent"
        if item.table:
            return None, "SELECT clause contains qualified columns LFC cannot represent"
        columns.append(item.name)
    return columns, None


def _analyze_where(where: exp.Where, dialect: str | None) -> tuple[str | None, str | None]:
    """Splits a WHERE clause into a cursor column and a row filter.

    Args:
        where: The ``exp.Where`` node off the parsed SELECT.
        dialect: ``sqlglot`` dialect to use when rendering the
            ``row_filter`` back to SQL text.

    Returns:
        Tuple of ``(cursor_column, row_filter)``.  ``cursor_column`` is
        the leftmost column participating in a range (``>``, ``<``,
        ``>=``, ``<=``) or ``BETWEEN`` predicate, or ``None`` when no
        such predicate is present.  ``row_filter`` is the remaining
        AND-clauses rendered back to SQL and joined by ``AND``, or
        ``None`` when no other predicates remain.
    """
    body = where.this
    if body is None:
        return None, None
    clauses = list(_flatten_and(body))
    cursor_column: str | None = None
    remaining: list[exp.Expr] = []
    for clause in clauses:
        column = _cursor_candidate(clause)
        if column and cursor_column is None:
            cursor_column = column
            continue
        remaining.append(clause)
    if not remaining:
        return cursor_column, None
    row_filter = " AND ".join(clause.sql(dialect=dialect) for clause in remaining)
    return cursor_column, row_filter


def _cursor_candidate(clause: exp.Expr) -> str | None:
    """Returns the cursor column name if *clause* is a range or BETWEEN predicate.

    Args:
        clause: One predicate fragment from a flattened WHERE clause.

    Returns:
        The bare column name on the LHS when *clause* is a range
        comparison or ``BETWEEN`` against a single column.  ``None``
        otherwise (including for equality predicates, ``IN`` lists, and
        any predicate whose LHS is not a bare column).
    """
    if isinstance(clause, _RANGE_COMPARISONS) or isinstance(clause, exp.Between):
        lhs = clause.this
        if isinstance(lhs, exp.Column) and not lhs.table:
            return lhs.name
    return None


def _flatten_and(node: exp.Expr) -> list[exp.Expr]:
    """Flattens an ``AND`` tree into a list of leaf predicates.

    Args:
        node: The expression rooted at the WHERE clause body.

    Returns:
        List of predicates in left-to-right order, treating nested
        ``AND`` nodes as transparent.  ``OR`` and other operators are
        returned as single leaves so the caller treats them as part of
        the row filter.
    """
    if isinstance(node, exp.And):
        return _flatten_and(node.left) + _flatten_and(node.right)
    return [node]
