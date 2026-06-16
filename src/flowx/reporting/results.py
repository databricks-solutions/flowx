"""Persist per-pipeline migration coverage to a Unity Catalog table.

Each migration run is stamped with a single UUID ``run_id`` (shared by every row of
the run), ``run_date`` (``CURRENT_TIMESTAMP()``), and ``run_by`` (``CURRENT_USER()``),
so coverage can be tracked over time and per user.  Rows are written via the
Databricks SDK Statement Execution API against a SQL warehouse (auto-detected when one
is not supplied).
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any

from flowx.reporting.coverage import COVERAGE_METRIC_COLUMNS, build_coverage_rows

logger = logging.getLogger(__name__)

# Full results-table schema: run metadata first, then the per-pipeline metrics.
# ``run_date`` / ``run_by`` are populated by SQL functions (not Python literals).
_METRIC_SQL_TYPES: dict[str, str] = {
    "pipeline": "STRING",
    "activities": "INT",
    "datasets": "INT",
    "linked_services": "INT",
    "collapsible_patterns": "INT",
    "databricks_native_activities": "INT",
    "control_flow_activities": "INT",
    "other_activities": "INT",
    "deterministic_activities": "INT",
    "agentic_activities": "INT",
    "unsupported_activities": "INT",
    "coverage_pct": "DOUBLE",
    "complexity_score": "INT",
    "complexity_size": "STRING",
}

RESULTS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "STRING"),
    ("run_date", "TIMESTAMP"),
    ("run_by", "STRING"),
    *((col, _METRIC_SQL_TYPES[col]) for col in COVERAGE_METRIC_COLUMNS),
)

_STRING_METRICS: frozenset[str] = frozenset({"pipeline", "complexity_size"})


def _sql_str(value: Any) -> str:
    """Renders a value as a single-quoted SQL string literal (quotes doubled)."""
    return "'" + str(value).replace("'", "''") + "'"


def _metric_value_sql(column: str, value: Any) -> str:
    """Renders one metric column value as a SQL literal."""
    if column in _STRING_METRICS:
        return _sql_str(value)
    if column == "coverage_pct":
        return repr(float(value or 0))
    return str(int(value or 0))


def build_create_table_sql(table_fqn: str) -> str:
    """Returns ``CREATE TABLE IF NOT EXISTS`` for the results table."""
    cols = ",\n  ".join(f"{name} {sql_type}" for name, sql_type in RESULTS_COLUMNS)
    return f"CREATE TABLE IF NOT EXISTS {table_fqn} (\n  {cols}\n)"


def build_insert_sql(table_fqn: str, rows: list[dict[str, Any]], run_id: str) -> str:
    """Returns a single multi-row ``INSERT`` stamping run metadata onto every row.

    ``run_id`` is a literal (same for the whole run); ``run_date`` and ``run_by`` use
    the ``CURRENT_TIMESTAMP()`` / ``CURRENT_USER()`` SQL functions so the workspace
    records the actual write time and identity.
    """
    column_names = ", ".join(name for name, _ in RESULTS_COLUMNS)
    tuples: list[str] = []
    for row in rows:
        metrics = ", ".join(_metric_value_sql(col, row.get(col)) for col in COVERAGE_METRIC_COLUMNS)
        tuples.append(f"({_sql_str(run_id)}, CURRENT_TIMESTAMP(), CURRENT_USER(), {metrics})")
    values = ",\n  ".join(tuples)
    return f"INSERT INTO {table_fqn} ({column_names}) VALUES\n  {values}"


def resolve_warehouse_id(client: Any, warehouse_id: str | None = None) -> str:
    """Resolves a SQL warehouse id, preferring RUNNING then serverless when auto-detecting.

    Args:
        client: A ``WorkspaceClient``.
        warehouse_id: An explicit id; returned as-is when provided.

    Returns:
        The resolved warehouse id.

    Raises:
        RuntimeError: When no warehouse is available to auto-detect.
    """
    if warehouse_id:
        return warehouse_id
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError(
            "No SQL warehouse found to write results. Pass --warehouse-id with a warehouse "
            "that can write to the target table."
        )

    def _rank(warehouse: Any) -> tuple[int, int]:
        state = str(getattr(getattr(warehouse, "state", None), "value", getattr(warehouse, "state", "")) or "")
        is_running = 1 if state.upper() == "RUNNING" else 0
        type_value = str(
            getattr(getattr(warehouse, "warehouse_type", None), "value", getattr(warehouse, "warehouse_type", "")) or ""
        )
        is_serverless = (
            1 if getattr(warehouse, "enable_serverless_compute", False) or "SERVERLESS" in type_value.upper() else 0
        )
        return (is_running, is_serverless)

    best = max(warehouses, key=_rank)
    logger.info("Auto-selected SQL warehouse '%s' (%s).", getattr(best, "name", "?"), best.id)
    return best.id


def _execute(client: Any, statement: str, warehouse_id: str) -> None:
    """Runs a SQL statement via the Statement Execution API; raises on failure."""
    resp = client.statement_execution.execute_statement(
        statement=statement, warehouse_id=warehouse_id, wait_timeout="50s"
    )
    state = getattr(getattr(resp, "status", None), "state", None)
    state_str = str(getattr(state, "value", state) or "")
    if state_str.upper() not in ("", "SUCCEEDED"):
        err = getattr(getattr(resp, "status", None), "error", None)
        raise RuntimeError(f"Statement failed ({state_str}): {getattr(err, 'message', err)}")


def write_results(
    metadata_dir: Path,
    table_fqn: str,
    warehouse_id: str | None = None,
    client: Any | None = None,
) -> tuple[str, int]:
    """Writes per-pipeline coverage rows for one run to *table_fqn*.

    Creates the table if needed, then inserts one row per pipeline stamped with a
    fresh ``run_id`` (and SQL ``run_date`` / ``run_by``).

    Args:
        metadata_dir: The migration ``metadata/`` directory.
        table_fqn: Target table as ``catalog.schema.table``.
        warehouse_id: Optional SQL warehouse id; auto-detected when omitted.
        client: Optional ``WorkspaceClient`` (injected in tests).

    Returns:
        ``(run_id, row_count)``.
    """
    rows = build_coverage_rows(metadata_dir)
    if not rows:
        logger.warning("No pipelines found in %s; nothing to record.", metadata_dir)
        return "", 0

    if client is None:
        from flowx.preparer.workspace_downloader import _get_workspace_client

        client = _get_workspace_client()

    resolved_wh = resolve_warehouse_id(client, warehouse_id)
    run_id = str(uuid.uuid4())
    _execute(client, build_create_table_sql(table_fqn), resolved_wh)
    _execute(client, build_insert_sql(table_fqn, rows, run_id), resolved_wh)
    logger.info("Recorded %d pipeline rows to %s (run_id=%s).", len(rows), table_fqn, run_id)
    return run_id, len(rows)
