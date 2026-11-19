"""Writes a validation run to a Unity Catalog table: one row per object per iteration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from flowx.models.validation import ValidationIteration
from flowx.reporting.results import execute_statement, resolve_warehouse_id, sql_string
from flowx.validate.agentic import load_iterations, load_report

VALIDATION_RESULTS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "STRING"),
    ("run_date", "TIMESTAMP"),
    ("run_by", "STRING"),
    ("iteration", "INT"),
    ("object_id", "STRING"),
    ("object_kind", "STRING"),
    ("verdict", "STRING"),
    ("score", "INT"),
    ("finding_count", "INT"),
    ("blocker_count", "INT"),
    ("major_count", "INT"),
    ("reviewer", "STRING"),
    ("ruleset_version", "STRING"),
    ("content_hash", "STRING"),
)


def build_rows(iterations: list[ValidationIteration]) -> list[dict[str, Any]]:
    """Returns one row per stamp per iteration, without the SQL-filled ``run_date`` and ``run_by``."""
    return [
        {
            "run_id": record.run_id,
            "iteration": record.iteration,
            "object_id": stamp.object_id,
            "object_kind": stamp.object_kind,
            "verdict": stamp.verdict,
            "score": stamp.score,
            "finding_count": len(stamp.findings),
            "blocker_count": sum(finding.severity == "blocker" for finding in stamp.findings),
            "major_count": sum(finding.severity == "major" for finding in stamp.findings),
            "reviewer": stamp.reviewer,
            "ruleset_version": stamp.ruleset_version,
            "content_hash": stamp.content_hash,
        }
        for record in iterations
        for stamp in record.stamps
    ]


def _value_sql(column: str, value: Any) -> str:
    return (
        str(int(value))
        if column in {"iteration", "score", "finding_count", "blocker_count", "major_count"}
        else sql_string(value)
    )


def build_create_table_sql(table_fqn: str) -> str:
    """Returns ``CREATE TABLE IF NOT EXISTS`` for the validation results table."""
    columns = ",\n  ".join(f"{name} {sql_type}" for name, sql_type in VALIDATION_RESULTS_COLUMNS)
    return f"CREATE TABLE IF NOT EXISTS {table_fqn} (\n  {columns}\n)"


def build_insert_sql(table_fqn: str, rows: list[dict[str, Any]]) -> str:
    """Returns one multi-row ``INSERT``, filling ``run_date`` and ``run_by`` from SQL functions."""
    names = [name for name, _ in VALIDATION_RESULTS_COLUMNS]
    values = [
        ", ".join(
            "CURRENT_TIMESTAMP()"
            if name == "run_date"
            else "CURRENT_USER()"
            if name == "run_by"
            else _value_sql(name, row[name])
            for name in names
        )
        for row in rows
    ]
    return f"INSERT INTO {table_fqn} ({', '.join(names)}) VALUES\n  " + ",\n  ".join(f"({row})" for row in values)


def write_validation_results(
    output_dir: Path, table_fqn: str, warehouse_id: str | None = None, client: Any | None = None
) -> tuple[str, int]:
    """Replaces the current run's rows in *table_fqn* with one row per object per iteration.

    Args:
        output_dir: The migration output folder.
        table_fqn: Target table as ``catalog.schema.table``.
        warehouse_id: SQL warehouse id; detected when omitted.
        client: ``WorkspaceClient``; created when omitted.

    Returns:
        ``(run_id, row_count)``.

    Raises:
        ValueError: When no validation iteration has been recorded.
    """
    report = load_report(output_dir)
    rows = build_rows(load_iterations(output_dir))
    if report is None or not rows:
        raise ValueError("No validation iterations recorded; run validate first.")
    if client is None:
        from flowx.preparer.workspace_downloader import _get_workspace_client

        client = _get_workspace_client()
    warehouse = resolve_warehouse_id(client, warehouse_id)
    execute_statement(client, build_create_table_sql(table_fqn), warehouse)
    execute_statement(client, f"DELETE FROM {table_fqn} WHERE run_id = {sql_string(report.run_id)}", warehouse)
    execute_statement(client, build_insert_sql(table_fqn, rows), warehouse)
    return report.run_id, len(rows)
