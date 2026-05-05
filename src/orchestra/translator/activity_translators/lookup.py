"""Translates ADF Lookup activities to Databricks LookupActivity IR."""

from __future__ import annotations

from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDefinitions
from flowx.models.ir import Activity, LookupActivity, TranslationContext
from flowx.translator.activity_translators.resolve import resolve_field


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translates a Lookup activity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing datasets.

    Returns:
        A :class:`LookupActivity` IR node.
    """
    type_properties = activity.type_properties or {}

    source_raw = type_properties.get("source", {})
    source_type = source_raw.get("type")
    source_properties = {k: v for k, v in source_raw.items() if k != "type"} if source_raw else {}

    source_query_raw = (
        source_raw.get("query") or source_raw.get("sqlReaderQuery") or source_raw.get("sqlReaderStoredProcedureName")
    )
    source_query = resolve_field(source_query_raw, context) if source_query_raw is not None else None

    first_row_only = type_properties.get("firstRowOnly", True)

    return LookupActivity(
        **base_kwargs,
        source_type=source_type,
        source_properties=source_properties,
        first_row_only=first_row_only,
        source_query=source_query,
    )
