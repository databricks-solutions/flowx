"""Translates ADF Wait activities to Databricks WaitActivity IR."""

from __future__ import annotations

from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDefinitions
from flowx.models.ir import Activity, TranslationContext, WaitActivity
from flowx.translator.activity_translators.resolve import resolve_field_int


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translates a Wait activity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing.

    Returns:
        A :class:`WaitActivity` IR node.
    """
    type_properties = activity.type_properties or {}

    wait_time_seconds = resolve_field_int(type_properties.get("waitTimeInSeconds", 0), context, default=0)

    return WaitActivity(
        **base_kwargs,
        wait_time_seconds=wait_time_seconds,
    )
