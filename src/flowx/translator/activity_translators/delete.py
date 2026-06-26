"""Translates ADF Delete activities to Databricks DeleteActivity IR."""

from __future__ import annotations

from typing import Any

from flowx.models.adf_ast import AdfActivity, AdfDefinitions
from flowx.models.ir import Activity, DeleteActivity, TranslationContext
from flowx.translator.activity_translators.resolve import resolve_field


def translate(
    activity: AdfActivity,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AdfDefinitions,
) -> Activity:
    """Translates a Delete activity.

    Args:
        activity: The ADF activity AST node.
        base_kwargs: Common fields (name, task_key, timeout, retries, depends_on, cluster).
        context: Current translation context.
        definitions: Full ADF definitions for cross-referencing datasets.

    Returns:
        A :class:`DeleteActivity` IR node.
    """
    type_properties = activity.type_properties or {}

    dataset_name = ""
    if activity.inputs:
        dataset_name = activity.inputs[0].reference_name

    recursive = type_properties.get("recursive", True)
    folder_path_raw = (
        type_properties.get("dataset", {}).get("folderPath")
        if isinstance(type_properties.get("dataset"), dict)
        else None
    )
    folder_path = resolve_field(folder_path_raw, context) if folder_path_raw is not None else None

    store_settings = type_properties.get("storeSettings", {})
    wildcard_folder_path_raw = store_settings.get("wildcardFolderPath")
    wildcard_folder_path = (
        resolve_field(wildcard_folder_path_raw, context) if wildcard_folder_path_raw is not None else None
    )

    effective_folder = folder_path or wildcard_folder_path

    return DeleteActivity(
        **base_kwargs,
        dataset_name=dataset_name,
        folder_path=effective_folder,
        recursive=recursive,
    )
