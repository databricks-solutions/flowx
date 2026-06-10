"""Translates ADF Lookup activities to Databricks LookupActivity IR."""

from __future__ import annotations

import re
from typing import Any

from orchestra.models.adf_ast import AdfActivity, AdfDefinitions
from orchestra.models.ir import Activity, LookupActivity, TranslationContext
from orchestra.translator.activity_translators.resolve import resolve_field


def _dataset_parameter_scope(activity: AdfActivity, context: TranslationContext) -> dict[str, str]:
    """Resolve the Lookup dataset reference's ``parameters`` binding.

    C-47 (LSC5-001): a file-source dataset's ``folderPath`` / ``fileName``
    can reference its own parameters via ``dataset().X``.  The Lookup's
    ``typeProperties.dataset.parameters`` block binds each dataset parameter
    (e.g. ``digitalCase``) to a pipeline-scoped value (e.g.
    ``@pipeline().parameters.digitalCaseCode``).  Resolve each binding
    against the pipeline ``TranslationContext`` so ``dataset().digitalCase``
    can be substituted with the resolved ``{{job.parameters.X}}`` ref / literal.

    Returns a ``{dataset_param_name: resolved_value}`` map (empty when the
    reference carries no parameters).
    """
    type_props = activity.type_properties or {}
    ref = type_props.get("dataset")
    if not isinstance(ref, dict):
        return {}
    params = ref.get("parameters")
    if not isinstance(params, dict):
        return {}
    return {name: resolve_field(value, context) for name, value in params.items()}


def _substitute_dataset_refs(value: Any, scope: dict[str, str]) -> Any:
    """Replace ``dataset().X`` occurrences in *value* with the resolved binding.

    C-47 (LSC5-001): without this, ``folderPath`` of
    ``@toLower(dataset().digitalCase)`` and ``fileName`` of
    ``@dataset().fileName`` pass through verbatim and the code generator
    bakes a literal broken ``abfss://.../@toLower(dataset().digitalCase)``
    default path that ``spark.read`` cannot load.

    Only string values carrying a ``dataset().`` reference are rewritten;
    everything else is returned unchanged.
    """
    if not isinstance(value, str) or "dataset(" not in value:
        return value
    result = value
    for name, resolved in scope.items():
        # dataset().X and dataset()['X'] / dataset()["X"] forms.
        result = re.sub(
            r"dataset\(\)\s*(?:\.\s*" + re.escape(name) + r"\b|\[\s*['\"]" + re.escape(name) + r"['\"]\s*\])",
            resolved,
            result,
        )
    return result


def _unwrap_expression(value: Any) -> Any:
    """Unwrap a ``{"value": X, "type": "Expression"}`` dict-wrapper.

    C-37 (LSC4-001): folder_path / file_name on file-source datasets
    sometimes ship as the ADF expression dict shape.  Without unwrapping,
    downstream code (notably ``_assemble_file_lookup_source_path``) calls
    ``.strip('/')`` on the dict and crashes with AttributeError, which
    has bundled 4 pipelines into "empty bundle directory" outcomes.
    """
    if isinstance(value, dict) and "value" in value and value.get("type") == "Expression":
        return value["value"]
    return value


# File-source dataset types that a Lookup can read directly.  Keeping
# this list local avoids tugging the broader copy translator in.
_FILE_DATASET_TYPES: frozenset[str] = frozenset(
    {"Json", "Parquet", "DelimitedText", "Avro", "Orc", "Excel", "Xml", "Binary"}
)


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

    # Resolve the Lookup's dataset reference (lookup-translator-ignores-dataset-reference):
    # typeProperties.dataset is the canonical place for ADF; activity.inputs
    # is the legacy fall-back used by the loader for flattened activity shapes.
    dataset_ref = _resolve_lookup_dataset(activity, definitions)
    if dataset_ref is not None:
        dataset_props = dataset_ref["properties"]
        dataset_type = dataset_ref["type"]
        type_props = dataset_props.get("typeProperties") or {}
        location = type_props.get("location") or {}
        if dataset_type in _FILE_DATASET_TYPES:
            source_properties.setdefault("dataset_type", dataset_type)
            # Stash the dataset path components so the code generator can
            # build the right spark.read call.  Avoid pulling in the full
            # copy translator dataset-path machinery — we only need the
            # raw container + folder + filename to surface to the user.
            # C-37 (LSC4-001): unwrap any ADF expression dict shapes so
            # downstream code can treat these as plain strings.
            container = _unwrap_expression(
                location.get("container") or location.get("fileSystem") or location.get("bucketName")
            )
            folder = _unwrap_expression(location.get("folderPath"))
            filename = _unwrap_expression(location.get("fileName"))
            # C-47 (LSC5-001): substitute dataset().X param refs using the
            # Lookup dataset reference's parameter bindings, then resolve the
            # result so the path default is a real literal / interpolated
            # {{job.parameters.X}} string rather than a verbatim dataset()
            # expression the code generator would bake into a broken path.
            ds_scope = _dataset_parameter_scope(activity, context)
            if ds_scope:
                if isinstance(folder, str) and folder:
                    folder = resolve_field(_substitute_dataset_refs(folder, ds_scope), context)
                if isinstance(filename, str) and filename:
                    filename = resolve_field(_substitute_dataset_refs(filename, ds_scope), context)
            if container:
                source_properties.setdefault("container", container)
            if folder:
                source_properties.setdefault("folder_path", folder)
            if filename:
                source_properties.setdefault("file_name", filename)
            # Forward type-specific format options (multiline, encoding, etc.)
            # so the generator can pass them as spark.read.option(...).
            format_settings = type_props.get("formatSettings") or {}
            if isinstance(format_settings, dict):
                for key in ("multiLineJson", "filePattern"):
                    if key in format_settings:
                        source_properties.setdefault(key, format_settings[key])
            # LSC3-005: surface the linked service URL when present so the
            # generator can assemble the abfss:// path for AzureBlobFS / ADLS
            # backed file datasets.
            ls_url = dataset_props.get("linked_service_url")
            if ls_url:
                source_properties.setdefault("linked_service_url", ls_url)

    return LookupActivity(
        **base_kwargs,
        source_type=source_type,
        source_properties=source_properties,
        first_row_only=first_row_only,
        source_query=source_query,
    )


def _resolve_lookup_dataset(
    activity: AdfActivity,
    definitions: AdfDefinitions,
) -> dict[str, Any] | None:
    """Resolves the Lookup's dataset reference to its full dataset record.

    Args:
        activity: The ADF Lookup activity AST node.
        definitions: Full ADF definitions for dataset lookup.

    Returns:
        Dict with keys ``type`` and ``properties`` describing the bound
        dataset, or ``None`` when no dataset is referenced.
    """
    type_props = activity.type_properties or {}
    ref = type_props.get("dataset")
    dataset_name: str | None = None
    if isinstance(ref, dict):
        dataset_name = ref.get("referenceName") or ref.get("dataset", {}).get("referenceName")
    if dataset_name is None and activity.inputs:
        dataset_name = activity.inputs[0].reference_name
    if dataset_name is None:
        return None
    # LSC3-005: ADF identifiers are case-insensitive; tolerate casing drift
    # between the pipeline's dataset reference and the source JSON filename.
    dataset = definitions.get_dataset(dataset_name)
    if dataset is None:
        return None
    properties = dict(dataset.properties or {})
    # Thread linkedService typeProperties.url through onto the properties so
    # the lookup notebook can assemble the abfss:// file path for file-source
    # datasets where the URL is only known on the linked service.
    linked_service = definitions.get_linked_service(dataset.linked_service_name)
    if linked_service is not None:
        ls_props = linked_service.properties or {}
        ls_type_props = ls_props.get("typeProperties") if isinstance(ls_props, dict) else None
        if isinstance(ls_type_props, dict) and "url" in ls_type_props:
            properties.setdefault("linked_service_url", ls_type_props["url"])
    return {"type": dataset.type, "properties": properties}
