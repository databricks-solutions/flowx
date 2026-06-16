"""Loads ADF JSON exports from a directory tree and produces typed AST objects (``AdfDefinitions``)."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flowx.models.adf_ast import (
    AdfActivity,
    AdfDataset,
    AdfDatasetReference,
    AdfDefinitions,
    AdfDependency,
    AdfLinkedService,
    AdfLinkedServiceReference,
    AdfParameter,
    AdfPipeline,
    AdfPolicy,
    AdfTrigger,
    AdfVariable,
    Inventory,
    InventoryItem,
    TranslationStrategy,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Activity-type classification registries
# ---------------------------------------------------------------------------

DETERMINISTIC_TYPES: set[str] = {
    "Copy",
    "DatabricksNotebook",
    "DatabricksSparkJar",
    "DatabricksSparkPython",
    "ForEach",
    "IfCondition",
    "SetVariable",
    "Switch",
    "Lookup",
    "WebActivity",
    "Delete",
    "ExecutePipeline",
    "DatabricksJob",
    "Wait",
    "Filter",
    "AppendVariable",
}

AGENTIC_TYPES: dict[str, str] = {
    "ExecuteDataFlow": "adf-to-databricks:adf-dataflow-converter",
    "Until": "adf-to-databricks:adf-pipeline-converter",
    "SqlServerStoredProcedure": "adf-to-databricks:adf-pipeline-converter",
    "AzureFunction": "adf-to-databricks:adf-pipeline-converter",
    "WebHook": "adf-to-databricks:adf-pipeline-converter",
    "Custom": "adf-to-databricks:adf-pipeline-converter",
    "ExecuteSSISPackage": "adf-to-databricks:adf-pipeline-converter",
    "AzureMLExecutePipeline": "adf-to-databricks:adf-pipeline-converter",
    "GetMetadata": "adf-to-databricks:adf-pipeline-converter",
    "Validation": "adf-to-databricks:adf-pipeline-converter",
    "Fail": "adf-to-databricks:adf-pipeline-converter",
    "Script": "adf-to-databricks:adf-pipeline-converter",
}

# Activity complexity weights (easiest first): Databricks-native ~1:1 tasks, then control-flow, then
# everything else (Copy/Web/Lookup/data movement/agentic) -- summed into each pipeline's complexity score.
_DATABRICKS_NATIVE_TYPES: frozenset[str] = frozenset(
    {"DatabricksNotebook", "DatabricksSparkJar", "DatabricksSparkPython", "DatabricksJob"}
)
_CONTROL_FLOW_TYPES: frozenset[str] = frozenset(
    {"ForEach", "IfCondition", "Switch", "SetVariable", "AppendVariable", "Filter", "Wait", "Until"}
)
_ACTIVITY_WEIGHT: dict[str, int] = {"databricks": 1, "control": 2, "other": 3}

# Complexity-score -> T-shirt size cutoffs (inclusive upper bounds).  Score is
# sum(activity weights) + #datasets + #linked_services + #collapsible_patterns.
_TSHIRT_CUTOFFS: tuple[tuple[int, str], ...] = ((5, "S"), (15, "M"), (30, "L"))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_adf_definitions(source_dir: Path) -> AdfDefinitions:
    """Loads all ADF JSON files from *source_dir* and return an :class:`AdfDefinitions`.

    Args:
        source_dir: Root directory containing ADF JSON exports.  May be a
            directory tree with ``pipelines/``, ``datasets/``, etc., or a
            single ARM template file.

    Returns:
        Fully populated :class:`AdfDefinitions` object.
    """
    source_dir = Path(source_dir).resolve()

    if source_dir.is_file() and source_dir.suffix == ".json":
        return _load_arm_template(source_dir)

    pipelines: list[AdfPipeline] = []
    datasets: dict[str, AdfDataset] = {}
    linked_services: dict[str, AdfLinkedService] = {}
    triggers: list[AdfTrigger] = []
    global_parameters: dict[str, Any] = {}

    factory_dir = _find_json_dir(source_dir, "factory", "factories")
    if factory_dir is not None:
        for json_file in sorted(factory_dir.glob("*.json")):
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
                factory_params = _parse_factory_global_parameters(data)
                global_parameters.update(factory_params)
            except Exception:
                logger.exception("Failed to parse factory file %s", json_file)

    pipeline_dir = _find_json_dir(source_dir, "pipelines", "pipeline")
    if pipeline_dir is not None:
        for json_file in sorted(pipeline_dir.glob("*.json")):
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
                pipelines.append(_parse_pipeline_json(data, fallback_name=json_file.stem))
            except Exception:
                logger.exception("Failed to parse pipeline file %s", json_file)

    dataset_dir = _find_json_dir(source_dir, "datasets", "dataset")
    if dataset_dir is not None:
        for json_file in sorted(dataset_dir.glob("*.json")):
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
                dataset = _parse_dataset_json(data, fallback_name=json_file.stem)
                datasets[dataset.name] = dataset
            except Exception:
                logger.exception("Failed to parse dataset file %s", json_file)

    linked_service_dir = _find_json_dir(source_dir, "linked_services", "linkedService", "linkedServices")
    if linked_service_dir is not None:
        for json_file in sorted(linked_service_dir.glob("*.json")):
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
                linked_service = _parse_linked_service_json(data, fallback_name=json_file.stem)
                linked_services[linked_service.name] = linked_service
            except Exception:
                logger.exception("Failed to parse linked-service file %s", json_file)

    trigger_dir = _find_json_dir(source_dir, "triggers", "trigger")
    if trigger_dir is not None:
        for json_file in sorted(trigger_dir.glob("*.json")):
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
                triggers.append(_parse_trigger_json(data, fallback_name=json_file.stem))
            except Exception:
                logger.exception("Failed to parse trigger file %s", json_file)

    return AdfDefinitions(
        pipelines=pipelines,
        datasets=datasets,
        linked_services=linked_services,
        triggers=triggers,
        global_parameters=global_parameters,
    )


def _parse_factory_global_parameters(data: dict[str, Any]) -> dict[str, Any]:
    """Extracts ``globalParameters`` from a factory JSON payload.

    Args:
        data: Raw JSON dictionary loaded from ``factory/<name>.json``.

    Returns:
        Mapping of global parameter name -> value.  Each value is either
        the scalar default (when ADF stores it bare) or the original
        ``{"type": ..., "value": ...}`` dict.
    """
    data = _normalize_arm(data)
    props = data.get("properties", data)
    raw = props.get("globalParameters") or {}
    if not isinstance(raw, dict):
        return {}

    result: dict[str, Any] = {}
    for name, value in raw.items():
        if isinstance(value, dict) and "value" in value:
            result[name] = value
        else:
            result[name] = value
    return result


def classify_activity(activity_type: str) -> tuple[TranslationStrategy, str | None]:
    """Classify an ADF activity type into a translation strategy.

    Args:
        activity_type: ADF activity type string (e.g. ``"Copy"``).

    Returns:
        A ``(strategy, agentic_skill_name)`` tuple.  *agentic_skill_name* is
        ``None`` for deterministic and unsupported strategies.
    """
    if activity_type in DETERMINISTIC_TYPES:
        return TranslationStrategy.DETERMINISTIC, None
    if activity_type in AGENTIC_TYPES:
        return TranslationStrategy.AGENTIC, AGENTIC_TYPES[activity_type]
    return TranslationStrategy.UNSUPPORTED, None


def build_inventory(definitions: AdfDefinitions) -> Inventory:
    """Walks all pipelines in *definitions* and classify every activity.

    Args:
        definitions: Parsed ADF definitions.

    Returns:
        :class:`Inventory` with one :class:`InventoryItem` per activity.
    """
    items: list[InventoryItem] = []
    deterministic = 0
    agentic = 0
    unsupported = 0

    for pipeline in definitions.pipelines:
        _classify_activities(pipeline.name, pipeline.activities, items)

    for item in items:
        if item.strategy is TranslationStrategy.DETERMINISTIC:
            deterministic += 1
        elif item.strategy is TranslationStrategy.AGENTIC:
            agentic += 1
        else:
            unsupported += 1

    return Inventory(
        items=items,
        deterministic_count=deterministic,
        agentic_count=agentic,
        unsupported_count=unsupported,
        pipeline_count=len(definitions.pipelines),
    )


# ---------------------------------------------------------------------------
# Internal helpers — parsing
# ---------------------------------------------------------------------------


def _find_json_dir(source_dir: Path, *candidate_names: str) -> Path | None:
    """Return the first existing subdirectory matching one of *candidate_names*.

    Args:
        source_dir: Parent directory to search within.
        *candidate_names: Case-insensitive directory name candidates.

    Returns:
        The resolved :class:`Path` of the matching directory, or ``None``.
    """
    for name in candidate_names:
        candidate = source_dir / name
        if candidate.is_dir():
            return candidate
    # Case-insensitive fallback
    lower_candidates = {candidate_name.lower() for candidate_name in candidate_names}
    for child in source_dir.iterdir():
        if child.is_dir() and child.name.lower() in lower_candidates:
            return child
    return None


def _parse_pipeline_json(data: dict[str, Any], *, fallback_name: str = "unknown") -> AdfPipeline:
    """Parses a single pipeline JSON payload into an :class:`AdfPipeline`.

    Args:
        data: Raw JSON dictionary (either a bare pipeline or wrapped in ``properties``).
        fallback_name: Name to use if the JSON does not contain one.

    Returns:
        Parsed :class:`AdfPipeline`.
    """
    raw_source = data
    data = _normalize_arm(data)
    props = data.get("properties", data)
    name = data.get("name") or props.get("name") or fallback_name

    activities_raw: list[dict[str, Any]] = props.get("activities", [])
    activities = [parse_activity(raw_activity) for raw_activity in activities_raw]

    parameters: dict[str, AdfParameter] | None = None
    raw_params = props.get("parameters")
    if raw_params:
        parameters = {}
        for pname, pval in raw_params.items():
            if isinstance(pval, dict):
                parameters[pname] = AdfParameter(
                    type=pval.get("type", "String"),
                    default_value=pval.get("defaultValue"),
                )
            else:
                parameters[pname] = AdfParameter(default_value=pval)

    variables: dict[str, AdfVariable] | None = None
    raw_vars = props.get("variables")
    if raw_vars:
        variables = {}
        for vname, vval in raw_vars.items():
            if isinstance(vval, dict):
                variables[vname] = AdfVariable(
                    type=vval.get("type", "String"),
                    default_value=vval.get("defaultValue"),
                )
            else:
                variables[vname] = AdfVariable(default_value=vval)

    annotations = props.get("annotations")
    folder_raw = props.get("folder")
    folder = folder_raw.get("name") if isinstance(folder_raw, dict) else folder_raw

    return AdfPipeline(
        name=name,
        activities=activities,
        parameters=parameters,
        variables=variables,
        annotations=annotations,
        folder=folder,
        raw=raw_source,
    )


def parse_activity(data: dict[str, Any]) -> AdfActivity:
    """Parses an activity dict into a typed :class:`AdfActivity` AST node.

    Args:
        data: Raw activity JSON dictionary.

    Returns:
        Parsed :class:`AdfActivity`.
    """
    name = data.get("name", "unnamed")
    adf_type = data.get("type", "Unknown")

    depends_on: list[AdfDependency] | None = None
    raw_deps = data.get("dependsOn")
    if raw_deps:
        depends_on = [
            AdfDependency(
                activity=dep.get("activity", ""),
                dependency_conditions=dep.get("dependencyConditions", ["Succeeded"]),
            )
            for dep in raw_deps
        ]

    policy: AdfPolicy | None = None
    raw_policy = data.get("policy")
    if raw_policy:
        policy = AdfPolicy(
            timeout=raw_policy.get("timeout"),
            retry=raw_policy.get("retry"),
            retry_interval_in_seconds=raw_policy.get("retryIntervalInSeconds"),
            secure_input=raw_policy.get("secureInput", False),
            secure_output=raw_policy.get("secureOutput", False),
        )

    # Type properties — prefer explicit typeProperties; fall back to
    # collecting non-common top-level keys (flattened format).
    type_properties = data.get("typeProperties")
    if type_properties is None:
        type_properties = _collect_type_properties(data)

    inputs = _parse_dataset_refs(data.get("inputs"))
    outputs = _parse_dataset_refs(data.get("outputs"))

    linked_service_name: AdfLinkedServiceReference | None = None
    raw_ls = data.get("linkedServiceName")
    if raw_ls and isinstance(raw_ls, dict):
        linked_service_name = AdfLinkedServiceReference(
            reference_name=raw_ls.get("referenceName", ""),
            type=raw_ls.get("type", "LinkedServiceReference"),
            parameters=raw_ls.get("parameters"),
        )

    if_true_activities: list[AdfActivity] | None = None
    if_false_activities: list[AdfActivity] | None = None
    child_activities: list[AdfActivity] | None = None

    if type_properties:
        raw_if_true = type_properties.get("ifTrueActivities")
        if raw_if_true:
            if_true_activities = [parse_activity(raw_activity) for raw_activity in raw_if_true]
        raw_if_false = type_properties.get("ifFalseActivities")
        if raw_if_false:
            if_false_activities = [parse_activity(raw_activity) for raw_activity in raw_if_false]
        raw_children = type_properties.get("activities")
        if raw_children:
            child_activities = [parse_activity(raw_activity) for raw_activity in raw_children]

    return AdfActivity(
        name=name,
        type=adf_type,
        depends_on=depends_on,
        policy=policy,
        type_properties=type_properties,
        inputs=inputs,
        outputs=outputs,
        linked_service_name=linked_service_name,
        if_true_activities=if_true_activities,
        if_false_activities=if_false_activities,
        activities=child_activities,
        raw=data,
    )


_COMMON_ACTIVITY_KEYS: frozenset[str] = frozenset(
    {
        "name",
        "type",
        "dependsOn",
        "policy",
        "userProperties",
        "description",
        "state",
        "onInactiveMarkAs",
        "additionalProperties",
        "inputs",
        "outputs",
        "linkedServiceName",
        "typeProperties",
    }
)


def _collect_type_properties(data: dict[str, Any]) -> dict[str, Any] | None:
    """Collects type-specific fields from a flattened activity dict.

    Args:
        data: Raw activity JSON dictionary.

    Returns:
        Synthesised type-properties dict, or ``None`` if no extra keys exist.
    """
    type_properties: dict[str, Any] = {k: v for k, v in data.items() if k not in _COMMON_ACTIVITY_KEYS}
    return type_properties if type_properties else None


def _parse_dataset_refs(raw: list[dict[str, Any]] | None) -> list[AdfDatasetReference] | None:
    """Parses a list of dataset reference dicts into typed objects.

    Args:
        raw: Raw list of dataset reference dictionaries, or ``None``.

    Returns:
        List of :class:`AdfDatasetReference` objects, or ``None``.
    """
    if not raw:
        return None
    refs: list[AdfDatasetReference] = []
    for item in raw:
        ds_ref = item.get("dataset", item)
        refs.append(
            AdfDatasetReference(
                reference_name=ds_ref.get("referenceName", ""),
                type=ds_ref.get("type", "DatasetReference"),
                parameters=ds_ref.get("parameters"),
            )
        )
    return refs


def _parse_dataset_json(data: dict[str, Any], *, fallback_name: str = "unknown") -> AdfDataset:
    """Parses a dataset JSON payload.

    Args:
        data: Raw JSON dictionary.
        fallback_name: Name to use if the JSON does not contain one.

    Returns:
        Parsed :class:`AdfDataset`.
    """
    data = _normalize_arm(data)
    props = data.get("properties", data)
    name = data.get("name") or fallback_name
    ds_type = props.get("type", "Unknown")
    ls_ref = props.get("linkedServiceName", {})
    ls_name = ls_ref.get("referenceName") if isinstance(ls_ref, dict) else ls_ref

    return AdfDataset(
        name=name,
        type=ds_type,
        properties=props,
        linked_service_name=ls_name,
    )


def _parse_linked_service_json(data: dict[str, Any], *, fallback_name: str = "unknown") -> AdfLinkedService:
    """Parses a linked-service JSON payload.

    Args:
        data: Raw JSON dictionary.
        fallback_name: Name to use if the JSON does not contain one.

    Returns:
        Parsed :class:`AdfLinkedService`.
    """
    data = _normalize_arm(data)
    props = data.get("properties", data)
    name = data.get("name") or fallback_name
    ls_type = props.get("type", "Unknown")

    return AdfLinkedService(name=name, type=ls_type, properties=props)


def _parse_trigger_json(data: dict[str, Any], *, fallback_name: str = "unknown") -> AdfTrigger:
    """Parses a trigger JSON payload.

    Args:
        data: Raw JSON dictionary.
        fallback_name: Name to use if the JSON does not contain one.

    Returns:
        Parsed :class:`AdfTrigger`.
    """
    data = _normalize_arm(data)
    props = data.get("properties", data)
    name = data.get("name") or fallback_name
    trigger_type = props.get("type", "Unknown")
    trigger_pipelines = props.get("pipelines")

    return AdfTrigger(
        name=name,
        type=trigger_type,
        properties=props,
        pipelines=trigger_pipelines,
    )


def _normalize_arm(data: dict[str, Any]) -> dict[str, Any]:
    """If *data* is an ARM template wrapper, unwrap to the inner resource definition.

    Args:
        data: Possibly ARM-wrapped JSON dictionary.

    Returns:
        The unwrapped resource dictionary, or *data* unchanged.
    """
    if "$schema" not in data and "resources" not in data:
        return data

    resources = data.get("resources", [])
    if not resources:
        return data

    adf_suffixes = ("/pipelines", "/datasets", "/linkedServices", "/triggers")
    for resource in resources:
        rtype = resource.get("type", "")
        if any(rtype.endswith(suffix) for suffix in adf_suffixes):
            result: dict[str, Any] = {}
            arm_name = resource.get("name", "")
            if "/" in arm_name:
                result["name"] = arm_name.rsplit("/", 1)[-1].strip("'])")
            else:
                result["name"] = arm_name
            result["properties"] = resource.get("properties", {})
            return result

    return data


def _load_arm_template(template_path: Path) -> AdfDefinitions:
    """Loads all ADF resources from a single ARM template file.

    Args:
        template_path: Path to the ARM template JSON file.

    Returns:
        Parsed :class:`AdfDefinitions`.
    """
    data = json.loads(template_path.read_text(encoding="utf-8"))
    resources = data.get("resources", [])

    pipelines: list[AdfPipeline] = []
    datasets: dict[str, AdfDataset] = {}
    linked_services: dict[str, AdfLinkedService] = {}
    triggers: list[AdfTrigger] = []
    global_parameters: dict[str, Any] = {}

    for resource in resources:
        rtype = resource.get("type", "")
        props = resource.get("properties", {})
        raw_name = resource.get("name", "")
        if "/" in raw_name:
            name = raw_name.rsplit("/", 1)[-1].strip("'])")
        else:
            name = raw_name

        wrapped = {"name": name, "properties": props}

        if rtype.endswith("/pipelines"):
            try:
                pipelines.append(_parse_pipeline_json(wrapped, fallback_name=name))
            except Exception:
                logger.exception("Failed to parse ARM pipeline resource %s", name)
        elif rtype.endswith("/datasets"):
            try:
                dataset = _parse_dataset_json(wrapped, fallback_name=name)
                datasets[dataset.name] = dataset
            except Exception:
                logger.exception("Failed to parse ARM dataset resource %s", name)
        elif rtype.endswith("/linkedServices"):
            try:
                linked_service = _parse_linked_service_json(wrapped, fallback_name=name)
                linked_services[linked_service.name] = linked_service
            except Exception:
                logger.exception("Failed to parse ARM linked-service resource %s", name)
        elif rtype.endswith("/triggers"):
            try:
                triggers.append(_parse_trigger_json(wrapped, fallback_name=name))
            except Exception:
                logger.exception("Failed to parse ARM trigger resource %s", name)
        elif rtype.endswith("/factories"):
            try:
                global_parameters.update(_parse_factory_global_parameters(wrapped))
            except Exception:
                logger.exception("Failed to parse ARM factory resource %s", name)

    return AdfDefinitions(
        pipelines=pipelines,
        datasets=datasets,
        linked_services=linked_services,
        triggers=triggers,
        global_parameters=global_parameters,
    )


# ---------------------------------------------------------------------------
# Internal helpers — classification
# ---------------------------------------------------------------------------


def _classify_activities(
    pipeline_name: str,
    activities: list[AdfActivity],
    items: list[InventoryItem],
) -> None:
    """Recursively classify activities and append to *items*.

    Args:
        pipeline_name: Name of the owning pipeline (for the inventory row).
        activities: Activities to classify.
        items: Accumulator list to append results to.
    """
    for activity in activities:
        strategy, skill = classify_activity(activity.type)
        dep_names = [dependency.activity for dependency in activity.depends_on] if activity.depends_on else None

        items.append(
            InventoryItem(
                pipeline_name=pipeline_name,
                activity_name=activity.name,
                activity_type=activity.type,
                strategy=strategy,
                agentic_skill=skill,
                depends_on=dep_names,
            )
        )

        if activity.if_true_activities:
            _classify_activities(pipeline_name, activity.if_true_activities, items)
        if activity.if_false_activities:
            _classify_activities(pipeline_name, activity.if_false_activities, items)
        if activity.activities:
            _classify_activities(pipeline_name, activity.activities, items)


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------


def _inventory_to_dict(inventory: Inventory, source_dir: str) -> dict[str, Any]:
    """Serialise an :class:`Inventory` to a JSON-friendly dictionary.

    Args:
        inventory: The inventory to serialise.
        source_dir: Original source directory path (for provenance).

    Returns:
        Dictionary suitable for ``json.dumps``.
    """
    pipeline_map: dict[str, list[dict[str, Any]]] = {}
    for item in inventory.items:
        entry: dict[str, Any] = {
            "name": item.activity_name,
            "type": item.activity_type,
            "strategy": item.strategy.value,
        }
        if item.agentic_skill:
            entry["skill"] = item.agentic_skill
        if item.depends_on:
            entry["depends_on"] = item.depends_on
        pipeline_map.setdefault(item.pipeline_name, []).append(entry)

    total = inventory.deterministic_count + inventory.agentic_count + inventory.unsupported_count
    coverage_pct = round((inventory.deterministic_count + inventory.agentic_count) / total * 100, 1) if total else 0.0

    return {
        "source_dir": source_dir,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipelines": [{"name": pname, "activities": acts} for pname, acts in pipeline_map.items()],
        "summary": {
            "pipeline_count": inventory.pipeline_count,
            "activity_count": total,
            "deterministic_count": inventory.deterministic_count,
            "agentic_count": inventory.agentic_count,
            "unsupported_count": inventory.unsupported_count,
            "coverage_pct": coverage_pct,
        },
    }


# ---------------------------------------------------------------------------
# Profile complexity report (CSV)
# ---------------------------------------------------------------------------


def _walk_activities(activities: list[AdfActivity]):
    """Yields every activity in *activities*, descending into container children."""
    for activity in activities:
        yield activity
        for child in (activity.if_true_activities, activity.if_false_activities, activity.activities):
            if child:
                yield from _walk_activities(child)


def _activity_category(activity_type: str) -> str:
    """Returns the complexity category of *activity_type*.

    One of ``"databricks"`` (native, simplest), ``"control"`` (control-flow /
    parameter-setting), or ``"other"`` (data movement, web, agentic, ...).
    """
    if activity_type in _DATABRICKS_NATIVE_TYPES:
        return "databricks"
    if activity_type in _CONTROL_FLOW_TYPES:
        return "control"
    return "other"


def _dataset_refs_for_activity(activity: AdfActivity) -> set[str]:
    """Collects dataset names referenced by a single activity.

    Looks at the activity's ``inputs``/``outputs`` plus the ``dataset`` /
    ``datasets`` references some activity types (Lookup, Delete, GetMetadata)
    carry inside ``typeProperties``.
    """
    names: set[str] = set()
    for ref in (activity.inputs or []) + (activity.outputs or []):
        if ref.reference_name:
            names.add(ref.reference_name)
    props = activity.type_properties or {}
    for key in ("dataset", "source", "sink"):
        candidate = props.get(key)
        if isinstance(candidate, dict) and candidate.get("referenceName"):
            names.add(candidate["referenceName"])
    return names


def _pipeline_reference_counts(
    pipeline: AdfPipeline, definitions: AdfDefinitions
) -> tuple[int, set[str], set[str], dict[str, int]]:
    """Returns ``(activity_count, dataset_names, linked_service_names, category_counts)``.

    Linked services are attributed both from activity-level references (e.g.
    DatabricksNotebook compute) and transitively via the datasets a pipeline
    touches (each dataset names its backing linked service).
    """
    dataset_names: set[str] = set()
    linked_service_names: set[str] = set()
    category_counts = {"databricks": 0, "control": 0, "other": 0}
    activity_count = 0
    for activity in _walk_activities(pipeline.activities):
        activity_count += 1
        category_counts[_activity_category(activity.type)] += 1
        dataset_names |= _dataset_refs_for_activity(activity)
        if activity.linked_service_name and activity.linked_service_name.reference_name:
            linked_service_names.add(activity.linked_service_name.reference_name)
    for dataset_name in dataset_names:
        dataset = definitions.datasets.get(dataset_name)
        if dataset and dataset.linked_service_name:
            linked_service_names.add(dataset.linked_service_name)
    return activity_count, dataset_names, linked_service_names, category_counts


def _complexity_score(category_counts: dict[str, int], n_datasets: int, n_linked: int, n_patterns: int) -> int:
    """Weighted complexity score: activity weights + datasets + linked services + patterns."""
    weighted = sum(category_counts[cat] * _ACTIVITY_WEIGHT[cat] for cat in category_counts)
    return weighted + n_datasets + n_linked + n_patterns


def _tshirt_size(score: int) -> str:
    """Maps a complexity score to a T-shirt size (S / M / L / XL)."""
    for cutoff, size in _TSHIRT_CUTOFFS:
        if score <= cutoff:
            return size
    return "XL"


def build_profile_rows(definitions: AdfDefinitions) -> list[dict[str, Any]]:
    """Builds one profile-report row per pipeline.

    Each row carries the source activity / dataset / linked-service counts, the
    number of collapsible motif patterns detected, and a weighted complexity
    score plus its T-shirt size.

    Args:
        definitions: Parsed ADF definitions.

    Returns:
        List of row dicts ordered by pipeline name.
    """
    from flowx.motifs.detector import detect_motifs

    rows: list[dict[str, Any]] = []
    for pipeline in sorted(definitions.pipelines, key=lambda p: p.name):
        activity_count, datasets, linked_services, category_counts = _pipeline_reference_counts(pipeline, definitions)
        try:
            n_patterns = len(detect_motifs(pipeline, definitions))
        except Exception as exc:  # noqa: BLE001 - profiling must never hard-fail on motif detection
            logger.warning("Motif detection failed for pipeline %r: %s", pipeline.name, exc)
            n_patterns = 0
        score = _complexity_score(category_counts, len(datasets), len(linked_services), n_patterns)
        rows.append(
            {
                "pipeline": pipeline.name,
                "activities": activity_count,
                "datasets": len(datasets),
                "linked_services": len(linked_services),
                "collapsible_patterns": n_patterns,
                "databricks_native_activities": category_counts["databricks"],
                "control_flow_activities": category_counts["control"],
                "other_activities": category_counts["other"],
                "complexity_score": score,
                "complexity_size": _tshirt_size(score),
            }
        )
    return rows


_PROFILE_CSV_COLUMNS: tuple[str, ...] = (
    "pipeline",
    "activities",
    "datasets",
    "linked_services",
    "collapsible_patterns",
    "databricks_native_activities",
    "control_flow_activities",
    "other_activities",
    "complexity_score",
    "complexity_size",
)


def write_profile_csv(rows: list[dict[str, Any]], path: Path) -> None:
    """Writes the per-pipeline profile rows to *path* as CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(_PROFILE_CSV_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _sanitize_filename(name: str) -> str:
    """Slugifies a pipeline name into a safe filename stem."""
    slug = re.sub(r"[^0-9A-Za-z._-]+", "_", name).strip("_")
    return slug or "pipeline"


def write_pipeline_arm(definitions: AdfDefinitions, metadata_dir: Path) -> list[Path]:
    """Writes each pipeline's original ARM JSON to ``metadata_dir/<pipeline>.arm.json``.

    Returns the list of written paths.  Pipelines whose source JSON was not
    retained are skipped (should not happen for parsed sources).
    """
    metadata_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for pipeline in definitions.pipelines:
        if pipeline.raw is None:
            logger.warning("No source ARM JSON retained for pipeline %r; skipping arm export.", pipeline.name)
            continue
        arm_path = metadata_dir / f"{_sanitize_filename(pipeline.name)}.arm.json"
        arm_path.write_text(json.dumps(pipeline.raw, indent=2), encoding="utf-8")
        written.append(arm_path)
    return written


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

# Orchestra-managed entries under the shared output_dir, cleared at the start of each fresh run.
_MANAGED_OUTPUT_DIRS: tuple[str, ...] = ("metadata", ".work", "resources", "src", "setup")
_MANAGED_OUTPUT_FILES: tuple[str, ...] = ("databricks.yml", "SETUP.md", "WARNINGS.md")


def clear_stale_outputs(output_dir: Path) -> None:
    """Removes a prior run's artifacts from a reused ``output_dir``.

    Discover begins a fresh migration, so a previous run's per-pipeline metadata, transient
    intermediates, and generated bundle must not survive into this run. Without this, a
    single-pipeline migration into a reused output directory ships the earlier run's other
    pipelines' source ARM and generated notebooks in the packaged bundle.

    Args:
        output_dir: Migration output directory shared by all three phases.

    Notes:
        Only orchestra-managed entries are removed (never the directory itself or unrelated
        files), so pointing ``output_dir`` at a populated directory stays safe.
    """
    for directory in _MANAGED_OUTPUT_DIRS:
        shutil.rmtree(output_dir / directory, ignore_errors=True)
    for filename in _MANAGED_OUTPUT_FILES:
        (output_dir / filename).unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    """Discover-phase entry point: load ADF, build the inventory + profile report.

    Exposed as a callable (not just an ``if __name__`` block) so the adapter can run the phase
    in-process instead of spawning a second interpreter. Clears any prior run's artifacts from the
    shared output directory first, so a reused ``output_dir`` never leaks stale pipelines into the
    bundle this run packages.
    """
    parser = argparse.ArgumentParser(description="Load ADF definitions and build a translation inventory.")
    parser.add_argument("--source-dir", required=True, type=Path, help="Root directory containing ADF JSON exports.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./flowx_output"),
        help=(
            "Migration output directory. Profile artifacts are written into its "
            "metadata/ subfolder (inventory.json, profile_report.csv, <pipeline>.arm.json)."
        ),
    )
    parser.add_argument(
        "--pipeline",
        type=str,
        default=None,
        help="Filter to a single pipeline by name. When omitted, all pipelines are included.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    definitions = load_adf_definitions(args.source_dir)
    logger.info("Loaded %d pipeline(s) from %s", len(definitions.pipelines), args.source_dir)

    # Filter to a single pipeline when --pipeline is specified
    if args.pipeline:
        matched = [pipeline for pipeline in definitions.pipelines if pipeline.name == args.pipeline]
        if not matched:
            available = [pipeline.name for pipeline in definitions.pipelines]
            logger.error(
                "Pipeline %r not found. Available pipelines: %s",
                args.pipeline,
                ", ".join(available) or "(none)",
            )
            return 1
        definitions = AdfDefinitions(
            pipelines=matched,
            datasets=definitions.datasets,
            linked_services=definitions.linked_services,
            triggers=definitions.triggers,
            global_parameters=definitions.global_parameters,
        )
        logger.info("Filtered to pipeline: %s", args.pipeline)

    inventory = build_inventory(definitions)

    output_dir: Path = args.output_dir.resolve()
    clear_stale_outputs(output_dir)
    metadata_dir = output_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    inventory_path = metadata_dir / "inventory.json"
    inventory_dict = _inventory_to_dict(inventory, str(args.source_dir))
    inventory_path.write_text(json.dumps(inventory_dict, indent=2), encoding="utf-8")
    logger.info("Wrote inventory to %s", inventory_path)

    profile_rows = build_profile_rows(definitions)
    csv_path = metadata_dir / "profile_report.csv"
    write_profile_csv(profile_rows, csv_path)
    logger.info("Wrote profile report to %s", csv_path)

    arm_paths = write_pipeline_arm(definitions, metadata_dir)
    logger.info("Wrote %d pipeline ARM JSON file(s) to %s", len(arm_paths), metadata_dir)

    summary = inventory_dict["summary"]
    print("\nADF Profile Summary")
    print("===================")
    print(f"Pipelines parsed:     {summary['pipeline_count']}")
    print(f"Total activities:     {summary['activity_count']}")
    print("\nStrategy Breakdown:")
    print(f"  Deterministic:      {summary['deterministic_count']}")
    print(f"  Agentic:            {summary['agentic_count']}")
    print(f"  Unsupported:        {summary['unsupported_count']}")
    print(f"\nCoverage:             {summary['coverage_pct']}%")
    print("\nComplexity by pipeline (metadata/profile_report.csv):")
    print(f"  {'pipeline':<32} {'acts':>4} {'ds':>3} {'ls':>3} {'patt':>4} {'score':>5}  size")
    for row in profile_rows:
        print(
            f"  {row['pipeline'][:32]:<32} {row['activities']:>4} {row['datasets']:>3} "
            f"{row['linked_services']:>3} {row['collapsible_patterns']:>4} "
            f"{row['complexity_score']:>5}  {row['complexity_size']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
