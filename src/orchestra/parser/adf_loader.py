"""Loads ADF JSON files from a directory structure and produce typed AST objects."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from orchestra.models.adf_ast import (
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
    lower_candidates = {n.lower() for n in candidate_names}
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
    data = _normalize_arm(data)
    props = data.get("properties", data)
    name = data.get("name") or props.get("name") or fallback_name

    activities_raw: list[dict[str, Any]] = props.get("activities", [])
    activities = [parse_activity(a) for a in activities_raw]

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
            if_true_activities = [parse_activity(a) for a in raw_if_true]
        raw_if_false = type_properties.get("ifFalseActivities")
        if raw_if_false:
            if_false_activities = [parse_activity(a) for a in raw_if_false]
        raw_children = type_properties.get("activities")
        if raw_children:
            child_activities = [parse_activity(a) for a in raw_children]

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
        dep_names = [d.activity for d in activity.depends_on] if activity.depends_on else None

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
# CLI entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load ADF definitions and build a translation inventory.")
    parser.add_argument("--source-dir", required=True, type=Path, help="Root directory containing ADF JSON exports.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./orchestra_output/ingest"),
        help="Directory to write inventory.json into.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    definitions = load_adf_definitions(args.source_dir)
    logger.info("Loaded %d pipeline(s) from %s", len(definitions.pipelines), args.source_dir)

    inventory = build_inventory(definitions)

    output_dir: Path = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    inventory_path = output_dir / "inventory.json"
    inventory_dict = _inventory_to_dict(inventory, str(args.source_dir))
    inventory_path.write_text(json.dumps(inventory_dict, indent=2), encoding="utf-8")
    logger.info("Wrote inventory to %s", inventory_path)

    summary = inventory_dict["summary"]
    print("\nADF Ingestion Summary")
    print("=====================")
    print(f"Pipelines parsed:     {summary['pipeline_count']}")
    print(f"Total activities:     {summary['activity_count']}")
    print("\nStrategy Breakdown:")
    print(f"  Deterministic:      {summary['deterministic_count']}")
    print(f"  Agentic:            {summary['agentic_count']}")
    print(f"  Unsupported:        {summary['unsupported_count']}")
    print(f"\nCoverage:             {summary['coverage_pct']}%")
