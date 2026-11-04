"""Offline contract tests for the Azure live-integration ADF fixtures."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from flowx.models.adf_ast import TranslationStrategy
from flowx.motifs.detector import detect_motifs
from flowx.sources.adf.loader import AGENTIC_TYPES, DETERMINISTIC_TYPES, build_inventory, load_adf_definitions

FIXTURE_ROOT = Path(__file__).parent / "azure" / "adf"
EXPECTED_TRIGGER_TYPES = {"ScheduleTrigger", "TumblingWindowTrigger", "BlobEventsTrigger"}
EXPECTED_DATASET_TYPES = {
    "DelimitedText",
    "Parquet",
    "Json",
    "Avro",
    "Orc",
    "AzureSqlTable",
    "AzureMySqlTable",
    "AzurePostgreSqlTable",
    "AzureDatabricksDeltaLakeDataset",
}
EXPECTED_MOTIFS = {
    "rest_api_pagination",
    "incremental_load_watermark",
    "metadata_driven_bulk_copy",
    "activity_and_notify",
}


def test_every_registered_activity_type_is_classified(definitions) -> None:
    """Every supported ADF activity is present and follows its registry strategy."""
    inventory = build_inventory(definitions)
    observed = {item.activity_type for item in inventory.items}

    assert DETERMINISTIC_TYPES <= observed
    assert AGENTIC_TYPES <= observed
    assert inventory.unsupported_count == 0
    assert all(
        item.strategy is TranslationStrategy.DETERMINISTIC
        for item in inventory.items
        if item.activity_type in DETERMINISTIC_TYPES
    )
    assert all(
        item.strategy is TranslationStrategy.AGENTIC for item in inventory.items if item.activity_type in AGENTIC_TYPES
    )


def test_strategy_counts_are_broad_and_stable(definitions) -> None:
    """Guard against accidentally shrinking the integration corpus."""
    counts = Counter(item.strategy.value for item in build_inventory(definitions).items)

    assert counts["deterministic"] == 36
    assert counts["agentic"] == 15
    assert counts["unsupported"] == 0


def test_datasets_linked_services_and_triggers_cover_live_matrix(definitions) -> None:
    """The live deployment spans requested formats, stores, and trigger kinds."""
    assert EXPECTED_DATASET_TYPES <= {dataset.type for dataset in definitions.datasets.values()}
    assert EXPECTED_TRIGGER_TYPES == {trigger.type for trigger in definitions.triggers}
    assert {
        "AzureBlobFS",
        "AzureBlobStorage",
        "AzureSqlDatabase",
        "AzureMySql",
        "AzurePostgreSql",
        "AzureDatabricks",
        "AzureKeyVault",
    } <= {linked_service.type for linked_service in definitions.linked_services.values()}


def test_dedicated_motif_pipelines_are_detected(definitions) -> None:
    """Each required migration motif is recognized by the production detector."""
    observed = {
        motif.definition.motif_id
        for pipeline in definitions.pipelines
        for motif in detect_motifs(pipeline, definitions)
    }

    assert EXPECTED_MOTIFS <= observed


def test_adf_export_shapes_match_real_contracts() -> None:
    """Guard ADF shapes that the permissive loader alone cannot validate."""
    triggers = [json.loads(path.read_text(encoding="utf-8")) for path in (FIXTURE_ROOT / "triggers").glob("*.json")]
    assert triggers
    assert all(trigger["properties"]["runtimeState"] == "Stopped" for trigger in triggers)

    tumbling = json.loads((FIXTURE_ROOT / "triggers" / "tumbling-window.json").read_text(encoding="utf-8"))
    assert "pipeline" in tumbling["properties"]
    assert "pipelines" not in tumbling["properties"]

    lookups = []
    for path in (FIXTURE_ROOT / "pipelines").glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        pending = [payload]
        while pending:
            value = pending.pop()
            if isinstance(value, dict):
                if value.get("type") == "Lookup":
                    lookups.append(value)
                pending.extend(value.values())
            elif isinstance(value, list):
                pending.extend(value)

    assert len(lookups) == 4
    assert all("dataset" in lookup["typeProperties"] for lookup in lookups)
    assert all("dataset" not in lookup for lookup in lookups)


def test_terraform_omits_read_only_trigger_runtime_state() -> None:
    """Terraform deploys stopped trigger definitions without PUTting the read-only state."""
    terraform = (FIXTURE_ROOT / "../terraform/main.tf").resolve().read_text(encoding="utf-8")
    trigger_resource = terraform.split('resource "azapi_resource" "triggers" {', 1)[1]
    assert 'if key != "runtimeState"' in trigger_resource


def test_linked_service_parameters_use_adf_string_interpolation() -> None:
    """ADF linked-service properties reject generic Expression objects."""
    for path in (FIXTURE_ROOT / "linked_services").glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        pending = [payload["properties"].get("typeProperties", {})]
        while pending:
            value = pending.pop()
            if isinstance(value, dict):
                assert value.get("type") != "Expression", path.name
                pending.extend(value.values())
            elif isinstance(value, list):
                pending.extend(value)


def test_linked_service_connection_strings_are_parseable() -> None:
    """ADF must parse connection strings before encrypting their payloads."""
    for path in (FIXTURE_ROOT / "linked_services").glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        connection_string = payload["properties"].get("typeProperties", {}).get("connectionString")
        if connection_string is None:
            continue
        if isinstance(connection_string, dict):
            assert connection_string.get("type") == "AzureKeyVaultSecret", path.name
            continue
        assert not connection_string.startswith("@"), path.name
        assert not (connection_string.startswith("<") and connection_string.endswith(">")), path.name
        pairs = [pair for pair in connection_string.split(";") if pair]
        assert pairs and all("=" in pair and pair.split("=", 1)[0] for pair in pairs), path.name


def test_every_adf_reference_target_exists() -> None:
    """Every named ADF dependency is deployed from this fixture corpus."""
    directories = {
        "LinkedServiceReference": "linked_services",
        "DatasetReference": "datasets",
        "PipelineReference": "pipelines",
        "DataFlowReference": "dataflows",
        "IntegrationRuntimeReference": "integration_runtimes",
    }
    targets = {}
    for reference_type, directory in directories.items():
        targets[reference_type] = {
            json.loads(path.read_text(encoding="utf-8"))["name"] for path in (FIXTURE_ROOT / directory).glob("*.json")
        }

    for path in FIXTURE_ROOT.glob("**/*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        pending = [payload]
        while pending:
            value = pending.pop()
            if isinstance(value, dict):
                reference_type = value.get("type")
                if reference_type in targets:
                    assert value.get("referenceName") in targets[reference_type], (path.name, value)
                pending.extend(value.values())
            elif isinstance(value, list):
                pending.extend(value)


def test_terraform_orders_adf_reference_tiers() -> None:
    """No fixture references an object in its own or a later Terraform tier."""
    fixtures: dict[str, tuple[int, dict]] = {}

    def reference_objects(value):
        if isinstance(value, dict):
            if value.get("type", "").endswith("Reference") and "referenceName" in value:
                yield value
            for child in value.values():
                yield from reference_objects(child)
        elif isinstance(value, list):
            for child in value:
                yield from reference_objects(child)

    def references(value):
        return (reference["referenceName"] for reference in reference_objects(value))

    for directory in ("linked_services", "datasets", "dataflows", "integration_runtimes", "pipelines", "triggers"):
        for path in (FIXTURE_ROOT / directory).glob("*.json"):
            payload = json.loads(path.read_text(encoding="utf-8"))
            if directory == "linked_services":
                linked_service_references = list(references(payload["properties"]))
                if payload["properties"]["type"] == "AzureKeyVault":
                    tier = 0
                elif linked_service_references and "key-vault" not in linked_service_references:
                    tier = 2
                else:
                    tier = 1
            elif directory == "datasets":
                tier = 3
            elif directory in {"dataflows", "integration_runtimes"}:
                tier = 4
            elif directory == "pipelines":
                has_pipeline_reference = any(
                    reference["type"] == "PipelineReference" for reference in reference_objects(payload["properties"])
                )
                tier = 6 if has_pipeline_reference else 5
            else:
                tier = 7
            fixtures[payload["name"]] = (tier, payload)

    for name, (tier, payload) in fixtures.items():
        for target in references(payload):
            assert fixtures[target][0] < tier, f"{name} (tier {tier}) references {target} (tier {fixtures[target][0]})"


def test_sanitized_arm_export_round_trip_preserves_inventory(definitions, tmp_path: Path) -> None:
    """The workflow's ``{name, properties}`` export retains the full inventory."""
    for source_dir in FIXTURE_ROOT.iterdir():
        if not source_dir.is_dir():
            continue
        target_dir = tmp_path / source_dir.name
        target_dir.mkdir()
        for source_file in source_dir.glob("*.json"):
            payload = json.loads(source_file.read_text(encoding="utf-8"))
            sanitized = {key: payload[key] for key in ("name", "properties")}
            (target_dir / source_file.name).write_text(json.dumps(sanitized), encoding="utf-8")

    assert build_inventory(load_adf_definitions(tmp_path)) == build_inventory(definitions)
