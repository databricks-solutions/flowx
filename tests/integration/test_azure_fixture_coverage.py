"""Offline contract tests for the Azure live-integration ADF fixtures."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest

from flowx.models.adf_ast import TranslationStrategy
from flowx.motifs.detector import detect_motifs
from flowx.sources.adf.loader import AGENTIC_TYPES, DETERMINISTIC_TYPES, build_inventory, load_adf_definitions

pytestmark = pytest.mark.integration

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


@pytest.fixture(scope="module")
def definitions():
    """Load the checked-in definitions through the production ADF loader."""
    return load_adf_definitions(FIXTURE_ROOT)


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
