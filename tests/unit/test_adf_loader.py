"""Unit tests for the ADF parser (adf_loader.py)."""

from __future__ import annotations

from flowx.models.adf_ast import (
    AdfDefinitions,
    TranslationStrategy,
)
from flowx.parser.adf_loader import (
    AGENTIC_TYPES,
    DETERMINISTIC_TYPES,
    _normalize_arm,
    _parse_pipeline_json,
    build_inventory,
    classify_activity,
    clear_stale_outputs,
    load_adf_definitions,
)

# ---------------------------------------------------------------------------
# load_adf_definitions
# ---------------------------------------------------------------------------


class TestLoadDefinitions:
    def test_load_definitions_from_fixtures(self, fixtures_dir):
        """All fixture pipelines load without error and the pipeline count matches."""
        defs = load_adf_definitions(fixtures_dir)
        assert isinstance(defs, AdfDefinitions)
        pipeline_names = {p.name for p in defs.pipelines}
        # We should have at least the core fixtures
        assert len(defs.pipelines) >= 8
        assert "pipeline_copy_csv_to_delta" in pipeline_names
        assert "pipeline_notebook_basic" in pipeline_names
        assert "pipeline_complex_etl" in pipeline_names
        assert "pipeline_foreach_switch" in pipeline_names
        assert "pipeline_all_activity_types" in pipeline_names
        assert "pipeline_mixed_agentic" in pipeline_names

    def test_load_definitions_loads_datasets(self, fixtures_dir):
        defs = load_adf_definitions(fixtures_dir)
        assert "ds_csv_adls_customers" in defs.datasets
        assert "ds_delta_customers" in defs.datasets

    def test_load_definitions_loads_linked_services(self, fixtures_dir):
        defs = load_adf_definitions(fixtures_dir)
        assert "ls_databricks_existing_cluster" in defs.linked_services
        assert "ls_databricks_new_cluster" in defs.linked_services

    def test_load_definitions_loads_triggers(self, fixtures_dir):
        defs = load_adf_definitions(fixtures_dir)
        assert len(defs.triggers) >= 1
        trigger_names = {t.name for t in defs.triggers}
        assert "tr_daily_schedule" in trigger_names


# ---------------------------------------------------------------------------
# classify_activity
# ---------------------------------------------------------------------------


class TestClassifyActivity:
    def test_classify_deterministic_types(self):
        """All 16 deterministic types are classified correctly."""
        expected = {
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
        assert DETERMINISTIC_TYPES == expected

        for atype in expected:
            strategy = classify_activity(atype)
            assert strategy is TranslationStrategy.DETERMINISTIC, f"{atype} should be DETERMINISTIC"

    def test_classify_agentic_types(self):
        """All agentic types are classified as AGENTIC."""
        for atype in AGENTIC_TYPES:
            strategy = classify_activity(atype)
            assert strategy is TranslationStrategy.AGENTIC, f"{atype} should be AGENTIC"

    def test_classify_unknown_types(self):
        """Unknown activity types are classified as UNSUPPORTED."""
        for unknown_type in ("Bogus", "SomeFutureActivity", "MagicTransform", ""):
            strategy = classify_activity(unknown_type)
            assert strategy is TranslationStrategy.UNSUPPORTED


# ---------------------------------------------------------------------------
# build_inventory
# ---------------------------------------------------------------------------


class TestBuildInventory:
    def test_build_inventory_counts(self, adf_definitions):
        """Inventory has correct total and per-strategy counts."""
        inv = build_inventory(adf_definitions)
        total = inv.deterministic_count + inv.agentic_count + inv.unsupported_count
        assert total == len(inv.items)
        assert inv.pipeline_count == len(adf_definitions.pipelines)
        # There should be at least some deterministic items
        assert inv.deterministic_count > 0

    def test_build_inventory_has_pipeline_names(self, adf_definitions):
        """Every inventory item references a valid pipeline name."""
        inv = build_inventory(adf_definitions)
        pipeline_names = {p.name for p in adf_definitions.pipelines}
        for item in inv.items:
            assert item.pipeline_name in pipeline_names

    def test_build_inventory_agentic_count_matches_items(self, adf_definitions):
        """The agentic count matches the number of items classified AGENTIC."""
        inv = build_inventory(adf_definitions)
        agentic_items = [i for i in inv.items if i.strategy is TranslationStrategy.AGENTIC]
        assert len(agentic_items) == inv.agentic_count


# ---------------------------------------------------------------------------
# Pipeline parsing details
# ---------------------------------------------------------------------------


class TestParsePipeline:
    def test_parse_pipeline_with_parameters(self):
        """Parameters are extracted correctly from pipeline JSON."""
        data = {
            "name": "test_pipeline",
            "properties": {
                "activities": [],
                "parameters": {
                    "env": {"type": "String", "defaultValue": "dev"},
                    "runDate": {"type": "String"},
                },
            },
        }
        pipeline = _parse_pipeline_json(data)
        assert pipeline.name == "test_pipeline"
        assert pipeline.parameters is not None
        assert "env" in pipeline.parameters
        assert pipeline.parameters["env"].type == "String"
        assert pipeline.parameters["env"].default_value == "dev"
        assert "runDate" in pipeline.parameters
        assert pipeline.parameters["runDate"].default_value is None

    def test_parse_activity_with_dependencies(self):
        """depends_on is parsed correctly from activity JSON."""
        data = {
            "name": "dep_pipeline",
            "properties": {
                "activities": [
                    {"name": "A", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}},
                    {
                        "name": "B",
                        "type": "Wait",
                        "dependsOn": [
                            {"activity": "A", "dependencyConditions": ["Succeeded"]},
                        ],
                        "typeProperties": {"waitTimeInSeconds": 1},
                    },
                    {
                        "name": "C",
                        "type": "Wait",
                        "dependsOn": [
                            {"activity": "A", "dependencyConditions": ["Failed"]},
                            {"activity": "B", "dependencyConditions": ["Completed"]},
                        ],
                        "typeProperties": {"waitTimeInSeconds": 1},
                    },
                ],
            },
        }
        pipeline = _parse_pipeline_json(data)
        activities_by_name = {a.name: a for a in pipeline.activities}

        assert activities_by_name["A"].depends_on is None or len(activities_by_name["A"].depends_on) == 0
        assert len(activities_by_name["B"].depends_on) == 1
        assert activities_by_name["B"].depends_on[0].activity == "A"
        assert activities_by_name["B"].depends_on[0].dependency_conditions == ["Succeeded"]

        assert len(activities_by_name["C"].depends_on) == 2
        assert activities_by_name["C"].depends_on[0].dependency_conditions == ["Failed"]
        assert activities_by_name["C"].depends_on[1].dependency_conditions == ["Completed"]

    def test_parse_activity_with_policy(self):
        """Timeout and retry are parsed from the activity policy."""
        data = {
            "name": "policy_pipeline",
            "properties": {
                "activities": [
                    {
                        "name": "Act1",
                        "type": "Copy",
                        "policy": {
                            "timeout": "0.12:00:00",
                            "retry": 3,
                            "retryIntervalInSeconds": 60,
                            "secureInput": True,
                            "secureOutput": False,
                        },
                        "typeProperties": {"source": {}, "sink": {}},
                    }
                ],
            },
        }
        pipeline = _parse_pipeline_json(data)
        act = pipeline.activities[0]
        assert act.policy is not None
        assert act.policy.timeout == "0.12:00:00"
        assert act.policy.retry == 3
        assert act.policy.retry_interval_in_seconds == 60
        assert act.policy.secure_input is True
        assert act.policy.secure_output is False

    def test_parse_pipeline_with_variables(self):
        """Variables are extracted correctly."""
        data = {
            "name": "var_pipeline",
            "properties": {
                "activities": [],
                "variables": {
                    "status": {"type": "String", "defaultValue": "pending"},
                    "counter": {"type": "Int", "defaultValue": 0},
                },
            },
        }
        pipeline = _parse_pipeline_json(data)
        assert pipeline.variables is not None
        assert "status" in pipeline.variables
        assert pipeline.variables["status"].default_value == "pending"
        assert pipeline.variables["counter"].default_value == 0

    def test_parse_pipeline_annotations_and_folder(self):
        """Annotations and folder are parsed correctly."""
        data = {
            "name": "annotated",
            "properties": {
                "activities": [],
                "annotations": ["etl", "daily"],
                "folder": {"name": "ETL/Ingestion"},
            },
        }
        pipeline = _parse_pipeline_json(data)
        assert pipeline.annotations == ["etl", "daily"]
        assert pipeline.folder == "ETL/Ingestion"


# ---------------------------------------------------------------------------
# ARM template normalization
# ---------------------------------------------------------------------------


class TestNormalizeArm:
    def test_normalize_arm_template(self):
        """ARM template wrapper is unwrapped to the inner pipeline."""
        arm_data = {
            "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
            "resources": [
                {
                    "type": "Microsoft.DataFactory/factories/pipelines",
                    "name": "[concat(parameters('factoryName'), '/MyPipeline')]",
                    "properties": {
                        "activities": [
                            {
                                "name": "DoStuff",
                                "type": "Copy",
                                "typeProperties": {"source": {}, "sink": {}},
                            }
                        ],
                    },
                }
            ],
        }
        result = _normalize_arm(arm_data)
        assert result["name"] == "MyPipeline"
        assert "activities" in result["properties"]

    def test_normalize_arm_passthrough(self):
        """Non-ARM data is returned unchanged."""
        data = {"name": "simple", "properties": {"activities": []}}
        result = _normalize_arm(data)
        assert result is data


# ---------------------------------------------------------------------------
# clear_stale_outputs
# ---------------------------------------------------------------------------


class TestClearStaleOutputs:
    """Discover must reset a reused output_dir so prior runs don't leak into the bundle."""

    def test_removes_prior_run_artifacts(self, tmp_path):
        """Stale per-pipeline metadata and a prior generated bundle are removed."""
        (tmp_path / "metadata").mkdir()
        (tmp_path / "metadata" / "OldPipeline.arm.json").write_text("{}", encoding="utf-8")
        (tmp_path / "resources").mkdir()
        (tmp_path / "resources" / "old_pipeline.yml").write_text("name: old", encoding="utf-8")
        (tmp_path / "src" / "notebooks").mkdir(parents=True)
        (tmp_path / "src" / "notebooks" / "old.py").write_text("print('old')", encoding="utf-8")
        (tmp_path / ".work").mkdir()
        (tmp_path / ".work" / "translation_report.json").write_text("{}", encoding="utf-8")
        (tmp_path / "databricks.yml").write_text("bundle: old", encoding="utf-8")
        (tmp_path / "SETUP.md").write_text("# old", encoding="utf-8")
        (tmp_path / "WARNINGS.md").write_text("# old", encoding="utf-8")

        clear_stale_outputs(tmp_path)

        assert not (tmp_path / "metadata").exists()
        assert not (tmp_path / "resources").exists()
        assert not (tmp_path / "src").exists()
        assert not (tmp_path / ".work").exists()
        assert not (tmp_path / "databricks.yml").exists()
        assert not (tmp_path / "SETUP.md").exists()
        assert not (tmp_path / "WARNINGS.md").exists()

    def test_preserves_unrelated_files(self, tmp_path):
        """Only orchestra-managed entries are removed; unrelated files stay put."""
        (tmp_path / "notes.txt").write_text("keep me", encoding="utf-8")
        (tmp_path / "user_data").mkdir()
        (tmp_path / "user_data" / "keep.csv").write_text("a,b", encoding="utf-8")

        clear_stale_outputs(tmp_path)

        assert (tmp_path / "notes.txt").read_text(encoding="utf-8") == "keep me"
        assert (tmp_path / "user_data" / "keep.csv").exists()

    def test_idempotent_on_empty_dir(self, tmp_path):
        """Clearing a directory with no orchestra artifacts is a no-op (no error)."""
        clear_stale_outputs(tmp_path)
        assert list(tmp_path.iterdir()) == []
