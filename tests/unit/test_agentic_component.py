"""Tests for generic agent-authored bundle components."""

from __future__ import annotations

import base64
import json

import pytest
import yaml

from flowx.bundler.dab_writer import pipeline_dict_to_ir, write_bundle
from flowx.ir_serde import activity_to_dict, pipeline_to_dict
from flowx.models.ir import AgenticComponentActivity, Pipeline
from flowx.preparer.workflow_preparer import prepare_workflow
from flowx.validate.bundle_invariants import check_bundle_dir

SOURCE_DEFINITION = {"type": "ExecuteDataFlow", "typeProperties": {"dataflow": "orders"}}
FILES = [
    {"path": "pipelines/orders.py", "content": "from pyspark import pipelines as dp\n"},
    {"path": "libraries/orders.whl", "binary_content": "UEsDBAoAAAAA"},
]
PIPELINE_DEFINITION = {
    "name": "orders_ingestion",
    "catalog": "${var.catalog}",
    "target": "${var.schema}",
    "ingestion_definition": {
        "connection_name": "flowx_orders_connection",
        "objects": [
            {
                "table": {
                    "source_catalog": "sales",
                    "source_schema": "dbo",
                    "source_table": "orders",
                    "destination_catalog": "${var.catalog}",
                    "destination_schema": "${var.schema}",
                    "destination_table": "orders",
                }
            }
        ],
    },
}
RESOURCES = [{"resource_key": "orders_ingestion", "definition": PIPELINE_DEFINITION}]
TASK = {"pipeline_task": {"pipeline_id": "${resources.pipelines.orders_ingestion.id}"}}


def _activity() -> AgenticComponentActivity:
    return AgenticComponentActivity(
        name="Ingest orders",
        task_key="ingest_orders",
        files=FILES,
        resources=RESOURCES,
        task=TASK,
        raw_definition=SOURCE_DEFINITION,
    )


def test_agentic_component_round_trips_through_serialized_ir():
    serialized = json.loads(json.dumps(activity_to_dict(_activity())))
    pipeline, _ = pipeline_dict_to_ir({"name": "orders", "tasks": [serialized]})
    restored = pipeline.tasks[0]

    assert type(restored).__name__ == "AgenticComponentActivity"
    assert activity_to_dict(restored) == serialized
    assert serialized == {
        "name": "Ingest orders",
        "task_key": "ingest_orders",
        "type": "AgenticComponentActivity",
        "files": FILES,
        "resources": RESOURCES,
        "task": TASK,
        "raw_definition": SOURCE_DEFINITION,
    }


def test_agentic_component_packages_authored_files_resource_and_pipeline_task(tmp_path):
    serialized_pipeline = json.loads(json.dumps(pipeline_to_dict(Pipeline(name="orders", tasks=[_activity()]))))
    restored_pipeline, _ = pipeline_dict_to_ir(serialized_pipeline)

    write_bundle(prepare_workflow(restored_pipeline), tmp_path)

    assert (tmp_path / "src" / "pipelines" / "orders.py").read_text(encoding="utf-8") == FILES[0]["content"]
    assert (tmp_path / "src" / "libraries" / "orders.whl").read_bytes() == base64.b64decode(FILES[1]["binary_content"])
    pipeline_resource = yaml.safe_load((tmp_path / "resources" / "orders_ingestion.yml").read_text(encoding="utf-8"))
    assert pipeline_resource == {"resources": {"pipelines": {"orders_ingestion": PIPELINE_DEFINITION}}}

    job_resource = yaml.safe_load((tmp_path / "resources" / "orders.yml").read_text(encoding="utf-8"))
    assert job_resource["resources"]["jobs"]["orders"]["tasks"] == [
        {"task_key": "ingest_orders", "pipeline_task": TASK["pipeline_task"]}
    ]
    assert check_bundle_dir(tmp_path).ok


def test_agentic_component_notebook_files_with_reserved_paths_stay_under_src(tmp_path):
    activity = AgenticComponentActivity(
        name="Custom notebook",
        task_key="custom_notebook",
        files=[
            {"path": "resources/custom.py", "content": "print('custom')\n"},
            {"path": "pyproject.toml", "content": "[project]\nname = 'custom'\n"},
        ],
        task={"notebook_task": {"notebook_path": "../src/resources/custom.py"}},
    )

    write_bundle(prepare_workflow(Pipeline(name="custom", tasks=[activity])), tmp_path)

    assert (tmp_path / "src" / "resources" / "custom.py").read_text(encoding="utf-8") == "print('custom')\n"
    assert (tmp_path / "src" / "pyproject.toml").read_text(encoding="utf-8") == "[project]\nname = 'custom'\n"
    assert not (tmp_path / "resources" / "custom.py").exists()
    assert not (tmp_path / "pyproject.toml").exists()
    job_resource = yaml.safe_load((tmp_path / "resources" / "custom.yml").read_text(encoding="utf-8"))
    assert job_resource["resources"]["jobs"]["custom"]["tasks"][0]["notebook_task"] == {
        "notebook_path": "../src/resources/custom.py"
    }


@pytest.mark.parametrize("path", ["../outside.py", "/tmp/outside.py"])
def test_agentic_component_rejects_file_paths_that_escape_src(path):
    activity = AgenticComponentActivity(
        name="Unsafe file",
        task_key="unsafe_file",
        files=[{"path": path, "content": "unsafe\n"}],
        task={"notebook_task": {"notebook_path": "../src/safe.py"}},
    )

    with pytest.raises(ValueError, match="relative to the bundle src directory"):
        prepare_workflow(Pipeline(name="unsafe", tasks=[activity]))
