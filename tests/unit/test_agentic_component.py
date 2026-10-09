"""Tests for generic agent-authored bundle components."""

from __future__ import annotations

import base64
import json

import pytest
import yaml

from flowx.bundler.dab_writer import _combine_airflow_workflows, pipeline_dict_to_ir, write_bundle
from flowx.ir_serde import activity_to_dict, merge_agentic_results, pipeline_to_dict
from flowx.models.dab import SetupTask
from flowx.models.ir import (
    AgenticComponentActivity,
    Dependency,
    ForEachActivity,
    IfConditionActivity,
    Pipeline,
    PlaceholderActivity,
    SwitchActivity,
    SwitchCase,
    WaitActivity,
)
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
SERVERLESS_ENVIRONMENT = {
    "environment_key": "serverless",
    "spec": {"environment_version": "2", "dependencies": ["requests==2.32.3"]},
}
WHEEL_ENVIRONMENT = {
    "environment_key": "serverless",
    "spec": {"environment_version": "2", "dependencies": ["../src/libraries/orders.whl"]},
}


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
    assert job_resource["resources"]["jobs"]["custom"]["tasks"][0]["notebook_task"]["notebook_path"] == (
        "../src/resources/custom.py"
    )


def test_agentic_component_notebook_path_tracks_shared_workflow_namespacing(tmp_path):
    custom = AgenticComponentActivity(
        name="Custom notebook",
        task_key="custom_notebook",
        files=[{"path": "resources/custom.py", "content": "print('custom')\n"}],
        task={"notebook_task": {"notebook_path": "../src/resources/custom.py"}},
    )
    other = AgenticComponentActivity(
        name="Other notebook",
        task_key="other_notebook",
        files=[{"path": "notebooks/other.py", "content": "print('other')\n"}],
        task={"notebook_task": {"notebook_path": "../src/notebooks/other.py"}},
    )
    combined = _combine_airflow_workflows(
        [
            prepare_workflow(Pipeline(name="custom", tasks=[custom])),
            prepare_workflow(Pipeline(name="other", tasks=[other])),
        ]
    )

    write_bundle(combined, tmp_path)

    assert (tmp_path / "src" / "custom" / "resources" / "custom.py").exists()
    custom_job = yaml.safe_load((tmp_path / "resources" / "custom.yml").read_text(encoding="utf-8"))
    assert custom_job["resources"]["jobs"]["custom"]["tasks"][0]["notebook_task"]["notebook_path"] == (
        "../src/custom/resources/custom.py"
    )
    assert (tmp_path / "src" / "other" / "notebooks" / "other.py").exists()
    other_job = yaml.safe_load((tmp_path / "resources" / "other.yml").read_text(encoding="utf-8"))
    assert other_job["resources"]["jobs"]["other"]["tasks"][0]["notebook_task"]["notebook_path"] == (
        "../src/other/notebooks/other.py"
    )


@pytest.mark.parametrize(
    "path",
    ["../outside.py", "/tmp/outside.py", "..\\outside.py", "C:\\tmp\\outside.py"],
)
def test_agentic_component_rejects_file_paths_that_escape_src(path):
    activity = AgenticComponentActivity(
        name="Unsafe file",
        task_key="unsafe_file",
        files=[{"path": path, "content": "unsafe\n"}],
        task={"notebook_task": {"notebook_path": "../src/safe.py"}},
    )

    with pytest.raises(ValueError, match="relative to the bundle src directory"):
        prepare_workflow(Pipeline(name="unsafe", tasks=[activity]))


@pytest.mark.parametrize(
    "owned_field",
    [
        "task_key",
        "depends_on",
        "run_if",
        "timeout_seconds",
        "max_retries",
        "min_retry_interval_millis",
        "retry_on_timeout",
    ],
)
def test_agentic_component_rejects_task_wiring_that_sets_flowx_owned_fields(owned_field):
    activity = AgenticComponentActivity(
        name="Rewired",
        task_key="rewired",
        resources=RESOURCES,
        task={**TASK, owned_field: "authored"},
    )

    with pytest.raises(ValueError, match="flowx-owned"):
        prepare_workflow(Pipeline(name="rewired", tasks=[activity]))


def test_agentic_component_task_identity_dependencies_and_policy_come_from_the_activity(tmp_path):
    upstream = AgenticComponentActivity(name="Upstream", task_key="upstream", resources=RESOURCES, task=TASK)
    downstream = AgenticComponentActivity(
        name="Downstream",
        task_key="downstream",
        depends_on=[Dependency(task_key="upstream", outcome="Succeeded")],
        timeout_seconds=600,
        max_retries=2,
        task={"notebook_task": {"notebook_path": "../src/notebooks/downstream.py"}},
        files=[{"path": "notebooks/downstream.py", "content": "print('downstream')\n"}],
    )

    write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[upstream, downstream])), tmp_path)

    job_resource = yaml.safe_load((tmp_path / "resources" / "orders.yml").read_text(encoding="utf-8"))
    tasks = {task["task_key"]: task for task in job_resource["resources"]["jobs"]["orders"]["tasks"]}
    assert tasks["downstream"]["depends_on"] == [{"task_key": "upstream"}]
    assert tasks["downstream"]["timeout_seconds"] == 600
    assert tasks["downstream"]["max_retries"] == 2
    assert "notebook_task" in tasks["downstream"]
    assert check_bundle_dir(tmp_path).ok


def test_merged_agentic_component_takes_identity_dependencies_and_policy_from_the_placeholder(tmp_path):
    """The agent's own key, wiring, and run policy on a merged component are replaced by the placeholder's."""
    report = tmp_path / "translation_report.json"
    placeholders = Pipeline(
        name="orders",
        tasks=[
            WaitActivity(name="Extract", task_key="extract", wait_time_seconds=5),
            PlaceholderActivity(
                name="Transform",
                task_key="transform",
                original_type="ExecuteDataFlow",
                depends_on=[Dependency(task_key="extract", outcome="Succeeded")],
                timeout_seconds=3600,
                max_retries=2,
                min_retry_interval_millis=60000,
            ),
            PlaceholderActivity(name="Load", task_key="load", original_type="Custom"),
        ],
    )
    report.write_text(json.dumps(pipeline_to_dict(placeholders)), encoding="utf-8")
    results = tmp_path / "agentic_results"
    results.mkdir()
    transform = {
        "type": "AgenticComponentActivity",
        "name": "Renamed",
        "task_key": "rewired",
        "depends_on": [],
        "timeout_seconds": 5,
        "resources": RESOURCES,
        "task": TASK,
    }
    load = {
        "type": "AgenticComponentActivity",
        "depends_on": [{"task_key": "extract", "outcome": "Succeeded"}],
        "max_retries": 9,
        "files": [{"path": "jobs/load.py", "content": "print('load')\n"}],
        "task": {"spark_python_task": {"python_file": "../src/jobs/load.py"}, "existing_cluster_id": "0101-abc"},
    }
    (results / "transform.json").write_text(
        json.dumps({"activity_name": "Transform", "task": transform}), encoding="utf-8"
    )
    (results / "load.json").write_text(json.dumps({"activity_name": "Load", "task": load}), encoding="utf-8")

    assert merge_agentic_results(report, results) == (2, 0)
    merged_pipeline, _ = pipeline_dict_to_ir(json.loads(report.read_text(encoding="utf-8")))
    write_bundle(prepare_workflow(merged_pipeline), tmp_path / "bundle")

    job_resource = yaml.safe_load((tmp_path / "bundle" / "resources" / "orders.yml").read_text(encoding="utf-8"))
    tasks = {task["task_key"]: task for task in job_resource["resources"]["jobs"]["orders"]["tasks"]}
    assert tasks["transform"] == {
        "task_key": "transform",
        "depends_on": [{"task_key": "extract"}],
        "timeout_seconds": 3600,
        "retry_on_timeout": True,
        "max_retries": 2,
        "min_retry_interval_millis": 60000,
        "pipeline_task": TASK["pipeline_task"],
    }
    assert tasks["load"] == {
        "task_key": "load",
        "spark_python_task": {"python_file": "../src/jobs/load.py"},
        "existing_cluster_id": "0101-abc",
    }
    assert [task.name for task in merged_pipeline.tasks] == ["Extract", "Transform", "Load"]


@pytest.mark.parametrize("resource_key", ["../databricks", "../../outside", "nested/pipeline", "nested\\pipeline", ""])
def test_agentic_component_rejects_resource_keys_that_are_not_plain_identifiers(resource_key):
    activity = AgenticComponentActivity(
        name="Unsafe resource",
        task_key="unsafe_resource",
        resources=[{"resource_key": resource_key, "definition": PIPELINE_DEFINITION}],
        task=TASK,
    )

    with pytest.raises(ValueError, match="must be a plain identifier"):
        prepare_workflow(Pipeline(name="unsafe", tasks=[activity]))


def test_agentic_component_resource_key_matching_the_job_key_fails_instead_of_overwriting_the_job(tmp_path):
    activity = AgenticComponentActivity(name="Ingest orders", task_key="ingest_orders", resources=RESOURCES, task=TASK)

    with pytest.raises(ValueError, match="matches a job resource key"):
        write_bundle(prepare_workflow(Pipeline(name="orders_ingestion", tasks=[activity])), tmp_path)

    assert not (tmp_path / "resources" / "orders_ingestion.yml").exists()


def test_agentic_component_resource_key_matching_a_dbt_factory_job_key_fails(tmp_path):
    activity = AgenticComponentActivity(
        name="Ingest orders",
        task_key="ingest_orders",
        resources=[{"resource_key": "orders_dbt", "definition": PIPELINE_DEFINITION}],
        task={"pipeline_task": {"pipeline_id": "${resources.pipelines.orders_dbt.id}"}},
    )
    workflow = prepare_workflow(Pipeline(name="orders", tasks=[activity]))
    workflow.setup_tasks.append(
        SetupTask(
            type="pydabs_dbt_factory",
            config={"hook_module": "resources.orders_dbt_job", "job_key": "orders_dbt"},
        )
    )

    with pytest.raises(ValueError, match="matches a job resource key"):
        write_bundle(workflow, tmp_path)


def test_agentic_components_declaring_an_identical_pipeline_resource_write_it_once(tmp_path):
    first = AgenticComponentActivity(name="First", task_key="first", resources=RESOURCES, task=TASK)
    second = AgenticComponentActivity(name="Second", task_key="second", resources=RESOURCES, task=TASK)
    serialized_pipeline = json.loads(json.dumps(pipeline_to_dict(Pipeline(name="orders", tasks=[first, second]))))
    restored_pipeline, _ = pipeline_dict_to_ir(serialized_pipeline)

    created_files = write_bundle(prepare_workflow(restored_pipeline), tmp_path)

    resource_path = (tmp_path / "resources" / "orders_ingestion.yml").resolve()
    assert created_files.count(resource_path) == 1
    pipeline_resource = yaml.safe_load(resource_path.read_text(encoding="utf-8"))
    assert pipeline_resource == {"resources": {"pipelines": {"orders_ingestion": PIPELINE_DEFINITION}}}
    job_resource = yaml.safe_load((tmp_path / "resources" / "orders.yml").read_text(encoding="utf-8"))
    assert [task["pipeline_task"] for task in job_resource["resources"]["jobs"]["orders"]["tasks"]] == [
        TASK["pipeline_task"],
        TASK["pipeline_task"],
    ]
    assert check_bundle_dir(tmp_path).ok


def test_agentic_components_declaring_one_pipeline_definition_with_different_extra_fields_write_it_once(tmp_path):
    first = AgenticComponentActivity(
        name="First",
        task_key="first",
        resources=[{**RESOURCES[0], "comment": "for first"}],
        task=TASK,
    )
    second = AgenticComponentActivity(
        name="Second",
        task_key="second",
        resources=[{**RESOURCES[0], "comment": "for second"}],
        task=TASK,
    )
    serialized_pipeline = json.loads(json.dumps(pipeline_to_dict(Pipeline(name="orders", tasks=[first, second]))))
    restored_pipeline, _ = pipeline_dict_to_ir(serialized_pipeline)

    created_files = write_bundle(prepare_workflow(restored_pipeline), tmp_path)

    resource_path = (tmp_path / "resources" / "orders_ingestion.yml").resolve()
    assert created_files.count(resource_path) == 1
    pipeline_resource = yaml.safe_load(resource_path.read_text(encoding="utf-8"))
    assert pipeline_resource == {"resources": {"pipelines": {"orders_ingestion": PIPELINE_DEFINITION}}}


def test_agentic_components_sharing_a_resource_key_with_different_definitions_fail(tmp_path):
    first = AgenticComponentActivity(name="First", task_key="first", resources=RESOURCES, task=TASK)
    second = AgenticComponentActivity(
        name="Second",
        task_key="second",
        resources=[{"resource_key": "orders_ingestion", "definition": {**PIPELINE_DEFINITION, "name": "other"}}],
        task=TASK,
    )

    with pytest.raises(ValueError, match="more than one pipeline resource with different definitions"):
        write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[first, second])), tmp_path)
    assert not (tmp_path / "databricks.yml").exists()


@pytest.mark.parametrize(
    ("files", "resources", "task"),
    [
        ([{"path": "notebooks/a.py", "content": None}], [], TASK),
        ([{"path": "notebooks/a.py", "content": ["line 1", "line 2"]}], [], TASK),
        ([{"path": "libraries/a.whl", "binary_content": None}], [], TASK),
        ([{"path": "libraries/a.whl", "content": "text", "binary_content": "UEsDBAoAAAAA"}], [], TASK),
        (["notebooks/a.py"], [], TASK),
        ([{"content": "print('no path')\n"}], [], TASK),
        ([{"path": None, "content": "print('null path')\n"}], [], TASK),
        ([], ["orders_ingestion"], TASK),
        ([], [{"resource_key": "orders_ingestion"}], TASK),
        ([], [{"resource_key": "orders_ingestion", "definition": "orders"}], TASK),
        ([], [{"resource_key": "orders_ingestion", "definition": ["orders"]}], TASK),
        ([], [], {"notebook_task": "../src/notebooks/a.py"}),
    ],
)
def test_agentic_component_rejects_malformed_authored_payloads(files, resources, task):
    activity = AgenticComponentActivity(name="Bad", task_key="bad", files=files, resources=resources, task=task)

    with pytest.raises(ValueError, match="Agentic component 'bad'"):
        prepare_workflow(Pipeline(name="orders", tasks=[activity]))


def test_agentic_component_task_fragment_keeps_its_own_description_and_compute(tmp_path):
    """Only the flowx-owned fields come from the activity; the fragment's compute is not overridden."""
    activity = AgenticComponentActivity(
        name="Run job",
        task_key="run_job",
        description="Copied from the source activity",
        existing_cluster_id="0101-123456-abcdefgh",
        files=[{"path": "jobs/run.py", "content": "print('run')\n"}],
        task={"spark_python_task": {"python_file": "../src/jobs/run.py"}, "job_cluster_key": "default_cluster"},
    )

    write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[activity])), tmp_path)

    job_resource = yaml.safe_load((tmp_path / "resources" / "orders.yml").read_text(encoding="utf-8"))
    assert job_resource["resources"]["jobs"]["orders"]["tasks"] == [
        {
            "task_key": "run_job",
            "spark_python_task": {"python_file": "../src/jobs/run.py"},
            "job_cluster_key": "default_cluster",
        }
    ]


def test_agentic_component_rejects_binary_content_that_is_not_plain_base64():
    activity = AgenticComponentActivity(
        name="Wheel",
        task_key="wheel",
        files=[{"path": "libraries/orders.whl", "binary_content": "data:application/zip;base64,UEsDBAoAAAAA"}],
        task={"notebook_task": {"notebook_path": "../src/notebooks/orders.py"}},
    )

    with pytest.raises(
        ValueError, match="Agentic component 'wheel' file 'libraries/orders.whl' binary_content must be valid base64"
    ):
        prepare_workflow(Pipeline(name="orders", tasks=[activity]))


@pytest.mark.parametrize("path", ["\\tmp\\outside.py", "C:outside.py", "\\\\server\\share\\outside.py"])
def test_agentic_component_rejects_windows_rooted_or_drive_relative_paths(path):
    """A path with a Windows root or drive but no full absolute form still escapes the bundle."""
    activity = AgenticComponentActivity(
        name="Unsafe file",
        task_key="unsafe_file",
        files=[{"path": path, "content": "unsafe\n"}],
        task={"notebook_task": {"notebook_path": "../src/safe.py"}},
    )

    with pytest.raises(ValueError, match="relative to the bundle src directory"):
        prepare_workflow(Pipeline(name="unsafe", tasks=[activity]))


@pytest.mark.parametrize(
    "task",
    [
        {},
        {"notebook_task": {"notebook_path": "../src/a.py"}, "pipeline_task": {"pipeline_id": "x"}},
    ],
)
def test_agentic_component_requires_exactly_one_executable_payload(task):
    activity = AgenticComponentActivity(name="No payload", task_key="no_payload", task=task)

    with pytest.raises(ValueError, match="exactly one executable payload"):
        prepare_workflow(Pipeline(name="orders", tasks=[activity]))


def test_agentic_components_authoring_different_content_at_one_path_fail(tmp_path):
    """The writer would rename the second file, leaving its task running the first component's code."""
    first = AgenticComponentActivity(
        name="First",
        task_key="first",
        files=[{"path": "jobs/run.py", "content": "print('first')\n"}],
        task={"spark_python_task": {"python_file": "../src/jobs/run.py"}, "existing_cluster_id": "0101-abc"},
    )
    second = AgenticComponentActivity(
        name="Second",
        task_key="second",
        files=[{"path": "jobs/run.py", "content": "print('second')\n"}],
        task={"spark_python_task": {"python_file": "../src/jobs/run.py"}, "existing_cluster_id": "0101-abc"},
    )

    with pytest.raises(ValueError, match="'jobs/run.py' shares its path"):
        write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[first, second])), tmp_path)
    assert not (tmp_path / "databricks.yml").exists()
    assert not (tmp_path / "resources").exists()
    assert not (tmp_path / "src").exists()


def test_agentic_components_sharing_identical_file_content_still_package(tmp_path):
    shared = [{"path": "jobs/common.py", "content": "print('shared')\n"}]
    first = AgenticComponentActivity(
        name="First",
        task_key="first",
        files=shared,
        task={"spark_python_task": {"python_file": "../src/jobs/common.py"}, "existing_cluster_id": "0101-abc"},
    )
    second = AgenticComponentActivity(
        name="Second",
        task_key="second",
        files=shared,
        task={"spark_python_task": {"python_file": "../src/jobs/common.py"}, "existing_cluster_id": "0101-abc"},
    )

    write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[first, second])), tmp_path)

    assert (tmp_path / "src" / "jobs" / "common.py").read_text(encoding="utf-8") == "print('shared')\n"


def test_agentic_component_file_at_a_generated_notebook_path_fails(tmp_path):
    """The writer would rename one of the two files, leaving one task running the other's code."""
    wait = WaitActivity(name="Pause", task_key="pause", wait_time_seconds=5)
    custom = AgenticComponentActivity(
        name="Custom",
        task_key="custom",
        files=[{"path": "notebooks/pause.py", "content": "print('custom')\n"}],
        task={"notebook_task": {"notebook_path": "../src/notebooks/pause.py"}},
    )

    with pytest.raises(ValueError, match="'notebooks/pause.py'"):
        write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[wait, custom])), tmp_path)
    assert not (tmp_path / "src").exists()


def test_agentic_component_file_at_a_generated_setup_notebook_path_fails(tmp_path):
    """Setup notebooks are written after authored files and would silently replace them."""
    custom = AgenticComponentActivity(
        name="Custom",
        task_key="custom",
        files=[{"path": "setup/create_volumes.py", "content": "print('custom')\n"}],
        task={"notebook_task": {"notebook_path": "../src/setup/create_volumes.py"}},
    )
    workflow = prepare_workflow(Pipeline(name="orders", tasks=[custom]))
    workflow.setup_tasks.append(SetupTask(type="volume", config={"volume_name": "landing"}))

    with pytest.raises(ValueError, match="'setup/create_volumes.py'"):
        write_bundle(workflow, tmp_path)
    assert not (tmp_path / "src").exists()


def _inside_if_condition(activity: AgenticComponentActivity) -> IfConditionActivity:
    return IfConditionActivity(
        name="Check", task_key="check", op="EQUAL_TO", left="1", right="1", if_true_activities=[activity]
    )


def _inside_switch(activity: AgenticComponentActivity) -> SwitchActivity:
    return SwitchActivity(
        name="Route",
        task_key="route",
        on_expression="orders",
        cases=[SwitchCase(value="orders", activities=[activity])],
    )


def _inside_for_each(activity: AgenticComponentActivity) -> ForEachActivity:
    return ForEachActivity(name="Loop", task_key="loop", items_expression='["a"]', inner_activities=[activity])


def _inside_for_each_with_siblings(activity: AgenticComponentActivity) -> ForEachActivity:
    sibling = WaitActivity(name="Pause", task_key="pause", wait_time_seconds=5)
    return ForEachActivity(name="Loop", task_key="loop", items_expression='["a"]', inner_activities=[activity, sibling])


@pytest.mark.parametrize(
    "wrap", [_inside_if_condition, _inside_switch, _inside_for_each, _inside_for_each_with_siblings]
)
def test_agentic_component_pipeline_resource_survives_control_flow(tmp_path, wrap):
    write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[wrap(_activity())])), tmp_path)

    pipeline_resource = yaml.safe_load((tmp_path / "resources" / "orders_ingestion.yml").read_text(encoding="utf-8"))
    assert pipeline_resource == {"resources": {"pipelines": {"orders_ingestion": PIPELINE_DEFINITION}}}
    assert check_bundle_dir(tmp_path).ok


def _notebook_tasks_by_path(bundle_dir) -> dict[str, dict]:
    """Collect every emitted task with a ``notebook_task`` across the bundle's job resources, keyed by notebook path."""
    found: dict[str, dict] = {}

    def visit(value) -> None:
        if isinstance(value, dict):
            notebook_task = value.get("notebook_task")
            if isinstance(notebook_task, dict):
                found[notebook_task["notebook_path"]] = value
            for item in value.values():
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    for resource_path in sorted((bundle_dir / "resources").glob("*.yml")):
        visit(yaml.safe_load(resource_path.read_text(encoding="utf-8")))
    return found


@pytest.mark.parametrize(
    "wrap",
    [lambda activity: activity, _inside_for_each, _inside_for_each_with_siblings],
    ids=["top_level", "for_each", "for_each_with_siblings"],
)
def test_agentic_component_notebook_widget_defaults_are_not_overridden(tmp_path, wrap):
    """The authored notebook's own widget default must apply, not an injected empty base parameter."""
    report = AgenticComponentActivity(
        name="Report",
        task_key="report",
        files=[
            {
                "path": "notebooks/report.py",
                "content": (
                    'dbutils.widgets.text("lookback_days", "7")\n'
                    'lookback_days = int(dbutils.widgets.get("lookback_days"))\n'
                ),
            }
        ],
        task={"notebook_task": {"notebook_path": "../src/notebooks/report.py"}},
    )

    write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[wrap(report)])), tmp_path)

    task = _notebook_tasks_by_path(tmp_path)["../src/notebooks/report.py"]
    assert "lookback_days" not in task["notebook_task"].get("base_parameters", {})
    assert "_authored" not in task


@pytest.mark.parametrize(
    ("fragment", "parameters_field"),
    [
        (
            {
                "notebook_task": {
                    "notebook_path": "../src/notebooks/load.py",
                    "base_parameters": {"item": "{{input.table_name}}"},
                }
            },
            ("notebook_task", "base_parameters"),
        ),
        (
            {"run_job_task": {"job_id": 123, "job_parameters": {"item": "{{input.table_name}}"}}},
            ("run_job_task", "job_parameters"),
        ),
    ],
    ids=["notebook_task", "run_job_task"],
)
def test_agentic_component_inside_for_each_keeps_its_own_item_parameter(tmp_path, fragment, parameters_field):
    load = AgenticComponentActivity(
        name="Load",
        task_key="load",
        files=[{"path": "notebooks/load.py", "content": "print('load')\n"}],
        task=fragment,
    )

    write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[_inside_for_each(load)])), tmp_path)

    job_resource = yaml.safe_load((tmp_path / "resources" / "orders.yml").read_text(encoding="utf-8"))
    body = job_resource["resources"]["jobs"]["orders"]["tasks"][0]["for_each_task"]["task"]
    payload, parameters = parameters_field
    assert body[payload][parameters] == {"item": "{{input.table_name}}"}
    assert "_authored" not in body


def test_agentic_component_notifications_are_added_only_when_the_fragment_sets_none(tmp_path):
    collapsed = {"destination": "email", "events": ["on_failure"], "args": {"addresses": ["flowx@example.com"]}}
    authored = AgenticComponentActivity(
        name="Authored",
        task_key="authored",
        notifications=collapsed,
        resources=RESOURCES,
        task={**TASK, "email_notifications": {"on_success": ["owner@example.com"]}},
    )
    plain = AgenticComponentActivity(name="Plain", task_key="plain", notifications=collapsed, task=TASK)

    write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[authored, plain])), tmp_path)

    job_resource = yaml.safe_load((tmp_path / "resources" / "orders.yml").read_text(encoding="utf-8"))
    tasks = {task["task_key"]: task for task in job_resource["resources"]["jobs"]["orders"]["tasks"]}
    assert tasks["authored"]["email_notifications"] == {"on_success": ["owner@example.com"]}
    assert tasks["plain"]["email_notifications"] == {"on_failure": ["flowx@example.com"]}


def test_agentic_component_with_its_own_compute_never_gets_a_second_binding(tmp_path):
    serverless = AgenticComponentActivity(
        name="Serverless",
        task_key="serverless",
        environments=[{**SERVERLESS_ENVIRONMENT, "environment_key": "default"}],
        task={"notebook_task": {"notebook_path": "/Workspace/Shared/etl/orders"}, "environment_key": "default"},
    )
    unbound = AgenticComponentActivity(
        name="Unbound",
        task_key="unbound",
        task={"notebook_task": {"notebook_path": "/Workspace/Shared/etl/customers"}},
    )

    write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[serverless, unbound])), tmp_path)

    job_resource = yaml.safe_load((tmp_path / "resources" / "orders.yml").read_text(encoding="utf-8"))
    tasks = {task["task_key"]: task for task in job_resource["resources"]["jobs"]["orders"]["tasks"]}
    assert tasks["serverless"] == {
        "task_key": "serverless",
        "notebook_task": {"notebook_path": "/Workspace/Shared/etl/orders"},
        "environment_key": "default",
    }
    assert tasks["unbound"]["job_cluster_key"] == "default_cluster"
    assert job_resource["resources"]["jobs"]["orders"]["environments"] == [
        {**SERVERLESS_ENVIRONMENT, "environment_key": "default"}
    ]
    assert check_bundle_dir(tmp_path).ok


def test_agentic_component_base_parameters_that_look_dynamic_are_written_as_given(tmp_path):
    activity = AgenticComponentActivity(
        name="Report",
        task_key="report",
        files=[{"path": "notebooks/report.py", "content": "print('report')\n"}],
        task={
            "notebook_task": {
                "notebook_path": "../src/notebooks/report.py",
                "base_parameters": {"cutoff": "datetime.now(timezone.utc) - timedelta(days=7)"},
            }
        },
    )

    write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[activity])), tmp_path)

    job_resource = yaml.safe_load((tmp_path / "resources" / "orders.yml").read_text(encoding="utf-8"))
    assert job_resource["resources"]["jobs"]["orders"]["tasks"] == [
        {
            "task_key": "report",
            "notebook_task": {
                "notebook_path": "../src/notebooks/report.py",
                "base_parameters": {"cutoff": "datetime.now(timezone.utc) - timedelta(days=7)"},
            },
        }
    ]


def test_agentic_component_environments_round_trip_through_serialized_ir():
    activity = AgenticComponentActivity(
        name="Run",
        task_key="run",
        environments=[SERVERLESS_ENVIRONMENT],
        task={"spark_python_task": {"python_file": "../src/jobs/run.py"}, "environment_key": "serverless"},
    )

    serialized = json.loads(json.dumps(activity_to_dict(activity)))
    restored_pipeline, _ = pipeline_dict_to_ir({"name": "orders", "tasks": [serialized]})

    assert serialized["environments"] == [SERVERLESS_ENVIRONMENT]
    assert restored_pipeline.tasks[0].environments == [SERVERLESS_ENVIRONMENT]
    assert activity_to_dict(restored_pipeline.tasks[0]) == serialized


def _serverless_component(payload_kind: str, task_key: str = "run") -> AgenticComponentActivity:
    if payload_kind == "spark_python_task":
        files = [{"path": "jobs/run.py", "content": "print('run')\n"}]
        payload = {"spark_python_task": {"python_file": "../src/jobs/run.py"}}
        environment = SERVERLESS_ENVIRONMENT
    else:
        files = [{"path": "libraries/orders.whl", "binary_content": "UEsDBAoAAAAA"}]
        payload = {"python_wheel_task": {"package_name": "orders", "entry_point": "main"}}
        environment = WHEEL_ENVIRONMENT
    return AgenticComponentActivity(
        name=task_key.title(),
        task_key=task_key,
        files=files,
        environments=[environment],
        task={**payload, "environment_key": "serverless"},
    )


@pytest.mark.parametrize("payload_kind", ["spark_python_task", "python_wheel_task"])
def test_serverless_agentic_component_gets_its_environment_in_the_job(tmp_path, payload_kind):
    component = _serverless_component(payload_kind)
    serialized_pipeline = json.loads(json.dumps(pipeline_to_dict(Pipeline(name="orders", tasks=[component]))))
    restored_pipeline, _ = pipeline_dict_to_ir(serialized_pipeline)

    write_bundle(prepare_workflow(restored_pipeline), tmp_path)

    job = yaml.safe_load((tmp_path / "resources" / "orders.yml").read_text(encoding="utf-8"))["resources"]["jobs"][
        "orders"
    ]
    assert job["environments"] == component.environments
    assert job["tasks"] == [{"task_key": "run", **component.task}]
    assert "job_clusters" not in job
    if payload_kind == "python_wheel_task":
        assert job["environments"][0]["spec"]["dependencies"] == ["../src/libraries/orders.whl"]
        assert (tmp_path / "src" / "libraries" / "orders.whl").exists()
    assert check_bundle_dir(tmp_path).ok


def _jobs_by_task_key(bundle_dir) -> dict[str, dict]:
    """Map every emitted task key (including ForEach bodies) to the job that holds it."""
    jobs_by_task_key: dict[str, dict] = {}
    for resource_path in sorted((bundle_dir / "resources").glob("*.yml")):
        jobs = (yaml.safe_load(resource_path.read_text(encoding="utf-8"))["resources"]).get("jobs") or {}
        for job in jobs.values():
            pending = list(job["tasks"])
            while pending:
                task = pending.pop()
                jobs_by_task_key[task["task_key"]] = job
                body = (task.get("for_each_task") or {}).get("task")
                if body:
                    pending.append(body)
    return jobs_by_task_key


@pytest.mark.parametrize(
    "wrap", [_inside_if_condition, _inside_switch, _inside_for_each, _inside_for_each_with_siblings]
)
def test_serverless_agentic_component_environment_lands_in_the_job_that_runs_it(tmp_path, wrap):
    write_bundle(
        prepare_workflow(Pipeline(name="orders", tasks=[wrap(_serverless_component("spark_python_task"))])),
        tmp_path,
    )

    jobs_by_task_key = _jobs_by_task_key(tmp_path)
    assert jobs_by_task_key["run"]["environments"] == [SERVERLESS_ENVIRONMENT]
    assert check_bundle_dir(tmp_path).ok


def test_agentic_component_environment_key_that_no_component_declares_fails(tmp_path):
    activity = AgenticComponentActivity(
        name="Run",
        task_key="run",
        files=[{"path": "jobs/run.py", "content": "print('run')\n"}],
        task={"spark_python_task": {"python_file": "../src/jobs/run.py"}, "environment_key": "missing"},
    )

    with pytest.raises(ValueError, match="environment_key 'missing'"):
        write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[activity])), tmp_path)
    assert not (tmp_path / "databricks.yml").exists()


def test_agentic_components_declaring_an_identical_environment_write_it_once(tmp_path):
    first = _serverless_component("spark_python_task", task_key="first")
    second = _serverless_component("spark_python_task", task_key="second")

    write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[first, second])), tmp_path)

    job = yaml.safe_load((tmp_path / "resources" / "orders.yml").read_text(encoding="utf-8"))["resources"]["jobs"][
        "orders"
    ]
    assert job["environments"] == [SERVERLESS_ENVIRONMENT]
    assert check_bundle_dir(tmp_path).ok


def test_agentic_components_declaring_one_environment_key_with_different_specs_fail(tmp_path):
    first = _serverless_component("spark_python_task", task_key="first")
    second = _serverless_component("spark_python_task", task_key="second")
    second.environments = [{"environment_key": "serverless", "spec": {"environment_version": "3"}}]

    with pytest.raises(ValueError, match="declared more than once with different specs"):
        write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[first, second])), tmp_path)
    assert not (tmp_path / "databricks.yml").exists()


@pytest.mark.parametrize(
    "environment",
    [
        "serverless",
        {"environment_key": "serverless"},
        {"environment_key": "serverless", "spec": "2"},
        {"environment_key": "", "spec": {"environment_version": "2"}},
        {"environment_key": 5, "spec": {"environment_version": "2"}},
        {**SERVERLESS_ENVIRONMENT, "comment": "extra"},
    ],
)
def test_agentic_component_rejects_malformed_environments(environment):
    activity = AgenticComponentActivity(
        name="Bad",
        task_key="bad",
        environments=[environment],
        task={"spark_python_task": {"python_file": "../src/jobs/run.py"}, "environment_key": "serverless"},
    )

    with pytest.raises(ValueError, match="Agentic component 'bad' environment"):
        prepare_workflow(Pipeline(name="orders", tasks=[activity]))


@pytest.mark.parametrize(
    "payload",
    [
        {"spark_python_task": {"python_file": "../src/jobs/run.py"}},
        {"python_wheel_task": {"package_name": "orders", "entry_point": "main"}},
        {"spark_jar_task": {"main_class_name": "com.example.Main"}},
    ],
)
def test_agentic_component_python_or_jar_task_without_compute_fails(payload):
    """flowx binds a cluster only for notebooks, so these would package with no compute and fail at deploy."""
    activity = AgenticComponentActivity(name="Run", task_key="run", task=payload)

    with pytest.raises(ValueError, match="'run' .* must name its compute"):
        prepare_workflow(Pipeline(name="orders", tasks=[activity]))


def test_agentic_component_file_without_content_or_binary_content_fails():
    activity = AgenticComponentActivity(
        name="Run",
        task_key="run",
        files=[{"path": "notebooks/run.py"}],
        task={"notebook_task": {"notebook_path": "../src/notebooks/run.py"}},
    )

    with pytest.raises(ValueError, match="'run' file 'notebooks/run.py' must set content or binary_content"):
        prepare_workflow(Pipeline(name="orders", tasks=[activity]))


@pytest.mark.parametrize("content", ["print('no trailing newline')", ""])
def test_agentic_component_text_file_is_written_byte_for_byte(tmp_path, content):
    activity = AgenticComponentActivity(
        name="Run",
        task_key="run",
        files=[{"path": "notebooks/run.py", "content": content}],
        task={"notebook_task": {"notebook_path": "../src/notebooks/run.py"}},
    )

    write_bundle(prepare_workflow(Pipeline(name="orders", tasks=[activity])), tmp_path)

    assert (tmp_path / "src" / "notebooks" / "run.py").read_bytes() == content.encode("utf-8")
