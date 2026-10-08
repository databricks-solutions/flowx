"""Tests for generic agent-authored bundle components."""

from __future__ import annotations

import base64
import json

import pytest
import yaml

from flowx.bundler.dab_writer import _combine_airflow_workflows, pipeline_dict_to_ir, write_bundle
from flowx.ir_serde import activity_to_dict, pipeline_to_dict
from flowx.models.dab import SetupTask
from flowx.models.ir import (
    AgenticComponentActivity,
    Dependency,
    ForEachActivity,
    IfConditionActivity,
    Pipeline,
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

    with pytest.raises(ValueError):
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
        task={"spark_python_task": {"python_file": "../src/jobs/run.py"}},
    )
    second = AgenticComponentActivity(
        name="Second",
        task_key="second",
        files=[{"path": "jobs/run.py", "content": "print('second')\n"}],
        task={"spark_python_task": {"python_file": "../src/jobs/run.py"}},
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
        task={"spark_python_task": {"python_file": "../src/jobs/common.py"}},
    )
    second = AgenticComponentActivity(
        name="Second",
        task_key="second",
        files=shared,
        task={"spark_python_task": {"python_file": "../src/jobs/common.py"}},
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
    """Collect every emitted ``notebook_task`` across the bundle's job resources, keyed by notebook path."""
    found: dict[str, dict] = {}

    def visit(value) -> None:
        if isinstance(value, dict):
            notebook_task = value.get("notebook_task")
            if isinstance(notebook_task, dict):
                found[notebook_task["notebook_path"]] = notebook_task
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

    notebook_task = _notebook_tasks_by_path(tmp_path)["../src/notebooks/report.py"]
    assert "lookback_days" not in notebook_task.get("base_parameters", {})
