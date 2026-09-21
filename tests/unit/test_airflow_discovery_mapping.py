from __future__ import annotations

from pathlib import Path

from flowx.discovery_inventory import build_source_inventory
from flowx.discovery_lineage import walk_nodes
from flowx.discovery_serde import source_graph_from_dict, source_graph_to_dict
from flowx.models.discovery import CONCEPT_GROUP, CONCEPT_LOOP, ContainerNode, GapNode
from flowx.sources.airflow.loader import load_airflow_dag_results, load_discovery_results


def _load(tmp_path: Path, source: str):
    dag_path = tmp_path / "dag.py"
    dag_path.write_text(source, encoding="utf-8")
    results = load_airflow_dag_results(dag_path, source_file="dags/dag.py")
    assert len(results) == 1
    return results[0]


def test_maps_assigned_dag_metadata_tasks_edges_and_collision_safe_keys(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="source_graph",
    schedule_interval="0 3 * * *",
    default_args={"retries": 3, "retry_delay": timedelta(minutes=4)},
    params={"environment": "prod"},
    dagrun_timeout=timedelta(hours=2),
    tags=["migration", "airflow"],
    description="Source faithful",
)
first = BashOperator(task_id="load.data", bash_command="echo first", dag=dag)
second = BashOperator(
    task_id="load_data",
    bash_command="echo second",
    trigger_rule="all_done",
    retries=5,
    dag=dag,
)
first >> second
""",
    )

    graph = result.graph
    assert graph.name == "source_graph"
    assert graph.description == "Source faithful"
    assert graph.parameters["environment"].default == "prod"
    assert graph.schedule is not None
    assert graph.schedule.expression == "0 3 * * *"
    assert graph.default_policy is not None
    assert graph.default_policy.max_retries == 3
    assert graph.default_policy.retry_interval_seconds == 240
    assert graph.run_timeout_seconds == 7200
    assert graph.tags == ["migration", "airflow"]
    assert graph.raw is not None
    assert graph.raw["capture_id"].startswith("dag:")
    assert graph.extensions["source_file"] == "dags/dag.py"
    assert graph.extensions["edge_captures"][0]["upstream_capture_id"] == "first"
    assert graph.extensions["edge_captures"][0]["downstream_capture_id"] == "second"

    nodes = {node.source_id: node for node in walk_nodes(graph.tasks) if not node.properties.get("structural_only")}
    assert nodes["first"].task_key == "load_data"
    assert nodes["second"].task_key == "load_data__2"
    assert [dependency.upstream for dependency in nodes["second"].dependencies] == ["load_data"]
    assert nodes["second"].run_condition == "all_done"
    assert nodes["second"].policy is not None
    assert nodes["second"].policy.max_retries == 5
    assert nodes["first"].raw is not None
    assert nodes["first"].raw["operator_fqn"] == "airflow.operators.bash.BashOperator"
    assert nodes["first"].raw["source_span"]["line"] > 0
    assert nodes["first"].raw["arguments"]["bash_command"]["value"] == "echo first"

    assert source_graph_from_dict(source_graph_to_dict(graph)) == graph


def test_maps_task_groups_and_dynamic_mapping_as_containers(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(dag_id="groups", schedule=None) as dag:
    start = BashOperator(task_id="start", bash_command="echo start")
    with TaskGroup(group_id="processing") as processing:
        mapped = BashOperator.partial(task_id="work", bash_command="echo {{ params.item }}").expand(
            params=[{"item": "a"}, {"item": "b"}]
        )
    end = BashOperator(task_id="end", bash_command="echo end")
    start >> processing >> end
""",
    )

    group = next(
        node for node in result.graph.tasks if isinstance(node, ContainerNode) and node.concept == CONCEPT_GROUP
    )
    assert group.properties == {"inventory_visible": False, "structural_only": True}
    mapped = group.branches["group"][0]
    assert isinstance(mapped, ContainerNode)
    assert mapped.concept == CONCEPT_LOOP
    assert mapped.task_key == "processing__work"
    assert mapped.properties["mapping"] == {"expand_arguments": ["params"], "has_partial": True}
    end = next(node for node in result.graph.tasks if node.name == "end")
    assert [dependency.upstream for dependency in end.dependencies] == ["processing__work"]
    inventory = build_source_inventory([result.graph], source="airflow", source_dir="/dags")
    assert inventory["summary"]["activity_count"] == 3
    assert [activity["name"] for activity in inventory["pipelines"][0]["activities"]] == [
        "start",
        "work",
        "end",
    ]


def test_maps_taskflow_xcom_and_cross_dag_control_lineage(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(dag_id="lineage", schedule=None)
def build():
    @task
    def extract():
        return 1

    @task
    def consume(value):
        return value

    raw = extract()
    done = consume(raw)
    trigger = TriggerDagRunOperator(task_id="trigger_child", trigger_dag_id="child_dag")
    done >> trigger

build()
""",
    )

    nodes = {node.task_key: node for node in walk_nodes(result.graph.tasks)}
    assert nodes["raw"].data_writes[0].signature == "xcom:raw"
    assert nodes["done"].data_reads[0].signature == "xcom:raw"
    assert nodes["trigger_child"].properties["invokes_workflow"] == "child_dag"
    assert result.graph.lineage is not None
    assert len(result.graph.lineage.data_edges) == 1
    assert result.graph.lineage.data_edges[0].source_task_key == "raw"
    assert result.graph.lineage.data_edges[0].target_task_key == "done"
    assert len(result.graph.lineage.control_edges) == 1
    assert result.graph.lineage.control_edges[0].target_workflow == "child_dag"


def test_persists_factory_and_task_callable_definitions(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator

def classic_callable():
    return "classic"

@task_group
def grouped():
    @task
    def nested():
        return "nested"
    nested()

@dag(dag_id="callables", schedule=None)
def build():
    @task
    def taskflow_callable():
        return "taskflow"

    classic = PythonOperator(task_id="classic", python_callable=classic_callable)
    taskflow = taskflow_callable()
    group = grouped()
    classic >> taskflow >> group

build()
""",
    )

    assert result.graph.raw is not None
    assert "def build():" in result.graph.raw["factory_definition"]["source"]
    nodes = {node.source_id: node for node in walk_nodes(result.graph.tasks)}
    assert "def classic_callable():" in nodes["classic"].raw["callable_definition"]["source"]
    assert "def taskflow_callable():" in nodes["taskflow"].raw["callable_definition"]["source"]
    assert "def grouped():" in nodes["group"].raw["callable_definition"]["source"]


def test_ordering_only_taskflow_dependency_does_not_create_xcom_lineage(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow.decorators import dag, task

@dag(dag_id="ordering_only", schedule=None)
def build():
    @task
    def first():
        return 1

    @task
    def second():
        return 2

    first_task = first()
    second_task = second()
    first_task >> second_task

build()
""",
    )

    nodes = {node.source_id: node for node in walk_nodes(result.graph.tasks)}
    assert [dependency.upstream for dependency in nodes["second_task"].dependencies] == ["first_task"]
    assert nodes["second_task"].data_reads == []
    assert result.graph.lineage is not None
    assert result.graph.lineage.data_edges == []


def test_preserves_unknown_trigger_dag_run_wait_semantics(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow import DAG
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(dag_id="dynamic_wait", schedule=None) as dag:
    trigger = TriggerDagRunOperator(
        task_id="trigger",
        trigger_dag_id="child",
        wait_for_completion=Variable.get("WAIT_FOR_CHILD") == "true",
    )
""",
    )

    trigger = result.graph.tasks[0]
    assert trigger.properties["invokes_wait"] is None
    assert result.graph.lineage is not None
    assert result.graph.lineage.control_edges[0].wait_for_completion is None


def test_preserves_mixed_task_kind_source_order_and_collision_allocation(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

@task
def taskflow_callable():
    return 1

@dag(dag_id="mixed_order", schedule=None)
def build():
    same = taskflow_callable()
    middle = BashOperator(task_id="same", bash_command="echo middle")
    last = taskflow_callable()

build()
""",
    )

    assert [(node.source_id, node.task_key) for node in result.graph.tasks] == [
        ("same", "same__2"),
        ("middle", "same"),
        ("last", "last"),
    ]
    assert [task.task_key for task in result.pipeline.tasks] == ["same", "same__2", "last"]
    assert [task.name for task in result.pipeline.tasks] == ["same", "same", "last"]


def test_records_unresolved_cross_workflow_invocations(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow import DAG
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

with DAG(dag_id="dynamic_targets", schedule=None) as dag:
    trigger = TriggerDagRunOperator(task_id="trigger", trigger_dag_id=Variable.get("CHILD_DAG"))
    run_now = DatabricksRunNowOperator(task_id="run_now", job_id=Variable.get("JOB_ID"))
""",
    )

    nodes = {node.task_key: node for node in result.graph.tasks}
    assert nodes["trigger"].properties["invokes_workflow"] == ""
    assert nodes["run_now"].properties["invokes_workflow"] == ""
    assert result.graph.lineage is not None
    control_edges = [
        (edge.via_task_key, edge.target_workflow, edge.resolved) for edge in result.graph.lineage.control_edges
    ]
    assert control_edges == [
        ("trigger", "", False),
        ("run_now", "", False),
    ]


def test_preserves_mapped_taskflow_xcom_lineage(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow.decorators import dag, task

@dag(dag_id="mapped_xcom", schedule=None)
def build():
    @task
    def extract():
        return [1, 2]

    @task
    def process(value, fixed=None):
        return value

    upstream = extract()
    mapped = process.expand(value=upstream)
    partial_mapped = process.partial(fixed=upstream).expand(value=[1, 2])

build()
""",
    )

    nodes = {node.source_id: node for node in result.graph.tasks}
    assert [asset.signature for asset in nodes["mapped"].data_reads] == ["xcom:upstream"]
    assert [asset.signature for asset in nodes["partial_mapped"].data_reads] == ["xcom:upstream"]
    assert result.graph.lineage is not None
    assert {(edge.source_task_key, edge.target_task_key) for edge in result.graph.lineage.data_edges} == {
        ("upstream", "mapped"),
        ("upstream", "partial_mapped"),
    }


def test_maps_context_managed_cosmos_task_group(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow import DAG
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig

with DAG(dag_id="cosmos_context", schedule=None) as dag:
    with DbtTaskGroup(
        group_id="transform",
        project_config=ProjectConfig("/opt/dbt"),
        profile_config=ProfileConfig(profile_name="analytics", target_name="prod"),
    ) as transform:
        pass
""",
    )

    transform = next(node for node in walk_nodes(result.graph.tasks) if node.source_id == "transform")
    assert transform.native_type == "DbtTaskGroup"
    assert transform.raw is not None
    assert transform.raw["operator_fqn"] == "cosmos.DbtTaskGroup"


def test_resolves_named_task_assets_and_lineage(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.sdk import Asset

orders = Dataset("s3://warehouse/orders")
customers = Asset(uri="s3://warehouse/customers")

with DAG(dag_id="named_assets", schedule=None) as dag:
    produce_orders = BashOperator(task_id="produce_orders", bash_command="echo orders", outlets=[orders])
    consume_orders = BashOperator(task_id="consume_orders", bash_command="echo orders", inlets=[orders])
    produce_customers = BashOperator(task_id="produce_customers", bash_command="echo customers", outlets=[customers])
    consume_customers = BashOperator(task_id="consume_customers", bash_command="echo customers", inlets=[customers])
""",
    )

    nodes = {node.task_key: node for node in result.graph.tasks}
    assert nodes["produce_orders"].data_writes[0].identity == "s3://warehouse/orders"
    assert nodes["consume_orders"].data_reads[0].identity == "s3://warehouse/orders"
    assert nodes["produce_customers"].data_writes[0].identity == "s3://warehouse/customers"
    assert nodes["consume_customers"].data_reads[0].identity == "s3://warehouse/customers"
    assert result.graph.lineage is not None
    assert {(edge.source_task_key, edge.target_task_key) for edge in result.graph.lineage.data_edges} == {
        ("produce_orders", "consume_orders"),
        ("produce_customers", "consume_customers"),
    }


def test_preserves_gap_source_order_and_task_group_scope(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(dag_id="ordered_gaps", schedule=None) as dag:
    first = BashOperator(task_id="first", bash_command="echo first")
    fanout = [BashOperator(task_id=f"work_{index}", bash_command="echo work") for index in range(3)]
    last = BashOperator(task_id="last", bash_command="echo last")
    with TaskGroup(group_id="nested") as nested:
        inner = BashOperator(task_id="inner", bash_command="echo inner")
        grouped_fanout = [BashOperator(task_id=f"grouped_{index}", bash_command="echo work") for index in range(2)]
""",
    )

    root_names = [node.name for node in result.graph.tasks]
    assert root_names[:3] == ["first", "unclaimed_task_call", "last"]
    nested = next(node for node in result.graph.tasks if isinstance(node, ContainerNode) and node.name == "nested")
    assert [node.name for node in nested.branches["group"]][:2] == ["inner", "unclaimed_task_call"]
    assert any(isinstance(node, GapNode) for node in nested.branches["group"])


def test_persists_complete_callable_dependency_closure(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
import math
from airflow import DAG
from airflow.operators.python import PythonOperator

SCALE = 3

class Multiplier:
    def apply(self, value):
        return value * SCALE

def helper(value):
    return Multiplier().apply(math.floor(value))

def callable_task():
    return helper(2.5)

with DAG(dag_id="callable_closure", schedule=None) as dag:
    run = PythonOperator(task_id="run", python_callable=callable_task)
""",
    )

    callable_definition = result.graph.tasks[0].raw["callable_definition"]
    closure = callable_definition["closure_source"]
    assert "import math" in closure
    assert "SCALE = 3" in closure
    assert "class Multiplier:" in closure
    assert "def helper(value):" in closure
    assert "def callable_task():" in closure


def test_allocates_unique_structural_task_group_keys(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(dag_id="group_key_collision", schedule=None) as dag:
    collision = BashOperator(task_id="processing", bash_command="echo collision")
    with TaskGroup(group_id="processing") as processing:
        inner = BashOperator(task_id="inner", bash_command="echo inner")
""",
    )

    keys = [node.task_key for node in walk_nodes(result.graph.tasks)]
    assert len(keys) == len(set(keys))
    group = next(node for node in result.graph.tasks if isinstance(node, ContainerNode))
    assert group.task_key == "processing__2"


def test_resolves_task_group_definitions_lexically(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow.decorators import dag, task_group

@task_group
def grouped():
    module_marker = "module"

@dag(dag_id="lexical_groups", schedule=None)
def build():
    @task_group
    def grouped():
        nested_marker = "nested"

    selected = grouped()

build()
""",
    )

    selected = next(node for node in result.graph.tasks if node.source_id == "selected")
    definition = selected.raw["callable_definition"]
    assert "nested_marker" in definition["source"]
    assert "module_marker" not in definition["source"]


def test_routes_conditionally_ambiguous_task_group_definition_to_gap(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow.decorators import dag, task_group
from airflow.models import Variable

@task_group
def grouped():
    module_marker = "module"

@dag(dag_id="ambiguous_group", schedule=None)
def build():
    if Variable.get("USE_LOCAL"):
        @task_group
        def grouped():
            conditional_marker = "conditional"

    selected = grouped()

build()
""",
    )

    assert not any(node.source_id == "selected" and node.concept == CONCEPT_GROUP for node in result.graph.tasks)
    assert any(isinstance(node, GapNode) for node in result.graph.tasks)


def test_unclaimed_comprehension_is_an_explicit_gap(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id="gap", schedule=None) as dag:
    head = BashOperator(task_id="head", bash_command="echo head")
    fanout = [BashOperator(task_id=f"work_{index}", bash_command="echo work") for index in range(3)]
""",
    )

    gaps = [node for node in result.graph.tasks if isinstance(node, GapNode)]
    assert gaps
    assert any("not captured" in (gap.reason or "") or "not claimed" in (gap.reason or "") for gap in gaps)
    assert all(gap.properties["strategy"] == "unsupported" for gap in gaps)


def test_maps_explicit_airflow_assets_without_guessing_logical_identity(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksCopyIntoOperator

with DAG(dag_id="assets", schedule=[Dataset("logical.orders")]) as dag:
    produce = BashOperator(
        task_id="produce",
        bash_command="echo ready",
        outlets=[Dataset("logical.orders")],
    )
    copy = DatabricksCopyIntoOperator(
        task_id="copy",
        file_location="s3://landing/orders",
        table_name="main.bronze.orders",
    )
    produce >> copy
""",
    )

    nodes = {node.task_key: node for node in walk_nodes(result.graph.tasks)}
    assert nodes["produce"].data_writes[0].signature == "logical.orders"
    assert nodes["produce"].data_writes[0].identity is None
    assert nodes["copy"].data_reads[0].identity == "s3://landing/orders"
    assert nodes["copy"].data_writes[0].identity == "main.bronze.orders"
    assert result.graph.schedule is not None
    assert result.graph.schedule.kind == "asset"


def test_maps_named_composed_airflow_three_asset_schedule(tmp_path: Path) -> None:
    result = _load(
        tmp_path,
        """
from airflow.sdk import Asset, DAG
from airflow.providers.standard.operators.bash import BashOperator

orders = Asset("x-databricks-table://main.raw.orders")
customers = Asset("x-databricks-table://main.raw.customers")

with DAG(dag_id="asset_expression", schedule=orders & customers) as dag:
    run = BashOperator(task_id="run", bash_command="echo ready")
""",
    )

    assert result.graph.schedule is not None
    assert result.graph.schedule.kind == "asset"
    assert result.pipeline.schedule == {
        "kind": "table_update",
        "table_names": ["main.raw.orders", "main.raw.customers"],
        "condition": "ALL_UPDATED",
        "pause_status": "UNPAUSED",
    }


def test_maps_multiple_dag_declarations_independently(tmp_path: Path) -> None:
    dag_path = tmp_path / "multiple.py"
    dag_path.write_text(
        """
from airflow import DAG
from airflow.operators.bash import BashOperator as ShellTask

first_dag = DAG(dag_id="first", schedule="@daily")
first_task = ShellTask(task_id="run", bash_command="echo first", dag=first_dag)

second_dag = DAG(dag_id="second", schedule="@hourly")
second_task = ShellTask(task_id="run", bash_command="echo second", dag=second_dag)
""",
        encoding="utf-8",
    )

    results = load_airflow_dag_results(dag_path, source_file="dags/multiple.py")

    assert [result.graph.name for result in results] == ["first", "second"]
    assert [result.graph.schedule.expression for result in results if result.graph.schedule is not None] == [
        "@daily",
        "@hourly",
    ]
    for result in results:
        task = next(node for node in walk_nodes(result.graph.tasks) if not node.properties.get("structural_only"))
        assert task.raw is not None
        assert task.raw["operator_fqn"] == "airflow.operators.bash.BashOperator"


def test_exclusion_metadata_is_reflected_in_persisted_graphs(tmp_path: Path) -> None:
    dag_path = tmp_path / "cross_dag.py"
    dag_path.write_text(
        """
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

parent = DAG(dag_id="parent", schedule=None)
trigger = TriggerDagRunOperator(task_id="trigger", trigger_dag_id="child", dag=parent)

child = DAG(dag_id="child", schedule=None)
work = BashOperator(task_id="work", bash_command="echo child", dag=child)
""",
        encoding="utf-8",
    )

    results = load_discovery_results(dag_path, exclude_dags={"child"})
    by_name = {result.graph.name: result for result in results}

    assert by_name["child"].graph.properties["migration_status"] == "excluded"
    assert by_name["parent"].graph.properties["reconciliation_status"] == "verified_with_gaps"
    parent_task = next(iter(by_name["parent"].graph.tasks))
    assert parent_task.properties["strategy"] == "agentic"
