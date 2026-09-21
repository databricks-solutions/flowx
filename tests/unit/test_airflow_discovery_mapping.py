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
