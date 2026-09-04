"""Regression coverage for the Airflow source-audit findings."""

from pathlib import Path

import pytest

from flowx.models.ir import ForEachActivity, NotebookActivity, PlaceholderActivity, SparkPythonActivity
from flowx.preparer.workflow_preparer import prepare_workflow
from flowx.sources.airflow.loader import load_airflow_dag, load_airflow_dags

_REPROS = Path(__file__).parents[1] / "resources" / "airflow" / "review_repros"

_REPRO_CORPUS = {
    "a1_assigned_dag.py": [("legacy_etl", "verified", 2)],
    "a2_task_key_collision.py": [("collide", "verified", 3)],
    "a8_classic_mapping.py": [("fan", "verified_with_gaps", 1)],
    "t1_loop.py": [("loop_dag", "verified", 3)],
    "t2_sparksubmit.py": [("ss_dag", "verified", 3)],
    "t3_collide.py": [("collide_dag", "verified", 3)],
    "t4_bashjinja.py": [("jinja_dag", "verified_with_gaps", 1)],
    "t5_alias.py": [("alias_dag", "verified", 2)],
    "t6_chain.py": [("chain_dag", "verified", 4)],
    "t7_subclass.py": [("sub_dag", "verified_with_gaps", 2)],
    "t8_helperfn.py": [("helper_dag", "verified", 2)],
    "t9_triggerrule.py": [("tr_dag", "verified", 4)],
    "t10_loopliteral.py": [("loop2", "verified", 2)],
    "t11_dagvar.py": [("assigned_dag", "verified", 2)],
    "t12_globals.py": [("etl_alpha", "verified", 1), ("etl_beta", "verified", 1)],
    "t13_sqlescape.py": [("sqlesc", "verified_with_gaps", 1)],
    "t14_retries.py": [("ret", "verified", 2)],
    "t15_magic.py": [("magic", "verified", 1)],
    "t16_sensor.py": [("sensor_mid", "verified", 3)],
    "t17_taskflow.py": [("tf", "verified", 3)],
    "t18_xcompush.py": [("deps", "verified", 1)],
    "t19_fncollide.py": [("fnc", "verified", 1)],
    "t20_sqlesc.py": [("sqlq", "verified", 2)],
    "t21_partialexpand.py": [("pe", "verified_with_gaps", 1)],
    "t22_expandbash.py": [("eb", "verified_with_gaps", 2)],
    "t23_tr2.py": [("tr2", "verified_with_gaps", 5)],
    "t24_sensorscope.py": [("ss2", "verified", 3)],
    "t25_tr3.py": [("tr3", "verified_with_gaps", 4)],
    "t26_loopedge.py": [("le", "verified", 3)],
    "t27_ss.py": [("ss3", "verified_with_gaps", 1)],
    "t28_nodash.py": [("nd", "verified_with_gaps", 1)],
    "t29_dagsem.py": [("dsem", "verified_with_gaps", 2)],
    "t30_dagvar2.py": [("legacy_etl", "verified", 2)],
    "t31_inject.py": [("inj", "verified", 1)],
    "t32_multiassigned.py": [("team_a_etl", "verified", 2), ("team_b_etl", "verified", 2)],
}


def _dependencies(pipeline) -> dict[str, list[str]]:
    return {
        task.task_key: sorted(dependency.task_key for dependency in (task.depends_on or [])) for task in pipeline.tasks
    }


@pytest.mark.parametrize(("fixture_name", "expected"), sorted(_REPRO_CORPUS.items()))
def test_promoted_review_repro_corpus_is_exercised(fixture_name: str, expected: list[tuple[str, str, int]]) -> None:
    pipelines = load_airflow_dags(_REPROS / fixture_name)

    assert [(pipeline.name, pipeline.reconciliation_status, len(pipeline.tasks)) for pipeline in pipelines] == expected


def test_assigned_dag_preserves_configuration_and_tasks() -> None:
    pipeline = load_airflow_dag(_REPROS / "a1_assigned_dag.py")

    assert pipeline.name == "legacy_etl"
    assert pipeline.schedule == {
        "kind": "schedule",
        "quartz_cron_expression": "0 0 3 ? * *",
        "timezone_id": "UTC",
        "pause_status": "UNPAUSED",
    }
    assert pipeline.tags["airflow_catchup"] == "true"
    assert {task.task_key for task in pipeline.tasks} == {"extract", "load"}
    assert _dependencies(pipeline)["load"] == ["extract"]
    assert next(task for task in pipeline.tasks if task.task_key == "extract").max_retries == 5


def test_task_key_collisions_allocate_distinct_keys_without_losing_edges() -> None:
    pipeline = load_airflow_dag(_REPROS / "a2_task_key_collision.py")

    assert [task.task_key for task in pipeline.tasks] == ["load_data", "load_data__2", "final"]
    assert _dependencies(pipeline)["final"] == ["load_data", "load_data__2"]
    edge_proofs = [item for item in pipeline.audit["transformations"] if item["code"] == "edge_captured"]
    assert {(item["upstream_capture_id"], item["downstream_capture_id"]) for item in edge_proofs} == {
        ("x", "z"),
        ("y", "z"),
    }


def test_bounded_loops_preserve_generated_tasks_and_edges() -> None:
    pipeline = load_airflow_dag(_REPROS / "t1_loop.py")

    assert [task.task_key for task in pipeline.tasks] == ["load_us", "load_eu", "load_apac"]
    assert _dependencies(pipeline) == {
        "load_us": [],
        "load_eu": ["load_us"],
        "load_apac": ["load_eu"],
    }


def test_aliases_chain_cross_downstream_and_single_return_factories_are_captured() -> None:
    alias_pipeline = load_airflow_dag(_REPROS / "t5_alias.py")
    chain_pipeline = load_airflow_dag(_REPROS / "t6_chain.py")
    helper_pipeline = load_airflow_dag(_REPROS / "t8_helperfn.py")

    assert {task.task_key for task in alias_pipeline.tasks} == {"aliased", "py"}
    assert _dependencies(chain_pipeline) == {
        "a": [],
        "b": ["a"],
        "c": ["a", "b"],
        "d": ["a", "b"],
    }
    assert [task.task_key for task in helper_pipeline.tasks] == ["first", "second"]
    assert _dependencies(helper_pipeline)["second"] == ["first"]
    helper_proofs = [
        item for item in helper_pipeline.audit["transformations"] if item["code"] == "helper_factory_expanded"
    ]
    assert [item["helper"] for item in helper_proofs] == ["make", "make"]


def test_module_callable_wins_over_unrelated_nested_definitions() -> None:
    pipeline = load_airflow_dag(_REPROS / "t19_fncollide.py")
    task = pipeline.tasks[0]

    assert isinstance(task, NotebookActivity)
    assert "CORRECT_BODY" in (task.generated_source or "")
    assert "WRONG_BODY" not in (task.generated_source or "")


def test_classic_callable_uses_nearest_lexical_definition(tmp_path: Path) -> None:
    dag = tmp_path / "lexical.py"
    dag.write_text(
        "from airflow.decorators import dag\n"
        "from airflow.operators.python import PythonOperator\n"
        "def process():\n"
        "    return 'MODULE_BODY'\n"
        "@dag(dag_id='lexical')\n"
        "def workflow():\n"
        "    def process():\n"
        "        return 'NESTED_BODY'\n"
        "    run = PythonOperator(task_id='run', python_callable=process)\n"
        "workflow()\n",
        encoding="utf-8",
    )

    task = load_airflow_dag(dag).tasks[0]

    assert isinstance(task, NotebookActivity)
    assert "NESTED_BODY" in (task.generated_source or "")
    assert "MODULE_BODY" not in (task.generated_source or "")


def test_classic_callable_respects_same_scope_definition_order(tmp_path: Path) -> None:
    dag = tmp_path / "definition_order.py"
    dag.write_text(
        "from airflow.decorators import dag\n"
        "from airflow.operators.python import PythonOperator\n"
        "@dag(dag_id='definition_order')\n"
        "def workflow():\n"
        "    def process():\n"
        "        return 'FIRST_BODY'\n"
        "    first = PythonOperator(task_id='first', python_callable=process)\n"
        "    def process():\n"
        "        return 'SECOND_BODY'\n"
        "    second = PythonOperator(task_id='second', python_callable=process)\n"
        "workflow()\n",
        encoding="utf-8",
    )

    first, second = load_airflow_dag(dag).tasks

    assert "FIRST_BODY" in (first.generated_source or "")
    assert "SECOND_BODY" not in (first.generated_source or "")
    assert "SECOND_BODY" in (second.generated_source or "")


def test_conditionally_ambiguous_classic_callable_becomes_placeholder(tmp_path: Path) -> None:
    dag = tmp_path / "ambiguous_callable.py"
    dag.write_text(
        "from airflow.decorators import dag\n"
        "from airflow.operators.python import PythonOperator\n"
        "FLAG = object()\n"
        "@dag(dag_id='ambiguous_callable')\n"
        "def workflow():\n"
        "    if FLAG:\n"
        "        def process():\n"
        "            return 'LEFT'\n"
        "    else:\n"
        "        def process():\n"
        "            return 'RIGHT'\n"
        "    run = PythonOperator(task_id='run', python_callable=process)\n"
        "workflow()\n",
        encoding="utf-8",
    )

    pipeline = load_airflow_dag(dag)
    task = next(task for task in pipeline.tasks if task.task_key == "run")

    assert isinstance(task, PlaceholderActivity)
    assert pipeline.reconciliation_status == "verified_with_gaps"


def test_literal_dag_factory_loop_and_multiple_assigned_dags_remain_distinct() -> None:
    generated = load_airflow_dags(_REPROS / "t12_globals.py")
    assigned = load_airflow_dags(_REPROS / "t32_multiassigned.py")

    assert [pipeline.name for pipeline in generated] == ["etl_alpha", "etl_beta"]
    assert [pipeline.name for pipeline in assigned] == ["team_a_etl", "team_b_etl"]
    assert all([task.task_key for task in pipeline.tasks] == ["extract", "load"] for pipeline in assigned)


def test_spark_submit_requires_a_single_invocation_and_known_option_arities() -> None:
    pipeline = load_airflow_dag(_REPROS / "t2_sparksubmit.py")
    first, second, third = pipeline.tasks

    assert isinstance(first, SparkPythonActivity)
    assert first.python_file == "/jobs/etl.py"
    assert first.parameters == ["--date", "2024-01-01"]
    assert isinstance(second, NotebookActivity)
    assert "cd /opt/app && spark-submit" in (second.generated_source or "")
    assert isinstance(third, NotebookActivity)
    assert "spark-submit /jobs/x.py && aws" in (third.generated_source or "")


def test_unknown_spark_submit_option_falls_back_to_bash(tmp_path: Path) -> None:
    dag = tmp_path / "spark.py"
    dag.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='spark') as dag:\n"
        "    run = BashOperator(task_id='run', bash_command='spark-submit --future-option value app.py')\n",
        encoding="utf-8",
    )

    task = load_airflow_dag(dag).tasks[0]

    assert isinstance(task, NotebookActivity)
    assert "--future-option value app.py" in (task.generated_source or "")


def test_spark_submit_option_cannot_consume_another_option_as_its_value(tmp_path: Path) -> None:
    dag = tmp_path / "spark_option_value.py"
    dag.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='spark_option_value') as dag:\n"
        "    run = BashOperator(\n"
        "        task_id='run',\n"
        "        bash_command='spark-submit --conf --driver-memory=4g app.py',\n"
        "    )\n",
        encoding="utf-8",
    )

    task = load_airflow_dag(dag).tasks[0]

    assert isinstance(task, NotebookActivity)
    assert "spark-submit --conf --driver-memory=4g app.py" in (task.generated_source or "")


def test_unresolved_jinja_in_generated_source_becomes_placeholder() -> None:
    pipeline = load_airflow_dag(_REPROS / "t4_bashjinja.py")

    assert isinstance(pipeline.tasks[0], PlaceholderActivity)
    assert pipeline.reconciliation_status == "verified_with_gaps"
    assert any(finding["code"] == "unresolved_airflow_template" for finding in pipeline.not_translatable)


@pytest.mark.parametrize("rule", ["none_failed_min_one_success", "none_failed_or_skipped"])
def test_unsupported_and_approximate_trigger_rules_are_explicit(tmp_path: Path, rule: str) -> None:
    unsupported = load_airflow_dag(_REPROS / "t23_tr2.py")
    assert all(isinstance(unsupported.tasks[index], PlaceholderActivity) for index in (1, 2, 3))

    dag = tmp_path / "approximate.py"
    dag.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='rules') as dag:\n"
        "    up = BashOperator(task_id='up', bash_command='echo up')\n"
        "    down = BashOperator(task_id='down', bash_command='echo down', "
        f"trigger_rule={rule!r})\n"
        "    up >> down\n",
        encoding="utf-8",
    )
    approximate = load_airflow_dag(dag)

    assert approximate.tasks[1].depends_on[0].outcome == "NONE_FAILED"
    finding = next(item for item in approximate.not_translatable if item["code"] == "approximated_trigger_rule")
    assert "every upstream task was skipped or excluded" in finding["message"]


def test_sensor_lift_requires_full_non_sensor_reachability(tmp_path: Path) -> None:
    guarded = load_airflow_dag(_REPROS / "t24_sensorscope.py")
    assert guarded.schedule is None
    assert {task.task_key for task in guarded.tasks} == {"wait", "gated", "independent"}

    dag = tmp_path / "dominating_sensor.py"
    dag.write_text(
        "from airflow import DAG\n"
        "from airflow.sensors.filesystem import FileSensor\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='dominating', schedule=None) as dag:\n"
        "    wait = FileSensor(task_id='wait', filepath='/mnt/input')\n"
        "    work = BashOperator(task_id='work', bash_command='echo work')\n"
        "    wait >> work\n",
        encoding="utf-8",
    )
    dominating = load_airflow_dag(dag)

    assert (dominating.schedule or {})["kind"] == "file_arrival"
    proof = next(item for item in dominating.audit["transformations"] if item["code"].startswith("sensor_lift"))
    assert proof["covered_capture_ids"] == ["work"]


def test_classic_mapping_with_unbound_args_links_a_failing_placeholder() -> None:
    pipeline = load_airflow_dag(_REPROS / "a8_classic_mapping.py")
    outer = pipeline.tasks[0]

    assert isinstance(outer, ForEachActivity)
    assert isinstance(outer.inner_activities[0], PlaceholderActivity)
    assert any(finding["code"] == "classic_mapping_arguments_unbound" for finding in pipeline.not_translatable)

    prepared = prepare_workflow(pipeline)
    inner_task = prepared.tasks[0]["for_each_task"]["task"]
    notebook_path = inner_task["notebook_task"]["notebook_path"]
    notebook = next(item for item in prepared.notebooks if notebook_path.endswith(item.relative_path))
    assert "raise NotImplementedError" in notebook.content


def test_shell_notebook_directives_remain_inert() -> None:
    magic = load_airflow_dag(_REPROS / "t15_magic.py").tasks[0]
    boundary = load_airflow_dag(_REPROS / "t31_inject.py").tasks[0]

    assert isinstance(magic, NotebookActivity)
    assert isinstance(boundary, NotebookActivity)
    magic_source = magic.generated_source or ""
    boundary_source = boundary.generated_source or ""
    assert magic_source.count("# MAGIC %sh") == 1
    assert "# MAGIC ## MAGIC %sql" in magic_source
    assert boundary_source.count("# MAGIC %sh") == 1
    assert "# MAGIC ## COMMAND ----------" in boundary_source
    assert "# MAGIC echo one" in boundary_source
    assert "# MAGIC echo two" in boundary_source


def test_unconsumed_operator_arguments_become_placeholder() -> None:
    pipeline = load_airflow_dag(_REPROS / "t29_dagsem.py")
    task = next(task for task in pipeline.tasks if task.task_key == "a")

    assert isinstance(task, PlaceholderActivity)
    finding = next(item for item in pipeline.not_translatable if item["code"] == "unconsumed_operator_arguments")
    assert finding["details"]["arguments"] == ["pool", "priority_weight", "queue"]
    proof = next(item for item in pipeline.audit["transformations"] if item["code"] == "operator_arguments_classified")
    classified = {item["name"]: item for item in proof["arguments"]}
    assert classified["task_id"]["rationale"] == "capture_identity"
    assert classified["bash_command"]["rationale"] == "operator_adapter"
    assert classified["pool"]["status"] == "unconsumed"


def test_unlowerable_retry_policy_becomes_placeholder(tmp_path: Path) -> None:
    source = tmp_path / "dynamic_retry.py"
    source.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='dynamic_retry') as dag:\n"
        "    BashOperator(task_id='work', bash_command='echo hi', retries=get_retries())\n",
        encoding="utf-8",
    )

    pipeline = load_airflow_dag(source)

    assert isinstance(pipeline.tasks[0], PlaceholderActivity)
    finding = next(item for item in pipeline.not_translatable if item["code"] == "unrepresented_task_policy")
    assert finding["details"]["arguments"] == ["retries"]


def test_dynamic_trigger_rule_becomes_placeholder(tmp_path: Path) -> None:
    source = tmp_path / "dynamic_trigger.py"
    source.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='dynamic_trigger') as dag:\n"
        "    BashOperator(task_id='work', bash_command='echo hi', trigger_rule=get_rule())\n",
        encoding="utf-8",
    )

    pipeline = load_airflow_dag(source)

    assert isinstance(pipeline.tasks[0], PlaceholderActivity)
    finding = next(item for item in pipeline.not_translatable if item["code"] == "unsupported_trigger_rule")
    assert finding["details"]["task_key"] == "work"
