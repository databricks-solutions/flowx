"""Tests for Dataproc and Managed Spark translation on the Airflow path."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest
import yaml

from flowx.bundler.dab_writer import main as package_main
from flowx.models.ir import Pipeline, PlaceholderActivity, SparkJarActivity, SparkPythonActivity, SqlActivity
from flowx.sources.airflow.loader import load_airflow_dag

FIXTURE = Path(__file__).parents[1] / "resources" / "airflow" / "dataproc_events_dag.py"

_HEADER = """\
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.managed_spark import (
    ManagedSparkCreateClusterOperator,
    ManagedSparkDeleteClusterOperator,
    ManagedSparkSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocBatchSensor, DataprocJobSensor
from airflow.operators.python import PythonOperator

with DAG(dag_id="dataproc_case", schedule_interval="0 2 * * *") as dag:
"""


def _load(tmp_path: Path, body: str, *, functions: str = "") -> Pipeline:
    source = functions + _HEADER + textwrap.indent(textwrap.dedent(body), "    ")
    dag_path = tmp_path / "dataproc_case.py"
    dag_path.write_text(source, encoding="utf-8")
    return load_airflow_dag(dag_path)


def _submit(job: str, *, task_id: str = "submit", operator: str = "DataprocSubmitJobOperator", extra: str = "") -> str:
    return f'{task_id} = {operator}(task_id="{task_id}", project_id="p", region="r", job={job}{extra})\n'


def _task(pipeline: Pipeline, task_key: str):
    return next(task for task in pipeline.tasks if task.task_key == task_key)


def _codes(pipeline: Pipeline) -> set[str]:
    return {finding["code"] for finding in pipeline.not_translatable}


def test_example_dag_lowers_to_one_native_spark_python_task() -> None:
    pipeline = load_airflow_dag(FIXTURE)

    assert [type(task) for task in pipeline.tasks] == [SparkPythonActivity]
    task = pipeline.tasks[0]
    assert task.task_key == "submit_events"
    assert task.python_file == "gs://customer-dataproc-artifacts/jobs/transform_events.py"
    assert task.parameters == [
        "--input",
        "gs://customer-events/raw/",
        "--output-table",
        "analytics.events_daily",
        "--run-date",
        "{{job.parameters.__flowx_airflow_run_date}}",
    ]
    assert task.depends_on is None
    assert task.cluster == {
        "num_workers": 4,
        "spark_conf": {"spark.sql.adaptive.enabled": "true", "spark.sql.shuffle.partitions": "200"},
        "_bind_default_cluster": True,
    }
    assert "n2-standard-8" not in json.dumps(task.cluster)
    assert (task.max_retries, task.min_retry_interval_millis, task.timeout_seconds) == (2, 300000, 7200)


def test_example_dag_reconciles_with_disclosed_behavior_changes() -> None:
    pipeline = load_airflow_dag(FIXTURE)

    assert pipeline.reconciliation_status == "verified_with_gaps"
    assert _codes(pipeline) == {
        "dataproc_cluster_settings_not_mapped",
        "dataproc_artifacts_require_access",
        "dataproc_execution_envelope_changed",
    }
    messages = {finding["code"]: finding["message"] for finding in pipeline.not_translatable}
    assert "yarn:yarn.nodemanager.resource.memory-mb removed" in messages["dataproc_cluster_settings_not_mapped"]
    assert "worker_config.machine_type_uri not mapped" in messages["dataproc_cluster_settings_not_mapped"]
    assert "image_version" in messages["dataproc_cluster_settings_not_mapped"]
    assert "'all_done'" in messages["dataproc_execution_envelope_changed"]
    proofs = {
        proof["code"]: proof for proof in pipeline.audit["transformations"] if proof["code"].startswith("dataproc")
    }
    assert proofs["dataproc_cluster_absorbed"]["capture_ids"] == ["create_cluster", "delete_cluster"]
    assert proofs["dataproc_sensor_collapsed"]["paired_capture_id"] == "submit_events"
    rewired = {
        proof["capture_id"] for proof in pipeline.audit["transformations"] if proof["code"] == "structural_task_rewired"
    }
    assert rewired == {"create_cluster", "delete_cluster", "wait_for_events"}


def test_managed_spark_aliases_produce_the_same_tasks(tmp_path: Path) -> None:
    job = (
        '{"placement": {"cluster_name": "c"}, "pyspark_job": {"main_python_file_uri": "gs://b/main.py", '
        '"args": ["{{ ds }}"], "properties": {"spark.x": "1"}}}'
    )

    def lowered(prefix: str) -> list[tuple[object, ...]]:
        pipeline = _load(
            tmp_path,
            f'create = {prefix}CreateClusterOperator(task_id="create", project_id="p", region="r", cluster_name="c", '
            'cluster_config={"worker_config": {"num_instances": 3}})\n'
            + _submit(job, operator=f"{prefix}SubmitJobOperator")
            + f'delete = {prefix}DeleteClusterOperator(task_id="delete", project_id="p", region="r", '
            'cluster_name="c")\n'
            "create >> submit >> delete\n",
        )
        return [
            (type(task).__name__, task.task_key, task.python_file, task.parameters, task.cluster)
            for task in pipeline.tasks
        ]

    assert lowered("Dataproc") == lowered("ManagedSpark")
    assert lowered("Dataproc")[0][4] == {
        "num_workers": 3,
        "spark_conf": {"spark.x": "1"},
        "_bind_default_cluster": True,
    }


def test_spark_job_becomes_jar_task_with_libraries(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        _submit(
            '{"spark_job": {"main_class": "com.example.Aggregate", "jar_file_uris": ["gs://b/app.jar"], '
            '"args": ["--x"]}}'
        ),
    )

    task = _task(pipeline, "submit")
    assert isinstance(task, SparkJarActivity)
    assert task.main_class_name == "com.example.Aggregate"
    assert task.libraries == [{"jar": "gs://b/app.jar"}]
    assert task.parameters == ["--x"]


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        ('{"spark_job": {"main_jar_file_uri": "gs://b/app.jar"}}', "names no main_class"),
        ('{"spark_job": {"main_class": "org.apache.spark.examples.SparkPi"}}', "Dataproc classpath"),
        ('{"spark_job": {"main_class": "C", "jar_file_uris": ["file:///usr/lib/spark/x.jar"]}}', "Dataproc node path"),
        (
            '{"pyspark_job": {"main_python_file_uri": "gs://b/m.py", "python_file_uris": ["gs://b/lib.zip"]}}',
            "python_file_uris",
        ),
        ('{"pyspark_job": {"main_python_file_uri": "gs://{{ var.value.bucket }}/m.py"}}', "templated"),
        (
            '{"pyspark_job": {"main_python_file_uri": "gs://b/m.py", "properties": {"yarn.x": "1"}}}',
            "not a Spark property",
        ),
    ],
)
def test_untranslatable_payload_details_fail_closed(tmp_path: Path, body: str, reason: str) -> None:
    task = _task(_load(tmp_path, _submit(body)), "submit")

    assert isinstance(task, PlaceholderActivity)
    assert reason in task.comment


def test_inline_spark_sql_becomes_sql_task(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        _submit('{"spark_sql_job": {"query_list": {"queries": ["INSERT INTO t SELECT 1", "SELECT \'{{ ds }}\'"]}}}'),
    )

    task = _task(pipeline, "submit")
    assert isinstance(task, SqlActivity)
    assert task.sql.startswith("INSERT INTO t SELECT 1;\n")
    assert "{{" not in task.sql


def test_spark_sql_from_query_file_fails_closed(tmp_path: Path) -> None:
    task = _task(_load(tmp_path, _submit('{"spark_sql_job": {"query_file_uri": "gs://b/q.sql"}}')), "submit")

    assert isinstance(task, PlaceholderActivity)
    assert "query file" in task.comment


@pytest.mark.parametrize(
    ("discriminator", "guidance"),
    [
        ("spark_r_job", "classic Jobs compute"),
        ("hive_job", "HiveQL"),
        ("hadoop_job", "MapReduce"),
        ("pig_job", "Pig"),
        ("flink_job", "Structured Streaming"),
        ("presto_job", "Presto"),
        ("trino_job", "Trino"),
    ],
)
def test_non_spark_engines_route_to_placeholders(tmp_path: Path, discriminator: str, guidance: str) -> None:
    pipeline = _load(tmp_path, _submit(f'{{"{discriminator}": {{}}}}'))

    task = _task(pipeline, "submit")
    assert isinstance(task, PlaceholderActivity)
    assert guidance in task.comment
    assert pipeline.reconciliation_status == "verified_with_gaps"


@pytest.mark.parametrize(
    ("job", "found"),
    [
        ('{"placement": {"cluster_name": "c"}}', "found none"),
        (
            '{"pyspark_job": {"main_python_file_uri": "gs://b/m.py"}, "spark_job": {"main_class": "C"}}',
            "found pyspark_job, spark_job",
        ),
    ],
)
def test_payload_without_exactly_one_engine_fails_reconciliation(tmp_path: Path, job: str, found: str) -> None:
    pipeline = _load(tmp_path, _submit(job))

    assert pipeline.reconciliation_status == "failed"
    failures = [finding for finding in pipeline.not_translatable if finding["severity"] == "failed"]
    assert [finding["code"] for finding in failures] == ["dataproc_payload_engine_invalid"]
    assert found in failures[0]["message"]


def test_payload_without_exactly_one_engine_blocks_packaging(tmp_path: Path) -> None:
    from flowx.ir_serde import pipeline_to_dict

    pipeline = _load(tmp_path, _submit('{"placement": {"cluster_name": "c"}}'))
    output_dir = tmp_path / "bundle"
    work_dir = output_dir / ".work"
    work_dir.mkdir(parents=True)
    (work_dir / "translation_report.json").write_text(json.dumps(pipeline_to_dict(pipeline)), encoding="utf-8")

    assert package_main(["--output-dir", str(output_dir), "--no-download-workspace-files"]) != 0
    assert not (output_dir / "databricks.yml").exists()


def test_unresolved_payload_fails_closed_to_a_placeholder(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, _submit("JOB_FROM_ELSEWHERE"))
    task = _task(pipeline, "submit")

    assert isinstance(task, PlaceholderActivity)
    assert "not statically resolvable" in task.comment
    assert pipeline.reconciliation_status == "verified_with_gaps"


def test_notebook_batch_routes_to_placeholder(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        'batch = DataprocCreateBatchOperator(task_id="batch", project_id="p", region="r", batch_id="b1", '
        'batch={"pyspark_notebook_batch": {"notebook_uri": "gs://b/n.ipynb"}})\n',
    )

    task = _task(pipeline, "batch")
    assert isinstance(task, PlaceholderActivity)
    assert "notebook source" in task.comment


def test_batch_and_matching_batch_sensor_collapse(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        'batch = DataprocCreateBatchOperator(task_id="batch", project_id="p", region="r", batch_id="b1", '
        'batch={"pyspark_batch": {"main_python_file_uri": "gs://b/m.py"}, '
        '"runtime_config": {"version": "2.2", "properties": {"spark.executor.cores": "4"}}})\n'
        'wait = DataprocBatchSensor(task_id="wait", project_id="p", region="r", batch_id="b1", poke_interval=10)\n'
        'after = PythonOperator(task_id="after", python_callable=print)\n'
        "batch >> wait >> after\n",
    )

    assert [task.task_key for task in pipeline.tasks] == ["batch", "after"]
    batch = _task(pipeline, "batch")
    assert isinstance(batch, SparkPythonActivity)
    assert batch.cluster == {"spark_conf": {"spark.executor.cores": "4"}, "_bind_default_cluster": True}
    assert [dependency.task_key for dependency in _task(pipeline, "after").depends_on] == ["batch"]
    assert "dataproc_settings_not_mapped" in _codes(pipeline)


def test_batch_sensor_with_mismatched_batch_id_is_retained(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        'batch = DataprocCreateBatchOperator(task_id="batch", project_id="p", region="r", batch_id="b1", '
        'batch={"pyspark_batch": {"main_python_file_uri": "gs://b/m.py"}})\n'
        'wait = DataprocBatchSensor(task_id="wait", project_id="p", region="r", batch_id="other")\n'
        "batch >> wait\n",
    )

    wait = _task(pipeline, "wait")
    assert isinstance(wait, PlaceholderActivity)
    assert "does not match the batch_id" in wait.comment


def test_cluster_read_by_another_task_is_not_collapsed(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        'create = DataprocCreateClusterOperator(task_id="create", project_id="p", region="r", cluster_name="c")\n'
        + _submit('{"placement": {"cluster_name": "c"}, "pyspark_job": {"main_python_file_uri": "gs://b/m.py"}}')
        + 'report = PythonOperator(task_id="report", python_callable=report_cluster)\n'
        "create >> submit >> report\n",
        functions="def report_cluster(ti):\n    return ti.xcom_pull(task_ids='create')\n\n",
    )

    create = _task(pipeline, "create")
    assert isinstance(create, PlaceholderActivity)
    assert "is read by" in create.comment
    assert isinstance(_task(pipeline, "submit"), SparkPythonActivity)


def test_cluster_whose_job_needs_manual_migration_is_retained(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        'create = DataprocCreateClusterOperator(task_id="create", project_id="p", region="r", cluster_name="c")\n'
        + _submit('{"placement": {"cluster_name": "c"}, "hive_job": {}}')
        + "create >> submit\n",
    )

    create = _task(pipeline, "create")
    assert isinstance(create, PlaceholderActivity)
    assert "needs manual migration" in create.comment


def test_teardown_gating_downstream_tasks_is_retained(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        'create = DataprocCreateClusterOperator(task_id="create", project_id="p", region="r", cluster_name="c")\n'
        + _submit('{"placement": {"cluster_name": "c"}, "pyspark_job": {"main_python_file_uri": "gs://b/m.py"}}')
        + 'delete = DataprocDeleteClusterOperator(task_id="delete", project_id="p", region="r", cluster_name="c", '
        'trigger_rule="all_done")\n'
        'notify = PythonOperator(task_id="notify", python_callable=print)\n'
        "create >> submit >> delete >> notify\n",
    )

    delete = _task(pipeline, "delete")
    assert isinstance(delete, PlaceholderActivity)
    assert "gates downstream tasks" in delete.comment


def test_synchronous_submission_does_not_pair_with_a_job_sensor(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        _submit('{"pyspark_job": {"main_python_file_uri": "gs://b/m.py"}}')
        + 'wait = DataprocJobSensor(task_id="wait", project_id="p", region="r", '
        "dataproc_job_id=\"{{ ti.xcom_pull(task_ids='submit') }}\")\n"
        "submit >> wait\n",
    )

    wait = _task(pipeline, "wait")
    assert isinstance(wait, PlaceholderActivity)
    assert "not submitted asynchronously" in wait.comment


def test_asynchronous_submission_without_a_sensor_discloses_the_new_wait(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        _submit('{"pyspark_job": {"main_python_file_uri": "gs://b/m.py"}}', extra=", asynchronous=True"),
    )

    messages = [finding["message"] for finding in pipeline.not_translatable]
    assert any("downstream tasks now wait" in message for message in messages)


def test_unconsumed_dataproc_argument_fails_closed(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path,
        _submit('{"pyspark_job": {"main_python_file_uri": "gs://b/m.py"}}', extra=", wait_timeout=60"),
    )

    task = _task(pipeline, "submit")
    assert isinstance(task, PlaceholderActivity)
    assert "wait_timeout" in task.comment


def test_arguments_carry_dataproc_rationales() -> None:
    pipeline = load_airflow_dag(FIXTURE)

    classified = next(
        proof
        for proof in pipeline.audit["transformations"]
        if proof["code"] == "operator_arguments_classified" and proof["capture_id"] == "submit_events"
    )
    rationales = {argument["name"]: argument["rationale"] for argument in classified["arguments"]}
    assert rationales["project_id"] == "gcp_placement_superseded_by_jobs_compute"
    assert rationales["asynchronous"] == "scheduler_wait_superseded_by_native_task"
    assert rationales["job"] == "dataproc_payload_translated"


def test_example_packages_onto_the_bound_job_cluster(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from flowx.ir_serde import pipeline_to_dict

    monkeypatch.setattr("flowx.preparer.activity_preparers.spark_python.download_dbfs_file", lambda _path: None)
    output_dir = tmp_path / "bundle"
    work_dir = output_dir / ".work"
    work_dir.mkdir(parents=True)
    report = pipeline_to_dict(load_airflow_dag(FIXTURE))
    (work_dir / "translation_report.json").write_text(json.dumps(report), encoding="utf-8")

    assert package_main(["--output-dir", str(output_dir), "--no-download-workspace-files"]) == 0

    bundle_text = "\n".join(path.read_text(encoding="utf-8") for path in output_dir.rglob("*.yml"))
    assert "spark_submit_task" not in bundle_text
    resource = yaml.safe_load((output_dir / "resources" / "dataproc_events.yml").read_text(encoding="utf-8"))
    job = resource["resources"]["jobs"]["dataproc_events"]
    assert job["tasks"][0]["job_cluster_key"] == "default_cluster"
    cluster = job["job_clusters"][0]["new_cluster"]
    assert cluster["num_workers"] == 4
    assert cluster["spark_conf"] == {"spark.sql.adaptive.enabled": "true", "spark.sql.shuffle.partitions": "200"}
    assert cluster["node_type_id"] == "${var.node_type_id}"
    assert "_bind_default_cluster" not in bundle_text
