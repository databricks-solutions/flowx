"""Shared preparer/bundler behaviour for Oozie Spark tasks: per-spec job clusters and local JARs."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from flowx.bundler.dab_writer import _report_reconciliation_failures, write_bundle
from flowx.ir_serde import pipeline_to_dict
from flowx.models.ir import Dependency, Pipeline, SparkJarActivity
from flowx.preparer.workflow_preparer import prepare_workflow


def _spark(key: str, workers: int, jar: Path, depends_on: list[Dependency] | None = None) -> SparkJarActivity:
    return SparkJarActivity(
        name=key,
        task_key=key,
        main_class_name="com.example.Main",
        libraries=[{"jar": f"file:{jar}"}],
        cluster={"_job_cluster_spec": {"num_workers": workers, "spark_conf": {"spark.sql.shuffle.partitions": "64"}}},
        depends_on=depends_on,
    )


def _job(tmp_path: Path, pipeline: Pipeline) -> dict:
    write_bundle(prepare_workflow(pipeline), tmp_path)
    document = yaml.safe_load(next((tmp_path / "resources").glob("*.yml")).read_text())
    return next(iter(document["resources"]["jobs"].values()))


def test_identical_specs_share_one_job_cluster_and_different_specs_get_their_own(tmp_path: Path):
    jar = tmp_path / "etl.jar"
    jar.write_bytes(b"PK\x03\x04fake")
    pipeline = Pipeline(
        name="spark_daily",
        tags={"source": "oozie"},
        tasks=[
            _spark("a", 4, jar),
            _spark("b", 4, jar, [Dependency(task_key="a")]),
            _spark("c", 8, jar, [Dependency(task_key="b")]),
        ],
    )
    job = _job(tmp_path / "bundle", pipeline)
    keys = {task["task_key"]: task["job_cluster_key"] for task in job["tasks"]}
    assert keys["a"] == keys["b"] != keys["c"]
    clusters = {cluster["job_cluster_key"]: cluster["new_cluster"] for cluster in job["job_clusters"]}
    assert set(clusters) == {keys["a"], keys["c"]}
    assert clusters[keys["c"]]["num_workers"] == 8
    assert clusters[keys["a"]]["spark_conf"] == {"spark.sql.shuffle.partitions": "64"}
    assert clusters[keys["a"]]["node_type_id"] == "${var.node_type_id}"
    assert all("_job_cluster_spec" not in task for task in job["tasks"])


def test_local_jar_is_copied_into_the_bundle(tmp_path: Path):
    jar = tmp_path / "etl.jar"
    jar.write_bytes(b"PK\x03\x04fake")
    job = _job(tmp_path / "bundle", Pipeline(name="spark_one", tags={"source": "oozie"}, tasks=[_spark("a", 2, jar)]))
    copied = list((tmp_path / "bundle").rglob("etl.jar"))
    assert copied and copied[0].read_bytes() == b"PK\x03\x04fake"
    jar_reference = job["tasks"][0]["libraries"][0]["jar"]
    assert (tmp_path / "bundle" / "resources" / jar_reference).resolve() == copied[0].resolve()


def test_package_gate_accepts_oozie_reports(tmp_path: Path):
    pipeline = Pipeline(
        name="spark_one",
        tags={"source": "oozie"},
        reconciliation_status="verified",
        audit={"source_file": "workflow.xml", "audited_activity_count": 0, "transformations": []},
    )
    report = tmp_path / "translation_report.json"
    report.write_text(json.dumps(pipeline_to_dict(pipeline)))
    assert _report_reconciliation_failures(report) == []
