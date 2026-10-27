"""An Oozie export goes through discover, convert, and package via the adapter and yields valid bundles."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from flowx.adapter.__main__ import main as adapter_main
from flowx.validate.bundle_invariants import check_bundle_dir, format_result

_APPS = Path(__file__).resolve().parents[1] / "resources" / "oozie" / "apps"


def test_oozie_export_packages_into_valid_bundles(tmp_path: Path):
    output = tmp_path / "flowx_output"
    common = ["--source", "oozie", "--source-path", str(_APPS), "--output-dir", str(output)]
    assert adapter_main(["discover", *common]) == 0
    assert adapter_main(["convert", *common]) == 0
    assert adapter_main(["package", "--output-dir", str(output)]) == 0

    inventory = json.loads((output / "metadata" / "inventory.json").read_text())
    assert inventory["summary"]["pipeline_count"] == 6
    assert inventory["summary"]["deterministic_count"] == 18
    assert inventory["summary"]["agentic_count"] == 4

    bundle_roots = sorted({path.parent for path in output.rglob("databricks.yml")})
    assert bundle_roots, "package should write at least one bundle"
    for bundle_root in bundle_roots:
        result = check_bundle_dir(bundle_root)
        assert result.ok, f"{bundle_root}\n{format_result(result)}"

    jobs = {}
    for resource in output.rglob("resources/*.yml"):
        for key, job in ((yaml.safe_load(resource.read_text()) or {}).get("resources", {}).get("jobs", {})).items():
            jobs[key] = job
            _check_job(job, resource.parent)
    daily = next(job for key, job in jobs.items() if "spark_jar_daily" in key)
    spark_tasks = [task for task in daily["tasks"] if "job_cluster_key" in task]
    assert len(spark_tasks) == 2
    spark_cluster_keys = {task["job_cluster_key"] for task in spark_tasks}
    assert len(spark_cluster_keys) == 1 and next(iter(spark_cluster_keys)).startswith("spec_")
    declared_cluster_keys = {cluster["job_cluster_key"] for cluster in daily["job_clusters"]}
    assert spark_cluster_keys <= declared_cluster_keys
    assert {parameter["name"] for parameter in daily["parameters"]} >= {"runDate"}
    assert {parameter["name"]: parameter["default"] for parameter in daily["parameters"]}["runDate"] == "2024-01-01"
    assert "shufflePartitions" not in {parameter["name"] for parameter in daily["parameters"]}
    for task in spark_tasks:
        assert [library["jar"] for library in task["libraries"]] == ["../src/lib/etl.jar", "../src/lib/dep.jar"]
    (daily_cluster,) = daily["job_clusters"]
    assert daily_cluster["new_cluster"]["spark_conf"] == {"spark.sql.shuffle.partitions": "200"}
    assert list(output.rglob("lib/etl.jar")), "the fixture JAR should be copied into the bundle"
    assert list(output.rglob("lib/dep.jar")), "the other lib/ JAR should be copied into the bundle too"
    (daily_entry,) = [entry for entry in inventory["pipelines"] if entry["name"] == "spark_jar_daily"]
    daily_paths = daily_entry["oozie"]["hdfs_paths"]
    assert {"action": "load_bookings", "field": "arg", "path": "${nameNode}/data/raw/bookings"} in daily_paths


def _check_job(job: dict, resource_directory: Path) -> None:
    tasks = {task["task_key"]: task for task in job["tasks"]}
    for task in tasks.values():
        for library in task.get("libraries", []):
            if "jar" in library and "://" not in library["jar"] and not library["jar"].startswith("dbfs:"):
                assert (resource_directory / library["jar"]).is_file(), f"dangling jar {library['jar']}"
        for dependency in task.get("depends_on", []):
            if "condition_task" in tasks.get(dependency["task_key"], {}):
                assert "outcome" in dependency, f"{task['task_key']} depends on a condition without an outcome"
        if "result_state" in task.get("condition_task", {}).get("left", ""):
            assert task.get("run_if") == "ALL_DONE", task["task_key"]
    for cluster in job.get("job_clusters", []):
        for value in cluster["new_cluster"].get("spark_conf", {}).values():
            assert "{{" not in str(value)
