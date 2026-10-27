"""The Oozie discover and convert phase entry points and the files they write."""

from __future__ import annotations

import json
from pathlib import Path

from flowx.sources.oozie import convert, discover

_GOOD = """<workflow-app name="w" xmlns="uri:oozie:workflow:1.0"><start to="load"/>
<action name="load"><spark xmlns="uri:oozie:spark-action:1.0"><class>c.M</class><jar>lib/etl.jar</jar></spark>
<ok to="end"/><error to="fail"/></action>
<action name="side"><pig><script>x</script></pig><ok to="end"/><error to="fail"/></action>
<kill name="fail"><message>x</message></kill><end name="end"/></workflow-app>"""

_WITH_PIG = """<workflow-app name="p" xmlns="uri:oozie:workflow:1.0"><start to="load"/>
<action name="load"><pig><script>x</script></pig><ok to="end"/><error to="fail"/></action>
<kill name="fail"><message>x</message></kill><end name="end"/></workflow-app>"""


def _source(tmp_path: Path) -> Path:
    source = tmp_path / "export"
    for name, workflow in (("good", _GOOD), ("broken", "<workflow-app")):
        (source / name / "lib").mkdir(parents=True)
        (source / name / "lib" / "etl.jar").write_bytes(b"jar")
        (source / name / "workflow.xml").write_text(workflow)
    return source


def test_discover_writes_inventory_profile_and_verbatim_xml(tmp_path: Path):
    output = tmp_path / "out"
    assert discover.main(["--source-dir", str(_source(tmp_path)), "--output-dir", str(output)]) == 0
    inventory = json.loads((output / "metadata" / "inventory.json").read_text())
    assert inventory["source"] == "oozie"
    entries = {entry["name"]: entry for entry in inventory["pipelines"]}
    assert entries["good"]["activities"] == [
        {"name": "load", "task_key": "load", "strategy": "deterministic", "oozie_type": "spark"}
    ]
    assert entries["broken"]["failed_count"] == 1
    assert "hdfs_paths" in entries["good"]["oozie"]
    assert (output / "metadata" / "profile_report.csv").is_file()
    assert (output / "metadata" / "good" / "workflow.xml").read_text() == _GOOD


def test_rerun_with_output_dir_inside_source_does_not_rediscover_copies(tmp_path: Path):
    source = _source(tmp_path)
    output = source / "flowx_output"
    for _ in range(2):
        assert discover.main(["--source-dir", str(source), "--output-dir", str(output)]) == 0
    inventory = json.loads((output / "metadata" / "inventory.json").read_text())
    assert sorted(entry["name"] for entry in inventory["pipelines"]) == ["broken", "good"]


def test_exclude_dag_spelling_excludes_an_app(tmp_path: Path):
    output = tmp_path / "out"
    arguments = ["--source-dir", str(_source(tmp_path)), "--output-dir", str(output), "--exclude-dag", "broken"]
    assert discover.main(arguments) == 0
    inventory = json.loads((output / "metadata" / "inventory.json").read_text())
    assert {entry["name"]: entry["excluded_count"] for entry in inventory["pipelines"]}["broken"] == 1


def test_discover_agentic_items_carry_a_reason(tmp_path: Path):
    source = tmp_path / "export" / "p"
    source.mkdir(parents=True)
    (source / "workflow.xml").write_text(_WITH_PIG)
    output = tmp_path / "out"
    assert discover.main(["--source-dir", str(tmp_path / "export"), "--output-dir", str(output)]) == 0
    inventory = json.loads((output / "metadata" / "inventory.json").read_text())
    items = inventory["pipelines"][0]["activities"]
    agentic = [item for item in items if item["strategy"] == "agentic"]
    assert agentic and all(item["reason"] for item in agentic)


def test_convert_writes_report_without_failed_apps(tmp_path: Path):
    output = tmp_path / "out"
    assert convert.main(["--source-dir", str(_source(tmp_path)), "--output-dir", str(output)]) == 0
    report = json.loads((output / ".work" / "translation_report.json").read_text())
    assert report["name"] == "good" and report["tags"]["source"] == "oozie"
    failed = json.loads((output / ".work" / "failed_apps.json").read_text())
    assert [entry["name"] for entry in failed] == ["broken"]
    assert (output / ".work" / "gaps.json").is_file()


def test_all_failed_returns_one(tmp_path: Path):
    source = tmp_path / "export" / "only"
    source.mkdir(parents=True)
    (source / "workflow.xml").write_text("<workflow-app")
    assert convert.main(["--source-dir", str(tmp_path / "export"), "--output-dir", str(tmp_path / "out")]) == 1
    assert discover.main(["--source-dir", str(tmp_path / "export"), "--output-dir", str(tmp_path / "out2")]) == 1
