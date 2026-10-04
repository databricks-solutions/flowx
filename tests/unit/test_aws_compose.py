"""Wiring a Step Functions job that starts a Glue workflow into one bundle.

Covers the ``combine`` adapter subcommand plus the end-to-end path: convert the
Step Functions source and the Glue source separately, combine the two reports,
package the merged report, and confirm the Step Functions ``run_job_task``
resolves to the Glue-converted job's bundle resource.
"""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from flowx.adapter.__main__ import main
from flowx.ir_serde import pipeline_to_dict
from flowx.sources.glue.loader import load_pipelines as load_glue
from flowx.sources.stepfunctions.loader import load_pipelines as load_stepfunctions

_RESOURCES = Path(__file__).parent.parent / "resources"
_SF_FIXTURE = _RESOURCES / "stepfunctions" / "orders_orchestrator.json"
_GLUE_FIXTURE = _RESOURCES / "glue" / "sales_workflow.json"


def _write_report(path: Path, fixture: Path, loader) -> None:
    """Translates *fixture* with *loader* and writes its single-pipeline report to *path*."""
    pipeline = loader(fixture)[0]
    path.write_text(json.dumps(pipeline_to_dict(pipeline)), encoding="utf-8")


def test_combine_merges_reports_into_one_payload(tmp_path: Path) -> None:
    """combine flattens two single-pipeline reports into one {"pipelines": [...]}."""
    sf_report = tmp_path / "sf.json"
    glue_report = tmp_path / "glue.json"
    _write_report(sf_report, _SF_FIXTURE, load_stepfunctions)
    _write_report(glue_report, _GLUE_FIXTURE, load_glue)

    merged = tmp_path / "merged.json"
    code = main(["combine", "--report", str(sf_report), "--report", str(glue_report), "--out", str(merged)])
    assert code == 0

    payload = json.loads(merged.read_text(encoding="utf-8"))
    assert {pipeline["name"] for pipeline in payload["pipelines"]} == {"orders_orchestrator", "sales_workflow"}


def test_combine_rejects_duplicate_job_keys(tmp_path: Path) -> None:
    """Two pipelines that normalise to the same resource key are a clean error, not a silent clobber."""
    report = tmp_path / "glue.json"
    _write_report(report, _GLUE_FIXTURE, load_glue)
    merged = tmp_path / "merged.json"
    code = main(["combine", "--report", str(report), "--report", str(report), "--out", str(merged)])
    assert code == 2
    assert not merged.exists()


def test_stepfunctions_run_job_resolves_to_glue_job(tmp_path: Path) -> None:
    """After combine + package, the SF run_job_task points at the Glue job's bundle resource."""
    sf_report = tmp_path / "sf.json"
    glue_report = tmp_path / "glue.json"
    _write_report(sf_report, _SF_FIXTURE, load_stepfunctions)
    _write_report(glue_report, _GLUE_FIXTURE, load_glue)

    merged = tmp_path / "merged.json"
    assert main(["combine", "--report", str(sf_report), "--report", str(glue_report), "--out", str(merged)]) == 0

    out_dir = tmp_path / "bundle"
    code = main(
        [
            "package",
            "--report",
            str(merged),
            "--output-dir",
            str(out_dir),
            "--catalog",
            "main",
            "--schema",
            "default",
            "--no-download-workspace-files",
            "--single-bundle",
        ]
    )
    assert code == 0

    sf_job = yaml.safe_load((out_dir / "resources" / "orders_orchestrator.yml").read_text(encoding="utf-8"))
    tasks = sf_job["resources"]["jobs"]["orders_orchestrator"]["tasks"]
    run_sales = next(task for task in tasks if task["task_key"] == "runsalesworkflow")
    assert run_sales["run_job_task"]["job_id"] == "${resources.jobs.sales_workflow.id}"

    # Both jobs live in one bundle (no per-pipeline subdirectory), so the reference resolves.
    assert (out_dir / "resources" / "sales_workflow.yml").exists()
    assert not (out_dir / "orders_orchestrator").exists()
