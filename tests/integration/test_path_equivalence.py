"""Guard #1: the in-process bundle path and the report round-trip (CLI) path
must produce identical bundles; and every generated bundle must satisfy the
structural invariants (guard #2)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from flowx.bundler.dab_writer import _pipeline_dict_to_workflow, write_bundle
from flowx.parser.adf_loader import load_adf_definitions
from flowx.preparer.workflow_preparer import prepare_workflow
from flowx.translator.engine import _pipeline_to_dict, translate_pipeline
from flowx.validate.bundle_invariants import check_bundle_dir, format_result

FIXTURES_DIR = Path(__file__).parent.parent / "resources" / "json"
_DEFS = load_adf_definitions(FIXTURES_DIR)
_PIPELINE_NAMES = sorted(p.name for p in _DEFS.pipelines)


def _jobs(bundle_dir: Path) -> dict:
    """Merge all `resources.jobs` mappings across the bundle's resource files."""
    jobs: dict = {}
    for path in sorted((bundle_dir / "resources").glob("*.yml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        jobs.update(((doc.get("resources") or {}).get("jobs") or {}))
    return jobs


@pytest.mark.parametrize("name", _PIPELINE_NAMES)
def test_inprocess_and_report_paths_agree(name: str, tmp_path: Path) -> None:
    pipeline = next(p for p in _DEFS.pipelines if p.name == name)
    report = translate_pipeline(pipeline, _DEFS)

    # Serialize the report BEFORE the in-process write (write_bundle mutates the
    # workflow it is given, not the IR, but serialize first to be safe).
    report_dict = _pipeline_to_dict(report.pipeline)

    in_process = tmp_path / "in_process"
    write_bundle(prepare_workflow(report.pipeline), in_process, catalog="c", schema="s")

    report_path = tmp_path / "report_path"
    write_bundle(_pipeline_dict_to_workflow(report_dict), report_path, catalog="c", schema="s")

    assert _jobs(in_process) == _jobs(report_path), (
        f"in-process vs report round-trip bundle diverged for pipeline '{name}'"
    )


@pytest.mark.parametrize("name", _PIPELINE_NAMES)
def test_generated_bundle_satisfies_invariants(name: str, tmp_path: Path) -> None:
    pipeline = next(p for p in _DEFS.pipelines if p.name == name)
    report = translate_pipeline(pipeline, _DEFS)
    out = tmp_path / "bundle"
    write_bundle(_pipeline_dict_to_workflow(_pipeline_to_dict(report.pipeline)), out, catalog="c", schema="s")
    result = check_bundle_dir(out)
    assert result.ok, format_result(result)
