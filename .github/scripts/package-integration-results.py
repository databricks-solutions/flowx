"""Check every ADF case's packaged bundle against the bundle invariants and summarize the inventory."""

from __future__ import annotations

import json
import os
from pathlib import Path

import yaml

from flowx.validate.bundle_invariants import check_bundle_dir, format_result

CASES_ROOT = Path("tests/integration/azure/adf/cases")


def main() -> int:
    """Map each case pipeline to its packaged bundle, check it, and append the summary for GitHub."""
    root = Path(os.environ["FLOWX_OUTPUT"])
    bundle_dirs = sorted(path.parent for path in root.glob("*/databricks.yml"))
    if (root / "databricks.yml").exists():
        bundle_dirs.append(root)
    assert bundle_dirs, "package produced no bundles"
    bundle_by_pipeline = {}
    for bundle_dir in bundle_dirs:
        for resource_file in bundle_dir.glob("resources/*.yml"):
            resources = yaml.safe_load(resource_file.read_text()) or {}
            for job in ((resources.get("resources") or {}).get("jobs") or {}).values():
                bundle_by_pipeline[job["name"]] = bundle_dir
    for case_file in sorted(CASES_ROOT.glob("*/case.json")):
        case = json.loads(case_file.read_text())
        for pipeline in case["pipelines"]:
            bundle_dir = bundle_by_pipeline[pipeline]
            result = check_bundle_dir(bundle_dir)
            assert result.ok, f"{case_file.parent.name}: {format_result(result)}"
    summary = json.loads((root / "metadata/inventory.json").read_text())["summary"]
    github_summary = os.getenv("GITHUB_STEP_SUMMARY")
    if github_summary:
        with open(github_summary, "a", encoding="utf-8") as handle:
            handle.write(
                f"- ADF cases: {summary['pipeline_count']} pipelines, {summary['activity_count']} activities.\n"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
