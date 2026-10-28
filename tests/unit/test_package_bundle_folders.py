"""Package records the bundle folders it wrote and moves the ones it no longer writes into the archive.

A report with several pipelines is packaged one bundle folder per pipeline, and a report with one
pipeline as one bundle at the top of the output folder. When a later package writes a different set
(an agentic unit that replaces pipelines under new names, or a convert narrowed to fewer pipelines),
the earlier bundles would otherwise stay beside the new ones, complete and deployable. Package keeps
``metadata/packaged_bundles.json`` and moves any folder on it that it no longer writes into
``metadata/archive/<UTC time>/`` with an ``archive.json``; discover's reset moves them too.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from flowx.bundler.dab_writer import main as package_main
from flowx.ir_serde import pipeline_to_dict
from flowx.models.ir import NotebookActivity, Pipeline
from flowx.route_agentic import REPORT_FILENAME, WORK_DIRNAME
from flowx.sources.adf.loader import clear_stale_outputs

RECORD = Path("metadata") / "packaged_bundles.json"


def _write_report(output_dir: Path, names: list[str]) -> None:
    """Write convert's report for one notebook pipeline per name."""
    pipelines = [
        pipeline_to_dict(
            Pipeline(
                name=name,
                tasks=[NotebookActivity(name="load", task_key="load", notebook_path=f"/Workspace/Shared/{name}")],
                tags={"source": "adf"},
            )
        )
        for name in names
    ]
    work = output_dir / WORK_DIRNAME
    work.mkdir(parents=True, exist_ok=True)
    (work / REPORT_FILENAME).write_text(json.dumps({"pipelines": pipelines}, indent=2), encoding="utf-8")


def _package(output_dir: Path, names: list[str]) -> int:
    _write_report(output_dir, names)
    return package_main(["--output-dir", str(output_dir), "--no-download-workspace-files"])


def _bundles(output_dir: Path) -> list[str]:
    """Every bundle folder in the output folder, outside the archive: "." for one at the top."""
    return sorted(
        path.parent.relative_to(output_dir).as_posix()
        for path in output_dir.rglob("databricks.yml")
        if "archive" not in path.relative_to(output_dir).parts
    )


def _archived(output_dir: Path) -> list[dict[str, Any]]:
    records = sorted((output_dir / "metadata" / "archive").glob("*/archive.json"))
    return [json.loads(record.read_text(encoding="utf-8")) for record in records]


def _record(output_dir: Path) -> Any:
    return json.loads((output_dir / RECORD).read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    ("names", "bundles"), [(["alpha", "beta"], ["alpha", "beta"]), (["alpha"], ["."])], ids=["several", "one"]
)
def test_package_records_the_bundle_folders_it_wrote(tmp_path: Path, names: list[str], bundles: list[str]) -> None:
    assert _package(tmp_path, names) == 0

    assert _record(tmp_path) == {"bundles": bundles}
    assert _bundles(tmp_path) == bundles
    assert _archived(tmp_path) == []


def test_a_bundle_package_no_longer_writes_moves_into_the_archive(tmp_path: Path) -> None:
    """The agentic case: a unit's authored pipeline replaces 'beta' under a new name."""
    assert _package(tmp_path, ["alpha", "beta"]) == 0
    beta_yml = (tmp_path / "beta" / "databricks.yml").read_bytes()

    assert _package(tmp_path, ["alpha", "beta_lfc"]) == 0

    assert _bundles(tmp_path) == ["alpha", "beta_lfc"]
    assert _record(tmp_path) == {"bundles": ["alpha", "beta_lfc"]}
    (record,) = _archived(tmp_path)
    moved = {entry["from"]: entry for entry in record["moved"]}
    assert "beta/databricks.yml" in moved and all(path.startswith("beta/") for path in moved)
    assert (tmp_path / moved["beta/databricks.yml"]["to"]).read_bytes() == beta_yml
    assert record["reason"].startswith("package")


def test_narrowing_to_one_pipeline_moves_every_earlier_bundle_folder(tmp_path: Path) -> None:
    assert _package(tmp_path, ["alpha", "beta"]) == 0

    assert _package(tmp_path, ["alpha"]) == 0

    assert _bundles(tmp_path) == ["."]
    assert _record(tmp_path) == {"bundles": ["."]}
    (record,) = _archived(tmp_path)
    assert {entry["from"].split("/")[0] for entry in record["moved"]} == {"alpha", "beta"}


def test_widening_from_one_pipeline_moves_the_top_level_bundle(tmp_path: Path) -> None:
    assert _package(tmp_path, ["alpha"]) == 0

    assert _package(tmp_path, ["alpha", "beta"]) == 0

    assert _bundles(tmp_path) == ["alpha", "beta"]
    (record,) = _archived(tmp_path)
    moved = {entry["from"] for entry in record["moved"]}
    assert {"databricks.yml", "SETUP.md"} <= moved
    assert any(path.startswith("resources/") for path in moved)
    assert not any(path.startswith(("alpha/", "beta/", "metadata/")) for path in moved)
    assert (tmp_path / "metadata").is_dir()


@pytest.mark.parametrize("pipeline", ["Src", "Resources"])
def test_widening_keeps_a_new_bundle_folder_named_like_a_top_level_entry(tmp_path: Path, pipeline: str) -> None:
    """A pipeline whose folder is src/ or resources/ gets a clean bundle there; the old entry is archived first."""
    folder = pipeline.lower()
    assert _package(tmp_path, ["alpha"]) == 0
    old_notebook = tmp_path / "src" / "notebooks" / "alpha.py"
    old_notebook.parent.mkdir(parents=True)
    old_notebook.write_text("print('alpha')\n", encoding="utf-8")

    assert _package(tmp_path, [pipeline, "load"]) == 0

    assert _bundles(tmp_path) == sorted(["load", folder])
    assert _record(tmp_path) == {"bundles": sorted(["load", folder])}
    assert (tmp_path / folder / "databricks.yml").is_file()
    assert (tmp_path / folder / "resources" / f"{folder}.yml").is_file()
    assert not (tmp_path / "resources" / "alpha.yml").exists()
    assert not old_notebook.exists()
    (record,) = _archived(tmp_path)
    moved = {entry["from"] for entry in record["moved"]}
    assert {"databricks.yml", "resources/alpha.yml", "src/notebooks/alpha.py"} <= moved


def test_narrowing_away_a_pipeline_named_like_a_top_level_entry_moves_its_bundle(tmp_path: Path) -> None:
    """A recorded src/ bundle folder is archived like any other once package stops writing it."""
    assert _package(tmp_path, ["Src", "load"]) == 0

    assert _package(tmp_path, ["load"]) == 0

    assert _bundles(tmp_path) == ["."]
    assert _record(tmp_path) == {"bundles": ["."]}
    (record,) = _archived(tmp_path)
    moved = {entry["from"] for entry in record["moved"]}
    assert {"src/databricks.yml", "src/resources/src.yml", "load/databricks.yml"} <= moved


def test_re_packaging_the_same_pipelines_moves_nothing(tmp_path: Path) -> None:
    assert _package(tmp_path, ["alpha", "beta"]) == 0

    assert _package(tmp_path, ["alpha", "beta"]) == 0

    assert _archived(tmp_path) == []


def test_a_bundle_folder_package_never_recorded_is_left_alone(tmp_path: Path) -> None:
    """Only folders package wrote are moved; a bundle someone else put in the output folder stays."""
    assert _package(tmp_path, ["alpha", "beta"]) == 0
    other = tmp_path / "team_bundle"
    other.mkdir()
    (other / "databricks.yml").write_text("bundle:\n  name: team_bundle\n", encoding="utf-8")

    assert _package(tmp_path, ["alpha"]) == 0

    assert (other / "databricks.yml").is_file()
    (record,) = _archived(tmp_path)
    assert not any(entry["from"].startswith("team_bundle/") for entry in record["moved"])


@pytest.mark.parametrize(
    "record",
    [
        {"bundles": ["../outside"]},
        {"bundles": ["alpha/src"]},
        {"bundles": [5]},
        {"bundles": "alpha"},
        ["alpha"],
        {"bundles": ["alpha"], "note": "x"},
    ],
    ids=["parent", "nested", "number", "not-a-list", "not-an-object", "unknown-key"],
)
def test_a_malformed_or_unsafe_record_is_refused_before_anything_is_written(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], record: Any
) -> None:
    (tmp_path / "metadata").mkdir()
    (tmp_path / RECORD).write_text(json.dumps(record), encoding="utf-8")

    assert _package(tmp_path, ["alpha", "beta"]) == 1

    assert "packaged_bundles.json" in capsys.readouterr().err
    assert _bundles(tmp_path) == []


def test_a_recorded_metadata_folder_is_never_moved(tmp_path: Path) -> None:
    """A hand-edited record naming metadata/ can't make package move it; the new top-level bundle is complete."""
    assert _package(tmp_path, ["alpha", "beta"]) == 0
    (tmp_path / RECORD).write_text(json.dumps({"bundles": ["alpha", "beta", "metadata", "src"]}), encoding="utf-8")
    kept = tmp_path / "metadata" / "notes.txt"
    kept.write_text("kept\n", encoding="utf-8")
    stale = tmp_path / "src" / "helper.py"
    stale.parent.mkdir()
    stale.write_text("print('stale')\n", encoding="utf-8")

    assert _package(tmp_path, ["alpha"]) == 0

    assert _bundles(tmp_path) == ["."]
    assert _record(tmp_path) == {"bundles": ["."]}
    assert kept.is_file()
    assert (tmp_path / "databricks.yml").is_file() and (tmp_path / "resources" / "alpha.yml").is_file()
    assert not stale.exists()
    (record,) = _archived(tmp_path)
    assert {entry["from"].split("/")[0] for entry in record["moved"]} == {"alpha", "beta", "src"}


def test_re_running_discover_moves_the_recorded_bundle_folders_into_the_archive(tmp_path: Path) -> None:
    """Discover's reset already moves a bundle at the top; the per-pipeline ones go with it."""
    assert _package(tmp_path, ["alpha", "beta"]) == 0

    record_path = clear_stale_outputs(tmp_path)

    assert record_path is not None
    assert _bundles(tmp_path) == []
    moved = {entry["from"].split("/")[0] for entry in json.loads(record_path.read_text(encoding="utf-8"))["moved"]}
    assert {"alpha", "beta", "metadata"} <= moved


def test_discover_still_resets_a_folder_whose_record_is_malformed(tmp_path: Path) -> None:
    """Discover is the hosted server's way out of a stuck folder, so a bad record is archived, not refused."""
    assert _package(tmp_path, ["alpha", "beta"]) == 0
    (tmp_path / RECORD).write_text("{not json", encoding="utf-8")

    record_path = clear_stale_outputs(tmp_path)

    assert record_path is not None
    assert not (tmp_path / RECORD).exists()
    assert sorted(path.name for path in tmp_path.iterdir()) == ["alpha", "beta", "metadata"]
