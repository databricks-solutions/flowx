"""Enrich writes its own ``agentic_insights.json`` and the inventory is rendered from it.

The inventory's ``insights`` block is always a copy of ``agentic_insights.json``, which records
the saved ``source_graphs.json`` hash it was checked against and its own content hash, so a
later phase can tell exactly which graph and which insights a decision was made on.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

import pytest

import flowx
from flowx.discovery_insights import (
    AGENTIC_INSIGHTS_FILENAME,
    INSIGHTS_KEY,
    agentic_insights_hash_violations,
    enrich_inventory,
    inventory_fingerprint,
    project_inventory,
    validate_insights,
)
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.discovery_serde import SOURCE_GRAPHS_FILENAME, read_source_graphs, write_source_graphs
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode
from flowx.sources.adf.loader import main as adf_discover_main


def _graphs() -> list[SourceGraph]:
    node = SourceNode(
        source_id="load",
        task_key="load",
        concept=CONCEPT_NOTEBOOK,
        source="adf",
        name="load",
        native_type="DatabricksNotebook",
        properties={STRATEGY_PROPERTY: "deterministic"},
    )
    return [SourceGraph(name="orders", source="adf", tasks=[node])]


def _discover(output_dir: Path) -> Path:
    """Write source_graphs.json and the inventory projected from it, as ADF discover does."""
    metadata = output_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    document = write_source_graphs(metadata / SOURCE_GRAPHS_FILENAME, _graphs(), source="adf")
    inventory = build_source_inventory(
        _graphs(), source="adf", source_dir="/src", source_graphs_sha256=document["document_sha256"]
    )
    path = metadata / "inventory.json"
    path.write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    return path


def _authored(output_dir: Path) -> dict[str, Any]:
    """Insights authored against the inventory now on disk, as an agent records what it read."""
    inventory = json.loads((output_dir / "metadata" / "inventory.json").read_text(encoding="utf-8"))
    return {**_insights(), "authored_against": inventory["source_graphs_sha256"]}


def _insights() -> dict[str, Any]:
    return {
        "overview": "One ingestion pipeline.",
        "pipeline_insights": [{"pipeline": "orders", "intent": "Load orders nightly."}],
    }


def test_enrich_writes_agentic_insights_and_renders_the_inventory_from_it(tmp_path: Path) -> None:
    inventory_path = _discover(tmp_path)
    before = json.loads(inventory_path.read_text(encoding="utf-8"))

    result = enrich_inventory(tmp_path, insights=_authored(tmp_path))

    assert result["ok"] is True
    assert (tmp_path / "metadata" / "agentic_insights.json").is_file()
    agentic_insights = json.loads((tmp_path / "metadata" / AGENTIC_INSIGHTS_FILENAME).read_text(encoding="utf-8"))
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    assert inventory[INSIGHTS_KEY] == agentic_insights
    assert {key: value for key, value in inventory.items() if key != INSIGHTS_KEY} == before
    assert agentic_insights["source_graphs_sha256"] == before["source_graphs_sha256"]
    assert "authored_against" not in agentic_insights
    assert "authored_against" not in inventory[INSIGHTS_KEY]
    assert agentic_insights["inventory_sha256"] == inventory_fingerprint(before)
    assert result["agentic_insights_sha256"] == agentic_insights["agentic_insights_sha256"]
    assert agentic_insights_hash_violations(agentic_insights) == []


def test_re_enriching_is_byte_identical_and_keeps_the_routing_fingerprint(tmp_path: Path) -> None:
    inventory_path = _discover(tmp_path)
    fingerprint_before = inventory_fingerprint(json.loads(inventory_path.read_text(encoding="utf-8")))

    enrich_inventory(tmp_path, insights=_authored(tmp_path))
    first = (inventory_path.read_bytes(), (tmp_path / "metadata" / AGENTIC_INSIGHTS_FILENAME).read_bytes())
    enrich_inventory(tmp_path, insights=_authored(tmp_path))
    second = (inventory_path.read_bytes(), (tmp_path / "metadata" / AGENTIC_INSIGHTS_FILENAME).read_bytes())

    assert first == second
    assert inventory_fingerprint(json.loads(inventory_path.read_text(encoding="utf-8"))) == fingerprint_before


def test_enrich_refuses_an_inventory_from_a_different_source_graphs_file(tmp_path: Path) -> None:
    inventory_path = _discover(tmp_path)
    write_source_graphs(tmp_path / "metadata" / SOURCE_GRAPHS_FILENAME, [], source="adf")
    inventory_bytes = inventory_path.read_bytes()

    result = enrich_inventory(tmp_path, insights=_insights())

    assert result["ok"] is False
    assert any("different source_graphs.json" in violation for violation in result["violations"])
    assert inventory_path.read_bytes() == inventory_bytes
    assert not (tmp_path / "metadata" / AGENTIC_INSIGHTS_FILENAME).exists()


def test_enrich_refuses_when_the_saved_source_graphs_file_is_missing(tmp_path: Path) -> None:
    inventory_path = _discover(tmp_path)
    (tmp_path / "metadata" / SOURCE_GRAPHS_FILENAME).unlink()
    inventory_bytes = inventory_path.read_bytes()

    result = enrich_inventory(tmp_path, insights=_insights())

    assert result["ok"] is False
    assert any(f"{SOURCE_GRAPHS_FILENAME} is missing" in violation for violation in result["violations"])
    assert inventory_path.read_bytes() == inventory_bytes
    assert not (tmp_path / "metadata" / AGENTIC_INSIGHTS_FILENAME).exists()


def test_enrich_refuses_when_the_saved_source_graphs_content_was_changed(tmp_path: Path) -> None:
    inventory_path = _discover(tmp_path)
    graphs_path = tmp_path / "metadata" / SOURCE_GRAPHS_FILENAME
    document = json.loads(graphs_path.read_text(encoding="utf-8"))
    document["graphs"][0]["name"] = "edited_after_discover"
    graphs_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    inventory_bytes = inventory_path.read_bytes()

    result = enrich_inventory(tmp_path, insights=_insights())

    assert result["ok"] is False
    assert any(f"{SOURCE_GRAPHS_FILENAME} is not usable" in violation for violation in result["violations"])
    assert inventory_path.read_bytes() == inventory_bytes
    assert not (tmp_path / "metadata" / AGENTIC_INSIGHTS_FILENAME).exists()


def test_inventory_without_a_recorded_graph_hash_still_enriches(tmp_path: Path) -> None:
    """An inventory from a source that does not persist graphs yet has nothing to check."""
    metadata = tmp_path / "metadata"
    metadata.mkdir(parents=True)
    inventory = build_source_inventory(_graphs(), source="airflow", source_dir="/dags")
    (metadata / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")

    result = enrich_inventory(tmp_path, insights=_insights())

    assert result["ok"] is True
    agentic_insights = json.loads((metadata / AGENTIC_INSIGHTS_FILENAME).read_text(encoding="utf-8"))
    assert "source_graphs_sha256" not in agentic_insights


def test_authored_insights_cannot_set_the_library_hashes() -> None:
    inventory = build_source_inventory(_graphs(), source="adf", source_dir="/src")
    raw = {**_insights(), "source_graphs_sha256": "x", "agentic_insights_sha256": "y"}

    violations = validate_insights(raw, inventory)

    assert any("source_graphs_sha256" in violation and "library" in violation for violation in violations)
    assert any("agentic_insights_sha256" in violation and "library" in violation for violation in violations)


def test_an_edited_agentic_insights_file_fails_its_hash_check(tmp_path: Path) -> None:
    _discover(tmp_path)
    enrich_inventory(tmp_path, insights=_authored(tmp_path))
    document = json.loads((tmp_path / "metadata" / AGENTIC_INSIGHTS_FILENAME).read_text(encoding="utf-8"))
    document["overview"] = "Edited after enrich."

    assert agentic_insights_hash_violations(document) != []


def test_insights_authored_against_an_older_inventory_are_refused(tmp_path: Path) -> None:
    """A different authored_against means discover ran again after the insights were written."""
    inventory_path = _discover(tmp_path)
    inventory_bytes = inventory_path.read_bytes()

    stale = enrich_inventory(tmp_path, insights={**_insights(), "authored_against": "an-older-discover"})

    assert stale["ok"] is False
    assert any("authored against a different inventory" in violation for violation in stale["violations"])
    assert inventory_path.read_bytes() == inventory_bytes
    assert not (tmp_path / "metadata" / AGENTIC_INSIGHTS_FILENAME).exists()


def test_insights_without_authored_against_are_recorded_with_the_library_stamped_hash(tmp_path: Path) -> None:
    """authored_against is optional: the library stamps the graphs hash it checked the insights against."""
    inventory_path = _discover(tmp_path)
    recorded_hash = json.loads(inventory_path.read_text(encoding="utf-8"))["source_graphs_sha256"]

    result = enrich_inventory(tmp_path, insights=_insights())

    assert result["ok"] is True
    agentic_insights = json.loads((tmp_path / "metadata" / AGENTIC_INSIGHTS_FILENAME).read_text(encoding="utf-8"))
    assert agentic_insights["source_graphs_sha256"] == recorded_hash
    assert "authored_against" not in agentic_insights


def test_authored_against_is_refused_when_the_inventory_records_no_graph_hash() -> None:
    inventory = build_source_inventory(_graphs(), source="airflow", source_dir="/dags")

    violations = validate_insights({**_insights(), "authored_against": "a-placeholder"}, inventory)

    assert any(
        "'authored_against' was given" in violation and "source_graphs_sha256" in violation for violation in violations
    )


def test_a_failed_first_inventory_write_leaves_no_insights_behind(tmp_path: Path, monkeypatch: Any) -> None:
    """With no previous agentic_insights.json, a failed inventory replace removes the new one."""
    inventory_path = _discover(tmp_path)
    metadata = tmp_path / "metadata"
    files_before = sorted(path.name for path in metadata.iterdir())
    inventory_before = inventory_path.read_bytes()
    real_replace = os.replace

    def failing_replace(source: Any, destination: Any) -> None:
        if Path(destination) == inventory_path:
            raise OSError("disk full")
        real_replace(source, destination)

    monkeypatch.setattr(os, "replace", failing_replace)
    with pytest.raises(OSError, match="disk full"):
        enrich_inventory(tmp_path, insights=_authored(tmp_path))

    assert not (metadata / AGENTIC_INSIGHTS_FILENAME).exists()
    assert inventory_path.read_bytes() == inventory_before
    assert sorted(path.name for path in metadata.iterdir()) == files_before


def test_a_failed_inventory_write_leaves_the_previous_insights_in_place(tmp_path: Path, monkeypatch: Any) -> None:
    """agentic_insights.json and inventory.json are replaced together or not at all."""
    inventory_path = _discover(tmp_path)
    enrich_inventory(tmp_path, insights=_authored(tmp_path))
    insights_path = tmp_path / "metadata" / AGENTIC_INSIGHTS_FILENAME
    insights_before, inventory_before = insights_path.read_bytes(), inventory_path.read_bytes()
    real_replace = os.replace

    def failing_replace(source: Any, destination: Any) -> None:
        if Path(destination) == inventory_path:
            raise OSError("disk full")
        real_replace(source, destination)

    monkeypatch.setattr(os, "replace", failing_replace)
    changed = {**_authored(tmp_path), "overview": "A different overview."}
    with pytest.raises(OSError, match="disk full"):
        enrich_inventory(tmp_path, insights=changed)

    assert insights_path.read_bytes() == insights_before
    assert inventory_path.read_bytes() == inventory_before


def test_a_source_graphs_file_that_is_not_an_object_is_a_violation_not_a_crash(tmp_path: Path) -> None:
    inventory_path = _discover(tmp_path)
    (tmp_path / "metadata" / SOURCE_GRAPHS_FILENAME).write_text("[]", encoding="utf-8")
    inventory_bytes = inventory_path.read_bytes()

    result = enrich_inventory(tmp_path, insights=_authored(tmp_path))

    assert result["ok"] is False
    assert any(
        f"{SOURCE_GRAPHS_FILENAME} is not usable" in violation and "JSON object" in violation
        for violation in result["violations"]
    )
    assert inventory_path.read_bytes() == inventory_bytes
    assert not (tmp_path / "metadata" / AGENTIC_INSIGHTS_FILENAME).exists()


@pytest.mark.parametrize("graphs", [[[]], [{"tasks": [1]}]])
def test_unhashed_source_graphs_with_malformed_entries_are_a_violation_not_a_crash(
    tmp_path: Path, graphs: list[Any]
) -> None:
    inventory_path = _discover(tmp_path)
    (tmp_path / "metadata" / SOURCE_GRAPHS_FILENAME).write_text(
        json.dumps({"contract_version": "1", "graphs": graphs}), encoding="utf-8"
    )
    inventory_bytes = inventory_path.read_bytes()

    result = enrich_inventory(tmp_path, insights=_authored(tmp_path))

    assert result["ok"] is False
    assert any(
        f"projected from a different {SOURCE_GRAPHS_FILENAME}" in violation for violation in result["violations"]
    )
    assert inventory_path.read_bytes() == inventory_bytes
    assert not (tmp_path / "metadata" / AGENTIC_INSIGHTS_FILENAME).exists()


def test_a_second_enrich_cannot_interleave_with_one_already_writing(tmp_path: Path, monkeypatch: Any) -> None:
    """Pause one enrich between its two replaces and run another: the two files must still agree."""
    inventory_path = _discover(tmp_path)
    enrich_inventory(tmp_path, insights=_authored(tmp_path))
    insights_path = tmp_path / "metadata" / AGENTIC_INSIGHTS_FILENAME
    paused, resume = threading.Event(), threading.Event()
    real_replace = os.replace

    def pausing_replace(source: Any, destination: Any) -> None:
        real_replace(source, destination)
        if threading.current_thread() is not threading.main_thread() and Path(destination) == insights_path:
            paused.set()
            resume.wait(timeout=10)

    monkeypatch.setattr(os, "replace", pausing_replace)
    first_errors: list[BaseException] = []

    def first_enrich() -> None:
        try:
            enrich_inventory(tmp_path, insights={**_authored(tmp_path), "overview": "First writer."})
        except BaseException as error:
            first_errors.append(error)

    first = threading.Thread(target=first_enrich)
    first.start()
    assert paused.wait(timeout=10)
    second = enrich_inventory(tmp_path, insights={**_authored(tmp_path), "overview": "Second writer."})
    resume.set()
    first.join(timeout=10)

    assert first_errors == []
    agentic_insights = json.loads(insights_path.read_text(encoding="utf-8"))
    assert json.loads(inventory_path.read_text(encoding="utf-8"))[INSIGHTS_KEY] == agentic_insights
    assert agentic_insights["overview"] == "First writer."
    assert second["ok"] is False
    assert any("another enrich" in violation for violation in second["violations"])
    assert not (tmp_path / "metadata" / ".enrich.lock").exists()
    assert not [path.name for path in (tmp_path / "metadata").iterdir() if path.name.endswith(".tmp")]


def test_a_lock_left_by_a_killed_enrich_blocks_the_next_one_until_removed(tmp_path: Path) -> None:
    """A leftover lock marks a write that may have stopped between the two replaces, so nothing is written."""
    inventory_path = _discover(tmp_path)
    lock_path = tmp_path / "metadata" / ".enrich.lock"
    lock_path.touch()
    inventory_bytes = inventory_path.read_bytes()

    refused = enrich_inventory(tmp_path, insights=_authored(tmp_path))

    assert refused["ok"] is False
    assert any(str(lock_path) in violation for violation in refused["violations"])
    assert inventory_path.read_bytes() == inventory_bytes
    assert not (tmp_path / "metadata" / AGENTIC_INSIGHTS_FILENAME).exists()
    lock_path.unlink()
    assert enrich_inventory(tmp_path, insights=_authored(tmp_path))["ok"] is True


_KILLED_ENRICH = """
import json, os, sys
from pathlib import Path
from flowx.discovery_insights import enrich_inventory

real_replace = os.replace

def replace_then_die(source, destination):
    real_replace(source, destination)
    os._exit(1)

os.replace = replace_then_die
enrich_inventory(Path(sys.argv[1]), insights=json.loads(sys.argv[2]))
"""


def test_the_enrich_after_a_killed_one_clears_its_temp_files(tmp_path: Path) -> None:
    """Kill an enrich between its two replaces, clear the lock as instructed, and enrich again."""
    inventory_path = _discover(tmp_path)
    metadata = tmp_path / "metadata"
    authored = _authored(tmp_path)
    source_root = Path(flowx.__file__).resolve().parents[1]
    killed = subprocess.run(
        [sys.executable, "-c", _KILLED_ENRICH, str(tmp_path), json.dumps(authored)],
        env={**os.environ, "PYTHONPATH": str(source_root)},
        check=False,
    )
    assert killed.returncode == 1
    assert [path.name for path in metadata.iterdir() if path.name.endswith(".tmp")]

    (metadata / ".enrich.lock").unlink()
    result = enrich_inventory(tmp_path, insights=authored)

    assert result["ok"] is True
    assert not [path.name for path in metadata.iterdir() if path.name.endswith(".tmp")]
    agentic_insights = json.loads((metadata / AGENTIC_INSIGHTS_FILENAME).read_text(encoding="utf-8"))
    assert json.loads(inventory_path.read_text(encoding="utf-8"))[INSIGHTS_KEY] == agentic_insights


def test_enrich_rebuilds_the_deterministic_inventory_from_the_saved_source_graphs(tmp_path: Path) -> None:
    """inventory.json comes from source_graphs.json plus the insights, not from whatever inventory.json held."""
    inventory_path = _discover(tmp_path)
    discovered = inventory_path.read_text(encoding="utf-8")
    edited = json.loads(discovered)
    edited["summary"]["activity_count"] = 999
    inventory_path.write_text(json.dumps(edited, indent=2), encoding="utf-8")

    assert enrich_inventory(tmp_path, insights=_authored(tmp_path))["ok"] is True

    enriched = json.loads(inventory_path.read_text(encoding="utf-8"))
    deterministic = {key: value for key, value in enriched.items() if key != INSIGHTS_KEY}
    assert json.dumps(deterministic, indent=2) == discovered


def test_projected_inventory_is_byte_identical_to_adf_discover_on_the_fixtures(tmp_path: Path) -> None:
    """Rebuilding from source_graphs.json gives exactly the bytes ADF discover wrote."""
    fixtures = Path(__file__).resolve().parents[1] / "resources" / "json"
    assert adf_discover_main(["--source-dir", str(fixtures), "--output-dir", str(tmp_path)]) == 0
    metadata = tmp_path / "metadata"
    discovered = (metadata / "inventory.json").read_text(encoding="utf-8")

    projected = project_inventory(read_source_graphs(metadata / SOURCE_GRAPHS_FILENAME), json.loads(discovered))

    assert json.dumps(projected, indent=2) == discovered
    inventory = json.loads(discovered)
    authored = {"authored_against": inventory["source_graphs_sha256"], "overview": "The fixture factory."}
    assert enrich_inventory(tmp_path, insights=authored)["ok"] is True
    enriched = json.loads((metadata / "inventory.json").read_text(encoding="utf-8"))
    assert json.dumps({key: value for key, value in enriched.items() if key != INSIGHTS_KEY}, indent=2) == discovered


def test_projected_inventory_matches_adf_discover_for_an_empty_pipeline(tmp_path: Path) -> None:
    """ADF discover counts a zero-activity pipeline but leaves it unlisted, and the rebuild must agree."""
    pipelines = tmp_path / "export" / "pipelines"
    pipelines.mkdir(parents=True)
    wait = {"name": "pause", "type": "Wait", "dependsOn": [], "typeProperties": {"waitTimeInSeconds": 1}}
    for name, activities in (("empty", []), ("orders", [wait])):
        document = {"name": name, "properties": {"activities": activities}}
        (pipelines / f"{name}.json").write_text(json.dumps(document), encoding="utf-8")
    output_dir = tmp_path / "out"
    assert adf_discover_main(["--source-dir", str(tmp_path / "export"), "--output-dir", str(output_dir)]) == 0
    metadata = output_dir / "metadata"
    discovered = (metadata / "inventory.json").read_text(encoding="utf-8")
    inventory = json.loads(discovered)
    assert inventory["summary"]["pipeline_count"] == 2
    assert [pipeline["name"] for pipeline in inventory["pipelines"]] == ["orders"]

    projected = project_inventory(read_source_graphs(metadata / SOURCE_GRAPHS_FILENAME), inventory)

    assert json.dumps(projected, indent=2) == discovered
    authored = {"authored_against": inventory["source_graphs_sha256"], "overview": "One empty pipeline."}
    assert enrich_inventory(output_dir, insights=authored)["ok"] is True
    enriched = json.loads((metadata / "inventory.json").read_text(encoding="utf-8"))
    assert json.dumps({key: value for key, value in enriched.items() if key != INSIGHTS_KEY}, indent=2) == discovered


def test_a_failed_rollback_never_truncates_the_insights_file_and_keeps_the_lock(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """If putting the previous insights back also fails, the file stays whole and the next enrich refuses."""
    inventory_path = _discover(tmp_path)
    enrich_inventory(tmp_path, insights=_authored(tmp_path))
    metadata = tmp_path / "metadata"
    insights_path = metadata / AGENTIC_INSIGHTS_FILENAME
    real_replace = os.replace

    def failing_replace(source: Any, destination: Any) -> None:
        if Path(destination) == inventory_path:
            raise OSError("disk full")
        real_replace(source, destination)

    def truncate_then_fail(self: Path, data: bytes) -> int:
        self.open("wb").close()
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", failing_replace)
    monkeypatch.setattr(Path, "write_bytes", truncate_then_fail)
    with pytest.raises(OSError):
        enrich_inventory(tmp_path, insights={**_authored(tmp_path), "overview": "A different overview."})
    monkeypatch.undo()

    assert json.loads(insights_path.read_text(encoding="utf-8"))["overview"]
    assert (metadata / ".enrich.lock").exists()
    refused = enrich_inventory(tmp_path, insights=_authored(tmp_path))
    assert refused["ok"] is False
    assert any(".enrich.lock" in violation for violation in refused["violations"])


@pytest.mark.parametrize("interrupted_file", [AGENTIC_INSIGHTS_FILENAME, "inventory.json"])
def test_an_interrupt_after_a_replace_succeeds_keeps_the_lock_and_rolls_nothing_back(
    tmp_path: Path, monkeypatch: Any, interrupted_file: str
) -> None:
    """Ctrl-C can land just after a rename finished, so it is treated like a kill rather than undone."""
    inventory_path = _discover(tmp_path)
    enrich_inventory(tmp_path, insights=_authored(tmp_path))
    metadata = tmp_path / "metadata"
    insights_path = metadata / AGENTIC_INSIGHTS_FILENAME
    inventory_before = inventory_path.read_bytes()
    real_replace = os.replace

    def replace_then_interrupt(source: Any, destination: Any) -> None:
        real_replace(source, destination)
        if Path(destination) == metadata / interrupted_file:
            raise KeyboardInterrupt

    monkeypatch.setattr(os, "replace", replace_then_interrupt)
    with pytest.raises(KeyboardInterrupt):
        enrich_inventory(tmp_path, insights={**_authored(tmp_path), "overview": "Interrupted writer."})
    monkeypatch.undo()

    assert json.loads(insights_path.read_text(encoding="utf-8"))["overview"] == "Interrupted writer."
    if interrupted_file == "inventory.json":
        assert json.loads(inventory_path.read_text(encoding="utf-8"))[INSIGHTS_KEY]["overview"] == "Interrupted writer."
    else:
        assert inventory_path.read_bytes() == inventory_before
    assert (metadata / ".enrich.lock").exists()
    refused = enrich_inventory(tmp_path, insights=_authored(tmp_path))
    assert refused["ok"] is False
    assert any(".enrich.lock" in violation for violation in refused["violations"])


def test_an_interrupt_before_either_replace_releases_the_lock(tmp_path: Path, monkeypatch: Any) -> None:
    """Nothing has been replaced yet, so both files still agree and the next enrich may run."""
    inventory_path = _discover(tmp_path)
    metadata = tmp_path / "metadata"
    inventory_before = inventory_path.read_bytes()

    def interrupted_write(self: Path, data: str, **kwargs: Any) -> int:
        raise KeyboardInterrupt

    monkeypatch.setattr(Path, "write_text", interrupted_write)
    with pytest.raises(KeyboardInterrupt):
        enrich_inventory(tmp_path, insights=_authored(tmp_path))
    monkeypatch.undo()

    assert inventory_path.read_bytes() == inventory_before
    assert not (metadata / AGENTIC_INSIGHTS_FILENAME).exists()
    assert not (metadata / ".enrich.lock").exists()
    assert enrich_inventory(tmp_path, insights=_authored(tmp_path))["ok"] is True
