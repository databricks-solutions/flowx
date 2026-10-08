"""Enrich writes its own ``source_insights.json`` and the inventory is rendered from it.

The inventory's ``insights`` block is always a copy of ``source_insights.json``, which records
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
    INSIGHTS_KEY,
    SOURCE_INSIGHTS_FILENAME,
    enrich_inventory,
    inventory_fingerprint,
    source_insights_hash_violations,
    validate_insights,
)
from flowx.discovery_inventory import STRATEGY_PROPERTY, build_source_inventory
from flowx.discovery_serde import SOURCE_GRAPHS_FILENAME, write_source_graphs
from flowx.models.discovery import CONCEPT_NOTEBOOK, SourceGraph, SourceNode


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


def test_enrich_writes_source_insights_and_renders_the_inventory_from_it(tmp_path: Path) -> None:
    inventory_path = _discover(tmp_path)
    before = json.loads(inventory_path.read_text(encoding="utf-8"))

    result = enrich_inventory(tmp_path, insights=_authored(tmp_path))

    assert result["ok"] is True
    source_insights = json.loads((tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).read_text(encoding="utf-8"))
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    assert inventory[INSIGHTS_KEY] == source_insights
    assert {key: value for key, value in inventory.items() if key != INSIGHTS_KEY} == before
    assert source_insights["source_graphs_sha256"] == before["source_graphs_sha256"]
    assert "authored_against" not in source_insights
    assert "authored_against" not in inventory[INSIGHTS_KEY]
    assert source_insights["inventory_sha256"] == inventory_fingerprint(before)
    assert result["source_insights_sha256"] == source_insights["source_insights_sha256"]
    assert source_insights_hash_violations(source_insights) == []


def test_re_enriching_is_byte_identical_and_keeps_the_routing_fingerprint(tmp_path: Path) -> None:
    inventory_path = _discover(tmp_path)
    fingerprint_before = inventory_fingerprint(json.loads(inventory_path.read_text(encoding="utf-8")))

    enrich_inventory(tmp_path, insights=_authored(tmp_path))
    first = (inventory_path.read_bytes(), (tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).read_bytes())
    enrich_inventory(tmp_path, insights=_authored(tmp_path))
    second = (inventory_path.read_bytes(), (tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).read_bytes())

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
    assert not (tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).exists()


def test_enrich_refuses_when_the_saved_source_graphs_file_is_missing(tmp_path: Path) -> None:
    inventory_path = _discover(tmp_path)
    (tmp_path / "metadata" / SOURCE_GRAPHS_FILENAME).unlink()
    inventory_bytes = inventory_path.read_bytes()

    result = enrich_inventory(tmp_path, insights=_insights())

    assert result["ok"] is False
    assert any(f"{SOURCE_GRAPHS_FILENAME} is missing" in violation for violation in result["violations"])
    assert inventory_path.read_bytes() == inventory_bytes
    assert not (tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).exists()


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
    assert not (tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).exists()


def test_inventory_without_a_recorded_graph_hash_still_enriches(tmp_path: Path) -> None:
    """An inventory from a source that does not persist graphs yet has nothing to check."""
    metadata = tmp_path / "metadata"
    metadata.mkdir(parents=True)
    inventory = build_source_inventory(_graphs(), source="airflow", source_dir="/dags")
    (metadata / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")

    result = enrich_inventory(tmp_path, insights=_insights())

    assert result["ok"] is True
    source_insights = json.loads((metadata / SOURCE_INSIGHTS_FILENAME).read_text(encoding="utf-8"))
    assert "source_graphs_sha256" not in source_insights


def test_authored_insights_cannot_set_the_library_hashes() -> None:
    inventory = build_source_inventory(_graphs(), source="adf", source_dir="/src")
    raw = {**_insights(), "source_graphs_sha256": "x", "source_insights_sha256": "y"}

    violations = validate_insights(raw, inventory)

    assert any("source_graphs_sha256" in violation and "library" in violation for violation in violations)
    assert any("source_insights_sha256" in violation and "library" in violation for violation in violations)


def test_an_edited_source_insights_file_fails_its_hash_check(tmp_path: Path) -> None:
    _discover(tmp_path)
    enrich_inventory(tmp_path, insights=_authored(tmp_path))
    document = json.loads((tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).read_text(encoding="utf-8"))
    document["overview"] = "Edited after enrich."

    assert source_insights_hash_violations(document) != []


def test_insights_authored_against_an_older_inventory_are_refused(tmp_path: Path) -> None:
    """A missing or different authored_against means discover ran again after the insights were written."""
    inventory_path = _discover(tmp_path)
    inventory_bytes = inventory_path.read_bytes()

    missing = enrich_inventory(tmp_path, insights=_insights())
    stale = enrich_inventory(tmp_path, insights={**_insights(), "authored_against": "an-older-discover"})

    assert missing["ok"] is False
    assert any("'authored_against' is required" in violation for violation in missing["violations"])
    assert stale["ok"] is False
    assert any("authored against a different inventory" in violation for violation in stale["violations"])
    assert inventory_path.read_bytes() == inventory_bytes
    assert not (tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).exists()


def test_authored_against_is_refused_when_the_inventory_records_no_graph_hash() -> None:
    inventory = build_source_inventory(_graphs(), source="airflow", source_dir="/dags")

    violations = validate_insights({**_insights(), "authored_against": "a-placeholder"}, inventory)

    assert any(
        "'authored_against' was given" in violation and "source_graphs_sha256" in violation for violation in violations
    )


def test_a_failed_first_inventory_write_leaves_no_insights_behind(tmp_path: Path, monkeypatch: Any) -> None:
    """With no previous source_insights.json, a failed inventory replace removes the new one."""
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

    assert not (metadata / SOURCE_INSIGHTS_FILENAME).exists()
    assert inventory_path.read_bytes() == inventory_before
    assert sorted(path.name for path in metadata.iterdir()) == files_before


def test_a_failed_inventory_write_leaves_the_previous_insights_in_place(tmp_path: Path, monkeypatch: Any) -> None:
    """source_insights.json and inventory.json are replaced together or not at all."""
    inventory_path = _discover(tmp_path)
    enrich_inventory(tmp_path, insights=_authored(tmp_path))
    insights_path = tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME
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
    assert not (tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).exists()


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
    assert not (tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).exists()


def test_a_second_enrich_cannot_interleave_with_one_already_writing(tmp_path: Path, monkeypatch: Any) -> None:
    """Pause one enrich between its two replaces and run another: the two files must still agree."""
    inventory_path = _discover(tmp_path)
    enrich_inventory(tmp_path, insights=_authored(tmp_path))
    insights_path = tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME
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
    source_insights = json.loads(insights_path.read_text(encoding="utf-8"))
    assert json.loads(inventory_path.read_text(encoding="utf-8"))[INSIGHTS_KEY] == source_insights
    assert source_insights["overview"] == "First writer."
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
    assert not (tmp_path / "metadata" / SOURCE_INSIGHTS_FILENAME).exists()
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
    source_insights = json.loads((metadata / SOURCE_INSIGHTS_FILENAME).read_text(encoding="utf-8"))
    assert json.loads(inventory_path.read_text(encoding="utf-8"))[INSIGHTS_KEY] == source_insights
