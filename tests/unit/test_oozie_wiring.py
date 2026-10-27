"""The Oozie source is reachable through the registry, the adapter CLI, and the session prompts."""

from __future__ import annotations

import json
from pathlib import Path

from flowx.adapter.__main__ import main as adapter_main
from flowx.adapter.session import _convert_options, _discover_options
from flowx.sources import available_sources, get_source


def test_registry_lists_oozie():
    assert "oozie" in available_sources()
    source = get_source("oozie")
    assert (source.discover_module, source.convert_module) == (
        "flowx.sources.oozie.discover",
        "flowx.sources.oozie.convert",
    )
    assert source.source_path_flag == "--oozie-source-path"
    assert source.extract_module is None and source.profile_module is None


def test_session_prompts_for_the_oozie_source_path():
    expected = "Where are the Oozie application directories?"
    for options in (_discover_options("oozie"), _convert_options("oozie")):
        prompts = {option.option_id: option.prompt for option in options}
        assert prompts["oozie_source_path"] == expected


def test_adapter_routes_discover_with_both_path_flags(tmp_path: Path):
    app = tmp_path / "export" / "app"
    app.mkdir(parents=True)
    (app / "workflow.xml").write_text('<workflow-app name="w"><start to="end"/><end name="end"/></workflow-app>')
    for index, flag in enumerate(("--source-path", "--oozie-source-path")):
        output = tmp_path / f"out{index}"
        argv = ["discover", "--source", "oozie", flag, str(tmp_path / "export"), "--output-dir", str(output)]
        assert adapter_main(argv) == 0
        assert json.loads((output / "metadata" / "inventory.json").read_text())["source"] == "oozie"


def test_profile_and_extract_are_refused_for_oozie():
    assert adapter_main(["profile", "--source", "oozie"]) == 2
    assert adapter_main(["extract", "--source", "oozie"]) == 2
