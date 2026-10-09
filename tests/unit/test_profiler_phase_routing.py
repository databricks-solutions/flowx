"""The adapter routes `profile` to the adf profiler and rejects unsupported sources."""

from __future__ import annotations

import flowx.adapter.__main__ as adapter_main
from flowx.sources import get_source


def test_adf_source_declares_a_profile_module():
    assert get_source("adf").profile_module == "flowx.sources.adf.profiler.runner"


def test_airflow_source_has_no_profile_module():
    assert get_source("airflow").profile_module is None


def test_profile_routes_to_runner(monkeypatch):
    captured = {}

    def fake_main(argv):
        captured["argv"] = argv
        return 0

    import flowx.sources.adf.profiler.runner as runner

    monkeypatch.setattr(runner, "main", fake_main)
    code = adapter_main.main(["profile", "--source", "adf", "--output-dir", "out"])
    assert code == 0
    assert captured["argv"] == ["--output-dir", "out"]


def test_profile_aliases_source_path_to_source_dir(monkeypatch):
    captured = {}
    import flowx.sources.adf.profiler.runner as runner

    monkeypatch.setattr(runner, "main", lambda argv: captured.setdefault("argv", argv) and 0)
    adapter_main.main(["profile", "--source", "adf", "--source-path", "ignored"])
    assert captured["argv"] == ["--source-dir", "ignored"]


def test_profile_rejects_airflow(capsys):
    code = adapter_main.main(["profile", "--source", "airflow"])
    assert code == 2
    assert "profiling is not supported for the airflow source" in capsys.readouterr().err


def test_profile_requires_source(capsys):
    code = adapter_main.main(["profile"])
    assert code == 2
    assert "--source is required" in capsys.readouterr().err
