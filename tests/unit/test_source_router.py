"""Unit tests for the source registry and the adapter's --source routing."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from flowx.adapter.__main__ import _run_phase, _split_source
from flowx.sources import available_sources, get_source

_DAG_FIXTURE = Path(__file__).resolve().parents[1] / "resources" / "airflow" / "orders_analytics_dag.py"


# --------------------------------------------------------------------------------------
# Registry
# --------------------------------------------------------------------------------------


def test_registry_lists_adf_and_airflow():
    assert set(available_sources()) >= {"adf", "airflow"}


def test_get_source_returns_phase_modules():
    airflow = get_source("airflow")
    assert airflow.discover_module == "flowx.sources.airflow.discover"
    assert airflow.convert_module == "flowx.sources.airflow.convert"


def test_get_unknown_source_raises():
    with pytest.raises(KeyError, match="Unknown source"):
        get_source("oozie")


# --------------------------------------------------------------------------------------
# --source extraction
# --------------------------------------------------------------------------------------


def test_split_source_absent_is_none():
    assert _split_source([]) == (None, [])
    assert _split_source(["--source-dir", "x"]) == (None, ["--source-dir", "x"])


def test_split_source_space_form():
    assert _split_source(["--source", "airflow", "--source-dir", "x"]) == ("airflow", ["--source-dir", "x"])


def test_split_source_equals_form():
    assert _split_source(["--source=airflow", "--output-dir", "o"]) == ("airflow", ["--output-dir", "o"])


# --------------------------------------------------------------------------------------
# Routing (end-to-end through the adapter phase runner, in-process)
# --------------------------------------------------------------------------------------


def test_unknown_source_returns_exit_2():
    rc = _run_phase("discover", ["--source", "nope", "--source-path", str(_DAG_FIXTURE)])
    assert rc == 2


def test_missing_source_is_required_for_discover():
    # No --source: discover/convert must error (there is no default source).
    rc = _run_phase("discover", ["--source-path", str(_DAG_FIXTURE)])
    assert rc == 2


def test_missing_source_is_required_for_convert():
    rc = _run_phase("convert", ["--source-path", str(_DAG_FIXTURE)])
    assert rc == 2


def test_airflow_discover_then_convert_route():
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        rc = _run_phase(
            "discover", ["--source", "airflow", "--source-path", str(_DAG_FIXTURE), "--output-dir", str(out)]
        )
        assert rc == 0
        assert (out / "metadata" / "inventory.json").exists()
        rc = _run_phase(
            "convert", ["--source", "airflow", "--source-path", str(_DAG_FIXTURE), "--output-dir", str(out)]
        )
        assert rc == 0
        assert (out / ".work" / "translation_report.json").exists()


def test_package_is_source_independent():
    # package ignores --source and routes to the shared bundler; drive the whole chain.
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        _run_phase("convert", ["--source", "airflow", "--source-path", str(_DAG_FIXTURE), "--output-dir", str(out)])
        rc = _run_phase("package", ["--output-dir", str(out)])
        assert rc == 0
        assert (out / "databricks.yml").exists()
        assert list((out / "resources").glob("*.yml"))
