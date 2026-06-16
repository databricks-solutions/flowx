# Integration test configuration — shared fixtures are in tests/conftest.py.
import pytest


@pytest.fixture
def pipeline_by_name(adf_definitions):
    """Return a lookup helper that finds a pipeline by name."""

    def _find(name: str):
        for p in adf_definitions.pipelines:
            if p.name == name:
                return p
        raise KeyError(f"Pipeline '{name}' not found in fixtures")

    return _find


@pytest.fixture
def live_pipeline_by_name(live_definitions):
    """Return a lookup helper that finds a live ADF pipeline by name."""

    def _find(name: str):
        for p in live_definitions.pipelines:
            if p.name == name:
                return p
        raise KeyError(f"Pipeline '{name}' not found in live ADF export")

    return _find
