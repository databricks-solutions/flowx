"""Shared integration-test configuration and fixtures."""

from pathlib import Path

import pytest

from flowx.sources.adf.loader import load_adf_definitions

INTEGRATION_ROOT = Path(__file__).parent
ADF_FIXTURE_ROOT = INTEGRATION_ROOT / "azure" / "adf"


def pytest_addoption(parser):
    """Register expected-artifact regeneration for ADF case tests."""
    parser.addoption(
        "--update-expected",
        action="store_true",
        default=False,
        help="regenerate ADF case expected artifacts from checked-in fixtures",
    )


@pytest.fixture
def pipeline_by_name(adf_definitions):
    """Return a lookup helper that finds a pipeline by name."""

    def _find(name: str):
        for p in adf_definitions.pipelines:
            if p.name == name:
                return p
        raise KeyError(f"Pipeline '{name}' not found in fixtures")

    return _find


@pytest.fixture(scope="module")
def definitions():
    """Load the checked-in definitions through the production ADF loader."""
    return load_adf_definitions(ADF_FIXTURE_ROOT)
