import sys
from pathlib import Path

import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

FIXTURES_DIR = Path(__file__).parent / "resources" / "json"


@pytest.fixture
def fixtures_dir():
    return FIXTURES_DIR


@pytest.fixture
def adf_definitions():
    """Load all ADF definitions from the test fixtures directory."""
    from flowx.parser.adf_loader import load_adf_definitions

    return load_adf_definitions(FIXTURES_DIR)
