import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

FIXTURES_DIR = Path(__file__).parent / "resources" / "json"


@pytest.fixture
def fixtures_dir():
    return FIXTURES_DIR


@pytest.fixture
def adf_definitions():
    """Load all ADF definitions from the test fixtures directory."""
    from flowx.sources.adf.loader import load_adf_definitions

    return load_adf_definitions(FIXTURES_DIR)


class ReplayTransport:
    """Serves recorded ADF profile REST responses for any URL containing a mapping key (the longest key wins).

    A value may be a dict (served every time), a list (served in order on successive calls,
    for pagination), an int (raised as `TransportHttpError` with that status), or an exception
    (raised as is, e.g. to fail a call a test doesn't expect). An unrecorded
    URL raises AssertionError so a missing fixture is loud. Every request is kept in `calls`.
    """

    def __init__(self, mapping: dict[str, Any]):
        self._mapping = mapping
        self._served: dict[str, int] = {}
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []

    def _lookup(self, url: str) -> dict[str, Any]:
        matches = [key for key in self._mapping if key in url]
        if not matches:
            raise AssertionError(f"no recorded response for {url}")
        key = max(matches, key=len)
        value = self._mapping[key]
        if isinstance(value, list):
            index = self._served.get(key, 0)
            self._served[key] = index + 1
            value = value[min(index, len(value) - 1)]
        if isinstance(value, int):
            from flowx.sources.adf.profile.azure_client import TransportHttpError

            raise TransportHttpError(value, url)
        if isinstance(value, BaseException):
            raise value
        return value

    def get_json(self, url: str) -> dict[str, Any]:
        self.calls.append(("GET", url, None))
        return self._lookup(url)

    def post_json(self, url: str, body: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(("POST", url, body))
        return self._lookup(url)


@pytest.fixture
def replay_transport():
    """Returns a factory that builds a ReplayTransport from a mapping of URL fragments to responses."""
    return ReplayTransport
