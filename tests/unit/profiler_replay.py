"""A replay transport for profiler tests: serves recorded Azure REST responses by URL."""

from __future__ import annotations

from typing import Any

from flowx.sources.adf.profiler.azure_client import TransportHttpError


class ReplayTransport:
    """Serves recorded JSON for any URL containing a mapping key (the longest key wins).

    A value may be a dict (served every time), a list (served in order on successive calls,
    for pagination), or an int (raised as `TransportHttpError` with that status). An unrecorded
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
            raise TransportHttpError(value, url)
        return value

    def get_json(self, url: str) -> dict[str, Any]:
        self.calls.append(("GET", url, None))
        return self._lookup(url)

    def post_json(self, url: str, body: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(("POST", url, body))
        return self._lookup(url)
