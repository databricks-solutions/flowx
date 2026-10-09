"""LiveTransport turns HTTP and auth failures into the profiler's own errors."""

from __future__ import annotations

import sys
import types

import pytest

from flowx.sources.adf.profiler import azure_client


class FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict:
        return self._payload


class FakeSession:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.requests: list[tuple[str, str, dict]] = []

    def get(self, url, headers, timeout):
        self.requests.append(("GET", url, headers))
        return self.response

    def post(self, url, headers, json, timeout):
        self.requests.append(("POST", url, headers))
        return self.response


class ClientAuthenticationError(Exception):
    pass


@pytest.fixture(autouse=True)
def fake_azure_core(monkeypatch):
    """Stand in for azure.core so these tests run without the `profile` extra installed."""
    exceptions = types.ModuleType("azure.core.exceptions")
    exceptions.ClientAuthenticationError = ClientAuthenticationError  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "azure", types.ModuleType("azure"))
    monkeypatch.setitem(sys.modules, "azure.core", types.ModuleType("azure.core"))
    monkeypatch.setitem(sys.modules, "azure.core.exceptions", exceptions)


class TokenCredential:
    def get_token(self, scope):
        return types.SimpleNamespace(token="abc")


def test_get_json_sends_bearer_token():
    session = FakeSession(FakeResponse(200, {"value": []}))
    transport = azure_client.LiveTransport(TokenCredential(), session=session)
    assert transport.get_json("https://management.azure.com/x") == {"value": []}
    assert session.requests[0][2]["Authorization"] == "Bearer abc"


def test_non_200_raises_transport_http_error():
    transport = azure_client.LiveTransport(TokenCredential(), session=FakeSession(FakeResponse(403, {})))
    with pytest.raises(azure_client.TransportHttpError) as exc:
        transport.post_json("https://management.azure.com/x", {})
    assert exc.value.status == 403


def test_credential_failure_becomes_profile_authentication_error():
    class BrokenCredential:
        def get_token(self, scope):
            raise ClientAuthenticationError("DefaultAzureCredential failed to retrieve a token")

    transport = azure_client.LiveTransport(BrokenCredential(), session=FakeSession(FakeResponse(200, {})))
    with pytest.raises(azure_client.ProfileAuthenticationError) as exc:
        transport.get_json("https://management.azure.com/x")
    assert "az login" in str(exc.value)


class SequencedSession:
    """Answers each request with the next canned response."""

    def __init__(self, *responses: FakeResponse):
        self._responses = list(responses)
        self.headers_seen: list[str] = []

    def get(self, url, headers, timeout):
        self.headers_seen.append(headers["Authorization"])
        return self._responses.pop(0)

    def post(self, url, headers, json, timeout):
        return self.get(url, headers, timeout)


class CountingCredential:
    """Issues token-1, token-2, ...; `lifetime` sets how long each one stays valid."""

    def __init__(self, lifetime_seconds: float):
        self.issued = 0
        self._lifetime = lifetime_seconds

    def get_token(self, scope):
        import time

        self.issued += 1
        return types.SimpleNamespace(token=f"token-{self.issued}", expires_on=time.time() + self._lifetime)


def test_token_is_refreshed_when_close_to_expiry():
    # A scan can outlive one ARM token; a token about to expire must be replaced, not reused.
    credential = CountingCredential(lifetime_seconds=60)
    session = SequencedSession(FakeResponse(200, {}), FakeResponse(200, {}))
    transport = azure_client.LiveTransport(credential, session=session)
    transport.get_json("https://management.azure.com/a")
    transport.get_json("https://management.azure.com/b")
    assert session.headers_seen == ["Bearer token-1", "Bearer token-2"]


def test_long_lived_token_is_reused():
    credential = CountingCredential(lifetime_seconds=3600)
    session = SequencedSession(FakeResponse(200, {}), FakeResponse(200, {}))
    transport = azure_client.LiveTransport(credential, session=session)
    transport.get_json("https://management.azure.com/a")
    transport.get_json("https://management.azure.com/b")
    assert credential.issued == 1


def test_unauthorized_response_retries_once_with_a_fresh_token():
    credential = CountingCredential(lifetime_seconds=3600)
    session = SequencedSession(FakeResponse(401, {}), FakeResponse(200, {"ok": True}))
    transport = azure_client.LiveTransport(credential, session=session)
    assert transport.get_json("https://management.azure.com/a") == {"ok": True}
    assert session.headers_seen == ["Bearer token-1", "Bearer token-2"]
