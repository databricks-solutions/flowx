"""LiveTransport turns HTTP and auth failures into the profiler's own errors."""

from __future__ import annotations

import sys
import types

import pytest

from flowx.sources.adf.profile import azure_client, core, discovery, pricing


class FakeResponse:
    def __init__(self, status_code: int, payload: dict, headers: dict | None = None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self) -> dict:
        return self._payload


class HtmlResponse(FakeResponse):
    def json(self) -> dict:
        raise ValueError("Expecting value: line 1 column 1 (char 0)")


class FakeSession:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.requests: list[tuple[str, str, dict]] = []
        self.allow_redirects: list[bool] = []

    def get(self, url, headers, timeout, allow_redirects):
        self.requests.append(("GET", url, headers))
        self.allow_redirects.append(allow_redirects)
        return self.response

    def post(self, url, headers, json, timeout, allow_redirects):
        self.requests.append(("POST", url, headers))
        self.allow_redirects.append(allow_redirects)
        return self.response


class ClientAuthenticationError(Exception):
    pass


@pytest.fixture(autouse=True)
def fake_azure_core(monkeypatch):
    """Stand in for azure.core so these tests run without the `azure` extra installed."""
    exceptions = types.ModuleType("azure.core.exceptions")
    exceptions.ClientAuthenticationError = ClientAuthenticationError  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "azure", types.ModuleType("azure"))
    monkeypatch.setitem(sys.modules, "azure.core", types.ModuleType("azure.core"))
    monkeypatch.setitem(sys.modules, "azure.core.exceptions", exceptions)


class TokenCredential:
    def get_token(self, scope):
        return types.SimpleNamespace(token="abc")


def test_tokens_are_requested_from_the_given_tenant():
    class TenantCredential:
        def __init__(self):
            self.requests: list[tuple[str, dict]] = []

        def get_token(self, scope, **kwargs):
            self.requests.append((scope, kwargs))
            return types.SimpleNamespace(token="abc")

    credential = TenantCredential()
    session = FakeSession(FakeResponse(200, {}))
    azure_client.LiveTransport(credential, tenant_id="t", session=session).get_json("https://management.azure.com/x")
    azure_client.LiveTransport(credential, session=session).get_json("https://management.azure.com/x")
    assert [kwargs for _, kwargs in credential.requests] == [{"tenant_id": "t"}, {}]


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


def test_an_azure_error_code_and_request_ids_are_kept_on_the_error():
    body = {"error": {"code": "AuthorizationFailed", "message": "The client 'abc' does not have authorization"}}
    headers = {"x-ms-request-id": "req-1", "x-ms-correlation-request-id": "corr-1", "Set-Cookie": "secret"}
    session = FakeSession(FakeResponse(403, body, headers))
    transport = azure_client.LiveTransport(TokenCredential(), session=session)
    with pytest.raises(azure_client.TransportHttpError) as exc:
        transport.post_json("https://management.azure.com/x", {})
    error = exc.value
    assert (error.status, error.code, error.request_id, error.correlation_id) == (
        403,
        "AuthorizationFailed",
        "req-1",
        "corr-1",
    )
    assert error.reason == "HTTP 403 AuthorizationFailed"
    assert str(error) == "HTTP 403 AuthorizationFailed from https://management.azure.com/x"
    assert "abc" not in repr(vars(error))


@pytest.mark.parametrize(
    "response",
    [
        HtmlResponse(502, {}),
        FakeResponse(403, {"error": "denied"}),
        FakeResponse(403, []),
        FakeResponse(403, {"error": {"code": "A" * 65}}),
        FakeResponse(403, {"error": {"code": "Authorization Failed; see https://evil.example.com"}}),
        FakeResponse(403, {"error": {"code": ["AuthorizationFailed"]}}),
    ],
)
def test_an_error_body_without_a_short_azure_code_leaves_the_code_empty(response):
    transport = azure_client.LiveTransport(TokenCredential(), session=FakeSession(response))
    with pytest.raises(azure_client.TransportHttpError) as exc:
        transport.get_json("https://management.azure.com/x")
    assert (exc.value.code, exc.value.request_id) == ("", "")
    assert exc.value.reason == f"HTTP {response.status_code}"


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

    def get(self, url, headers, timeout, allow_redirects):
        self.headers_seen.append(headers["Authorization"])
        return self._responses.pop(0)

    def post(self, url, headers, json, timeout, allow_redirects):
        return self.get(url, headers, timeout, allow_redirects)


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


class RoutingSession:
    def __init__(self, responses: dict[str, dict]):
        self.responses = responses
        self.requests: list[tuple[str, dict]] = []

    def _respond(self, url, headers):
        self.requests.append((url, headers))
        return FakeResponse(200, self.responses[max((key for key in self.responses if key in url), key=len)])

    def get(self, url, headers, timeout, allow_redirects):
        return self._respond(url, headers)

    def post(self, url, headers, json, timeout, allow_redirects):
        return self._respond(url, headers)


def test_bearer_token_never_follows_an_off_host_next_link(monkeypatch):
    monkeypatch.setattr(pricing, "fetch_live_pricing", lambda region: pricing.DEFAULT_PRICING)
    factory_id = "/subscriptions/SUB/resourceGroups/RG/providers/Microsoft.DataFactory/factories/demo-factory"
    session = RoutingSession(
        {
            "/subscriptions?": {"value": [{"subscriptionId": "SUB", "displayName": "Demo"}]},
            "/factories?": {"value": [{"name": "demo-factory", "location": "East US", "id": factory_id}]},
            "/integrationRuntimes?": {"value": []},
            "/queryPipelineRuns": {"value": []},
            "Microsoft.CostManagement/query": {
                "properties": {
                    "columns": [{"name": "PreTaxCost"}, {"name": "MeterSubcategory"}, {"name": "ResourceId"}],
                    "rows": [[1.0, "", factory_id]],
                    "nextLink": "https://attacker.example.com/subscriptions/SUB/providers/Microsoft.CostManagement/query",
                }
            },
        }
    )
    transport = azure_client.LiveTransport(TokenCredential(), session=session)
    result = core.AdfScanner(credential=None, days=45, transport=transport).scan()
    assert all(url.startswith("https://management.azure.com/") for url, _ in session.requests)
    assert [actual.cost for actual in result.actual_costs] == [1.0]
    assert any("outside https://management.azure.com" in warning for warning in result.warnings)


UNTRUSTED_URLS = [
    "https://evil.example.com/subscriptions?page=2",
    "https://management.azure.com.evil.com/subscriptions?page=2",
    "http://management.azure.com/subscriptions?page=2",
    "https://management.azure.com@evil.com/subscriptions?page=2",
    "https://user@management.azure.com/subscriptions?page=2",
    "https://user:secret@management.azure.com/subscriptions?page=2",
    "https://management.azure.com:444/subscriptions?page=2",
    "https://management.azure.com:443/subscriptions?page=2",
    "https://management.azure.com./subscriptions?page=2",
    "https://manag\u0435ment.azure.com/subscriptions?page=2",
    "https://xn--managment-k0h.azure.com/subscriptions?page=2",
]


@pytest.mark.parametrize("url", UNTRUSTED_URLS)
def test_untrusted_url_is_refused_before_a_token_is_fetched(url):
    credential = CountingCredential(lifetime_seconds=3600)
    session = FakeSession(FakeResponse(200, {}))
    transport = azure_client.LiveTransport(credential, session=session)
    with pytest.raises(azure_client.UntrustedUrlError):
        transport.post_json(url, {})
    assert session.requests == []
    assert credential.issued == 0


@pytest.mark.parametrize("next_link", UNTRUSTED_URLS)
def test_discovery_refuses_a_next_link_off_management_azure_com(next_link):
    first_page = {"value": [{"subscriptionId": "SUB", "displayName": "Demo"}], "nextLink": next_link}
    session = RoutingSession({"/subscriptions?api-version": first_page})
    transport = azure_client.LiveTransport(TokenCredential(), session=session)
    with pytest.raises(azure_client.UntrustedUrlError):
        discovery.list_subscriptions(transport)
    assert [url for url, _ in session.requests] == ["https://management.azure.com/subscriptions?api-version=2022-12-01"]


def test_redirects_are_not_followed():
    session = FakeSession(FakeResponse(200, {}))
    transport = azure_client.LiveTransport(TokenCredential(), session=session)
    transport.get_json("https://management.azure.com/a")
    transport.post_json("https://management.azure.com/b", {})
    assert session.allow_redirects == [False, False]


def test_the_real_host_in_upper_case_is_allowed():
    session = FakeSession(FakeResponse(200, {"ok": True}))
    transport = azure_client.LiveTransport(TokenCredential(), session=session)
    assert transport.get_json("https://MANAGEMENT.AZURE.COM/subscriptions") == {"ok": True}
    assert session.requests == [("GET", "https://MANAGEMENT.AZURE.COM/subscriptions", session.requests[0][2])]
    assert session.requests[0][2]["Authorization"] == "Bearer abc"


FACTORY = {"name": "demo-factory", "subscription_id": "SUB", "resource_group": "RG"}


def test_factory_listing_refuses_an_off_host_next_link_and_logs(caplog):
    page = {"value": [{"name": "demo-factory", "location": "westeurope"}], "nextLink": "https://evil.example.com/next"}
    session = RoutingSession({"/factories?": page})
    transport = azure_client.LiveTransport(TokenCredential(), session=session)
    with caplog.at_level("ERROR", logger=discovery.logger.name):
        assert discovery.list_adf_factories(transport, subscription_id="SUB") == []
    assert len(session.requests) == 1
    assert "Error discovering ADF factories in SUB" in caplog.text
    assert "Refusing to send the Azure token to https://evil.example.com/next" in caplog.text


def test_runtime_listing_refuses_an_off_host_next_link_and_logs(caplog):
    page = {"value": [{"name": "ir", "properties": {"type": "Managed"}}], "nextLink": "https://evil.example.com/next"}
    session = RoutingSession({"/integrationRuntimes?": page})
    transport = azure_client.LiveTransport(TokenCredential(), session=session)
    with caplog.at_level("WARNING", logger=discovery.logger.name):
        assert discovery.integration_runtime_types(transport, FACTORY) == {}
    assert len(session.requests) == 1
    assert "Cannot list integration runtimes for demo-factory" in caplog.text
    assert "Refusing to send the Azure token to https://evil.example.com/next" in caplog.text
