"""Live Azure access for the ADF profile phase: dependency checks, credentials and tokens, errors,
and the HTTP transport. Every azure/requests import is lazy so importing this module costs nothing
until the profile phase actually runs.
"""

from __future__ import annotations

import time
from typing import Any, Protocol
from urllib.parse import urlparse

AZURE_MANAGEMENT_URL = "https://management.azure.com"
_MANAGEMENT_HOST = "management.azure.com"
_MANAGEMENT_SCOPE = "https://management.azure.com/.default"
REQUEST_TIMEOUT_SECONDS = 30
# Refresh a little before expiry so a token can't lapse between the check and the request.
_TOKEN_REFRESH_MARGIN_SECONDS = 300

AUTH_HINT = (
    "Authenticate for your environment:\n"
    "  - Local / Claude Code:   run `az login` (a browser opens), or `az login --use-device-code`.\n"
    "  - Genie Code / serverless / CI: set a service principal via env vars\n"
    "      AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / AZURE_TENANT_ID\n"
    "      (on Databricks, read them from a secret scope)."
)
_INSTALL_HINT = (
    "The `flowx profile` phase needs the Azure stack, which is an optional extra.\n"
    "Install it with:  pip install 'azure-identity>=1.17' 'requests>=2.31'   "
    "(from a source checkout: pip install -e '.[azure]')\n" + AUTH_HINT
)


class MissingProfileDependencyError(RuntimeError):
    """Raised when the profile phase runs but the `azure` extra isn't installed."""


class ProfileAuthenticationError(RuntimeError):
    """Raised when no Azure credential works; the message says how to set one up."""

    def __init__(self, detail: str):
        super().__init__(f"Azure authentication failed: {detail}\n{AUTH_HINT}")


def require_azure() -> None:
    """Verify every module of the `azure` extra is importable; otherwise raise with setup guidance."""
    try:
        import azure.identity  # noqa: F401
        import requests  # noqa: F401
    except ModuleNotFoundError as error:
        raise MissingProfileDependencyError(_INSTALL_HINT) from error


def get_credential() -> Any:
    """Return a DefaultAzureCredential.

    It walks env-var service principal (AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET)
    -> managed identity -> `az` CLI token -> shared cache, so the same code works whether the user
    ran `az login` locally or set env vars on serverless. Any tenant is allowed so `--tenant-id`
    can pick one per token request.
    """
    require_azure()
    from azure.identity import DefaultAzureCredential

    return DefaultAzureCredential(additionally_allowed_tenants=["*"])


class TransportHttpError(Exception):
    """A non-200 answer from Azure. `status` lets callers turn 401/403 into "missing role" notes."""

    def __init__(self, status: int, url: str):
        super().__init__(f"HTTP {status} from {url}")
        self.status = status
        self.url = url


def is_https_on(url: str, host: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme == "https" and parsed.netloc.lower() == host


class UntrustedUrlError(Exception):
    def __init__(self, url: str):
        super().__init__(f"Refusing to send the Azure token to {url}: only https://{_MANAGEMENT_HOST} is allowed")
        self.url = url


class Transport(Protocol):
    """The only way the profiler talks to Azure, so tests can replay recorded responses."""

    def get_json(self, url: str) -> dict[str, Any]: ...

    def post_json(self, url: str, body: dict[str, Any]) -> dict[str, Any]: ...


class LiveTransport:
    """Calls Azure Resource Manager over HTTPS with a bearer token from `credential`.

    Tokens are requested from `tenant_id` when one is given. A long scan can outlive one token
    (they last about an hour), so the token is refreshed shortly before it expires, and a 401 is
    retried once with a fresh one. A credential that can't produce a token raises
    ProfileAuthenticationError with setup guidance instead of a raw SDK traceback.
    """

    def __init__(self, credential: Any, *, tenant_id: str | None = None, session: Any = None):
        self._credential = credential
        self._tenant_id = tenant_id
        self._session = session
        self._headers: dict[str, str] | None = None
        self._token_expires_on = 0.0

    def _authorized_headers(self) -> dict[str, str]:
        if self._headers is None or time.time() >= self._token_expires_on - _TOKEN_REFRESH_MARGIN_SECONDS:
            from azure.core.exceptions import ClientAuthenticationError

            try:
                if self._tenant_id:
                    access_token = self._credential.get_token(_MANAGEMENT_SCOPE, tenant_id=self._tenant_id)
                else:
                    access_token = self._credential.get_token(_MANAGEMENT_SCOPE)
            except ClientAuthenticationError as error:
                raise ProfileAuthenticationError(str(error)) from error
            self._token_expires_on = float(getattr(access_token, "expires_on", float("inf")))
            self._headers = {"Authorization": f"Bearer {access_token.token}", "Content-Type": "application/json"}
        return self._headers

    def _http(self) -> Any:
        if self._session is None:
            import requests

            self._session = requests.Session()
        return self._session

    def _send(self, method: str, url: str, body: dict[str, Any] | None) -> dict[str, Any]:
        if not is_https_on(url, _MANAGEMENT_HOST):
            raise UntrustedUrlError(url)
        for attempt in range(2):
            headers = self._authorized_headers()
            if method == "GET":
                response = self._http().get(
                    url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=False
                )
            else:
                response = self._http().post(
                    url, headers=headers, json=body, timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=False
                )
            if response.status_code == 401 and attempt == 0:
                self._headers = None
                continue
            if response.status_code != 200:
                raise TransportHttpError(response.status_code, url)
            payload: dict[str, Any] = response.json()
            return payload
        raise AssertionError("unreachable")

    def get_json(self, url: str) -> dict[str, Any]:
        return self._send("GET", url, None)

    def post_json(self, url: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._send("POST", url, body)
