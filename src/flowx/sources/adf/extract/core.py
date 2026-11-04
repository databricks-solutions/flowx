"""Pull one Azure Data Factory's definitions from Azure Resource Manager and write them to disk.

This module is the whole extractor. It only depends on the standard library at import time;
``requests`` and ``azure-identity`` are imported when a live transport or credential is built.
That keeps it cheap to import and lets ``notebook`` inline this exact file into the generated
notebook, so the CLI and the notebook can never drift apart. Do not import anything from
``flowx`` here.

Each resource type is fetched with one paginated *list* call. List responses already carry the
full definition (a pipeline's activities, a dataset's properties, ...), so a factory with
thousands of pipelines costs tens of requests rather than one per object, which keeps the run
well inside Azure Resource Manager's read throttling.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import re
import shutil
import tempfile
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

EXTRACTOR_VERSION = "1"
MANIFEST_NAME = "_manifest.json"
ARM_BASE_URL = "https://management.azure.com"
ARM_SCOPE = "https://management.azure.com/.default"
API_VERSION = "2018-06-01"

_REQUEST_TIMEOUT_SECONDS = 60
_TOKEN_REFRESH_MARGIN_SECONDS = 300
_RETRYABLE_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
_MAX_BACKOFF_SECONDS = 60.0

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class ResourceType:
    """One kind of factory child resource: the ARM collection to list and the folder to write.

    ``required`` types abort the run when they can't be listed; optional ones (newer resource
    types some factories or roles can't read) are recorded as warnings and the export is still
    usable by discover.

    Attributes:
        collection: ARM collection name under the factory, for example ``linkedservices``.
        folder: Folder the definitions are written to, as in a factory's Git repository.
        required: Whether the run fails when this type can't be listed.
    """

    collection: str
    folder: str
    required: bool


RESOURCE_TYPES: tuple[ResourceType, ...] = (
    ResourceType(collection="pipelines", folder="pipeline", required=True),
    ResourceType(collection="datasets", folder="dataset", required=True),
    ResourceType(collection="linkedservices", folder="linkedService", required=True),
    ResourceType(collection="triggers", folder="trigger", required=True),
    ResourceType(collection="dataflows", folder="dataflow", required=False),
    ResourceType(collection="integrationRuntimes", folder="integrationRuntime", required=False),
    ResourceType(collection="credentials", folder="credential", required=False),
)
FACTORY_FOLDER = "factory"


class ExtractError(RuntimeError):
    """The extraction could not finish; the message says what to fix."""


class AzureAuthenticationError(ExtractError):
    """No usable Azure credential, or the identity lacks access to the factory."""


class SetupError(ExtractError):
    """Something must be installed or configured before the extraction can start."""


class HttpError(ExtractError):
    """An answer from Azure Resource Manager the extractor can't use, after any retries.

    Attributes:
        status: HTTP status code of the answer.
        url: The URL that was requested.
    """

    def __init__(self, status: int, url: str, detail: str = ""):
        message = f"HTTP {status} from {url}"
        if detail:
            message = f"{message}: {detail}"
        super().__init__(message)
        self.status = status
        self.url = url


class Transport(Protocol):
    """How the extractor reads from Azure, so tests can replay recorded responses."""

    def get_json(self, url: str) -> dict[str, Any]: ...


AUTH_HINT = (
    "Sign in for your environment:\n"
    "  - Local / Claude Code: `az login --tenant <tenant-id>` (or add --use-device-code).\n"
    "  - Databricks notebook / Genie Code: device-code sign-in.\n"
    "If your organisation blocks device-code sign-in (for example AADSTS53003, Conditional Access),\n"
    "use the factory's Git repository or an ADF Studio ARM export instead, or an identity an admin\n"
    "sets up: a service principal (AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / AZURE_TENANT_ID, from a\n"
    "secret scope on Databricks) or a Unity Catalog service credential.\n"
    "The identity needs Reader on the factory."
)

AUTH_MODES = ("default", "device-code", "service-credential")
# Same pins as the `azure` extra; named directly because this module also runs inlined in a notebook.
_INSTALL_HINT = "Install them with: pip install 'azure-identity>=1.17' 'requests>=2.31'"


def _service_credential(name: str | None) -> Any:
    if not name:
        raise SetupError("The service-credential sign-in needs the name of a Unity Catalog service credential.")
    try:
        from databricks.sdk.runtime import dbutils
    except ImportError as error:
        raise SetupError(
            "Unity Catalog service credentials only work inside a Databricks notebook or job.\n" + AUTH_HINT
        ) from error
    # The SDK's dbutils stubs don't declare the service-credential API, so treat it as untyped.
    runtime_dbutils: Any = dbutils
    try:
        return runtime_dbutils.credentials.getServiceCredentialsProvider(name)
    except Exception as error:  # noqa: BLE001 -- dbutils wraps JVM errors in several types
        raise AzureAuthenticationError(
            f"Could not use service credential {name!r}: {error}\n"
            "Check that it exists, that you have ACCESS on it, and that it is bound to this workspace. "
            "Service credentials only work in the notebook's own Python process: call "
            "flowx.sources.adf.extract.runner.main([...]) from a notebook cell rather than running "
            "`python -m flowx.adapter` as a separate process."
        ) from error


def build_credential(
    *, tenant_id: str | None = None, auth_mode: str = "default", service_credential: str | None = None
) -> Any:
    """Returns an Azure credential for the requested sign-in mode.

    ``default`` uses ``DefaultAzureCredential``: service-principal env vars, managed identity, or
    an ``az login`` session, in that order. ``device-code`` prints a URL and code to sign in from
    any browser. ``service-credential`` uses a Unity Catalog service credential (an access
    connector's managed identity), so a Databricks notebook or job runs with no sign-in and no
    secret.

    Args:
        tenant_id: Tenant to sign in to, for ``device-code``.
        auth_mode: One of ``AUTH_MODES``.
        service_credential: Unity Catalog service credential name, for ``service-credential``.

    Returns:
        An object with ``get_token(scope, ...)``, as azure-identity credentials have.

    Raises:
        SetupError: The Azure packages are missing, the mode is unknown, or a service credential
            is requested outside Databricks.
        AzureAuthenticationError: The service credential can't be used.

    Notes:
        Secrets are never accepted as arguments; a service principal must come from the environment
        (for example a secret scope).
    """
    if auth_mode == "service-credential":
        return _service_credential(service_credential)
    try:
        from azure.identity import DefaultAzureCredential, DeviceCodeCredential
    except ModuleNotFoundError as error:
        raise SetupError(
            f"The extract phase needs the optional Azure packages.\n{_INSTALL_HINT}\n" + AUTH_HINT
        ) from error

    if auth_mode == "device-code":
        return DeviceCodeCredential(tenant_id=tenant_id) if tenant_id else DeviceCodeCredential()
    if auth_mode != "default":
        raise SetupError(f"Unknown auth mode {auth_mode!r}; use one of: {', '.join(AUTH_MODES)}.")
    return DefaultAzureCredential(additionally_allowed_tenants=["*"])


class ArmTransport:
    """Reads from Azure Resource Manager with a bearer token, retrying throttled requests.

    A 401 refreshes the token once. 408, 429 and 5xx answers are retried with exponential backoff
    and jitter, honouring ``Retry-After`` when Azure sends it, up to ``max_attempts`` tries. The
    remaining read budget Azure reports is kept in ``last_remaining_reads`` for the summary.

    Attributes:
        request_count: Requests sent, including retries.
        retry_count: Requests that were retried after throttling, a server error or a connection error.
        last_remaining_reads: Azure's last ``x-ms-ratelimit-remaining-subscription-reads`` value.
    """

    def __init__(
        self,
        credential: Any,
        *,
        tenant_id: str | None = None,
        session: Any = None,
        max_attempts: int = 6,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._credential = credential
        self._tenant_id = tenant_id
        self._session = session
        self._max_attempts = max_attempts
        self._sleep = sleep
        self._authorization: str | None = None
        self._token_expires_on = 0.0
        self.request_count = 0
        self.retry_count = 0
        self.last_remaining_reads: str | None = None

    def _bearer(self) -> str:
        if self._authorization is None or time.time() >= self._token_expires_on - _TOKEN_REFRESH_MARGIN_SECONDS:
            try:
                if self._tenant_id:
                    token = self._credential.get_token(ARM_SCOPE, tenant_id=self._tenant_id)
                else:
                    token = self._credential.get_token(ARM_SCOPE)
            except Exception as error:  # noqa: BLE001 -- azure-identity raises several unrelated types
                raise AzureAuthenticationError(f"Azure sign-in failed: {error}\n{AUTH_HINT}") from error
            # A token without an expiry is kept until Azure answers 401, which triggers a refresh.
            self._token_expires_on = float(getattr(token, "expires_on", 0) or "inf")
            self._authorization = f"Bearer {token.token}"
        return self._authorization

    def _http(self) -> Any:
        if self._session is None:
            try:
                import requests
            except ModuleNotFoundError as error:
                raise SetupError(f"The extract phase needs the optional Azure packages.\n{_INSTALL_HINT}") from error
            self._session = requests.Session()
        return self._session

    def _wait_before_retry(self, attempt: int, retry_after: str | None) -> None:
        delay: float | None = None
        if retry_after:
            try:
                delay = float(retry_after)
            except ValueError:
                delay = None
        if delay is None:
            delay = min(_MAX_BACKOFF_SECONDS, 2.0**attempt) + random.uniform(0, 1)
        self.retry_count += 1
        self._sleep(min(delay, _MAX_BACKOFF_SECONDS))

    def get_json(self, url: str) -> dict[str, Any]:
        refreshed_after_401 = False
        attempt = 0
        while True:
            attempt += 1
            self.request_count += 1
            session = self._http()
            try:
                response = session.get(
                    url,
                    headers={"Authorization": self._bearer(), "Accept": "application/json"},
                    timeout=_REQUEST_TIMEOUT_SECONDS,
                )
            except AzureAuthenticationError:
                raise
            except Exception as error:  # noqa: BLE001 -- connection resets and timeouts are transient
                if attempt >= self._max_attempts:
                    raise ExtractError(f"Could not reach {url}: {error}") from error
                self._wait_before_retry(attempt, None)
                continue

            headers = getattr(response, "headers", {}) or {}
            remaining = headers.get("x-ms-ratelimit-remaining-subscription-reads")
            if remaining is not None:
                self.last_remaining_reads = str(remaining)

            status = response.status_code
            if status == 200:
                return _json_body(response, url)
            if status == 401 and not refreshed_after_401:
                refreshed_after_401 = True
                self._authorization = None
                continue
            if status in _RETRYABLE_STATUSES and attempt < self._max_attempts:
                self._wait_before_retry(attempt, headers.get("Retry-After"))
                continue
            raise HttpError(status, url, _error_detail(response))


def _json_body(response: Any, url: str) -> dict[str, Any]:
    """Returns a successful response's JSON object, or raises ``HttpError`` when it isn't one."""
    try:
        payload = response.json()
    except ValueError as error:
        raise HttpError(response.status_code, url, "the response was not JSON") from error
    if not isinstance(payload, dict):
        raise HttpError(response.status_code, url, "the response was not a JSON object")
    return payload


def _error_detail(response: Any) -> str:
    try:
        body = response.json()
    except Exception:  # noqa: BLE001 -- the body may be empty or not JSON
        return ""
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        return f"{error.get('code', '')} {error.get('message', '')}".strip()
    return ""


def factory_url(subscription_id: str, resource_group: str, factory_name: str) -> str:
    """Returns the ARM URL of the factory resource (without an api-version).

    Args:
        subscription_id: Azure subscription ID of the factory.
        resource_group: Resource group of the factory.
        factory_name: Data Factory name.

    Returns:
        ``https://management.azure.com/subscriptions/.../factories/<factory_name>``.
    """
    return (
        f"{ARM_BASE_URL}/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
        f"/providers/Microsoft.DataFactory/factories/{factory_name}"
    )


def list_collection(transport: Transport, url: str) -> list[dict[str, Any]]:
    """Lists every item of an ARM collection, following ``nextLink`` until the last page.

    Args:
        transport: How to read from Azure Resource Manager.
        url: Collection URL without an api-version, for example ``<factory_url>/pipelines``.

    Returns:
        Every item of every page, in the order Azure returned them.

    Raises:
        HttpError: A page could not be read, or Azure returned a ``nextLink`` it had already returned.
    """
    items: list[dict[str, Any]] = []
    next_url: str | None = f"{url}?api-version={API_VERSION}"
    visited: set[str] = set()
    while next_url:
        if next_url in visited:
            raise HttpError(200, next_url, "Azure returned the same nextLink twice")
        visited.add(next_url)
        page = transport.get_json(next_url)
        items.extend(item for item in page.get("value", []) if isinstance(item, dict))
        next_url = page.get("nextLink")
    return items


@dataclass(slots=True, kw_only=True)
class FactoryExport:
    """Everything read from one factory, before it is written to disk.

    Attributes:
        subscription_id: Azure subscription ID of the factory.
        resource_group: Resource group of the factory.
        factory_name: Data Factory name.
        factory: The factory resource itself (location, global parameters, Git configuration).
        resources: Definitions per output folder (``pipeline``, ``dataset``, ...).
        warnings: Optional resource types that couldn't be listed.
    """

    subscription_id: str
    resource_group: str
    factory_name: str
    factory: dict[str, Any]
    resources: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def extract_factory(
    transport: Transport,
    *,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    progress: Callable[[str], None] | None = None,
) -> FactoryExport:
    """Reads the factory and all of its child resources.

    Args:
        transport: How to read from Azure Resource Manager.
        subscription_id: Azure subscription ID of the factory.
        resource_group: Resource group of the factory.
        factory_name: Data Factory name.
        progress: Called with a short message after each resource type is listed.

    Returns:
        The factory and its resources, with a warning for each optional type that couldn't be listed.

    Raises:
        AzureAuthenticationError: The identity can't read the factory or a required resource type.
        ExtractError: The factory doesn't exist, or a required resource type can't be listed.
    """
    base_url = factory_url(subscription_id, resource_group, factory_name)
    try:
        factory = transport.get_json(f"{base_url}?api-version={API_VERSION}")
    except HttpError as error:
        if error.status in (401, 403):
            raise AzureAuthenticationError(
                f"The signed-in identity cannot read factory {factory_name!r} ({error}).\n{AUTH_HINT}"
            ) from error
        if error.status == 404:
            raise ExtractError(
                f"Factory {factory_name!r} was not found in resource group {resource_group!r} of subscription "
                f"{subscription_id} ({error}). Check the names (ADF Studio > Manage > Properties) and the tenant."
            ) from error
        raise

    export = FactoryExport(
        subscription_id=subscription_id,
        resource_group=resource_group,
        factory_name=factory_name,
        factory=factory,
    )
    for resource_type in RESOURCE_TYPES:
        try:
            items = list_collection(transport, f"{base_url}/{resource_type.collection}")
        except AzureAuthenticationError:
            raise
        except ExtractError as error:
            if resource_type.required and isinstance(error, HttpError) and error.status in (401, 403):
                raise AzureAuthenticationError(
                    f"The signed-in identity cannot list {resource_type.collection} of factory {factory_name!r} "
                    f"({error}).\n{AUTH_HINT}"
                ) from error
            if resource_type.required:
                raise ExtractError(f"Could not list {resource_type.collection}: {error}") from error
            export.warnings.append(f"Skipped {resource_type.collection}: {error}")
            continue
        export.resources[resource_type.folder] = items
        if progress is not None:
            progress(f"{resource_type.collection}: {len(items)}")
    return export


_UNSAFE_FILENAME_CHARACTERS = re.compile(r"[^A-Za-z0-9._-]+")


def assign_filenames(names: list[str]) -> dict[str, str]:
    """Maps each resource name to a unique, filesystem-safe ``.json`` filename.

    ADF names can contain characters files can't, and two names can clean up to the same string
    or differ only by case (which collides on macOS and Windows). Colliding names get a short
    stable hash of the original name, so nothing is silently overwritten.

    Args:
        names: Resource names, as ADF returns them.

    Returns:
        Each name mapped to its filename.
    """
    cleaned = {name: _UNSAFE_FILENAME_CHARACTERS.sub("_", name).strip("_") or "unnamed" for name in names}
    claimed: dict[str, int] = {}
    for stem in cleaned.values():
        claimed[stem.lower()] = claimed.get(stem.lower(), 0) + 1

    filenames: dict[str, str] = {}
    for name, stem in cleaned.items():
        if claimed[stem.lower()] > 1:
            stem = f"{stem}__{hashlib.sha1(name.encode('utf-8')).hexdigest()[:8]}"
        filenames[name] = f"{stem}.json"
    return filenames


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def build_manifest(export: FactoryExport, filenames: dict[str, dict[str, str]], factory_file: str) -> dict[str, Any]:
    """Describes the export: where it came from, what it contains, and whether it is complete.

    Args:
        export: What was read from the factory.
        filenames: For each folder, each resource name mapped to its filename.
        factory_file: Filename of the factory definition in ``factory/``.

    Returns:
        The content of ``_manifest.json``.
    """
    return {
        "extractor": "flowx.sources.adf.extract",
        "extractor_version": EXTRACTOR_VERSION,
        "api_version": API_VERSION,
        "extracted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "factory": {
            "name": export.factory_name,
            "resource_group": export.resource_group,
            "subscription_id": export.subscription_id,
            "location": export.factory.get("location"),
        },
        "complete": not export.warnings,
        "warnings": list(export.warnings),
        "counts": {folder: len(items) for folder, items in export.resources.items()},
        "files": {FACTORY_FOLDER: {export.factory_name: factory_file}, **filenames},
    }


def write_export(export: FactoryExport, output_dir: str | Path, *, overwrite: bool = False) -> dict[str, Any]:
    """Writes the native ADF Git layout plus ``_manifest.json`` and returns the manifest.

    Files are written to a temporary folder next to ``output_dir`` and moved into place only when
    everything was written, so an interrupted run never leaves a half-written export that looks
    complete. An existing, non-empty ``output_dir`` is only replaced when ``overwrite`` is set,
    and it is kept until the new export is in place.

    Args:
        export: What was read from the factory.
        output_dir: Folder to write; created if needed.
        overwrite: Replace an existing, non-empty ``output_dir``.

    Returns:
        The manifest written to ``_manifest.json``.

    Raises:
        ExtractError: ``output_dir`` isn't empty and ``overwrite`` isn't set.
    """
    destination = Path(output_dir)
    if destination.exists() and any(destination.iterdir()) and not overwrite:
        raise ExtractError(f"{destination} already exists and is not empty; pass overwrite to replace it.")
    destination.parent.mkdir(parents=True, exist_ok=True)

    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        factory_file = assign_filenames([export.factory_name])[export.factory_name]
        _write_json(staging / FACTORY_FOLDER / factory_file, export.factory)

        all_filenames: dict[str, dict[str, str]] = {}
        for folder, items in export.resources.items():
            names = [str(item.get("name", "")) for item in items]
            filenames = assign_filenames(names)
            all_filenames[folder] = filenames
            for item, name in zip(items, names):
                _write_json(staging / folder / filenames[name], item)

        manifest = build_manifest(export, all_filenames, factory_file)
        _write_json(staging / MANIFEST_NAME, manifest)
        _move_into_place(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return manifest


def _move_into_place(staging: Path, destination: Path) -> None:
    """Swaps the finished staging tree into ``destination``, keeping any old export until it succeeds."""
    previous: Path | None = None
    if destination.exists():
        # A unique name, so a folder flowx didn't create is never deleted.
        previous = destination.with_name(f".{destination.name}.previous-{uuid.uuid4().hex[:8]}")
        _move_tree(destination, previous)
    try:
        _move_tree(staging, destination)
    except BaseException:
        shutil.rmtree(destination, ignore_errors=True)
        if previous is not None:
            _move_tree(previous, destination)
        raise
    if previous is not None:
        try:
            shutil.rmtree(previous)
        except OSError:
            logger.warning("Couldn't remove export backup at %s", previous)


def _move_tree(source: Path, target: Path) -> None:
    try:
        os.replace(source, target)
    except OSError:
        # Some mounted filesystems (for example Unity Catalog volumes) don't support renaming
        # directories, so fall back to copying the tree.
        shutil.copytree(source, target)
        shutil.rmtree(source, ignore_errors=True)


def run_extraction(
    *,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    output_dir: str | Path,
    tenant_id: str | None = None,
    auth_mode: str = "default",
    service_credential: str | None = None,
    overwrite: bool = False,
    transport: Transport | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Signs in, extracts the factory and writes it to ``output_dir``.

    This is the single entry point the CLI and the generated notebook both call.

    Args:
        subscription_id: Azure subscription ID of the factory.
        resource_group: Resource group of the factory.
        factory_name: Data Factory name.
        output_dir: Folder to write the export to.
        tenant_id: Tenant to request tokens from (ignored for ``service-credential``).
        auth_mode: One of ``AUTH_MODES``.
        service_credential: Unity Catalog service credential name, for ``service-credential``.
        overwrite: Replace an existing, non-empty ``output_dir``.
        transport: Reads from Azure; tests pass a fake. Built from the credential when omitted.
        progress: Called with a short message after each resource type is listed.

    Returns:
        The manifest, plus a ``requests`` summary when the live transport was used.

    Raises:
        SetupError: A package is missing or the sign-in mode can't be used here.
        AzureAuthenticationError: Sign-in failed or the identity can't read the factory.
        ExtractError: Anything else that stopped the extraction.
    """
    if transport is None:
        credential = build_credential(tenant_id=tenant_id, auth_mode=auth_mode, service_credential=service_credential)
        # A service credential's identity already lives in the access connector's tenant.
        token_tenant = None if auth_mode == "service-credential" else tenant_id
        transport = ArmTransport(credential, tenant_id=token_tenant)
    export = extract_factory(
        transport,
        subscription_id=subscription_id,
        resource_group=resource_group,
        factory_name=factory_name,
        progress=progress,
    )
    manifest = write_export(export, output_dir, overwrite=overwrite)
    if isinstance(transport, ArmTransport):
        manifest["requests"] = {
            "made": transport.request_count,
            "retried": transport.retry_count,
            "remaining_subscription_reads": transport.last_remaining_reads,
        }
    return manifest
