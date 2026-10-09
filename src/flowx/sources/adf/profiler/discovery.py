"""Live ADF enumeration for the profiler: subscriptions, factories, and integration runtimes.

Ported from the ADF branch of the standalone script's ResourceDiscovery (Synapse and Fabric
removed). The original went through the Azure SDK clients; this goes through the injectable
Transport against the same Resource Manager endpoints, so a recorded scan can be replayed.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from flowx.sources.adf.profiler.azure_client import AZURE_MANAGEMENT_URL, ProfileAuthenticationError, Transport

logger = logging.getLogger(__name__)

_SUBSCRIPTIONS_API_VERSION = "2022-12-01"
_DATA_FACTORY_API_VERSION = "2018-06-01"
# Activity runs name their IR with the region appended, e.g. "managedvnetir (West US)".
_REGION_SUFFIX = re.compile(r"\s+\([^()]*\)$")


def _list_all(transport: Transport, url: str) -> list[dict[str, Any]]:
    """GET a Resource Manager list, following `nextLink` pages until the last one."""
    items: list[dict[str, Any]] = []
    next_url: str | None = url
    while next_url:
        page = transport.get_json(next_url)
        items.extend(page.get("value", []))
        next_url = page.get("nextLink")
    return items


def list_subscriptions(transport: Transport, subscription_id: str | None = None) -> list[dict[str, str]]:
    """Subscriptions the credential can see, optionally narrowed to one ID."""
    subscriptions = []
    for item in _list_all(transport, f"{AZURE_MANAGEMENT_URL}/subscriptions?api-version={_SUBSCRIPTIONS_API_VERSION}"):
        if subscription_id and item.get("subscriptionId") != subscription_id:
            continue
        subscriptions.append(
            {"subscription_id": item.get("subscriptionId", ""), "display_name": item.get("displayName", "")}
        )
    return subscriptions


def _factory_to_dict(factory: dict[str, Any], subscription_id: str) -> dict[str, Any]:
    parts = factory.get("id", "").split("/")
    resource_group = parts[parts.index("resourceGroups") + 1] if "resourceGroups" in parts else "unknown"
    return {
        "id": factory.get("id", ""),
        "name": factory.get("name", ""),
        "location": factory.get("location", ""),
        "resource_group": resource_group,
        "subscription_id": subscription_id,
    }


def list_adf_factories(
    transport: Transport,
    *,
    subscription_id: str,
    resource_group: str | None = None,
    factory_name: str | None = None,
) -> list[dict[str, Any]]:
    """ADF factories in one subscription.

    Matches the original's filtering exactly: a factory name narrows the result only when a
    resource group is given too; on its own it is ignored and the whole subscription is listed.
    Any listing failure other than authentication is logged and yields no factories for that subscription.
    """
    subscription_url = f"{AZURE_MANAGEMENT_URL}/subscriptions/{subscription_id}"
    provider = "providers/Microsoft.DataFactory/factories"
    version = f"api-version={_DATA_FACTORY_API_VERSION}"
    try:
        if resource_group and factory_name:
            url = f"{subscription_url}/resourceGroups/{resource_group}/{provider}/{factory_name}?{version}"
            return [_factory_to_dict(transport.get_json(url), subscription_id)]
        if resource_group:
            url = f"{subscription_url}/resourceGroups/{resource_group}/{provider}?{version}"
        else:
            url = f"{subscription_url}/{provider}?{version}"
        return [_factory_to_dict(item, subscription_id) for item in _list_all(transport, url)]
    except ProfileAuthenticationError:
        raise
    except Exception as error:
        logger.error("Error discovering ADF factories in %s: %s", subscription_id, error)
        return []


def factory_base_url(factory: dict[str, Any]) -> str:
    """The Resource Manager URL of one factory, the prefix for all its run and IR calls."""
    return (
        f"{AZURE_MANAGEMENT_URL}/subscriptions/{factory['subscription_id']}"
        f"/resourceGroups/{factory['resource_group']}"
        f"/providers/Microsoft.DataFactory/factories/{factory['name']}"
    )


def integration_runtime_types(transport: Transport, factory: dict[str, Any]) -> dict[str, str]:
    """Map each integration runtime in `factory` to its kind: `SelfHosted`, `ManagedVNet`, or `Managed`.

    ADF gives every Azure IR the type `Managed`, whether or not it runs in a managed virtual
    network, so an IR counts as `ManagedVNet` only when its properties reference one.
    """
    url = f"{factory_base_url(factory)}/integrationRuntimes?api-version={_DATA_FACTORY_API_VERSION}"
    try:
        runtimes = _list_all(transport, url)
    except ProfileAuthenticationError:
        raise
    except Exception as error:
        logger.warning("Cannot list integration runtimes for %s: %s", factory.get("name"), error)
        return {}
    return {runtime["name"]: _runtime_kind(runtime.get("properties", {})) for runtime in runtimes}


def _runtime_kind(properties: dict[str, Any]) -> str:
    runtime_type = properties.get("type", "Unknown")
    if runtime_type == "Managed" and properties.get("managedVirtualNetwork"):
        return "ManagedVNet"
    return str(runtime_type)


def classify_ir(ir_name: str, ir_type_map: dict[str, str]) -> str:
    """Which rate card an activity run bills against, from the IR that actually ran it.

    `ir_name` is the run's `effectiveIntegrationRuntime` (region suffix and all); `ir_type_map`
    comes from `integration_runtime_types`. A known IR is classified by its kind, so an AutoResolve
    IR inside a managed virtual network gets the VNet rate; anything unknown falls back to Azure IR.
    """
    if not ir_name:
        return "azure_ir"
    ir_type = ir_type_map.get(_REGION_SUFFIX.sub("", ir_name), "")
    if ir_type == "SelfHosted":
        return "shir"
    if ir_type == "ManagedVNet":
        return "managed_vnet_ir"
    return "azure_ir"
