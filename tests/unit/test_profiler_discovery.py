"""Live ADF discovery, exercised against hand-authored ARM responses."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from flowx.sources.adf.profiler import discovery

from .profiler_replay import ReplayTransport

FIXTURES = Path(__file__).parents[1] / "resources" / "azure" / "handauthored"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


@pytest.mark.parametrize(
    ("ir_name", "ir_type_map", "expected"),
    [
        ("", {}, "azure_ir"),
        ("AutoResolveIntegrationRuntime", {"AutoResolveIntegrationRuntime": "Managed"}, "azure_ir"),
        ("DefaultIntegrationRuntime", {}, "azure_ir"),
        ("shir-1", {"shir-1": "SelfHosted"}, "shir"),
        # Every Azure IR has type "Managed"; only a managed virtual network makes it the VNet rate card.
        ("plain-azure-ir", {"plain-azure-ir": "Managed"}, "azure_ir"),
        ("vnet-ir", {"vnet-ir": "ManagedVNet"}, "managed_vnet_ir"),
        # ADF reports the effective IR with a " (Region)" suffix; the lookup must still find it.
        ("managedvnetir (West US)", {"managedvnetir": "ManagedVNet"}, "managed_vnet_ir"),
        ("shir-1 (East US)", {"shir-1": "SelfHosted"}, "shir"),
        # An AutoResolve IR inside a managed virtual network bills at the VNet rate too.
        (
            "AutoResolveIntegrationRuntime (West US)",
            {"AutoResolveIntegrationRuntime": "ManagedVNet"},
            "managed_vnet_ir",
        ),
        ("mystery", {}, "azure_ir"),
    ],
)
def test_classify_ir(ir_name, ir_type_map, expected):
    assert discovery.classify_ir(ir_name, ir_type_map) == expected


def test_list_subscriptions_honors_filter():
    transport = ReplayTransport({"/subscriptions?": _load("subscriptions.json")})
    subscriptions = discovery.list_subscriptions(transport, subscription_id="SUB")
    assert subscriptions == [{"subscription_id": "SUB", "display_name": "Demo Subscription"}]


def test_list_adf_factories_lists_whole_subscription():
    transport = ReplayTransport({"/providers/Microsoft.DataFactory/factories?": _load("factories.json")})
    factories = discovery.list_adf_factories(transport, subscription_id="SUB")
    assert [factory["name"] for factory in factories] == ["demo-factory", "second-factory"]
    assert factories[0] == {
        "id": "/subscriptions/SUB/resourceGroups/RG/providers/Microsoft.DataFactory/factories/demo-factory",
        "name": "demo-factory",
        "location": "East US",
        "resource_group": "RG",
        "subscription_id": "SUB",
    }


def test_factory_name_without_resource_group_does_not_filter():
    # Ported as-is: the original only narrows to one factory when a resource group is also given.
    transport = ReplayTransport({"/providers/Microsoft.DataFactory/factories?": _load("factories.json")})
    factories = discovery.list_adf_factories(transport, subscription_id="SUB", factory_name="demo-factory")
    assert len(factories) == 2


def test_resource_group_and_factory_name_fetch_one_factory():
    transport = ReplayTransport(
        {"/resourceGroups/RG/providers/Microsoft.DataFactory/factories/demo-factory?": _load("factory_single.json")}
    )
    factories = discovery.list_adf_factories(
        transport, subscription_id="SUB", resource_group="RG", factory_name="demo-factory"
    )
    assert [factory["name"] for factory in factories] == ["demo-factory"]


def test_factory_listing_failure_yields_no_factories():
    transport = ReplayTransport({"/providers/Microsoft.DataFactory/factories?": 403})
    assert discovery.list_adf_factories(transport, subscription_id="SUB") == []


def test_list_follows_next_link():
    first_page = {"value": [{"subscriptionId": "A", "displayName": "a"}], "nextLink": "https://next/page2"}
    second_page = {"value": [{"subscriptionId": "B", "displayName": "b"}]}
    transport = ReplayTransport({"/subscriptions?": first_page, "https://next/page2": second_page})
    assert [sub["subscription_id"] for sub in discovery.list_subscriptions(transport)] == ["A", "B"]


def test_integration_runtime_types_by_name():
    transport = ReplayTransport({"/integrationRuntimes?": _load("integration_runtimes.json")})
    factory = discovery.list_adf_factories(
        ReplayTransport({"/factories?": _load("factories.json")}), subscription_id="SUB"
    )[0]
    assert discovery.integration_runtime_types(transport, factory) == {
        "AutoResolveIntegrationRuntime": "Managed",
        "shir-1": "SelfHosted",
    }
    method, url, _ = transport.calls[0]
    assert url.startswith(discovery.factory_base_url(factory))


class ExplodingTransport:
    def __init__(self, error: Exception):
        self._error = error

    def get_json(self, url):
        raise self._error

    def post_json(self, url, body):
        raise self._error


def test_factory_listing_network_error_yields_no_factories():
    assert discovery.list_adf_factories(ExplodingTransport(TimeoutError("slow")), subscription_id="SUB") == []


def test_integration_runtime_network_error_yields_empty_map():
    factory = {"name": "f", "subscription_id": "SUB", "resource_group": "RG"}
    assert discovery.integration_runtime_types(ExplodingTransport(TimeoutError("slow")), factory) == {}


def test_managed_virtual_network_marks_the_ir_as_managed_vnet():
    transport = ReplayTransport({"/integrationRuntimes?": _load("integration_runtimes_managed_vnet.json")})
    factory = {"name": "f", "subscription_id": "SUB", "resource_group": "RG"}
    assert discovery.integration_runtime_types(transport, factory) == {
        "managedvnetir": "ManagedVNet",
        "plain-azure-ir": "Managed",
    }
