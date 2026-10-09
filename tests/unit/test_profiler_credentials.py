"""Credential selection and the setup-guidance errors, without installing azure."""

from __future__ import annotations

import builtins
import sys
import types

import pytest

from flowx.sources.adf.profiler import azure_client


def test_require_azure_raises_actionable_error_when_missing(monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("azure"):
            raise ModuleNotFoundError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(azure_client.MissingProfileDependencyError) as exc:
        azure_client.require_azure()
    message = str(exc.value)
    assert "profile" in message
    assert "az login" in message
    assert "service principal" in message.lower()


def test_authentication_error_carries_setup_guidance():
    message = str(azure_client.ProfileAuthenticationError("DefaultAzureCredential failed"))
    assert "DefaultAzureCredential failed" in message
    assert "az login" in message
    assert "service principal" in message.lower()


@pytest.fixture
def fake_azure(monkeypatch):
    """Install a stand-in azure.identity so credential selection is testable."""

    class ClientSecretCredential:
        def __init__(self, *, tenant_id, client_id, client_secret):
            self.tenant_id = tenant_id

    class DefaultAzureCredential:
        pass

    identity = types.ModuleType("azure.identity")
    identity.ClientSecretCredential = ClientSecretCredential  # type: ignore[attr-defined]
    identity.DefaultAzureCredential = DefaultAzureCredential  # type: ignore[attr-defined]
    for name, module in {
        "azure": types.ModuleType("azure"),
        "azure.identity": identity,
        "azure.mgmt": types.ModuleType("azure.mgmt"),
        "azure.mgmt.resource": types.ModuleType("azure.mgmt.resource"),
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    return identity


def test_explicit_service_principal_wins(fake_azure):
    credential = azure_client.get_credential(tenant_id="t", client_id="c", client_secret="s")
    assert isinstance(credential, fake_azure.ClientSecretCredential)
    assert credential.tenant_id == "t"


def test_partial_service_principal_falls_back_to_default_chain(fake_azure):
    credential = azure_client.get_credential(tenant_id="t", client_id="c")
    assert isinstance(credential, fake_azure.DefaultAzureCredential)


def test_only_azure_identity_is_required(monkeypatch):
    identity = types.ModuleType("azure.identity")
    identity.DefaultAzureCredential = type("DefaultAzureCredential", (), {})  # type: ignore[attr-defined]
    identity.ClientSecretCredential = type("ClientSecretCredential", (), {})  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "azure", types.ModuleType("azure"))
    monkeypatch.setitem(sys.modules, "azure.identity", identity)
    monkeypatch.delitem(sys.modules, "azure.mgmt", raising=False)
    monkeypatch.delitem(sys.modules, "azure.mgmt.resource", raising=False)
    assert isinstance(azure_client.get_credential(), identity.DefaultAzureCredential)
