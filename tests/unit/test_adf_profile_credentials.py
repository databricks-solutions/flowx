"""Credential selection and the setup-guidance errors, without installing azure."""

from __future__ import annotations

import builtins
import sys
import types

import pytest

from flowx.sources.adf.profile import azure_client


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


def test_require_azure_also_checks_requests(monkeypatch):
    monkeypatch.setitem(sys.modules, "azure", types.ModuleType("azure"))
    monkeypatch.setitem(sys.modules, "azure.identity", types.ModuleType("azure.identity"))
    monkeypatch.setitem(sys.modules, "requests", None)
    with pytest.raises(azure_client.MissingProfileDependencyError) as exc:
        azure_client.require_azure()
    assert "requests" in str(exc.value)


def test_authentication_error_carries_setup_guidance():
    message = str(azure_client.ProfileAuthenticationError("DefaultAzureCredential failed"))
    assert "DefaultAzureCredential failed" in message
    assert "az login" in message
    assert "service principal" in message.lower()


def test_credential_is_the_default_chain_allowing_any_tenant(monkeypatch):
    class DefaultAzureCredential:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    identity = types.ModuleType("azure.identity")
    identity.DefaultAzureCredential = DefaultAzureCredential  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "azure", types.ModuleType("azure"))
    monkeypatch.setitem(sys.modules, "azure.identity", identity)
    credential = azure_client.get_credential()
    assert isinstance(credential, DefaultAzureCredential)
    assert credential.kwargs == {"additionally_allowed_tenants": ["*"]}
