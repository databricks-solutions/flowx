"""Unit tests for workspace_downloader.py graceful failure paths."""

from __future__ import annotations

from unittest.mock import patch

from flowx.preparer import workspace_downloader
from flowx.preparer.workspace_downloader import (
    auth_available,
    download_dbfs_file,
    download_notebook,
    enable_workspace_downloads,
    prompt_for_auth_if_missing,
    workspace_downloads_enabled,
)


class TestDownloadNotebook:
    def test_returns_none_when_sdk_not_available(self):
        """download_notebook returns None when databricks-sdk is not installed."""
        with patch.dict("sys.modules", {"databricks": None, "databricks.sdk": None}):
            result = download_notebook("/Shared/flowx/transform")
            assert result is None

    def test_returns_none_on_import_error(self):
        """download_notebook returns None when the SDK import raises ImportError."""
        # Force an ImportError by making the module import fail

        original = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__

        def mock_import(name, *args, **kwargs):
            if "databricks.sdk" in name:
                raise ImportError("No module named 'databricks.sdk'")
            return original(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            result = download_notebook("/Shared/flowx/transform")
            assert result is None


class TestDownloadDbfsFile:
    def test_returns_none_when_sdk_not_available(self):
        """download_dbfs_file returns None when databricks-sdk is not installed."""
        with patch.dict("sys.modules", {"databricks": None, "databricks.sdk": None}):
            result = download_dbfs_file("dbfs:/scripts/etl.py")
            assert result is None

    def test_returns_none_on_import_error(self):
        """download_dbfs_file returns None when the SDK import raises ImportError."""

        original = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__

        def mock_import(name, *args, **kwargs):
            if "databricks.sdk" in name:
                raise ImportError("No module named 'databricks.sdk'")
            return original(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            result = download_dbfs_file("dbfs:/scripts/etl.py")
            assert result is None


class TestDownloadsToggle:
    def test_disabled_by_default(self, monkeypatch):
        """The module-level toggle defaults to False so library users keep current behavior."""
        monkeypatch.setattr(workspace_downloader, "_downloads_enabled", False)
        assert workspace_downloads_enabled() is False

    def test_enable_and_disable(self, monkeypatch):
        monkeypatch.setattr(workspace_downloader, "_downloads_enabled", False)
        enable_workspace_downloads(True)
        try:
            assert workspace_downloads_enabled() is True
            enable_workspace_downloads(False)
            assert workspace_downloads_enabled() is False
        finally:
            monkeypatch.setattr(workspace_downloader, "_downloads_enabled", False)


class TestAuthAvailable:
    def test_returns_true_when_env_profile_set(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        assert auth_available() is True

    def test_returns_true_when_host_and_token_set(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-abc")
        assert auth_available() is True

    def test_returns_true_when_oauth_m2m_env_set(self, monkeypatch):
        # The MCP path: flowx hosted as a Databricks App injects the service
        # principal's OAuth client id/secret (no PAT, no profile).
        monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "sp-client-id")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "sp-secret")
        monkeypatch.setattr(workspace_downloader, "_local_workspace_accessible", lambda: False)
        monkeypatch.setattr(workspace_downloader, "_list_profiles", lambda: [])
        assert auth_available() is True

    def test_returns_false_when_no_env_and_no_profiles(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        monkeypatch.setattr(workspace_downloader, "_local_workspace_accessible", lambda: False)
        monkeypatch.setattr(workspace_downloader, "_is_databricks_runtime", lambda: False)
        monkeypatch.setattr(workspace_downloader, "_list_profiles", lambda: [])
        assert auth_available() is False

    def test_returns_false_when_host_set_without_token_or_client_creds(self, monkeypatch):
        # HOST alone must not satisfy the check (incomplete credentials).
        monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
        monkeypatch.setattr(workspace_downloader, "_local_workspace_accessible", lambda: False)
        monkeypatch.setattr(workspace_downloader, "_is_databricks_runtime", lambda: False)
        monkeypatch.setattr(workspace_downloader, "_list_profiles", lambda: [])
        assert auth_available() is False


class TestPromptForAuthIfMissing:
    def test_no_prompt_when_auth_available(self, monkeypatch):
        monkeypatch.setattr(workspace_downloader, "auth_available", lambda: True)
        assert prompt_for_auth_if_missing(["/Shared/foo"]) is True

    def test_non_interactive_falls_back_to_placeholders(self, monkeypatch, capsys):
        monkeypatch.setattr(workspace_downloader, "auth_available", lambda: False)
        assert prompt_for_auth_if_missing(["/Shared/foo"], interactive=False) is True
        err = capsys.readouterr().err
        assert "databricks auth login" in err

    def test_interactive_user_aborts(self, monkeypatch):
        monkeypatch.setattr(workspace_downloader, "auth_available", lambda: False)
        monkeypatch.setattr("builtins.input", lambda _prompt: "n")
        assert prompt_for_auth_if_missing(["/Shared/foo"], interactive=True) is False

    def test_interactive_user_accepts(self, monkeypatch):
        monkeypatch.setattr(workspace_downloader, "auth_available", lambda: False)
        monkeypatch.setattr("builtins.input", lambda _prompt: "y")
        assert prompt_for_auth_if_missing(["/Shared/foo"], interactive=True) is True
