"""Unit tests for workspace_downloader.py graceful failure paths."""

from __future__ import annotations

from unittest.mock import patch

from flowx.preparer.workspace_downloader import download_dbfs_file, download_notebook


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
