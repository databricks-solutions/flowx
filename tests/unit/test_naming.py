"""Unit tests for the naming helpers used by activity preparers."""

from __future__ import annotations

import pytest

from flowx.preparer.activity_preparers.naming import (
    notebook_filename,
    to_snake_case,
    workspace_notebook_filename,
)


class TestToSnakeCase:
    @pytest.mark.parametrize(
        ("source", "expected"),
        [
            ("BronzeIngest", "bronze_ingest"),
            ("copySQLToBlob", "copy_sql_to_blob"),
            ("ETL_Main", "etl_main"),
            ("spaces and dashes-here", "spaces_and_dashes_here"),
        ],
    )
    def test_converts_to_snake(self, source: str, expected: str) -> None:
        assert to_snake_case(source) == expected


class TestNotebookFilename:
    def test_uses_activity_name_when_provided(self) -> None:
        assert notebook_filename("bronze_ingest", "BronzeIngest") == "bronze_ingest.py"

    def test_falls_back_to_task_key_when_no_activity_name(self) -> None:
        assert notebook_filename("bronze_ingest", None) == "bronze_ingest.py"

    def test_default_when_both_inputs_empty(self) -> None:
        assert notebook_filename("", None) == "notebook.py"


class TestWorkspaceNotebookFilename:
    def test_preserves_basename_verbatim(self) -> None:
        """Underscores, digits, and case are preserved exactly — no snake casing."""
        assert workspace_notebook_filename("/Shared/test_notebook_001") == "test_notebook_001.py"
        assert workspace_notebook_filename("/Workspace/Users/foo/MyNotebook") == "MyNotebook.py"

    def test_strips_existing_py_extension(self) -> None:
        """A workspace path that already has ``.py`` is not double-suffixed."""
        assert workspace_notebook_filename("/Shared/etl/runner.py") == "runner.py"

    def test_non_py_extension_is_folded_into_stem(self) -> None:
        """A non-``.py`` extension is kept as part of the bundle filename stem to
        preserve disambiguation between e.g. ``foo.sql`` and ``foo.py``."""
        assert workspace_notebook_filename("/Shared/etl/runner.sql") == "runner_sql.py"

    def test_sanitises_special_characters(self) -> None:
        """Spaces collapse to underscores; trailing separator characters are stripped."""
        assert workspace_notebook_filename("/Shared/My Folder/My Notebook!") == "My_Notebook.py"
        assert workspace_notebook_filename("/Shared/foo bar baz") == "foo_bar_baz.py"

    def test_returns_empty_for_no_segment(self) -> None:
        """Empty / trailing-slash paths return an empty string so the caller can
        fall back to the activity-derived name."""
        assert workspace_notebook_filename("") == ""
        assert workspace_notebook_filename("/Shared/") == ""

    def test_returns_empty_when_only_special_chars(self) -> None:
        assert workspace_notebook_filename("/Shared/!!!") == ""
