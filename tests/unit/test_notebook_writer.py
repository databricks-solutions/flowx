"""Unit tests for the DAB notebook writer's collision handling."""

from __future__ import annotations

import logging
from pathlib import Path

from flowx.bundler.notebook_writer import write_notebooks
from flowx.models.dab import DabNotebook


class TestWriteNotebooks:
    def test_writes_single_notebook(self, tmp_path: Path) -> None:
        out = write_notebooks(
            [DabNotebook(relative_path="notebooks/a.py", content="print('a')")],
            tmp_path,
        )
        assert len(out) == 1
        assert (tmp_path / "notebooks" / "a.py").read_text().rstrip() == "print('a')"

    def test_coalesces_identical_duplicates(self, tmp_path: Path) -> None:
        """Two writes with identical content + path are written once, not twice."""
        notebooks = [
            DabNotebook(relative_path="notebooks/a.py", content="print('a')"),
            DabNotebook(relative_path="notebooks/a.py", content="print('a')"),
        ]
        out = write_notebooks(notebooks, tmp_path)
        assert len(out) == 1
        assert (tmp_path / "notebooks" / "a.py").read_text().rstrip() == "print('a')"

    def test_disambiguates_on_content_collision(self, tmp_path: Path, caplog) -> None:
        """Same path + different content: the second write is suffixed __1 and a warning fires."""
        notebooks = [
            DabNotebook(relative_path="notebooks/run.py", content="print('first')"),
            DabNotebook(relative_path="notebooks/run.py", content="print('second')"),
        ]
        with caplog.at_level(logging.WARNING, logger="flowx.bundler.notebook_writer"):
            out = write_notebooks(notebooks, tmp_path)
        assert len(out) == 2
        assert (tmp_path / "notebooks" / "run.py").read_text().rstrip() == "print('first')"
        assert (tmp_path / "notebooks" / "run__1.py").read_text().rstrip() == "print('second')"
        assert any("share a basename" in rec.message for rec in caplog.records)

    def test_binary_content_takes_precedence_over_text(self, tmp_path: Path) -> None:
        out = write_notebooks(
            [DabNotebook(relative_path="lib/x.jar", binary_content=b"\x01\x02\x03")],
            tmp_path,
        )
        assert len(out) == 1
        assert (tmp_path / "lib" / "x.jar").read_bytes() == b"\x01\x02\x03"
