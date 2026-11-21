"""Tests that the flowx skills check recommended patterns against the Databricks platform limits read live.

flowx keeps no copy of the platform limits, because they change. Enrich checks every pattern it recommends
against the current docs, and route qualifies every target pattern the same way, with one method for any source
or component that names no source system, limit or size. The skills are text the agent follows, so these tests
hold the text to that promise; test_skills_standard.py keeps source names out of every skill file.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

REPO_ROOT = Path(__file__).parents[2]
SKILLS_DIR = REPO_ROOT / "skills"
ENRICH_SKILL = SKILLS_DIR / "flowx-enrich" / "SKILL.md"
ROUTE_PATTERNS = SKILLS_DIR / "flowx-route" / "references" / "target-patterns.md"
LIMIT_RECORD = "`limit: <name> = <value> (source: <page>, read <date>)`"
SKIPPED_DIRS = {
    ".git",
    ".venv",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
    ".claude",
    "node_modules",
    "__pycache__",
    "flowx_output",
    "dist",
    "build",
}

# The steps of the method, in the order route's reference gives them.
METHOD_STEPS = (
    "## 1. Gather the facts about the source and the target",
    "## 2. Find every current option in the official documentation",
    "## 3. Check each option against the current limits",
    "## 4. Mark what the docs don't settle",
    "## 5. Show each ruled-out option, and leave the choice to the customer",
)


def _repository_files() -> list[Path]:
    """Return the repository's own files, skipping environments, caches, run output and this test."""
    repository_files = []
    for folder, subfolders, file_names in os.walk(REPO_ROOT):
        subfolders[:] = [name for name in subfolders if name not in SKIPPED_DIRS]
        for file_name in file_names:
            path = Path(folder) / file_name
            if path != Path(__file__):
                repository_files.append(path)
    return repository_files


def test_flowx_keeps_no_copy_of_the_platform_limits() -> None:
    assert not list(SKILLS_DIR.rglob("platform-limits.json"))
    for path in _repository_files():
        assert b"platform-limits.json" not in path.read_bytes(), path.relative_to(REPO_ROOT)


def test_enrich_checks_every_recommended_pattern_against_the_live_limits() -> None:
    enrich = ENRICH_SKILL.read_text(encoding="utf-8")

    assert "**Check every pattern you recommend against the current Databricks platform limits.**" in enrich
    assert "Don't recommend a pattern a limit rules out." in enrich
    assert LIMIT_RECORD in enrich
    assert "`not qualified: needs <fact>`" in enrich
    assert "`not qualified: limits not checked`" in enrich
    assert "Never use a remembered number." in enrich


def test_route_reads_the_limits_live_and_records_them_the_way_enrich_does() -> None:
    route = ROUTE_PATTERNS.read_text(encoding="utf-8")

    assert "**Limits** are read live" in route
    assert LIMIT_RECORD in route
    assert "*not qualified: needs <fact>*" in route
    assert "*not qualified: limits not checked*" in route
    assert "never use a remembered number" in route


def test_route_gives_one_method_for_any_source_with_no_fixed_check_ids() -> None:
    route = ROUTE_PATTERNS.read_text(encoding="utf-8")

    headings = [line for line in route.splitlines() if line.startswith("## ")]
    assert headings == list(METHOD_STEPS)
    one_line = " ".join(route.split())
    assert "Don't take the list of options from memory" in one_line
    assert "*Assumption:*" in one_line
    assert "Never pick the pattern on the customer's behalf" in one_line
    assert not re.search(r"\b[ACDM]\d+\b", route), "names a fixed pattern or check id"
