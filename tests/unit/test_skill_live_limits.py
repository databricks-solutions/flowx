"""Tests that the flowx skills check recommended patterns against the Databricks platform limits read live.

flowx keeps no copy of the platform limits, because they change. Enrich checks every pattern it recommends
against the current docs, and route reads the limits its checks rest on the same way. The skills are text the
agent follows, so these tests hold the text to that promise.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

REPO_ROOT = Path(__file__).parents[2]
SKILLS_DIR = REPO_ROOT / "skills"
ENRICH_SKILL = SKILLS_DIR / "flowx-enrich" / "SKILL.md"
ROUTE_SKILL = SKILLS_DIR / "flowx-route" / "SKILL.md"
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

# The three claims the public docs don't state, by the route check that rests on each.
ASSUMPTIONS = {
    "C1": "each destination table is written by one pipeline only",
    "C2": "a running gateway holds one concurrent update",
    "C6": "a running gateway holds one concurrent update",
    "C12": "integrated CDC's release state, which its page doesn't label; state what the page says",
}


def _route_checks_limits_column() -> dict[str, str]:
    """Return each route check's id (C1 to C13) with the text of its Limits column."""
    limits_by_check = {}
    for line in ROUTE_SKILL.read_text(encoding="utf-8").splitlines():
        match = re.match(r"\| (C\d+) [^|]*\|[^|]*\| ([^|]*) \|", line)
        if match:
            limits_by_check[match.group(1)] = match.group(2)
    return limits_by_check


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
    route = ROUTE_SKILL.read_text(encoding="utf-8")

    assert "**Limits** are read live" in route
    assert LIMIT_RECORD in route
    assert "*not qualified: needs <fact>*" in route
    assert "*not qualified: limits not checked*" in route
    assert "never use a remembered number" in route


def test_route_names_each_limit_in_words_and_marks_what_the_docs_do_not_state() -> None:
    limits_by_check = _route_checks_limits_column()

    assert list(limits_by_check) == [f"C{number}" for number in range(1, 14)]
    for check, limits in limits_by_check.items():
        assert not re.search(r"\b[a-z]+(?:_[a-z]+)+\b", limits), f"{check} names a limit by an id: {limits}"
    marked = {check for check, limits in limits_by_check.items() if "*Assumption:*" in limits}
    assert marked == set(ASSUMPTIONS)
    for check, claim in ASSUMPTIONS.items():
        assert f"*Assumption:* {claim}" in limits_by_check[check], check
