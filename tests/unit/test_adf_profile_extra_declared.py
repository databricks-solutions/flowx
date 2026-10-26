"""The `azure` optional-dependency extra (shared with `flowx extract`) declares what the profiler imports."""

from __future__ import annotations

import tomllib
from pathlib import Path

ROOT = Path(__file__).parents[2]


def _azure_extra() -> str:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    extras = data["project"]["optional-dependencies"]
    assert "azure" in extras, "missing [project.optional-dependencies] azure"
    return " ".join(extras["azure"])


def test_azure_extra_lists_the_azure_auth_stack():
    joined = _azure_extra()
    for required in ("azure-identity", "requests"):
        assert required in joined, f"azure extra missing {required}"


def test_azure_extra_has_no_unused_heavy_dependencies():
    # The port talks to Azure over plain REST; these were the original script's and aren't imported.
    joined = _azure_extra()
    for unused in ("pandas", "aiohttp", "tqdm", "azure-mgmt-datafactory", "azure-mgmt-resource"):
        assert unused not in joined, f"azure extra still declares unused {unused}"
