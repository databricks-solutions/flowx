"""The `profile` optional-dependency extra declares exactly what the profiler imports."""

from __future__ import annotations

import tomllib
from pathlib import Path

ROOT = Path(__file__).parents[2]


def _profile_extra() -> str:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    extras = data["project"]["optional-dependencies"]
    assert "profile" in extras, "missing [project.optional-dependencies] profile"
    return " ".join(extras["profile"])


def test_profile_extra_lists_the_azure_auth_stack():
    joined = _profile_extra()
    for required in ("azure-identity", "requests"):
        assert required in joined, f"profile extra missing {required}"


def test_profile_extra_has_no_unused_heavy_dependencies():
    # The port talks to Azure over plain REST; these were the original script's and aren't imported.
    joined = _profile_extra()
    for unused in ("pandas", "aiohttp", "tqdm", "azure-mgmt-datafactory", "azure-mgmt-resource"):
        assert unused not in joined, f"profile extra still declares unused {unused}"
