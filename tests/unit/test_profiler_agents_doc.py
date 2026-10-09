"""The profile phase is documented and installable from the plugin's pip-based venv."""

from __future__ import annotations

import tomllib
from pathlib import Path

ROOT = Path(__file__).parents[2]


def test_agents_doc_mentions_profile_phase_and_module():
    text = (ROOT / "AGENTS.md").read_text()
    assert "flowx profile" in text or "adapter profile" in text
    assert "sources/adf/profiler" in text
    assert "metadata/tco/" in text


def test_skill_installs_the_same_azure_identity_as_the_profile_extra():
    # The plugin venv installs with pip, so the skill names the package directly; keep it in step
    # with pyproject's `profile` extra instead of maintaining a separate requirements file.
    extra = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["optional-dependencies"]["profile"]
    azure_identity = next(requirement for requirement in extra if requirement.startswith("azure-identity"))
    skill = (ROOT / "skills" / "flowx-profile" / "SKILL.md").read_text()
    assert f"pip install '{azure_identity}'" in skill
    assert not (ROOT / "requirements-profile.txt").exists()
