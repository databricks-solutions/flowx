"""The flowx-profile skill exists, is registered, and carries the credential guidance."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).parents[2]


def test_skill_file_has_frontmatter_and_credential_matrix():
    skill = ROOT / "skills" / "flowx-profile" / "SKILL.md"
    assert skill.exists()
    text = skill.read_text()
    assert "name: flowx-profile" in text
    assert "profile --source adf" in text
    assert "az login" in text
    assert "service principal" in text.lower()
    assert "secret scope" in text.lower()
    assert "metadata/tco/" in text
    assert (ROOT / "skills" / "flowx-profile" / "sources" / "adf.md").exists()


def test_skill_does_not_promise_an_mcp_command_that_does_not_exist():
    # The MCP dispatcher has no `profile` command yet; the skill must not tell agents to call it.
    text = (ROOT / "skills" / "flowx-profile" / "SKILL.md").read_text()
    assert 'command="profile"' not in text


def test_skill_registered_in_plugin_manifest():
    manifest = json.loads((ROOT / ".claude-plugin" / "plugin.json").read_text())
    assert "./skills/flowx-profile" in manifest["skills"]


def test_adf_guide_lists_required_roles():
    text = (ROOT / "skills" / "flowx-profile" / "sources" / "adf.md").read_text()
    for role in ("Reader", "Data Factory Contributor", "Cost Management Reader"):
        assert role in text


def test_adf_guide_does_not_claim_shir_vm_resolution():
    # This port doesn't resolve self-hosted IR VM sizes at all; the guide must not say it does.
    text = (ROOT / "skills" / "flowx-profile" / "sources" / "adf.md").read_text()
    assert "VM sizes are resolved" not in text


def test_skill_describes_the_compute_meter_filter_accurately():
    skill = (ROOT / "skills" / "flowx-profile" / "SKILL.md").read_text()
    guide = (ROOT / "skills" / "flowx-profile" / "sources" / "adf.md").read_text()
    assert "Azure Data Factory v2" in skill
    assert "empty meter subcategory" not in guide
