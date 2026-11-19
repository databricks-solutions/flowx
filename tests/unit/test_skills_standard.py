"""Tests that every flowx skill follows one standard, and that the skill text stays true to the code.

The skills are text an agent follows, so nothing else catches a skill that drifts from the Agent Skills
specification (https://agentskills.io/specification), Anthropic's skill authoring guide, or the shared layout
the flowx skills use. These tests run on every skill, so a new skill or a changed one is held to the same rules.

The second half ties the ADF coverage map and the activity mapping to the code they describe, so a new trigger
type or activity type in the code fails here until the skill text says what happens to it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from flowx.adapter.constants import INPUT_GLOBAL_PARAMETER_RESOLUTION
from flowx.sources.adf.loader import AGENTIC_TYPES, DETERMINISTIC_TYPES

REPO_ROOT = Path(__file__).parents[2]
SKILLS_DIR = REPO_ROOT / "skills"
SHARED_DIR = SKILLS_DIR / "shared"
VENDORED_FOLDER = "airflow-to-dabs"
SKILL_FOLDERS = sorted(path.parent for path in SKILLS_DIR.glob("*/SKILL.md"))

# The six frontmatter fields the Agent Skills specification allows. Claude Code ignores any other field, and a
# claude.ai upload or the Skills API rejects one.
STANDARD_FIELDS = {"name", "description", "license", "compatibility", "metadata", "allowed-tools"}
SKILL_NAME = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
MAX_NAME_LENGTH = 64
MAX_DESCRIPTION_LENGTH = 1024
MAX_SKILL_LINES = 500
# The specification recommends under 5,000 tokens for SKILL.md; characters divided by four is the usual estimate.
MAX_SKILL_TOKENS = 5000
CHARACTERS_PER_TOKEN = 4
CONTENTS_REQUIRED_OVER_LINES = 100
SHARED_LAYOUT = ["Before you start", "How to run", "Steps", "Output", "Reference"]
STEP_HEADING = re.compile(r"^### Step (\d+)( \(optional\))?: \S")
TIME_SENSITIVE = re.compile(r"\b(currently|for now|not yet|as of)\b", re.IGNORECASE)
MARKDOWN_LINK = re.compile(r"\]\(([^)\s#]*)(#[^)\s]*)?\)")


def _skill_id(folder: Path) -> str:
    return folder.name


def _split_skill(folder: Path) -> tuple[dict[str, object], str]:
    """Return a skill's frontmatter as a dict and its body as text."""
    text = (folder / "SKILL.md").read_text(encoding="utf-8")
    match = re.match(r"---\n(.*?)\n---\n", text, re.DOTALL)
    assert match, f"{folder.name}: SKILL.md must start with YAML frontmatter between --- lines"
    frontmatter = yaml.safe_load(match.group(1))
    assert isinstance(frontmatter, dict), f"{folder.name}: frontmatter is not a mapping"
    return frontmatter, text[match.end() :]


def _markdown_files() -> list[Path]:
    """Return every Markdown file in the skills, without the vendored upstream copy."""
    return [path for path in SKILLS_DIR.rglob("*.md") if VENDORED_FOLDER not in path.parts]


def _own_reference_files(folder: Path) -> list[Path]:
    return [path for path in folder.rglob("*.md") if path.name != "SKILL.md" and VENDORED_FOLDER not in path.parts]


def _heading_anchor(heading: str) -> str:
    """Return the anchor GitHub gives a Markdown heading."""
    return re.sub(r"[^\w\- ]", "", heading.strip().lower()).replace(" ", "-")


@pytest.mark.parametrize("folder", SKILL_FOLDERS, ids=_skill_id)
def test_frontmatter_uses_only_the_standard_fields(folder: Path) -> None:
    frontmatter, _ = _split_skill(folder)

    assert set(frontmatter) <= STANDARD_FIELDS, f"unexpected fields: {sorted(set(frontmatter) - STANDARD_FIELDS)}"


@pytest.mark.parametrize("folder", SKILL_FOLDERS, ids=_skill_id)
def test_name_follows_the_naming_rules_and_matches_its_folder(folder: Path) -> None:
    name = str(_split_skill(folder)[0]["name"])

    assert name == folder.name
    assert len(name) <= MAX_NAME_LENGTH
    assert SKILL_NAME.match(name)
    assert "anthropic" not in name and "claude" not in name


@pytest.mark.parametrize("folder", SKILL_FOLDERS, ids=_skill_id)
def test_description_says_what_the_skill_does_and_when_to_use_it(folder: Path) -> None:
    description = " ".join(str(_split_skill(folder)[0]["description"]).split())

    assert 0 < len(description) <= MAX_DESCRIPTION_LENGTH
    assert re.search(r"\bUse (when|after)\b", description), "the description needs a 'Use when ...' sentence"
    assert not re.search(r"\b(I can|You can)\b", description), "write the description in the third person"


@pytest.mark.parametrize("folder", SKILL_FOLDERS, ids=_skill_id)
def test_skill_md_stays_under_the_length_limits(folder: Path) -> None:
    text = (folder / "SKILL.md").read_text(encoding="utf-8")

    assert len(text.splitlines()) < MAX_SKILL_LINES
    assert len(text) / CHARACTERS_PER_TOKEN < MAX_SKILL_TOKENS


@pytest.mark.parametrize("folder", SKILL_FOLDERS, ids=_skill_id)
def test_skill_md_uses_the_shared_layout(folder: Path) -> None:
    _, body = _split_skill(folder)
    sections = re.findall(r"^## (.+)$", body, re.MULTILINE)
    step_headings = [line for line in body.splitlines() if line.startswith("### Step")]
    step_numbers = []
    for heading in step_headings:
        match = STEP_HEADING.match(heading)
        assert match, f"step heading should read '### Step N: ...': {heading}"
        step_numbers.append(int(match.group(1)))

    assert sections == SHARED_LAYOUT
    assert step_numbers == list(range(1, len(step_numbers) + 1)), "steps are numbered from 1 with no gaps"


@pytest.mark.parametrize("folder", SKILL_FOLDERS, ids=_skill_id)
def test_every_reference_file_is_linked_straight_from_skill_md(folder: Path) -> None:
    skill_text = (folder / "SKILL.md").read_text(encoding="utf-8")

    for reference in _own_reference_files(folder):
        assert str(reference.relative_to(folder)) in skill_text, f"{reference.relative_to(folder)} is not linked"


@pytest.mark.parametrize("path", _markdown_files(), ids=lambda path: str(path.relative_to(SKILLS_DIR)))
def test_every_link_between_skill_files_resolves(path: Path) -> None:
    for match in MARKDOWN_LINK.finditer(path.read_text(encoding="utf-8")):
        link, anchor = match.group(1), match.group(2)
        if link.startswith(("http://", "https://", "mailto:")):
            continue
        target = (path.parent / link).resolve() if link else path
        assert target.exists(), f"broken link: {link}"
        if anchor and target.suffix == ".md":
            headings = re.findall(r"^#+ (.+)$", target.read_text(encoding="utf-8"), re.MULTILINE)
            assert anchor[1:] in {_heading_anchor(heading) for heading in headings}, f"broken anchor: {link}{anchor}"


@pytest.mark.parametrize("path", _markdown_files(), ids=lambda path: str(path.relative_to(SKILLS_DIR)))
def test_long_reference_files_start_with_a_contents_list(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    if path.name == "SKILL.md" or len(text.splitlines()) <= CONTENTS_REQUIRED_OVER_LINES:
        return

    assert re.search(r"^## Contents$", text, re.MULTILINE), "a reference file over 100 lines needs '## Contents'"


@pytest.mark.parametrize("path", _markdown_files(), ids=lambda path: str(path.relative_to(SKILLS_DIR)))
def test_skill_text_has_no_time_sensitive_wording(path: Path) -> None:
    found = TIME_SENSITIVE.findall(path.read_text(encoding="utf-8"))

    assert not found, f"state what is true instead of: {found}"


@pytest.mark.parametrize("shared_file", sorted(SHARED_DIR.glob("*.md")), ids=lambda path: path.name)
def test_each_shared_file_is_used_by_at_least_two_skills(shared_file: Path) -> None:
    users = {
        folder.name
        for folder in SKILL_FOLDERS
        for path in [folder / "SKILL.md", *_own_reference_files(folder)]
        if f"shared/{shared_file.name}" in path.read_text(encoding="utf-8")
    }

    assert len(users) >= 2, f"{shared_file.name} is used by {sorted(users)}; keep it with that skill instead"


def test_the_shared_folder_is_not_a_skill() -> None:
    assert not (SHARED_DIR / "SKILL.md").exists()


def test_activity_mapping_lists_every_activity_type_the_loader_classifies() -> None:
    mapping = (SKILLS_DIR / "flowx-convert" / "references" / "activity-mapping.md").read_text(encoding="utf-8")

    for activity_type in sorted(DETERMINISTIC_TYPES | AGENTIC_TYPES):
        assert re.search(rf"\b{activity_type}\b", mapping), f"{activity_type} is missing from activity-mapping.md"


def test_adf_coverage_map_covers_every_trigger_type_convert_translates() -> None:
    coverage = (SKILLS_DIR / "flowx-convert" / "references" / "adf-coverage.md").read_text(encoding="utf-8")
    translate_source = (REPO_ROOT / "src" / "flowx" / "sources" / "adf" / "translate.py").read_text(encoding="utf-8")
    trigger_types = set(re.findall(r'trigger_type == "(\w+Trigger)"', translate_source))

    assert trigger_types, "found no trigger types in translate.py; the pattern needs updating"
    for trigger_type in sorted(trigger_types):
        assert f"`{trigger_type}`" in coverage, f"{trigger_type} is missing from adf-coverage.md"


def test_adf_coverage_map_names_the_global_parameter_input_and_its_choices() -> None:
    coverage = (SKILLS_DIR / "flowx-convert" / "references" / "adf-coverage.md").read_text(encoding="utf-8")
    session_source = (REPO_ROOT / "src" / "flowx" / "adapter" / "session.py").read_text(encoding="utf-8")

    assert f"`{INPUT_GLOBAL_PARAMETER_RESOLUTION}`" in coverage
    for choice in ("literal", "bundle_variable"):
        assert f"'{choice}'" in session_source, f"session.py no longer offers {choice}"
        assert f"`{choice}`" in coverage
