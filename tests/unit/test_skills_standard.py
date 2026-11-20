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
        relative = str(reference.relative_to(folder))
        assert f"]({relative})" in skill_text or f"]({relative}#" in skill_text, f"{relative} is not linked"


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

    sections = re.findall(r"^## (.+)$", text, re.MULTILINE)
    assert sections[:1] == ["Contents"], "a reference file over 100 lines needs '## Contents' as its first section"


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
    strategy_by_type = dict(re.findall(r"^\| (\w+) \| (Deterministic|Agentic) \|", mapping, re.MULTILINE))

    for activity_type in sorted(DETERMINISTIC_TYPES | AGENTIC_TYPES):
        expected = "Deterministic" if activity_type in DETERMINISTIC_TYPES else "Agentic"
        assert activity_type in strategy_by_type, f"{activity_type} has no row in activity-mapping.md"
        assert strategy_by_type[activity_type] == expected, f"{activity_type} is {expected} in the loader"


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

    translate_source = (REPO_ROOT / "src" / "flowx" / "sources" / "adf" / "translate.py").read_text(encoding="utf-8")
    choices_match = re.search(r'"--global-parameter-resolution",\s*choices=\(([^)]*)\)', translate_source)

    assert choices_match, "found no --global-parameter-resolution choices in translate.py; the pattern needs updating"
    choices = re.findall(r'"(\w+)"', choices_match.group(1))
    assert choices
    assert f"`{INPUT_GLOBAL_PARAMETER_RESOLUTION}`" in coverage
    for choice in choices:
        assert f"'{choice}'" in session_source, f"session.py no longer offers {choice}"
        assert f"`{choice}`" in coverage, f"adf-coverage.md does not name the {choice} choice"


# ---------------------------------------------------------------------------
# Prescription checks. The skills should keep a general method and point to the official docs, to what a
# flowx step reports, or to the code, rather than restate product facts, tool output or one scenario.
# Each check scans every skill Markdown file. A line a check flags but that is rightly prescriptive goes in
# skill_prescription_allow.yaml with its reason; an entry that no longer matches any line fails the suite,
# so the list cannot go stale.
# ---------------------------------------------------------------------------

ALLOW_LIST = Path(__file__).with_name("skill_prescription_allow.yaml")
PRESCRIPTION_CLASSES = {
    "time-word",
    "price",
    "unsourced-number",
    "named-source",
    "ui-path",
    "restated-strategy",
    "restated-file",
    "picks-for-the-user",
}
TIME_WORDS = re.compile(
    r"\b(currently|for now|not yet|as of|today('s)?|in the future|recently|at present|at the moment|soon|upcoming)\b",
    re.IGNORECASE,
)
SENTENCE_STARTING_WITH_CURRENT = re.compile(r"(^|[.!?]\s+)Current\b")
PRICE = re.compile(r"\$\s?\d|\d[\d.,]*\s?(USD|EUR|GBP)\b|\b(USD|EUR|GBP)\s?\d|\d[^\n]{0,40}\bper (vCore-hour|DBU)\b")
# Words that would tie a skill to one source system, estate or tool. shared/airflow-coverage.md is exempt: it is
# the map of what the Airflow code does with each operator, and the operators carry the names of their systems.
SOURCE_SPECIFIC_TERMS = (
    "SQL Server",
    "Oracle",
    "PostgreSQL",
    "MySQL",
    "Salesforce",
    "Teradata",
    "Snowflake",
    "BigQuery",
    "SAP",
    "Workday",
    "ServiceNow",
    "Lakebridge",
    "gateway",
    "integrated CDC",
    "Σ",
    "⌈",
)
SOURCE_TERMS_EXEMPT = {"shared/airflow-coverage.md"}
SOURCE_TYPE_NAME = re.compile(r"`\w+Source`")
MAX_SOURCE_TYPES_PER_SENTENCE = 2
STRATEGY_FILES = ("flowx-discover", "flowx-convert", "flowx-migrate")
EXAMPLE_LIST = re.compile(r"\b(e\.g\.|for example)", re.IGNORECASE)
WRITTEN_FILE_NAME = re.compile(r"`(?:[\w./-]*/)?([\w.-]+\.(?:json|csv|html))`")
PICKS_FOR_THE_USER = re.compile(
    r"best-first|ahead of like-for-like|\blist\b[^.]*\bfirst\b|\bprefer\b[^.]*\bover\b", re.IGNORECASE
)
PICK_FILES = ("flowx-enrich", "flowx-route")
INLINE_CODE = re.compile(r"`[^`]*`")
SOURCE_ROOT = REPO_ROOT / "src"


def _relative(path: Path) -> str:
    return str(path.relative_to(SKILLS_DIR))


def _allow_entries() -> list[dict[str, str]]:
    entries = yaml.safe_load(ALLOW_LIST.read_text(encoding="utf-8")) or []
    assert isinstance(entries, list), "skill_prescription_allow.yaml must be a list"
    return entries


def _allowed(path: Path, line: str, check_class: str) -> bool:
    relative = _relative(path)
    return any(
        entry["path"] == relative and entry["class"] == check_class and entry["phrase"] in line
        for entry in _allow_entries()
    )


def _prose_lines(path: Path) -> list[tuple[int, str]]:
    """Return (line number, line) for every line outside fenced code blocks."""
    lines, in_fence = [], False
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        if not in_fence:
            lines.append((number, line))
    return lines


def _all_lines(path: Path) -> list[tuple[int, str]]:
    return list(enumerate(path.read_text(encoding="utf-8").splitlines(), start=1))


def _sentences(path: Path) -> list[str]:
    """Return the prose of a file split into sentences, with each paragraph's lines joined."""
    paragraphs = "\n".join(line for _, line in _prose_lines(path)).split("\n\n")
    sentences = []
    for paragraph in paragraphs:
        joined = " ".join(paragraph.split())
        sentences.extend(part for part in re.split(r"(?<=[.!?])\s+(?=[A-Z`*])", joined) if part)
    return sentences


def _flagged(path: Path, check_class: str, pattern_matches: object) -> list[str]:
    """Return the lines a check flags in a file, leaving out the ones the allow list covers."""
    flagged = []
    for number, line in _all_lines(path):
        if pattern_matches(line) and not _allowed(path, line, check_class):  # type: ignore[operator]
            flagged.append(f"{_relative(path)}:{number}: {line.strip()}")
    return flagged


def _has_time_word(line: str) -> bool:
    without_code = INLINE_CODE.sub("", line)
    return bool(TIME_WORDS.search(without_code) or SENTENCE_STARTING_WITH_CURRENT.search(without_code))


def _has_price(line: str) -> bool:
    return bool(PRICE.search(line))


def _source_terms(line: str) -> list[str]:
    without_code = INLINE_CODE.sub("", line)
    return [term for term in SOURCE_SPECIFIC_TERMS if re.search(rf"(?<![\w-]){re.escape(term)}(?![\w-])", without_code)]


def _strategy_mismatches(sentence: str) -> list[str]:
    """Return the backticked ADF activity types an example list gives a strategy the loader doesn't."""
    if not EXAMPLE_LIST.search(sentence):
        return []
    says_agentic = re.search(r"\bagentic\b", sentence, re.IGNORECASE)
    says_unsupported = re.search(r"\bunsupported\b", sentence, re.IGNORECASE)
    if bool(says_agentic) == bool(says_unsupported):
        return []
    named = set(re.findall(r"`(\w+)`", sentence))
    if says_agentic:
        return sorted(name for name in named & DETERMINISTIC_TYPES)
    return sorted(name for name in named & (DETERMINISTIC_TYPES | AGENTIC_TYPES))


def _source_text() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in SOURCE_ROOT.rglob("*.py"))


def _has_pick(line: str) -> bool:
    return bool(PICKS_FOR_THE_USER.search(line)) and not re.search(r"\b(facts?|customer)\b", line)


@pytest.mark.parametrize("path", _markdown_files(), ids=_relative)
def test_skill_text_has_no_time_word(path: Path) -> None:
    flagged = _flagged(path, "time-word", _has_time_word)

    assert not flagged, "state what is true without a date, or point to a page that carries one:\n" + "\n".join(flagged)


@pytest.mark.parametrize("path", _markdown_files(), ids=_relative)
def test_skill_text_states_no_price(path: Path) -> None:
    flagged = _flagged(path, "price", _has_price)

    assert not flagged, "point to the pricing page or the report's pricing source instead:\n" + "\n".join(flagged)


@pytest.mark.parametrize("path", _markdown_files(), ids=_relative)
def test_skill_text_names_no_source_system(path: Path) -> None:
    if _relative(path) in SOURCE_TERMS_EXEMPT:
        return
    flagged = _flagged(path, "named-source", lambda line: bool(_source_terms(line)))
    for sentence in _sentences(path):
        if len(SOURCE_TYPE_NAME.findall(sentence)) > MAX_SOURCE_TYPES_PER_SENTENCE:
            flagged.append(f"{_relative(path)}: lists source types: {sentence[:120]}")

    assert not flagged, "use a placeholder or what a flowx step reports instead:\n" + "\n".join(flagged)


@pytest.mark.parametrize(
    "path", [path for path in _markdown_files() if path.parts[len(SKILLS_DIR.parts)] in STRATEGY_FILES], ids=_relative
)
def test_example_lists_give_each_activity_type_the_loaders_strategy(path: Path) -> None:
    mismatches = [
        f"{name} in: {sentence[:120]}" for sentence in _sentences(path) for name in _strategy_mismatches(sentence)
    ]

    assert not mismatches, "name activities by their strategy in inventory.json instead:\n" + "\n".join(mismatches)


@pytest.mark.parametrize("path", _markdown_files(), ids=_relative)
def test_every_file_name_the_skills_give_exists_in_the_code(path: Path) -> None:
    source_text = _source_text()
    missing = [
        f"{_relative(path)}:{number}: {name}"
        for number, line in _all_lines(path)
        for name in WRITTEN_FILE_NAME.findall(line)
        if "<" not in name and name not in source_text and not _allowed(path, line, "restated-file")
    ]

    assert not missing, "no file of that name appears in src/:\n" + "\n".join(missing)


@pytest.mark.parametrize(
    "path", [path for path in _markdown_files() if path.parts[len(SKILLS_DIR.parts)] in PICK_FILES], ids=_relative
)
def test_enrich_and_route_order_patterns_by_the_facts_not_by_convention(path: Path) -> None:
    flagged = _flagged(path, "picks-for-the-user", _has_pick)

    assert not flagged, "give the reason from the facts and leave the choice to the customer:\n" + "\n".join(flagged)


def test_every_allow_entry_has_a_reason_and_still_matches_a_line() -> None:
    for entry in _allow_entries():
        assert set(entry) == {"path", "phrase", "class", "reason"}, entry
        assert entry["class"] in PRESCRIPTION_CLASSES, entry
        assert len(entry["reason"].split()) >= 5, f"give a real reason: {entry}"
        path = SKILLS_DIR / entry["path"]
        assert path.exists(), entry
        assert any(entry["phrase"] in line for _, line in _all_lines(path)), f"no line matches: {entry}"


# Lines the prescription audit found in the skills at a9c0b86, before they were fixed. Each check must flag its
# lines, so a check that stops catching what it was written for fails here.
LINES_THE_AUDIT_FOUND = {
    "time-word": [
        "Current deterministic coverage: `PythonOperator` (callable body → generated PySpark notebook) and",
        "This allows the package phase to target different output formats in the future.",
    ],
    "price": [
        "  vCore-hours are priced at $0.274 per vCore-hour.",
        "  - Mapping Data Flow: 8 vCores × $0.274 per vCore-hour.",
    ],
    "named-source": [
        "applies it, only when the loop's Copy reads a SQL Server source (`SqlServerSource`, `AzureSqlSource`,",
        "connection through a gateway): tell the user it is a prerequisite for the migrated job.",
        "convert it to Databricks SQL, for example with Lakebridge. |",
    ],
    "picks-for-the-user": [
        "  convention (not validated), order them best-first and list the `simplification_pattern: true` ones",
        "  ahead of like-for-like ports.",
    ],
}
SENTENCES_THE_AUDIT_FOUND = [
    "For `agentic` activities, explain that each is translated by the agent using LLM-assisted reasoning from the"
    " activity's ARM JSON (no built-in deterministic translator exists), e.g. `ExecuteDataFlow`, `Switch`, `Until`,"
    " stored procedures.",
    "For `unsupported` activities, warn clearly, e.g. `ExecuteSSISPackage`, recommend manual conversion to a PySpark"
    " notebook.",
]
SOURCE_TYPE_LIST_THE_AUDIT_FOUND = (
    "only when the loop's Copy reads a SQL Server source (`SqlServerSource`, `AzureSqlSource`,"
    " `AzureSqlDatabaseSource`, `SqlMISource`, `SqlSource` or `MicrosoftSqlServerSource`)."
)
FILE_NAME_THE_AUDIT_FOUND = "- **Parse errors**: logged to `parse_errors.json`, skipped in inventory"
LINE_CHECKS = {
    "time-word": _has_time_word,
    "price": _has_price,
    "named-source": lambda line: bool(_source_terms(line)),
    "picks-for-the-user": _has_pick,
}


@pytest.mark.parametrize(
    ("check_class", "line"),
    [(check_class, line) for check_class, lines in LINES_THE_AUDIT_FOUND.items() for line in lines],
)
def test_each_line_check_flags_the_lines_the_audit_found(check_class: str, line: str) -> None:
    assert LINE_CHECKS[check_class](line)


@pytest.mark.parametrize("sentence", SENTENCES_THE_AUDIT_FOUND)
def test_the_strategy_check_flags_the_example_lists_the_audit_found(sentence: str) -> None:
    assert _strategy_mismatches(sentence)


def test_the_source_check_flags_a_sentence_that_lists_source_types() -> None:
    assert len(SOURCE_TYPE_NAME.findall(SOURCE_TYPE_LIST_THE_AUDIT_FOUND)) > MAX_SOURCE_TYPES_PER_SENTENCE


def test_the_file_check_flags_a_file_name_the_code_never_writes() -> None:
    names = WRITTEN_FILE_NAME.findall(FILE_NAME_THE_AUDIT_FOUND)

    assert names == ["parse_errors.json"]
    assert names[0] not in _source_text()
