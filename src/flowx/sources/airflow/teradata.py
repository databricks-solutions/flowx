"""Teradata work inside Airflow tasks: client utilities in shell commands and BTEQ / Teradata SQL scripts.

Databricks has no Teradata client, so a task that drives ``bteq``, TPT, or the load utilities, or runs
Teradata SQL, cannot run as translated. flowx turns each one into a gap and attaches the script it runs,
so the gap list doubles as the inventory of scripts that need SQL conversion.
"""

from __future__ import annotations

import hashlib
import re
import shlex
from pathlib import Path
from typing import Any

UTILITIES = frozenset(
    {"bteq", "tbuild", "tdload", "fastload", "fastexport", "fexp", "mload", "multiload", "tpump", "arcmain"}
)

# Script content above this size is recorded by path, size, and digest only.
MAXIMUM_ATTACHED_SCRIPT_BYTES = 64 * 1024

_SEPARATORS = frozenset({"|", "||", "&&", ";", "&", "(", ")"})
_HEREDOC = re.compile(
    r"<<-?\s*(['\"]?)(?P<marker>\w+)\1[^\n]*\n(?P<body>.*?)\n[ \t]*(?P=marker)[ \t]*(?:\n|$)", re.DOTALL
)


_JINJA_EXPRESSION = re.compile(r"\{\{.*?\}\}|\{%.*?%\}", re.DOTALL)
_SCRIPT_ARGUMENT = re.compile(r"(?:<|-f)\s*([^\s;|&<>]+)")


def _mask_jinja(command: str) -> tuple[str, dict[str, str]]:
    """Replaces Jinja expressions with space-free markers so a path like ``x_{{ ds }}.bteq`` stays one word."""
    expressions: dict[str, str] = {}

    def mask(match: re.Match[str]) -> str:
        marker = f"\x00{len(expressions)}\x00"
        expressions[marker] = match.group(0)
        return marker

    return _JINJA_EXPRESSION.sub(mask, command), expressions


def _unmask(word: str, expressions: dict[str, str]) -> str:
    for marker, expression in expressions.items():
        word = word.replace(marker, expression)
    return word


def _shell_words(command: str) -> tuple[list[str] | None, dict[str, str]]:
    """Splits a command into shell words, with here-document bodies removed and Jinja kept whole.

    Returns None for the words when the remaining command still does not parse.
    """
    masked, expressions = _mask_jinja(_HEREDOC.sub(" ;\n", command))
    lexer = shlex.shlex(masked, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    try:
        return [_unmask(word, expressions) for word in lexer], expressions
    except ValueError:
        return None, expressions


def _command_name(word: str) -> str:
    return word.rsplit("/", 1)[-1]


def _utility_name(word: str) -> str | None:
    name = _command_name(word)
    return name if name in UTILITIES else None


def find_utilities(command: str) -> list[str]:
    """Returns the Teradata utilities a shell command runs, in order of first use.

    A word whose basename is a utility counts wherever it appears, which can over-report (for example
    ``echo bteq``) but never misses a call. An unparseable command falls back to a word search.
    """
    words, _ = _shell_words(command)
    if words is None:
        words = re.findall(r"[\w./-]+", _HEREDOC.sub(" ;\n", command))
    found: list[str] = []
    for word in words:
        name = _utility_name(word)
        if name is not None and name not in found:
            found.append(name)
    return found


def script_references(command: str) -> list[str]:
    """Returns the script files a shell command feeds to its Teradata utilities.

    Recognizes ``utility < file``, ``utility -f file``, and ``cat file | utility``. A command that does not
    parse falls back to finding ``< file`` and ``-f file`` in each segment that names a utility.
    """
    words, expressions = _shell_words(command)
    references: list[str] = []

    def add(path: str) -> None:
        if path not in references:
            references.append(path)

    if words is None:
        masked, expressions = _mask_jinja(_HEREDOC.sub(" ;\n", command))
        for segment in re.split(r"[;&|\n]", masked):
            if any(_utility_name(word) for word in segment.split()):
                for match in _SCRIPT_ARGUMENT.finditer(segment):
                    add(_unmask(match.group(1), expressions))
        return references

    for index, word in enumerate(words):
        if _utility_name(word) is None:
            continue
        position = index + 1
        while position < len(words) and words[position] not in _SEPARATORS:
            if words[position] in ("<", "-f") and position + 1 < len(words):
                add(words[position + 1])
                position += 1
            position += 1
        if index >= 2 and words[index - 1] == "|":
            start = index - 2
            while start >= 0 and words[start] not in _SEPARATORS:
                start -= 1
            segment = words[start + 1 : index - 1]
            if segment and _command_name(segment[0]) == "cat":
                for path in segment[1:]:
                    if not path.startswith("-"):
                        add(path)
    return references


def heredoc_scripts(command: str) -> list[str]:
    """Returns the bodies of here-documents in a shell command, where inline BTEQ scripts usually live."""
    return [match.group("body") for match in _HEREDOC.finditer(command)]


def script_record(path: str, search_paths: tuple[Path, ...]) -> dict[str, Any]:
    """Describes a script file a Teradata task runs, attaching its content when it is found and small.

    An absolute path is read as given; a relative one is looked up in the DAG's template search paths.
    """
    candidates = [Path(path)] if Path(path).is_absolute() else [root / path for root in search_paths]
    for candidate in candidates:
        if not candidate.is_file():
            continue
        try:
            data = candidate.read_bytes()
        except OSError as error:
            return {"path": path, "unresolved": f"could not be read ({error})"}
        record: dict[str, Any] = {
            "path": path,
            "resolved_path": str(candidate),
            "size_bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        if len(data) <= MAXIMUM_ATTACHED_SCRIPT_BYTES:
            try:
                record["content"] = data.decode("utf-8")
            except UnicodeDecodeError:
                record["content_note"] = "not UTF-8; read the file with its BTEQ script encoding"
        return record
    searched = ", ".join(str(candidate.parent) for candidate in candidates) or "an empty search path"
    return {"path": path, "unresolved": f"was not found in {searched}"}


def gap_details(
    utilities: list[str], scripts: list[dict[str, Any]], inline_scripts: list[str]
) -> tuple[str, dict[str, Any]]:
    """Returns the placeholder guidance and the ``raw_definition`` entry for a Teradata task."""
    described = ", ".join(record["path"] for record in scripts)
    runs = f"runs Teradata {', '.join(utilities)}" if utilities else "runs Teradata SQL"
    target = f" with {described}" if described else (" with an inline script" if inline_scripts else "")
    message = (
        f"This task {runs}{target}, and Databricks has no Teradata client. Convert the script to Databricks "
        "SQL (for example with Lakebridge) and replace this task with the converted SQL or notebook."
    )
    details: dict[str, Any] = {"utilities": utilities, "scripts": scripts}
    if inline_scripts:
        details["inline_scripts"] = inline_scripts
    return message, details
