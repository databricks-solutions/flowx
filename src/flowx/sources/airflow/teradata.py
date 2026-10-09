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


def _tokens(command: str) -> list[str] | None:
    lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    try:
        return list(lexer)
    except ValueError:
        return None


def _utility_name(token: str) -> str | None:
    name = token.rsplit("/", 1)[-1]
    return name if name in UTILITIES else None


def find_utilities(command: str) -> list[str]:
    """Returns the Teradata utilities a shell command runs, in order of first use.

    A word whose basename is a utility counts wherever it appears, which can over-report (for example
    ``echo bteq``) but never misses a call. An unparseable command falls back to a word search.
    """
    tokens = _tokens(command)
    if tokens is None:
        tokens = re.findall(r"[\w./-]+", command)
    found: list[str] = []
    for token in tokens:
        name = _utility_name(token)
        if name is not None and name not in found:
            found.append(name)
    return found


def script_references(command: str) -> list[str]:
    """Returns the script files a shell command feeds to its Teradata utilities.

    Recognizes ``utility < file``, ``utility -f file``, and ``cat file | utility``.
    """
    tokens = _tokens(command)
    if tokens is None:
        return []
    references: list[str] = []

    def add(path: str) -> None:
        if path not in references:
            references.append(path)

    for index, token in enumerate(tokens):
        if _utility_name(token) is None:
            continue
        position = index + 1
        while position < len(tokens) and tokens[position] not in _SEPARATORS:
            if tokens[position] in ("<", "-f") and position + 1 < len(tokens):
                add(tokens[position + 1])
                position += 1
            position += 1
        if index >= 2 and tokens[index - 1] == "|":
            start = index - 2
            while start >= 0 and tokens[start] not in _SEPARATORS:
                start -= 1
            segment = tokens[start + 1 : index - 1]
            if segment and segment[0].rsplit("/", 1)[-1] == "cat":
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
