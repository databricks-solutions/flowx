"""Teradata work inside Airflow tasks: client utilities in shell commands and BTEQ / Teradata SQL scripts.

Databricks has no Teradata client, so a task that drives ``bteq``, TPT, or the load utilities, or runs
Teradata SQL, cannot run as translated. flowx turns each one into a gap and attaches the script it runs,
so the gap list doubles as the inventory of scripts that need SQL conversion.
"""

from __future__ import annotations

import hashlib
import re
import shlex
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from flowx.sources.airflow.template_files import find_script_file

UTILITIES = frozenset(
    {"bteq", "tbuild", "tdload", "fastload", "fastexport", "fexp", "mload", "multiload", "tpump", "arcmain"}
)

# Script content above this size is recorded by path, size, and digest only.
MAXIMUM_ATTACHED_SCRIPT_BYTES = 64 * 1024

_SEPARATORS = frozenset({"|", "||", "&&", ";", "&", "(", ")"})
_HEREDOC = re.compile(
    r"<<-?\s*(['\"]?)(?P<marker>[^\s'\"<>;|&]+)\1(?P<rest>[^\n]*)\n(?P<body>.*?)\n[ \t]*(?P=marker)[ \t]*(?:\n|$)",
    re.DOTALL,
)
_JINJA_EXPRESSION = re.compile(r"\{\{.*?\}\}|\{%.*?%\}", re.DOTALL)
_SCRIPT_ARGUMENT = re.compile(r"(?:<|-f)\s*([^\s;|&<>]+)")
_ASSIGNMENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=", re.DOTALL)

# Commands that run the command after them, with the options of theirs that take a value.
_PREFIX_COMMANDS: dict[str, frozenset[str]] = {
    "sudo": frozenset({"-u", "-g", "-C", "-p", "-h"}),
    "env": frozenset({"-u", "-C", "-S"}),
    "nice": frozenset({"-n"}),
    "nohup": frozenset(),
    "time": frozenset(),
    "exec": frozenset({"-a"}),
    "command": frozenset(),
    "stdbuf": frozenset({"-i", "-o", "-e"}),
}
# Commands whose arguments hold another command to run, such as ``ssh host "bteq < x"`` or ``bash -c "..."``.
_WRAPPER_COMMANDS = frozenset({"ssh", "sh", "bash", "ksh", "zsh", "dash", "su"})


@dataclass(slots=True, kw_only=True)
class _Segment:
    """One simple command: its command name, its arguments, and whether it pipes into the next segment."""

    name: str
    arguments: list[str] = field(default_factory=list)
    pipes_to_next: bool = False


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


def _strip_heredocs(command: str) -> str:
    return _HEREDOC.sub(lambda match: f"{match.group('rest')}\n", command)


def _shell_words(command: str) -> list[str] | None:
    """Splits a command into shell words and separators, with here-document bodies removed and Jinja kept whole.

    Each line break becomes a separator, so every line of a script starts a new command. Returns None when
    the command still does not parse.
    """
    masked, expressions = _mask_jinja(_strip_heredocs(command))
    lexer = shlex.shlex(masked.replace("\n", "\n;"), posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    try:
        return [_unmask(word, expressions) for word in lexer]
    except ValueError:
        return None


def _command_name(word: str) -> str:
    return word.lstrip("`").rsplit("/", 1)[-1]


def _command_start(words: list[str]) -> int:
    """Returns the index of a segment's command word, past variable assignments and prefix commands."""
    index = 0
    while index < len(words):
        word = words[index]
        name = _command_name(word)
        if _ASSIGNMENT.match(word):
            index += 1
        elif name in _PREFIX_COMMANDS:
            index += 1
            while index < len(words) and (words[index].startswith("-") or _ASSIGNMENT.match(words[index])):
                index += 2 if words[index] in _PREFIX_COMMANDS[name] else 1
        elif name == "timeout":
            index += 1
            while index < len(words) and words[index].startswith("-"):
                index += 1
            index += 1
        else:
            return index
    return index


def _segments(command: str) -> list[_Segment] | None:
    """Breaks a command into simple commands, or returns None when it does not parse."""
    words = _shell_words(command)
    if words is None:
        return None
    segments: list[_Segment] = []
    current: list[str] = []

    def close(separator: str | None) -> None:
        start = _command_start(current)
        if start < len(current):
            segments.append(
                _Segment(
                    name=_command_name(current[start]),
                    arguments=current[start + 1 :],
                    pipes_to_next=separator == "|",
                )
            )
        current.clear()

    for word in words:
        if word in _SEPARATORS:
            close(word)
        else:
            current.append(word)
    close(None)
    return segments


def _utility_segments(command: str) -> list[_Segment] | None:
    """Returns the segments that run a Teradata utility, including commands nested inside wrappers."""
    segments = _segments(command)
    if segments is None:
        return None
    found: list[_Segment] = []
    for segment in segments:
        if segment.name in UTILITIES:
            found.append(segment)
        elif segment.name in _WRAPPER_COMMANDS:
            for argument in segment.arguments:
                if any(character.isspace() for character in argument):
                    found.extend(_utility_segments(argument) or [])
            if segment.name == "ssh":
                arguments = segment.arguments
                found.extend(
                    _Segment(name=_command_name(argument), arguments=arguments[index + 1 :])
                    for index, argument in enumerate(arguments)
                    if _command_name(argument) in UTILITIES
                )
    return found


def find_utilities(command: str) -> list[str]:
    """Returns the Teradata utilities a shell command runs, in order of first use.

    Only a command word counts: the first word of each command, after assignments and prefixes such as
    ``sudo`` or ``timeout 600``, including commands passed to ``ssh`` or ``bash -c``. A command that does
    not parse falls back to a search for utility names anywhere in it.
    """
    segments = _utility_segments(command)
    if segments is None:
        names = [_command_name(word) for word in re.findall(r"[\w./`-]+", _strip_heredocs(command))]
    else:
        names = [segment.name for segment in segments]
    found: list[str] = []
    for name in names:
        if name in UTILITIES and name not in found:
            found.append(name)
    return found


def script_references(command: str) -> list[str]:
    """Returns the script files a shell command feeds to its Teradata utilities.

    Recognizes ``utility < file``, ``utility -f file``, and ``cat file | utility``. A command that does not
    parse falls back to finding ``< file`` and ``-f file`` in each part that names a utility.
    """
    references: list[str] = []

    def add(path: str) -> None:
        if path not in references:
            references.append(path)

    segments = _segments(command)
    if segments is None:
        masked, expressions = _mask_jinja(_strip_heredocs(command))
        for part in re.split(r"[;&|\n]", masked):
            if any(_command_name(word) in UTILITIES for word in part.split()):
                for match in _SCRIPT_ARGUMENT.finditer(part):
                    add(_unmask(match.group(1), expressions))
        return references

    for segment in _utility_segments(command) or []:
        arguments = segment.arguments
        for index, argument in enumerate(arguments[:-1]):
            if argument in ("<", "-f"):
                add(arguments[index + 1])
    for previous, following in zip(segments, segments[1:], strict=False):
        if previous.name == "cat" and previous.pipes_to_next and following.name in UTILITIES:
            for path in previous.arguments:
                if not path.startswith("-") and path not in ("<", ">", ">>"):
                    add(path)
    return references


def heredoc_scripts(command: str) -> list[str]:
    """Returns the bodies of here-documents that feed a Teradata utility, where inline BTEQ usually lives.

    A here-document belongs to the command on its own line, such as ``bteq <<EOF`` or
    ``cat <<EOF | bteq``; one that writes a config file with ``cat <<EOF > cfg`` is not a script.
    """
    scripts: list[str] = []
    for match in _HEREDOC.finditer(command):
        line_start = command.rfind("\n", 0, match.start()) + 1
        line = command[line_start : match.start()] + match.group("rest")
        if find_utilities(line):
            scripts.append(match.group("body"))
    return scripts


def script_record(path: str, search_paths: tuple[Path, ...], *, remote: bool = False) -> dict[str, Any]:
    """Describes a script file a Teradata task runs, attaching its content when it is found and small.

    The file is read only from the DAG folder or template search path. A script on an SSH host is never
    read, because a local file at the same path is not the one the task runs.
    """
    if remote:
        return {"path": path, "unresolved": "is on the SSH host, so discover cannot read it"}
    candidate, detail = find_script_file(path, search_paths)
    if candidate is None:
        return {"path": path, "unresolved": detail}
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
