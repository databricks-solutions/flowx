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
from flowx.sources.airflow.templating import CLOSED_JINJA_STATEMENT_OR_COMMENT, JINJA_STATEMENT_OR_COMMENT

UTILITIES = frozenset(
    {"bteq", "tbuild", "tdload", "fastload", "fastexport", "fexp", "mload", "multiload", "tpump", "arcmain"}
)

# Script content above this size is recorded by path, size, and digest only.
MAXIMUM_ATTACHED_SCRIPT_BYTES = 64 * 1024

# The token a line break becomes, so each line of a script starts a new command and ends a comment.
_LINE_BREAK = "\x01"
_SEPARATORS = frozenset({"|", "||", "&&", ";", "&", "(", ")", _LINE_BREAK})
_HEREDOC = re.compile(
    r"<<-?\s*(['\"]?)(?P<marker>[^\s'\"<>;|&]+)\1(?P<rest>[^\n]*)\n(?P<body>.*?)\n[ \t]*(?P=marker)[ \t]*(?:\n|$)",
    re.DOTALL,
)
_JINJA_EXPRESSION = re.compile(r"\{\{.*?\}\}", re.DOTALL)
_ASSIGNMENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=", re.DOTALL)

# Shell keywords that can stand before a command, as in ``do bteq < x`` or ``if ...; then bteq < x``.
_KEYWORDS = frozenset({"if", "then", "elif", "else", "do", "while", "until", "!", "{", "time"})
# Commands that run the command after them, with the options of theirs that take a value.
_PREFIX_COMMANDS: dict[str, frozenset[str]] = {
    "sudo": frozenset({"-u", "-g", "-C", "-p", "-h"}),
    "env": frozenset({"-u", "-C", "-S"}),
    "nice": frozenset({"-n"}),
    "nohup": frozenset(),
    "exec": frozenset({"-a"}),
    "command": frozenset(),
    "stdbuf": frozenset({"-i", "-o", "-e"}),
    "xargs": frozenset({"-I", "-n", "-P", "-L", "-d", "-E", "-s", "-a"}),
    "timeout": frozenset({"-s", "-k", "--signal", "--kill-after"}),
}
# Prefix commands that take a positional value before the command, such as the duration of ``timeout 600``.
_PREFIX_POSITIONAL_VALUES = {"timeout": 1}
# Shells that run the string after ``-c``; ``su`` runs it as another user.
_SHELL_COMMANDS = frozenset({"sh", "bash", "ksh", "zsh", "dash", "su"})
_SSH_OPTIONS_WITH_VALUES = frozenset(
    {
        "-B",
        "-b",
        "-c",
        "-D",
        "-E",
        "-e",
        "-F",
        "-I",
        "-i",
        "-J",
        "-L",
        "-l",
        "-m",
        "-O",
        "-o",
        "-p",
        "-Q",
        "-R",
        "-S",
        "-W",
        "-w",
    }
)


@dataclass(slots=True, kw_only=True)
class _Segment:
    """One simple command: its command name, its arguments, and whether it pipes into the next segment."""

    name: str
    arguments: list[str] = field(default_factory=list)
    pipes_to_next: bool = False


def _mask_jinja(command: str) -> tuple[str, dict[str, str]]:
    """Replaces Jinja expressions with space-free markers so a path like ``x_{{ ds }}.bteq`` stays one word.

    Closed Jinja statements and comments are replaced by a command separator.
    """
    command = CLOSED_JINJA_STATEMENT_OR_COMMENT.sub(" ; ", command)
    expressions: dict[str, str] = {}

    def mask(match: re.Match[str]) -> str:
        marker = f"\x00{len(expressions)}\x00"
        expressions[marker] = match.group(0)
        return marker

    return _JINJA_EXPRESSION.sub(mask, command), expressions


def _unmask(word: str, expressions: dict[str, str]) -> str:
    for marker, expression in expressions.items():
        word = word.replace(marker, expression)
    return word.replace(_LINE_BREAK, "\n")


def _strip_heredocs(command: str) -> str:
    return _HEREDOC.sub(lambda match: f"{match.group('rest')}\n", command)


def _shell_words(command: str) -> list[str] | None:
    """Splits a command into shell words and separators, with here-document bodies and comments removed.

    Jinja expressions stay whole, each line break becomes a separator, and ``#`` starts a comment only at
    the start of a word, as in bash. Returns None when the command does not parse.
    """
    masked, expressions = _mask_jinja(_strip_heredocs(command))
    lexer = shlex.shlex(masked.replace("\n", f" {_LINE_BREAK} "), posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    try:
        raw_words = list(lexer)
    except ValueError:
        return None
    words: list[str] = []
    in_comment = False
    for word in raw_words:
        if word == _LINE_BREAK:
            in_comment = False
            words.append(word)
        elif not in_comment and word.startswith("#"):
            in_comment = True
        elif not in_comment:
            words.append(word if word in _SEPARATORS else _unmask(word, expressions))
    return words


def parses(command: str) -> bool:
    """Returns whether a shell command, and every command a wrapper in it runs, parses outside here-documents.

    Unlike utility detection, this never falls back to the text before an unclosed Jinja tag.
    """
    return _utility_segments(command, lenient=False) is not None


def _command_name(word: str) -> str:
    return word.strip("`").rsplit("/", 1)[-1]


def _command_start(words: list[str]) -> int:
    """Returns the index of a segment's command word, past keywords, assignments, and prefix commands."""
    index = 0
    while index < len(words):
        word = words[index]
        name = _command_name(word)
        if word in _KEYWORDS or _ASSIGNMENT.match(word):
            index += 1
        elif name in _PREFIX_COMMANDS:
            index += 1
            while index < len(words) and (words[index].startswith("-") or _ASSIGNMENT.match(words[index])):
                index += 2 if words[index] in _PREFIX_COMMANDS[name] else 1
            index += _PREFIX_POSITIONAL_VALUES.get(name, 0)
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
                    arguments=[argument.strip("`") for argument in current[start + 1 :]],
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


def _lenient_segments(command: str) -> list[_Segment] | None:
    """Breaks a command into simple commands, retrying on the text before an unclosed Jinja tag if it fails."""
    segments = _segments(command)
    if segments is not None:
        return segments
    masked = CLOSED_JINJA_STATEMENT_OR_COMMENT.sub(" ; ", command)
    unclosed = JINJA_STATEMENT_OR_COMMENT.search(masked)
    return None if unclosed is None else _segments(masked[: unclosed.start()])


def _nested_commands(segment: _Segment) -> list[str]:
    """Returns the command strings a wrapper runs: the ``-c`` string of a shell, or an ``ssh`` remote command."""
    arguments = segment.arguments
    if segment.name in _SHELL_COMMANDS:
        for index, argument in enumerate(arguments[:-1]):
            if argument.startswith("-") and not argument.startswith("--") and "c" in argument[1:]:
                return [arguments[index + 1]]
        return []
    if segment.name == "ssh":
        index = 0
        while index < len(arguments) and arguments[index].startswith("-"):
            index += 2 if arguments[index] in _SSH_OPTIONS_WITH_VALUES else 1
        remote = arguments[index + 1 :]
        return [" ".join(remote)] if remote else []
    return []


def _utility_segments(command: str, *, lenient: bool = True) -> list[_Segment] | None:
    """Returns the segments that run a Teradata utility, including commands nested inside wrappers.

    Returns None when the command or any command nested in it does not parse. When *lenient*, a command
    that does not parse is retried on the text before its first unclosed Jinja tag.
    """
    segments = (_lenient_segments if lenient else _segments)(command)
    if segments is None:
        return None
    found: list[_Segment] = []
    for segment in segments:
        if segment.name in UTILITIES:
            found.append(segment)
        for nested in _nested_commands(segment):
            nested_found = _utility_segments(nested, lenient=lenient)
            if nested_found is None:
                return None
            found.extend(nested_found)
    return found


def find_utilities(command: str) -> list[str]:
    """Returns the Teradata utilities a shell command runs, in order of first use.

    Only a command word counts: the first word of each command, after keywords, assignments, and prefixes
    such as ``sudo`` or ``timeout 600``, including the command a shell runs with ``-c`` and an ``ssh`` remote
    command. A command that does not parse, or wraps one that does not, yields nothing, unless it parses up to
    an unclosed Jinja tag; the caller turns it into its own gap.
    """
    found: list[str] = []
    for segment in _utility_segments(command) or []:
        if segment.name not in found:
            found.append(segment.name)
    return found


def script_references(command: str) -> list[str]:
    """Returns the script files a shell command feeds to its Teradata utilities.

    Recognizes ``utility < file``, ``utility -f file``, and ``cat file | utility``.
    """
    references: list[str] = []

    def add(path: str) -> None:
        if path not in references:
            references.append(path)

    for segment in _utility_segments(command) or []:
        arguments = segment.arguments
        for index, argument in enumerate(arguments[:-1]):
            if argument in ("<", "-f"):
                add(arguments[index + 1])
    segments = _lenient_segments(command) or []
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
    if _JINJA_EXPRESSION.search(path):
        return {"path": path, "unresolved": "contains a template expression that Airflow resolves at run time"}
    candidate, detail = find_script_file(path, search_paths)
    if candidate is None:
        return {"path": path, "unresolved": detail}
    return script_record_at(path, candidate)


def script_record_at(name: str, path: Path) -> dict[str, Any]:
    """Describes a script already found at *path*: its size and digest, and its content when small and UTF-8."""
    try:
        data = path.read_bytes()
    except OSError as error:
        return {"path": name, "unresolved": f"could not be read ({error})"}
    record: dict[str, Any] = {
        "path": name,
        "resolved_path": str(path),
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
