"""Environment variables a shell command reads from the machine it runs on.

A BashOperator or SSHOperator command runs with the Airflow worker's (or SSH host's) environment. A
``$NAME`` the command does not set itself comes from that machine, and Databricks compute does not have
it, so a task that reads one cannot run as translated.
"""

from __future__ import annotations

import re

# Variables every POSIX shell, or bash itself, provides, so they exist on Databricks compute too.
SHELL_PROVIDED = frozenset(
    {
        "HOME",
        "PATH",
        "PWD",
        "OLDPWD",
        "USER",
        "LOGNAME",
        "SHELL",
        "HOSTNAME",
        "TMPDIR",
        "LANG",
        "IFS",
        "RANDOM",
        "SECONDS",
        "LINENO",
        "UID",
        "EUID",
        "PPID",
        "OPTARG",
        "OPTIND",
        "REPLY",
        "PIPESTATUS",
        "FUNCNAME",
    }
)

_JINJA = re.compile(r"\{\{.*?\}\}|\{%.*?%\}|\{#.*?#\}", re.DOTALL)
_QUOTED_HEREDOC = re.compile(
    r"<<-?\s*(['\"])(?P<marker>[^\s'\"<>;|&]+)\1[^\n]*\n.*?\n[ \t]*(?P=marker)[ \t]*(?:\n|$)", re.DOTALL
)
_NAME = r"[A-Za-z_][A-Za-z0-9_]*"
_REFERENCE = re.compile(rf"\$(?:\{{[#!]?(?P<braced>{_NAME})|(?P<plain>{_NAME}))")
_ASSIGNED = (
    re.compile(rf"(?:^|[\s;&|(`]|\$\()(?P<name>{_NAME})(?:\[[^\]]*\])?\+?="),
    re.compile(rf"\bfor\s+(?P<name>{_NAME})\s+in\b"),
    re.compile(rf"\bgetopts\s+\S+\s+(?P<name>{_NAME})"),
)
_READ = re.compile(rf"\bread\s+(?:-[A-Za-z]+\s+(?:\S+\s+)?)*(?P<names>{_NAME}(?:[ \t]+{_NAME})*)")


def _unquoted_text(command: str) -> str:
    """Blanks out single-quoted text and backslash-escaped dollars, which the shell does not expand."""
    output: list[str] = []
    index = 0
    in_double = False
    while index < len(command):
        character = command[index]
        if character == "\\" and index + 1 < len(command):
            output.append("  ")
            index += 2
            continue
        if character == '"':
            in_double = not in_double
        elif character == "'" and not in_double:
            end = command.find("'", index + 1)
            end = len(command) if end == -1 else end
            output.append(" " * (end + 1 - index))
            index = end + 1
            continue
        output.append(character)
        index += 1
    return "".join(output)


def assigned_names(command: str) -> set[str]:
    """Returns the variable names a command sets itself, by assignment, ``for``, ``read``, or ``getopts``."""
    names = {match.group("name") for pattern in _ASSIGNED for match in pattern.finditer(command)}
    for match in _READ.finditer(command):
        names.update(match.group("names").split())
    return names


def environment_references(command: str) -> list[str]:
    """Returns the environment variables a shell command reads but never sets, in order of first use.

    Special parameters (``$1``, ``$?``, ``$@``), variables every shell provides, text in single quotes or
    quoted here-documents, and Jinja expressions are not references.
    """
    text = _unquoted_text(_QUOTED_HEREDOC.sub("\n", _JINJA.sub(" ", command)))
    assigned = assigned_names(text)
    references: list[str] = []
    for match in _REFERENCE.finditer(text):
        name = match.group("braced") or match.group("plain")
        if name not in assigned and name not in SHELL_PROVIDED and name not in references:
            references.append(name)
    return references
