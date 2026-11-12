"""dbt commands run from a shell task, such as a BashOperator running ``dbt run --select orders``.

A shell command maps to a Databricks dbt task only when it does nothing but run dbt: an optional ``cd``
into the project, then dbt commands joined by ``&&``. Anything else, such as a pipe, a redirect, a
command substitution, or a variable in an argument, is left to the general shell handling.
"""

from __future__ import annotations

import posixpath
import re
import shlex
from dataclasses import dataclass, field

# dbt subcommands a Databricks dbt task can run.
SUBCOMMANDS = frozenset(
    {
        "build",
        "clean",
        "clone",
        "compile",
        "debug",
        "deps",
        "docs",
        "list",
        "ls",
        "parse",
        "retry",
        "run",
        "run-operation",
        "seed",
        "show",
        "snapshot",
        "source",
        "test",
    }
)

# Options that choose a dbt profile. Databricks generates the profile from the task's warehouse, so they
# are dropped from the command and reported.
_PROFILE_OPTIONS = frozenset({"--profiles-dir", "--profile", "--target", "-t"})
_PROJECT_OPTION = "--project-dir"
# A command word naming the dbt executable through a variable, such as ``$DBT_PATH`` or ``${DBT_BIN}``.
_DBT_VARIABLE = re.compile(r"\$\{?[A-Za-z0-9_]*dbt[A-Za-z0-9_]*\}?", re.IGNORECASE)
_SEPARATORS = frozenset({"&&", ";"})
_PUNCTUATION = ";&|<>()"


@dataclass(slots=True, kw_only=True)
class ShellDbt:
    """What a dbt-only shell command runs.

    Attributes:
        commands: Each dbt command, starting with ``dbt``, without the profile options.
        project_dir: The project folder from ``cd``, ``--project-dir``, or the task's working directory,
            or None when the command does not say.
        executable: The dbt executable as the command names it, such as ``dbt`` or ``$DBT_PATH``.
        dropped_options: The profile options removed from the commands, as written.
    """

    commands: list[str]
    project_dir: str | None
    executable: str
    dropped_options: list[str] = field(default_factory=list)


def _is_dbt_executable(word: str) -> bool:
    return word == "dbt" or word.endswith("/dbt") or _DBT_VARIABLE.fullmatch(word) is not None


def _join(base: str | None, path: str) -> str:
    return posixpath.normpath(posixpath.join(base, path)) if base and not posixpath.isabs(path) else path


def _segments(command: str) -> list[tuple[str | None, list[str]]] | None:
    """Splits a command into ``(separator before it, words)`` simple commands, or None if it does not parse."""
    lexer = shlex.shlex(command.replace("\\\n", " ").replace("\n", " ; "), posix=True, punctuation_chars=_PUNCTUATION)
    lexer.whitespace_split = True
    lexer.commenters = ""
    try:
        words = list(lexer)
    except ValueError:
        return None
    segments: list[tuple[str | None, list[str]]] = []
    separator: str | None = None
    current: list[str] = []
    for word in [*words, ";"]:
        if word in _SEPARATORS:
            if current:
                segments.append((separator, current))
                current = []
            separator = word
        elif set(word) <= set(_PUNCTUATION) or word in ("{", "}") or word.startswith("#") or "`" in word:
            # A pipe, redirect, subshell, background job, comment, brace group, or command substitution.
            return None
        else:
            current.append(word)
    return segments


def parse(command: str, working_directory: str | None = None) -> ShellDbt | None:
    """Returns what a dbt-only shell command runs, or None when the command does anything else.

    *working_directory* is the task's ``cwd``, which a relative ``cd`` or ``--project-dir`` starts from.
    """
    if "{{" in command or "{%" in command:
        return None
    segments = _segments(command)
    if not segments:
        return None
    project_dir = working_directory
    executable: str | None = None
    commands: list[str] = []
    dropped: list[str] = []
    for separator, words in segments:
        if words[0] == "cd" and not commands:
            if len(words) != 2 or "$" in words[1]:
                return None
            project_dir = _join(project_dir, words[1])
            continue
        if not _is_dbt_executable(words[0]) or len(words) < 2 or words[1] not in SUBCOMMANDS:
            return None
        if commands and separator != "&&":
            # After ``;`` the shell runs the next command even when this one fails; a dbt task stops.
            return None
        if executable not in (None, words[0]):
            return None
        executable = words[0]
        arguments: list[str] = []
        index = 1
        while index < len(words):
            word = words[index]
            name, has_value, value = word.partition("=")
            if "$" in word:
                return None
            if name in _PROFILE_OPTIONS or name == _PROJECT_OPTION:
                if not has_value:
                    if index + 1 >= len(words):
                        return None
                    value = words[index + 1]
                    index += 1
                if name == _PROJECT_OPTION:
                    project_dir = _join(project_dir, value)
                else:
                    dropped.append(f"{name} {value}")
            else:
                arguments.append(word)
            index += 1
        commands.append(shlex.join(["dbt", *arguments]))
    if executable is None:
        return None
    return ShellDbt(commands=commands, project_dir=project_dir, executable=executable, dropped_options=dropped)
