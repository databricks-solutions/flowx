"""Programs a shell command runs that Databricks compute does not provide.

A BashOperator or SSHOperator command runs on the Airflow worker or SSH host, which often has client
tools installed (``sqlcmd``, the AWS CLI, ``kubectl``, a vendor loader). A ``%sh`` notebook runs on
Databricks compute, which has a standard Linux userland but none of those, so a command that calls one
cannot run as translated.
"""

from __future__ import annotations

import re

from flowx.sources.airflow import teradata

# Shell builtins and reserved words, which every shell provides.
_SHELL_BUILTINS = frozenset(
    {
        ".",
        ":",
        "[",
        "[[",
        "]]",
        "{",
        "}",
        "!",
        "alias",
        "break",
        "builtin",
        "caller",
        "case",
        "cd",
        "command",
        "continue",
        "declare",
        "do",
        "done",
        "echo",
        "elif",
        "else",
        "esac",
        "eval",
        "exec",
        "exit",
        "export",
        "false",
        "fi",
        "for",
        "function",
        "getopts",
        "hash",
        "if",
        "in",
        "jobs",
        "kill",
        "let",
        "local",
        "mapfile",
        "popd",
        "printf",
        "pushd",
        "pwd",
        "read",
        "readarray",
        "readonly",
        "return",
        "select",
        "set",
        "shift",
        "shopt",
        "source",
        "test",
        "then",
        "time",
        "trap",
        "true",
        "type",
        "typeset",
        "ulimit",
        "umask",
        "unset",
        "until",
        "wait",
        "while",
    }
)

# Programs on the PATH of Databricks compute: GNU coreutils and the usual Linux userland, plus Python.
AVAILABLE_PROGRAMS = frozenset(
    {
        "awk",
        "base64",
        "basename",
        "bash",
        "bunzip2",
        "bzip2",
        "cat",
        "chmod",
        "comm",
        "cp",
        "curl",
        "cut",
        "date",
        "dd",
        "df",
        "diff",
        "dirname",
        "du",
        "egrep",
        "env",
        "expr",
        "fgrep",
        "find",
        "grep",
        "gunzip",
        "gzip",
        "head",
        "hostname",
        "id",
        "join",
        "ln",
        "ls",
        "md5sum",
        "mkdir",
        "mktemp",
        "mv",
        "nice",
        "nohup",
        "paste",
        "pip",
        "pip3",
        "printenv",
        "python",
        "python3",
        "readlink",
        "realpath",
        "rm",
        "rmdir",
        "sed",
        "seq",
        "sh",
        "sha1sum",
        "sha256sum",
        "sleep",
        "sort",
        "split",
        "stat",
        "tail",
        "tar",
        "tee",
        "timeout",
        "touch",
        "tr",
        "truncate",
        "uname",
        "uniq",
        "unzip",
        "wc",
        "wget",
        "which",
        "whoami",
        "xargs",
        "yes",
        "zcat",
        "zip",
    }
)

_DEFINED_FUNCTION = re.compile(r"(?:^|[;&|\n{(]\s*)(?:function\s+)?([A-Za-z_][\w-]*)\s*\(\s*\)")


def unavailable_programs(command: str) -> list[str]:
    """Returns the programs a shell command runs that Databricks compute does not have, in order of first use.

    Builtins, reserved words, functions the command defines, and standard Linux and Python programs are
    available. A program named by path (``./load.sh``, ``/opt/tools/bin/load``) is not, since the file is on
    the source machine. A command that does not parse yields nothing; it is reported for that reason.
    """
    names = teradata.command_names(command)
    if names is None:
        return []
    defined = set(_DEFINED_FUNCTION.findall(command))
    return [
        name
        for name in names
        if name not in _SHELL_BUILTINS
        and name not in AVAILABLE_PROGRAMS
        and name not in defined
        and not name.startswith("$")
        and "\x00" not in name
    ]
