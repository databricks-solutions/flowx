"""Read ``job.properties`` and look up Oozie properties with the right precedence.

Oozie resolves ``${name}`` from several layers: an action's ``<configuration>`` first, then the workflow's
``<global>``, then ``job.properties`` (later: the coordinator's configuration), then ``<parameters>`` defaults.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Cluster addresses Oozie needs and Databricks doesn't; ``${nameNode}/data`` should become ``/data``.
STRIPPED_PROPERTIES: frozenset[str] = frozenset({"nameNode", "jobTracker", "resourceManager"})

_SEPARATOR = re.compile(r"(?<!\\)[=:]|(?<!\\)\s")
_ESCAPES = {"t": "\t", "n": "\n", "r": "\r", "f": "\f"}
_UNICODE_ESCAPE = re.compile(r"\\u([0-9A-Fa-f]{4})")
_PARAMETER_UNSAFE = re.compile(r"[^A-Za-z0-9_]")


def _unescape(text: str) -> str:
    """Decodes Java escapes (``\\t``, ``\\uXXXX``, ``\\x`` -> ``x``). A malformed ``\\u`` stays literal."""
    result: list[str] = []
    index = 0
    while index < len(text):
        character = text[index]
        if character != "\\" or index + 1 >= len(text):
            result.append(character)
            index += 1
            continue
        following = text[index + 1]
        if following == "u":
            unicode_match = _UNICODE_ESCAPE.match(text, index)
            if unicode_match is None:
                result.append("\\u")
                index += 2
                continue
            result.append(chr(int(unicode_match.group(1), 16)))
            index = unicode_match.end()
            continue
        result.append(_ESCAPES.get(following, following))
        index += 2
    return "".join(result)


def _logical_lines(text: str) -> list[str]:
    """Joins backslash-continued lines, dropping the continuation's leading whitespace (Java semantics)."""
    lines: list[str] = []
    pending = ""
    for raw_line in text.splitlines():
        if not pending and raw_line.lstrip()[:1] in ("#", "!"):
            lines.append(raw_line)
            continue
        line = raw_line.lstrip() if pending else raw_line
        trailing = len(line) - len(line.rstrip("\\"))
        if trailing % 2 == 1:
            pending += line[:-1]
            continue
        lines.append(pending + line)
        pending = ""
    if pending:
        lines.append(pending)
    return lines


def parse_properties(text: str) -> dict[str, str]:
    """Parses Java ``.properties`` text (the format of Oozie's ``job.properties``)."""
    values: dict[str, str] = {}
    for line in _logical_lines(text):
        stripped = line.strip()
        if not stripped or stripped[0] in "#!":
            continue
        match = _SEPARATOR.search(stripped)
        if match is None:
            values[_unescape(stripped)] = ""
            continue
        key = stripped[: match.start()]
        remainder = stripped[match.end() :].lstrip()
        if match.group().isspace() and remainder[:1] in ("=", ":"):
            remainder = remainder[1:].lstrip()
        values[_unescape(key)] = _unescape(remainder)
    return values


def parameter_name(property_name: str) -> str:
    """Turns an Oozie property name into a job-parameter name that's safe inside ``{{job.parameters.X}}``."""
    return _PARAMETER_UNSAFE.sub("_", property_name)


@dataclass(frozen=True, slots=True, kw_only=True)
class PropertyScope:
    """Layers of property values, highest precedence first."""

    layers: tuple[dict[str, str], ...]

    def lookup(self, name: str) -> str | None:
        """Returns the value from the first layer that defines *name*, or ``None``."""
        for layer in self.layers:
            if name in layer:
                return layer[name]
        return None

    def with_layer(self, layer: dict[str, str]) -> PropertyScope:
        """Returns a new scope with *layer* on top (for an action's own ``<configuration>``)."""
        return PropertyScope(layers=(layer, *self.layers))
