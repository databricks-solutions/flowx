"""Collects what translating one Oozie app found, for the inventory and the pipeline's findings."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

from flowx.sources.oozie.loader.properties import STRIPPED_PROPERTIES, PropertyScope
from flowx.sources.oozie.rules import RULES

_HDFS_REFERENCE = re.compile(r"(?:s?web)?hdfs://|viewfs://|maprfs://|\$\{nameNode\}", re.IGNORECASE)
_PLAIN_PROPERTY_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_.-]*)\}")


def expand_properties(text: str, properties: PropertyScope, _expanding: tuple[str, ...] = ()) -> str:
    """Replaces each plain ``${name}`` with its value, recursively, so a path hidden behind a property shows.

    ``${nameNode}`` and the other cluster addresses stay as written (they're how HDFS paths are spotted), as do
    undefined properties, EL functions, and any property already being expanded (a cycle).
    """

    def substitute(match: re.Match[str]) -> str:
        name = match.group(1)
        value = properties.lookup(name)
        if name in STRIPPED_PROPERTIES or value is None or name in _expanding:
            return match.group(0)
        return expand_properties(value, properties, (*_expanding, name))

    return _PLAIN_PROPERTY_REFERENCE.sub(substitute, text)


@dataclass(slots=True, kw_only=True)
class AppReport:
    hdfs_paths: list[dict[str, str]] = field(default_factory=list)
    missing_files: list[dict[str, str]] = field(default_factory=list)
    dropped_spark_conf: list[dict[str, str]] = field(default_factory=list)
    credentials_dropped: list[str] = field(default_factory=list)
    kill_messages: dict[str, str] = field(default_factory=dict)
    agentic_reasons: dict[str, str] = field(default_factory=dict)
    node_types: dict[str, str] = field(default_factory=dict)
    findings: list[dict[str, Any]] = field(default_factory=list)
    transformations: list[dict[str, str]] = field(default_factory=list)

    def note(self, rule: str, node: str, detail: str = "") -> None:
        """Records that *rule* was applied to *node* (unknown rule IDs are a programming error)."""
        if rule not in RULES:
            raise KeyError(f"unknown Oozie rule id {rule!r}")
        self.transformations.append({"rule": rule, "node": node, "detail": detail})

    def gap(self, code: str, message: str, details: dict[str, Any] | None = None) -> None:
        self.findings.append({"code": code, "severity": "gap", "message": message, "details": details or {}})

    def record_hdfs_paths(self, node: str, field_name: str, text: str, properties: PropertyScope | None = None) -> None:
        """Records *text* when it points at HDFS, since paths are left unchanged (spec decision B).

        With *properties*, ``${name}`` references are expanded first, and the expanded path is what's recorded.
        """
        path = expand_properties(text, properties) if properties is not None else text
        if _HDFS_REFERENCE.search(path):
            self.hdfs_paths.append({"action": node, "field": field_name, "path": path})

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("findings")
        payload.pop("transformations")
        return payload
