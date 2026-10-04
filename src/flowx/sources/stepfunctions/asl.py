"""Amazon States Language (ASL) parsing for the Step Functions source.

Parses a state machine definition -- either a bare ASL document
(``{"StartAt": ..., "States": {...}}``) or an AWS ``describe-state-machine``
payload that wraps the ASL JSON under a ``definition`` key -- into a typed,
read-only model the translator walks. Parsing never raises on unknown state
types; the translator decides how each state maps to the flowx IR.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class State:
    """A single Amazon States Language state.

    Attributes:
        name: The state's key within the ``States`` map.
        type: The ASL ``Type`` (``Task``, ``Choice``, ``Map``, ``Parallel``,
            ``Wait``, ``Pass``, ``Succeed``, ``Fail``).
        definition: The raw state dictionary, read by the translator for
            type-specific fields (``Resource``, ``Next``, ``Choices``, ...).
    """

    name: str
    type: str
    definition: dict[str, Any]

    @property
    def next_state(self) -> str | None:
        """Returns the ``Next`` transition target, or ``None`` when the path ends here."""
        return self.definition.get("Next")

    @property
    def is_terminal(self) -> bool:
        """Reports whether the state ends its path (``End: true`` or a terminal type)."""
        return bool(self.definition.get("End")) or self.type in ("Succeed", "Fail")


@dataclass(frozen=True, slots=True)
class StateMachine:
    """A parsed Step Functions state machine.

    Attributes:
        name: Logical name, taken from the describe payload's ``name``, the
            ``stateMachineArn``, or the source file stem.
        start_at: The ``StartAt`` state name.
        states: Mapping of state name to :class:`State`.
        comment: The optional top-level ASL ``Comment``.
    """

    name: str
    start_at: str
    states: dict[str, State]
    comment: str | None = None


def parse_state_machine(raw: dict[str, Any], *, default_name: str) -> StateMachine:
    """Parses a raw ASL or describe-state-machine payload into a :class:`StateMachine`.

    Args:
        raw: Parsed JSON of a bare ASL document or a describe-state-machine payload.
        default_name: Name to use when the payload carries no ``name`` or ``stateMachineArn``.

    Returns:
        The typed state machine.

    Raises:
        ValueError: When the document has no ``States`` map or no ``StartAt`` entry.
    """
    name = raw.get("name") or _name_from_arn(raw.get("stateMachineArn")) or default_name
    body = _definition_body(raw)
    states_raw = body.get("States")
    start_at = body.get("StartAt")
    if not isinstance(states_raw, dict) or not isinstance(start_at, str):
        raise ValueError(f"{name!r} is not a valid state machine: missing 'States' or 'StartAt'.")
    states = {
        state_name: State(name=state_name, type=str(spec.get("Type", "")), definition=spec)
        for state_name, spec in states_raw.items()
        if isinstance(spec, dict)
    }
    return StateMachine(name=name, start_at=start_at, states=states, comment=body.get("Comment"))


def iter_substates(definition: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Returns the ``(StartAt, States)`` sub-machines nested in a Map or Parallel state.

    A ``Map`` state carries one sub-machine under ``ItemProcessor`` (or the
    legacy ``Iterator``); a ``Parallel`` state carries one per entry of
    ``Branches``. Each is itself an ASL body with its own ``StartAt``/``States``.

    Args:
        definition: The raw Map or Parallel state dictionary.

    Returns:
        A list of ``(start_at, states)`` pairs, one per nested sub-machine.
    """
    branches = definition.get("Branches")
    if isinstance(branches, list):
        return [(body["StartAt"], body["States"]) for body in branches if _is_asl_body(body)]
    processor = definition.get("ItemProcessor") or definition.get("Iterator")
    if isinstance(processor, dict) and _is_asl_body(processor):
        return [(processor["StartAt"], processor["States"])]
    return []


def build_states(states_raw: dict[str, Any]) -> dict[str, State]:
    """Builds a ``{name: State}`` map from a raw ASL ``States`` dictionary."""
    return {
        name: State(name=name, type=str(spec.get("Type", "")), definition=spec)
        for name, spec in states_raw.items()
        if isinstance(spec, dict)
    }


def load_state_machine_files(source_dir: Path) -> list[tuple[str, dict[str, Any]]]:
    """Loads every state machine JSON file under *source_dir*.

    Args:
        source_dir: A single ``.json`` file or a directory scanned recursively.

    Returns:
        ``(file_stem, raw_json)`` pairs in sorted path order. Files that are not
        readable JSON objects are skipped.
    """
    files = [source_dir] if source_dir.is_file() else sorted(source_dir.rglob("*.json"))
    results: list[tuple[str, dict[str, Any]]] = []
    for path in files:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(raw, dict):
            results.append((path.stem, raw))
    return results


def _definition_body(raw: dict[str, Any]) -> dict[str, Any]:
    """Returns the ASL body, unwrapping a describe-state-machine payload when present."""
    if "States" in raw:
        return raw
    definition = raw.get("definition")
    if isinstance(definition, str):
        try:
            return json.loads(definition)
        except json.JSONDecodeError:
            return raw
    if isinstance(definition, dict):
        return definition
    return raw


def _name_from_arn(arn: Any) -> str | None:
    """Returns the trailing name segment of a state machine ARN, or ``None``."""
    if not isinstance(arn, str) or not arn:
        return None
    return arn.rsplit(":", 1)[-1] or None


def _is_asl_body(candidate: Any) -> bool:
    """Reports whether *candidate* is a dict with ``StartAt`` and a ``States`` map."""
    return (
        isinstance(candidate, dict)
        and isinstance(candidate.get("StartAt"), str)
        and isinstance(candidate.get("States"), dict)
    )
