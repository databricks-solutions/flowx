"""Rewrite Amazon States Language JSONPath I/O into Databricks task parameters.

An ASL state threads JSON between states: ``Parameters`` builds the payload a
Task receives, with keys ending in ``.$`` carrying a JSONPath reference into the
state input, and ``ResultPath`` places a state's result back into that input for
downstream states to read. Databricks has no implicit state passing, so this
module rewrites each ``.$`` reference into an explicit Databricks dynamic value:

- a reference to a field a prior state produced becomes a task value,
  ``{{tasks.<producer>.values.<field>}}``,
- a reference to a field that traces to the state-machine input becomes a job
  parameter, ``{{job.parameters.<field>}}`` (and the parameter is declared).

The dataflow is tracked in execution order: the translator registers each Task's
``ResultPath`` field as it is visited, so a later Task's ``Parameters`` resolve
against the fields produced before it. References this pass cannot reduce
(context object ``$$``, intrinsic ``States.*`` functions, bracket notation,
whole-state ``$``) are left verbatim with a note.

See ``design/stepfunctions-jsonpath.md`` for the mapping and its limits.
"""

from __future__ import annotations

import json
import re
from typing import Any

_TOP_FIELD = re.compile(r"^\$\.([A-Za-z_]\w*)")


def result_field(definition: dict[str, Any]) -> str | None:
    """Returns the top-level field name a state writes its result under, or ``None``.

    ``None`` when ``ResultPath`` is ``null`` (result discarded), ``"$"`` or absent
    (the result replaces the whole state input, so there is no single field), or a
    path this pass does not track.
    """
    if "ResultPath" not in definition:
        return None
    result_path = definition["ResultPath"]
    if not isinstance(result_path, str):
        return None
    return _top_field(result_path)


def resolve_parameters(
    parameters: dict[str, Any],
    producers: dict[str, str],
    job_parameters: dict[str, str],
    notes: list[dict[str, Any]],
    state_name: str,
) -> dict[str, str]:
    """Resolves an ASL ``Parameters`` block into Databricks task parameters.

    Args:
        parameters: The state's raw ``Parameters`` mapping.
        producers: Field name -> producing task key, for fields written by prior states.
        job_parameters: Accumulator of declared job parameters (name -> default); mutated.
        notes: Accumulator for unresolved-reference notes; mutated.
        state_name: The state being resolved, for note attribution.

    Returns:
        A mapping of parameter name to a literal value or a Databricks dynamic
        value reference, suitable for a task's ``base_parameters``.
    """
    resolved: dict[str, str] = {}
    for key, value in parameters.items():
        if key.endswith(".$"):
            resolved[key[:-2]] = _resolve_reference(value, producers, job_parameters, notes, state_name)
        else:
            resolved[key] = value if isinstance(value, str) else json.dumps(value)
    return resolved


def _resolve_reference(
    path: Any,
    producers: dict[str, str],
    job_parameters: dict[str, str],
    notes: list[dict[str, Any]],
    state_name: str,
) -> str:
    """Resolves a single ASL ``.$`` reference to a Databricks dynamic value or literal."""
    if not isinstance(path, str):
        return str(path)
    if path.startswith("$$") or path.startswith("States."):
        notes.append(
            {"state": state_name, "issue": f"JSONPath {path!r} (context/intrinsic) not resolved; left verbatim"}
        )
        return path
    if not path.startswith("$"):
        return path
    field = _top_field(path)
    if field is None:
        notes.append({"state": state_name, "issue": f"JSONPath {path!r} not resolved; left verbatim"})
        return path
    remainder = path[len(f"$.{field}") :]
    if field in producers:
        if remainder:
            notes.append(
                {"state": state_name, "issue": f"{path!r} -> task value {field!r}; index {remainder!r} in the notebook"}
            )
        return f"{{{{tasks.{producers[field]}.values.{field}}}}}"
    job_parameters.setdefault(field, "")
    if remainder:
        notes.append(
            {"state": state_name, "issue": f"{path!r} -> job parameter {field!r}; index {remainder!r} in the notebook"}
        )
    return f"{{{{job.parameters.{field}}}}}"


def _top_field(path: str) -> str | None:
    """Returns the first dotted field of a ``$.field[...]`` path, or ``None`` for ``$`` / bracket notation."""
    match = _TOP_FIELD.match(path)
    return match.group(1) if match else None
