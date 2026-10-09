"""Phase-1 in-engine agentic conversion: apply a routing decision, then fill the gaps.

``convert`` always produces a deterministic ``.work/translation_report.json`` -- that translation is
**unchanged** by this module. Routing then decides, per connected component, whether each part
converts deterministically or agentically (:mod:`flowx.routing` computes the recommendation and
records the fingerprint-bound ``metadata/conversion_plan.json``). This module carries out the
**post-convert alteration and fill** the decision implies, using an immutable-baseline and pure-rebuild
pattern similar to :mod:`flowx.agentic`, always keeping the work in-engine so the package phase,
structural validation, and provenance all still apply:

* A report without a routing record is a fresh convert. When route first routes from one, it saves an
  exact copy of ``.work/translation_report.json`` and ``.work/gaps.json`` to ``.work/route_baseline/``
  and records their hashes; that copy is never written again, and later routes rebuild from it.
* The agent's conversion output lives in ``metadata/agentic_conversion.json``: one entry per agentic
  routing unit (the authored pipelines, their canonical hash and the history of earlier outputs it
  replaced), plus the fills of convert's own gaps merged after routing. Filling a unit again with the
  same pipelines is a no-op; different pipelines replace the stored entry and add to its history.
  Switching every unit back to deterministic writes the baseline back byte for byte (unless fills of
  convert's own gaps are stored). Route and the fill never write modify's configured copy.
* :func:`rebuild` is a pure function: baseline, then the stored gap fills, then the plan's agentic
  units and their stored outputs, giving the edited report + gaps + routing record. The same inputs
  always give identical bytes. The routing record holds each unit's outcome (deterministic,
  agentic-applied, or agentic-not-viable until filled) and a fingerprint.
* A routing unit is a component, or an accepted grouping of whole components the plan joins into one
  agentic unit. The agent authors the fill: a routed-agentic unit is filled **only** by
  ``fill-agentic`` (N pipelines -> M, e.g. one Lakeflow Connect pipeline, or one same-named pipeline
  to keep it 1:1). The name-matched :func:`flowx.ir_serde.merge_agentic_results` stays for convert's
  own gaps and refuses routed-agentic pipelines.
* :func:`validate_report_structurally` packages the merged report through the same
  ``prepare -> write_bundle`` path the package phase uses and runs the existing
  :func:`flowx.validate.bundle_invariants.check_bundle_dir` over the output, so a fill can never
  introduce duplicate keys, dangling dependencies, a cycle, or a dangling pipeline/run_job reference.

There is no LLM here: the tool edits the report and validates the authored fill; the fill itself is
supplied by the agent/harness, following the same ask -> author -> continue pattern as the existing
agentic flow.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from flowx.discovery_serde import canonical_sha256
from flowx.models.conversion_plan import DECISION_AGENTIC, ConversionPlan

# The report + gaps live under the shared output dir's transient .work/ folder, beside the pipeline IR.
WORK_DIRNAME = ".work"
REPORT_FILENAME = "translation_report.json"
GAPS_FILENAME = "gaps.json"
# The immutable baseline copies, saved on the first route from a fresh convert.
BASELINE_DIRNAME = "route_baseline"
BASELINE_REPORT_FILENAME = "translation_report.json"
BASELINE_GAPS_FILENAME = "gaps.json"
# The recorded plan + inventory the fill binds against live under metadata/, beside the agent's output.
METADATA_DIRNAME = "metadata"
INVENTORY_FILENAME = "inventory.json"
AGENTIC_OUTPUT_FILENAME = "agentic_conversion.json"

# Top-level report key holding the routing record. It lives in the report, so a fresh convert (which
# rewrites the report) clears it; the package phase and ir_serde read only the pipelines beside it.
ROUTING_RECORD_KEY = "_routing_record"

# What became of each routing unit in the record: a deterministic unit is left as convert wrote it;
# an agentic one stays "not viable" until fill-agentic fills it, and package refuses until then.
OUTCOME_DETERMINISTIC = "deterministic"
OUTCOME_AGENTIC_APPLIED = "agentic-applied"
OUTCOME_AGENTIC_NOT_VIABLE = "agentic-not-viable"

ROUTED_AGENTIC_MERGE_REFUSED = (
    "this pipeline is routed agentic; fill its component with fill-agentic "
    "(run fill-agentic again with new pipelines to change the approach)"
)

# Guidance stamped onto every placeholder the alteration produces.
_PLACEHOLDER_COMMENT = (
    "Routed agentic by the conversion plan; replace the whole unit with agent-authored pipeline(s) using fill-agentic."
)

# Where a task can hold nested tasks in the report IR (IfCondition / ForEach / Switch containers).
_NESTED_TASK_KEYS = ("inner_activities", "if_true_activities", "if_false_activities", "default_activities")


# --------------------------------------------------------------------------- #
# Reading the recorded / authored plan.
# --------------------------------------------------------------------------- #


def agentic_pipeline_names(plan: dict[str, Any]) -> set[str]:
    """Return the member pipelines of every component the plan decides ``"agentic"``.

    Reads either an authored plan (the agent-supplied ``{"components": [...]}``) or the recorded
    ``conversion_plan.json`` -- both carry ``members`` and ``decision`` per component.
    """
    names: set[str] = set()
    for component in plan.get("components", []):
        if isinstance(component, dict) and component.get("decision") == DECISION_AGENTIC:
            names.update(str(member) for member in component.get("members", []) if isinstance(member, str))
    return names


def routing_units(plan: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """The plan's routing units, keyed by unit id, in plan order.

    A unit is one component (its sorted members and decision), or an accepted suggested grouping,
    which joins its components into one agentic unit keyed by the grouping id that also lists the
    ``components`` it joins. The record, the fill, package and the audit all work per unit.
    """
    accepted = [
        grouping
        for grouping in plan.get("suggested_groupings") or []
        if isinstance(grouping, dict) and grouping.get("accepted") is True
    ]
    grouping_of = {
        str(component_id): grouping for grouping in accepted for component_id in grouping.get("components") or []
    }
    units: dict[str, dict[str, Any]] = {}
    for component in plan.get("components", []):
        if not isinstance(component, dict):
            continue
        component_id = str(component.get("component_id"))
        grouping = grouping_of.get(component_id)
        if grouping is None:
            units[component_id] = {
                "members": sorted(str(member) for member in component.get("members", [])),
                "decision": component.get("decision"),
            }
        elif str(grouping.get("grouping_id")) not in units:
            units[str(grouping.get("grouping_id"))] = {
                "members": sorted(str(member) for member in grouping.get("members") or []),
                "decision": DECISION_AGENTIC,
                "components": [str(component_id) for component_id in grouping.get("components") or []],
            }
    return units


# --------------------------------------------------------------------------- #
# The routing record stamped onto the report.
# --------------------------------------------------------------------------- #


def routing_record(report: Any) -> dict[str, Any] | None:
    """The routing record a report carries, or ``None`` when route has not edited it."""
    record = report.get(ROUTING_RECORD_KEY) if isinstance(report, dict) else None
    return record if isinstance(record, dict) else None


def routing_record_mismatches(record: dict[str, Any], plan: dict[str, Any]) -> list[str]:
    """Where a report's routing record disagrees with a plan's units, members or decisions.

    Returns one message per unit that differs; an empty list means the record was routed under these
    same decisions.
    """
    recorded = record.get("components")
    if not isinstance(recorded, dict):
        recorded = {}
    planned = routing_units(plan)
    mismatches: list[str] = []
    for unit_id in sorted(set(recorded) | set(planned)):
        entry = recorded.get(unit_id)
        decided = planned.get(unit_id)
        if not isinstance(entry, dict):
            mismatches.append(f"component {unit_id!r} is in the plan but not in the routing record")
        elif decided is None:
            mismatches.append(f"component {unit_id!r} is in the routing record but not in the plan")
        elif sorted(entry.get("members") or []) != decided["members"]:
            mismatches.append(f"component {unit_id!r} was routed with different members than the plan lists")
        elif entry.get("decision") != decided["decision"]:
            mismatches.append(
                f"component {unit_id!r} was routed {entry.get('decision')!r} but the plan decides "
                f"{decided['decision']!r}"
            )
        elif decided["decision"] != DECISION_AGENTIC and entry.get("outcome") != OUTCOME_DETERMINISTIC:
            mismatches.append(f"a fill was applied to component {unit_id!r}, which the plan routes deterministic")
    return mismatches


def routed_agentic_pipelines(report: Any) -> set[str]:
    """The pipelines only fill-agentic may fill: routed-agentic members and the pipelines it authored.

    Every pipeline convert wrote is a member of some recorded unit, so a pipeline in the report that
    belongs to none was authored by fill-agentic. Empty when the report has no routing record.
    """
    record = routing_record(report)
    components = record.get("components") if record is not None else None
    if not isinstance(components, dict):
        return set()
    names: set[str] = set()
    all_members: set[str] = set()
    for entry in components.values():
        if isinstance(entry, dict):
            members = {str(member) for member in entry.get("members") or []}
            all_members.update(members)
            if entry.get("decision") == DECISION_AGENTIC:
                names.update(members)
    authored = {str(pipeline.get("name")) for pipeline in _report_pipelines(report)} - all_members
    return names | authored


# --------------------------------------------------------------------------- #
# The deterministic baseline and the agent's conversion output.
# --------------------------------------------------------------------------- #


def load_baseline(output_dir: Path, *, fresh: bool) -> tuple[dict[str, Any], list[dict[str, Any]], bytes, bytes]:
    """Read the deterministic baseline a rebuild starts from: ``(report, gaps, report_bytes, gaps_bytes)``.

    With ``fresh`` (the live report carries no routing record, so convert just wrote it) the live
    report and gaps *are* the baseline. Otherwise this reads the copy route saved under
    ``.work/route_baseline/``. The exact bytes are kept so the routing record can hash them.

    Raises:
        ValueError: The saved baseline is missing or is not valid JSON.
    """
    work = Path(output_dir) / WORK_DIRNAME
    if fresh:
        report_path, gaps_path = work / REPORT_FILENAME, work / GAPS_FILENAME
    else:
        report_path = work / BASELINE_DIRNAME / BASELINE_REPORT_FILENAME
        gaps_path = work / BASELINE_DIRNAME / BASELINE_GAPS_FILENAME
        if not report_path.exists() or not gaps_path.exists():
            raise ValueError("the deterministic baseline is missing; re-run convert, then route")
    report_bytes = report_path.read_bytes()
    gaps_bytes = gaps_path.read_bytes() if gaps_path.exists() else b"[]"
    try:
        report = json.loads(report_bytes)
        gaps = json.loads(gaps_bytes)
    except json.JSONDecodeError as error:
        message = f"the deterministic baseline is not valid JSON ({error}); re-run convert, then route"
        raise ValueError(message) from error
    return report, gaps if isinstance(gaps, list) else [], report_bytes, gaps_bytes


def _save_baseline_bytes(output_dir: Path, report_bytes: bytes, gaps_bytes: bytes) -> None:
    """Save an immutable copy of the baseline files as exact bytes (byte-for-byte)."""
    baseline_dir = Path(output_dir) / WORK_DIRNAME / BASELINE_DIRNAME
    baseline_dir.mkdir(parents=True, exist_ok=True)
    (baseline_dir / BASELINE_REPORT_FILENAME).write_bytes(report_bytes)
    (baseline_dir / BASELINE_GAPS_FILENAME).write_bytes(gaps_bytes)


@dataclass(slots=True, kw_only=True)
class AgenticOutput:
    """The agent's conversion output, as read from ``metadata/agentic_conversion.json``.

    Attributes:
        document: The file's content as stored: ``{"components": {<unit id>: entry}, "gap_fills": [...]}``.
        outputs: The unit entries that check out (sorted members, a hash matching their pipelines),
            keyed by unit id. Only these are ever applied.
        gap_fills: The fills of convert's own gaps that check out, in stored order.
        edited: A label for every entry that no longer checks out because it was edited by hand.
    """

    document: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    gap_fills: list[dict[str, Any]] = field(default_factory=list)
    edited: list[str] = field(default_factory=list)


def _output_checks_out(entry: Any) -> bool:
    """Whether a stored unit output is as fill-agentic wrote it: sorted members, a hash matching its pipelines."""
    if not isinstance(entry, dict):
        return False
    members = entry.get("members")
    pipelines = entry.get("pipelines")
    return (
        isinstance(members, list)
        and all(isinstance(member, str) for member in members)
        and members == sorted(set(members))
        and isinstance(pipelines, list)
        and bool(pipelines)
        and entry.get("output_sha256") == canonical_sha256(pipelines)
        and isinstance(entry.get("replaced", []), list)
    )


def _gap_fill_checks_out(entry: Any) -> bool:
    """Whether a stored gap fill is as the merge wrote it: a task whose hash matches, bound to a baseline."""
    return (
        isinstance(entry, dict)
        and isinstance(entry.get("pipeline"), str)
        and isinstance(entry.get("activity_name"), str)
        and bool(entry["activity_name"])
        and isinstance(entry.get("task"), dict)
        and entry.get("task_sha256") == canonical_sha256(entry["task"])
        and isinstance(entry.get("baseline_report_sha256"), str)
    )


def load_agentic_output(output_dir: Path) -> AgenticOutput:
    """Read ``metadata/agentic_conversion.json``; empty when the agent has written nothing yet.

    Route, fill-agentic, the merge and package all read the store here. A rebuild applies only the
    entries that check out, so an edited entry is never applied: package refuses while one exists,
    and filling (or merging) that entry again replaces it.

    Raises:
        ValueError: The file is not valid JSON or not shaped as ``{"components": {...}, "gap_fills": [...]}``.
    """
    path = Path(output_dir) / METADATA_DIRNAME / AGENTIC_OUTPUT_FILENAME
    if not path.exists():
        return AgenticOutput()
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"{AGENTIC_OUTPUT_FILENAME} is not valid JSON: {error}") from error
    components = document.get("components", {}) if isinstance(document, dict) else None
    gap_fills = document.get("gap_fills", []) if isinstance(document, dict) else None
    if not isinstance(components, dict) or not isinstance(gap_fills, list):
        raise ValueError(f"{AGENTIC_OUTPUT_FILENAME} must hold a 'components' object and a 'gap_fills' list")
    outputs = {unit_id: entry for unit_id, entry in components.items() if _output_checks_out(entry)}
    edited = sorted(set(components) - set(outputs))
    checked_fills: list[dict[str, Any]] = []
    for index, fill in enumerate(gap_fills):
        if _gap_fill_checks_out(fill):
            checked_fills.append(fill)
        else:
            label = fill.get("activity_name") if isinstance(fill, dict) else None
            edited.append(f"gap fill {label!r}" if label else f"gap_fills[{index}]")
    return AgenticOutput(
        document={"components": components, "gap_fills": gap_fills},
        outputs=outputs,
        gap_fills=checked_fills,
        edited=edited,
    )


def save_agentic_output(output_dir: Path, document: dict[str, Any]) -> None:
    """Write ``metadata/agentic_conversion.json`` atomically."""
    _write_json_atomic(Path(output_dir) / METADATA_DIRNAME / AGENTIC_OUTPUT_FILENAME, document)


def record_gap_fills(output_dir: Path, fills: list[dict[str, Any]], baseline_report_sha256: str) -> None:
    """Store fills of convert's own gaps, merged after routing, so every rebuild applies them.

    Each fill is ``{"pipeline", "activity_name", "task"}``; it is stored with its task's hash and the
    baseline it was merged against, replacing an earlier fill of the same pipeline and activity. The
    baseline itself is never changed.
    """
    stored = load_agentic_output(output_dir).document or {"components": {}, "gap_fills": []}
    by_key = {
        (fill.get("pipeline"), fill.get("activity_name")): fill
        for fill in stored.get("gap_fills", [])
        if isinstance(fill, dict)
    }
    for fill in fills:
        task = copy.deepcopy(fill["task"])
        by_key[(fill["pipeline"], fill["activity_name"])] = {
            "pipeline": fill["pipeline"],
            "activity_name": fill["activity_name"],
            "task": task,
            "task_sha256": canonical_sha256(task),
            "baseline_report_sha256": baseline_report_sha256,
        }
    save_agentic_output(output_dir, {"components": stored.get("components", {}), "gap_fills": list(by_key.values())})


def applied_gap_fills(gap_fills: list[dict[str, Any]] | None, baseline_report_sha256: str) -> list[dict[str, Any]]:
    """The stored gap fills a rebuild from this baseline applies, in stored order."""
    return [fill for fill in gap_fills or [] if fill.get("baseline_report_sha256") == baseline_report_sha256]


# --------------------------------------------------------------------------- #
# The pure rebuild step: deterministic, idempotent report transformation.
# --------------------------------------------------------------------------- #


def rebuild(
    baseline_report: dict[str, Any],
    baseline_gaps: list[dict[str, Any]],
    plan: dict[str, Any],
    outputs: dict[str, dict[str, Any]] | None,
    gap_fills: list[dict[str, Any]] | None,
    baseline_report_bytes: bytes,
    baseline_gaps_bytes: bytes,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Pure rebuild: derive the routed report, gaps, and record from the baseline, fills and plan.

    Returns ``(report, gaps, record)``, all deterministically derived from the inputs. It starts from
    a copy of the baseline, applies the stored fills of convert's own gaps that were merged against this
    baseline, then turns each agentic unit's tasks into placeholders and gaps, and finally, per unit:

    * **deterministic:** the baseline pipelines stay; outcome is ``deterministic``.
    * **agentic with a stored output whose members match:** the output's pipelines replace the members
      and the members' routed gaps are dropped. Outcome is ``agentic-applied``.
    * **agentic without a matching output:** placeholders and tagged gaps as ``alter_report`` does.
      Outcome is ``agentic-not-viable``, which package refuses.

    Each unit's record entry holds its members, decision, outcome, ``output_sha256`` (only while its
    stored output is applied, otherwise ``None``) and ``fingerprint``; the record also holds
    ``gap_fills_sha256``, the hash of the gap fills applied, so a later merge shows in it. A plan with
    no agentic unit and no applicable gap fill returns the baseline itself and no record
    (byte-identical to convert).

    Args:
        baseline_report: Parsed baseline report JSON.
        baseline_gaps: Parsed baseline gaps JSON.
        plan: Routing plan dict.
        outputs: The stored unit outputs that check out (see :func:`load_agentic_output`).
        gap_fills: The stored gap fills that check out; those merged against another baseline are skipped.
        baseline_report_bytes: Raw bytes of the baseline report file, hashed into the record.
        baseline_gaps_bytes: Raw bytes of the baseline gaps file, hashed into the record.
    """
    from flowx.ir_serde import replace_task_by_name

    baseline_report_sha256 = hashlib.sha256(baseline_report_bytes).hexdigest()
    applicable_fills = applied_gap_fills(gap_fills, baseline_report_sha256)
    agentic_names = agentic_pipeline_names(plan)
    if not agentic_names and not applicable_fills:
        return baseline_report, baseline_gaps, {}

    report = copy.deepcopy(baseline_report)
    pipelines_by_name = {pipeline.get("name"): pipeline for pipeline in _report_pipelines(report)}
    for fill in applicable_fills:
        pipeline = pipelines_by_name.get(fill["pipeline"])
        if pipeline is not None:
            replace_task_by_name(pipeline.get("tasks", []), fill["activity_name"], copy.deepcopy(fill["task"]))
    new_report, new_gaps = alter_report(report, copy.deepcopy(baseline_gaps), agentic_names)
    if not isinstance(new_report.get("pipelines"), list):
        new_report = {"pipelines": [new_report]}

    units: dict[str, dict[str, Any]] = {}
    for unit_id, unit in routing_units(plan).items():
        members = set(unit["members"])
        stored = (outputs or {}).get(unit_id)
        output = (
            stored
            if unit["decision"] == DECISION_AGENTIC and stored and set(stored.get("members", [])) == members
            else None
        )
        output_sha256 = output.get("output_sha256") if output else None
        if output:
            outcome = OUTCOME_AGENTIC_APPLIED
            new_report = replace_unit_pipelines(new_report, members, output.get("pipelines", []))
            new_gaps = [gap for gap in new_gaps if not (isinstance(gap, dict) and gap.get("pipeline") in members)]
        elif unit["decision"] == DECISION_AGENTIC:
            outcome = OUTCOME_AGENTIC_NOT_VIABLE
        else:
            outcome = OUTCOME_DETERMINISTIC
        fingerprint = canonical_sha256(
            {"members": unit["members"], "decision": unit["decision"], "output_sha256": output_sha256}
        )
        units[unit_id] = {**unit, "outcome": outcome, "output_sha256": output_sha256, "fingerprint": fingerprint}
    record = {
        "conversion_plan_sha256": canonical_sha256(plan),
        "baseline_report_sha256": baseline_report_sha256,
        "baseline_gaps_sha256": hashlib.sha256(baseline_gaps_bytes).hexdigest(),
        "gap_fills_sha256": canonical_sha256(applicable_fills),
        "components": units,
    }
    return new_report, new_gaps, record


# --------------------------------------------------------------------------- #
# The edit step: rewrite the report for routed-agentic groups.
# --------------------------------------------------------------------------- #


def _report_pipelines(report: dict[str, Any]) -> list[dict[str, Any]]:
    """The pipeline dicts in a report, whether it is single-pipeline or a ``{"pipelines": [...]}`` wrapper."""
    if isinstance(report, dict) and "pipelines" in report and isinstance(report["pipelines"], list):
        return [pipeline for pipeline in report["pipelines"] if isinstance(pipeline, dict)]
    return [report]


def _placeholder_and_gap(task: dict[str, Any], pipeline_name: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Turn one task into a placeholder + gap, preserving its identity and edges.

    Idempotent: a task that is already a ``PlaceholderActivity`` (a re-run of the edit) is kept as-is
    and its gap is rebuilt from the recorded ``original_type`` / ``raw_definition`` rather than
    wrapping the placeholder in another placeholder.
    """
    name = task.get("name")
    if task.get("type") == "PlaceholderActivity":
        original_type = str(task.get("original_type", "unknown"))
        gap: dict[str, Any] = {
            "activity_name": name,
            "activity_type": original_type,
            "raw_definition": task.get("raw_definition"),
            "pipeline": pipeline_name,
        }
        return task, gap

    original_type = str(task.get("type", "unknown"))
    placeholder: dict[str, Any] = {
        "name": name,
        "task_key": task.get("task_key"),
        "type": "PlaceholderActivity",
        "original_type": original_type,
        "comment": _PLACEHOLDER_COMMENT,
        # Keep the deterministic translation as context so the agent can author against it.
        "raw_definition": task,
    }
    if task.get("depends_on"):
        placeholder["depends_on"] = task["depends_on"]
    gap = {
        "activity_name": name,
        "activity_type": original_type,
        "raw_definition": task,
        "pipeline": pipeline_name,
    }
    return placeholder, gap


def alter_report(
    report: dict[str, Any],
    gaps: list[dict[str, Any]],
    agentic_pipelines: Iterable[str],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Rewrite the report so every routed-agentic pipeline's tasks become placeholder gaps.

    Returns ``(new_report, new_gaps)``. Pipelines not in ``agentic_pipelines`` are copied through
    untouched. When ``agentic_pipelines`` is empty the inputs are returned unchanged (identity), so a
    no-decision / all-deterministic route leaves today's output byte-for-byte intact.
    """
    agentic = {name for name in agentic_pipelines}
    if not agentic:
        return report, gaps

    report = copy.deepcopy(report)

    # Task names owned by pipelines that are NOT routed agentic. Convert gaps are untagged (they carry
    # no pipeline), so a name that also belongs to a non-routed pipeline is ambiguous: dropping it
    # could remove that pipeline's gap, so it is preserved. Computed before mutating routed pipelines.
    nonrouted_task_names: set[str] = set()
    for pipeline in _report_pipelines(report):
        if pipeline.get("name") in agentic:
            continue
        for task in pipeline.get("tasks", []):
            if isinstance(task, dict):
                nonrouted_task_names.add(str(task.get("name")))

    # Emit exactly one pipeline-tagged gap per routed task, replacing (never appending to) any prior
    # gap for a routed pipeline's tasks. Without this, a task that convert already recorded as an
    # agentic gap -- or a re-run of the edit -- would leave duplicate/again-appended gaps.
    fresh_gaps: list[dict[str, Any]] = []
    routed_task_names: set[str] = set()
    for pipeline in _report_pipelines(report):
        if pipeline.get("name") not in agentic:
            continue
        placeholders: list[dict[str, Any]] = []
        for task in pipeline.get("tasks", []):
            if not isinstance(task, dict):
                continue
            placeholder, gap = _placeholder_and_gap(task, str(pipeline.get("name")))
            placeholders.append(placeholder)
            fresh_gaps.append(gap)
            routed_task_names.add(str(task.get("name")))
        pipeline["tasks"] = placeholders

    kept_gaps: list[dict[str, Any]] = []
    for gap in gaps:
        pipeline_name = gap.get("pipeline") if isinstance(gap, dict) else None
        activity_name = gap.get("activity_name") if isinstance(gap, dict) else None
        # Drop our own prior tagged gaps for now-routed pipelines (idempotent re-run).
        if pipeline_name in agentic:
            continue
        # Drop an untagged convert gap only when the name belongs *exclusively* to a routed pipeline --
        # a fresh tagged gap now supersedes it. When the same name also names a task in a non-routed
        # pipeline, the gap is ambiguous and kept, so a non-routed pipeline never loses its gap.
        if pipeline_name is None and activity_name in routed_task_names and activity_name not in nonrouted_task_names:
            continue
        kept_gaps.append(gap)
    return report, kept_gaps + fresh_gaps


def _write_atomic(path: Path, content: bytes) -> None:
    """Write bytes atomically (temp file + ``os.replace``)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(content)
    os.replace(temporary, path)


def _write_json_atomic(path: Path, document: Any) -> None:
    """Write JSON atomically, matching the report's 2-space indentation."""
    _write_atomic(path, json.dumps(document, indent=2, default=str).encode("utf-8"))


def baseline_pipeline_mismatch(baseline_report: dict[str, Any], inventory: dict[str, Any]) -> str | None:
    """Say how a baseline report's pipelines differ from the inventory's, or ``None`` when they match.

    The baseline is the convert output a rebuild starts from; when discover ran again after convert
    its pipelines no longer match, and routing it would ship removed pipelines or miss new ones. A
    report pipeline with no tasks is not counted as extra: ADF discover leaves pipelines without
    activities out of the inventory, while convert still writes them.
    """
    inventory_names = {
        str(pipeline.get("name"))
        for pipeline in inventory.get("pipelines", [])
        if isinstance(pipeline, dict) and pipeline.get("name") is not None
    }
    report_pipelines = _report_pipelines(baseline_report)
    report_names = {str(pipeline.get("name")) for pipeline in report_pipelines}
    with_tasks = {str(pipeline.get("name")) for pipeline in report_pipelines if pipeline.get("tasks")}
    missing, extra = sorted(inventory_names - report_names), sorted(with_tasks - inventory_names)
    if not missing and not extra:
        return None
    return (
        f"the translation report was converted from a different discover (missing {missing}, extra {extra}); "
        "re-run convert, then route"
    )


def apply_plan_to_report(
    output_dir: Path, plan: dict[str, Any], *, inventory: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Apply a routing decision to the report on disk by rebuilding it from the deterministic baseline.

    A report without a routing record is a fresh convert, so it becomes the baseline: the first time
    the rebuild changes anything, route saves it and its gaps to ``.work/route_baseline/``; gap fills
    merged against an earlier baseline are then dropped (merge them again). A routed report is rebuilt
    from the saved copy. The rebuild applies the stored gap fills, the plan and the stored unit outputs
    from ``metadata/agentic_conversion.json``, and the report + gaps are written atomically. When the
    rebuild changes nothing the baseline bytes are written back, so switching back to deterministic
    restores convert's output exactly and a never-routed report is left untouched. Route never writes
    modify's configured copy; package asks for ``modify`` again when it is out of date.

    With ``inventory`` given and an agentic unit in the plan, the baseline's pipelines must be the
    inventory's (see :func:`baseline_pipeline_mismatch`).

    Returns a summary dict with the altered pipeline names, the resulting gap count, each unit's
    outcome and how many stale gap fills were dropped.

    Raises:
        FileNotFoundError: when the translation report is missing (run convert first).
        ValueError: when a routed report's saved baseline or the agent's output cannot be read, or the
            baseline no longer matches the inventory.
    """
    work = Path(output_dir) / WORK_DIRNAME
    report_path = work / REPORT_FILENAME
    gaps_path = work / GAPS_FILENAME
    if not report_path.exists():
        raise FileNotFoundError(f"No {REPORT_FILENAME} under {work}; run the convert phase first.")

    report = json.loads(report_path.read_bytes())
    record = routing_record(report)
    baseline_report, baseline_gaps, baseline_report_bytes, baseline_gaps_bytes = load_baseline(
        output_dir, fresh=record is None
    )
    agentic = agentic_pipeline_names(plan)
    if inventory is not None and agentic:
        mismatch = baseline_pipeline_mismatch(baseline_report, inventory)
        if mismatch:
            raise ValueError(mismatch)
    stored = load_agentic_output(output_dir)
    dropped = 0
    if record is None and stored.document.get("gap_fills"):
        baseline_report_sha256 = hashlib.sha256(baseline_report_bytes).hexdigest()
        kept = applied_gap_fills(stored.gap_fills, baseline_report_sha256)
        dropped = len(stored.document["gap_fills"]) - len(kept)
        if dropped:
            save_agentic_output(output_dir, {"components": stored.document["components"], "gap_fills": kept})
            stored = load_agentic_output(output_dir)
    new_report, new_gaps, new_record = rebuild(
        baseline_report,
        baseline_gaps,
        plan,
        stored.outputs,
        stored.gap_fills,
        baseline_report_bytes,
        baseline_gaps_bytes,
    )
    summary: dict[str, Any] = {"agentic_pipelines": sorted(agentic), "dropped_gap_fills": dropped}
    if not new_record:
        if record is not None:
            _write_atomic(report_path, baseline_report_bytes)
            _write_atomic(gaps_path, baseline_gaps_bytes)
        return {**summary, "gaps": len(baseline_gaps), "altered": record is not None, "outcomes": {}}
    if record is None:
        _save_baseline_bytes(output_dir, baseline_report_bytes, baseline_gaps_bytes)
    old_gaps_bytes = gaps_path.read_bytes() if gaps_path.exists() else b"[]"
    old_report_without_record = {key: value for key, value in report.items() if key != ROUTING_RECORD_KEY}
    new_gaps_bytes = json.dumps(new_gaps, indent=2, default=str).encode("utf-8")
    altered = old_report_without_record != new_report or old_gaps_bytes != new_gaps_bytes
    new_report[ROUTING_RECORD_KEY] = new_record
    _write_json_atomic(report_path, new_report)
    _write_atomic(gaps_path, new_gaps_bytes)
    outcomes = {unit_id: entry["outcome"] for unit_id, entry in new_record["components"].items()}
    return {**summary, "gaps": len(new_gaps), "altered": altered, "outcomes": outcomes}


def apply_plan(output_dir: Path, plan: ConversionPlan) -> dict[str, Any]:
    """Apply a recorded, typed plan to the IR after convert: the library's one routing entry point.

    Checks the plan still matches the inventory, source graphs and agentic insights it was decided
    on, that ``agentic_insights.json`` agrees with the inventory, and that no decision is pending,
    then rebuilds the translation report from the deterministic baseline (see
    :func:`apply_plan_to_report`). Each routed-agentic unit is then filled only by ``fill-agentic``;
    ``convert --merge-agentic`` stays for convert's own gaps. Phase 1 decides whole components (or
    accepted groupings of them), so the reserved per-node assignments are not read here.

    Raises:
        FileNotFoundError: The inventory or translation report is missing.
        ValueError: The plan is stale against the current discovery outputs, the insights files
            disagree, a decision is pending, or the baseline was converted from another discover.
    """
    from flowx.routing import insights_file_violations, plan_binding_violations

    inventory_path = Path(output_dir) / METADATA_DIRNAME / INVENTORY_FILENAME
    if not inventory_path.exists():
        raise FileNotFoundError(f"No {INVENTORY_FILENAME} under {inventory_path.parent}; run the discover phase first.")
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    problems = plan_binding_violations(plan, inventory) + insights_file_violations(Path(output_dir), inventory)
    if problems:
        raise ValueError("; ".join(problems))
    pending = plan.pending_components()
    if pending:
        raise ValueError(
            f"components {pending} are still pending; decide them in metadata/conversion_plan.json and re-run route"
        )
    return apply_plan_to_report(output_dir, plan.to_dict(), inventory=inventory)


# --------------------------------------------------------------------------- #
# fill-agentic: replace a routed-agentic unit with the agent's pipelines.
# --------------------------------------------------------------------------- #


def replace_unit_pipelines(
    report: dict[str, Any],
    unit_members: Iterable[str],
    authored_pipelines: list[dict[str, Any]],
) -> dict[str, Any]:
    """Replace a routed unit's pipelines with the agent-authored pipeline(s).

    Drops every pipeline whose name is in ``unit_members`` and appends the ``authored_pipelines``
    (each a pipeline IR dict, typically carrying ``AgenticComponentActivity`` nodes). Pipelines
    outside the unit are preserved in order. Returns a ``{"pipelines": [...]}`` report; the input is
    not mutated.
    """
    members = {name for name in unit_members}
    kept = [copy.deepcopy(pipeline) for pipeline in _report_pipelines(report) if pipeline.get("name") not in members]
    kept.extend(copy.deepcopy(pipeline) for pipeline in authored_pipelines)
    return {"pipelines": kept}


def _resolve_agentic_unit(
    output_dir: Path, members: set[str]
) -> tuple[str | None, dict[str, Any] | None, str | None, str | None]:
    """Bind ``members`` to a routed-**agentic** unit in the recorded, fingerprint-bound plan.

    Reads ``metadata/conversion_plan.json`` and ``metadata/inventory.json`` and returns
    ``(unit_id, plan, source, error)``, where ``plan`` is the recorded plan document and ``source``
    the inventory's source, which every authored pipeline must carry. The error is set (and the other
    three are ``None``) when: the inventory's source cannot be routed agentic
    (:data:`flowx.routing.AGENTIC_ROUTING_SOURCES`); the plan or inventory is missing; the plan no
    longer matches the current inventory, source graphs or insights, or the insights files disagree;
    a decision is still pending; ``members`` do not exactly equal one unit's members (a partial,
    superset, or mistyped group); or the exactly-matching unit is routed deterministic rather than
    agentic. Requiring an exact match to a routed-agentic unit stops a caller from swapping
    deterministic pipelines or a partial group. The plan is returned so the fill can check the
    report's routing record against it.
    """
    from flowx.routing import (
        agentic_routing_supported,
        agentic_routing_unsupported_note,
        insights_file_violations,
        inventory_source,
        plan_binding_violations,
    )

    metadata = Path(output_dir) / METADATA_DIRNAME
    inventory_path = metadata / INVENTORY_FILENAME
    try:
        recorded = ConversionPlan.load(Path(output_dir))
    except ValueError as error:
        return None, None, None, str(error)
    if recorded is None:
        return None, None, None, "No metadata/conversion_plan.json; record a routing decision with `route` first."
    if not inventory_path.exists():
        return None, None, None, "No metadata/inventory.json; run the discover phase first."

    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    if not agentic_routing_supported(inventory):
        return None, None, None, agentic_routing_unsupported_note(inventory)
    stale = plan_binding_violations(recorded, inventory)
    if stale:
        return None, None, None, f"conversion_plan.json is stale: {stale[0]}; re-run `route` before filling."
    disagree = insights_file_violations(Path(output_dir), inventory)
    if disagree:
        return None, None, None, disagree[0]
    pending = recorded.pending_components()
    if pending:
        return (
            None,
            None,
            None,
            f"components {pending} are still pending; decide them and re-run `route` before filling.",
        )
    plan = recorded.to_dict()

    for unit_id, unit in routing_units(plan).items():
        if set(unit["members"]) != members:
            continue
        if unit["decision"] == DECISION_AGENTIC:
            return unit_id, plan, inventory_source(inventory), None
        return (
            None,
            None,
            None,
            (
                f"members {sorted(members)} match component {unit_id!r}, which is routed {unit['decision']!r}, "
                "not agentic; only routed-agentic components can be filled."
            ),
        )
    return (
        None,
        None,
        None,
        (
            f"members {sorted(members)} do not exactly match any component or accepted grouping in the recorded "
            "plan (partial, superset, or mistyped); pass the exact member set of one routed-agentic unit."
        ),
    )


def _authored_source_tag_violations(authored_pipelines: list[dict[str, Any]], source: str) -> list[str]:
    """Reports each authored pipeline that does not carry ``tags.source`` equal to ``source``.

    The authored pipelines replace pipelines of the routed inventory, so one that omits or mis-sets
    the source tag is an authoring error. Catching it here fails the fill closed (nothing written)
    with a clear message, rather than letting the mis-tagged pipeline reach the package preflight
    where it is only rejected much later.
    """
    violations: list[str] = []
    for index, pipeline in enumerate(authored_pipelines):
        label = pipeline.get("name") if isinstance(pipeline, dict) and pipeline.get("name") else f"pipeline[{index}]"
        tags = pipeline.get("tags") if isinstance(pipeline, dict) else None
        authored_source = tags.get("source") if isinstance(tags, dict) else None
        if authored_source != source:
            violations.append(
                f"{label}: authored pipeline must carry tags.source == {source!r}, got {authored_source!r}"
            )
    return violations


def _placeholder_task_names(tasks: Any) -> list[str]:
    """The names of every ``PlaceholderActivity`` among ``tasks``, looking inside containers too."""
    names: list[str] = []
    for task in tasks if isinstance(tasks, list) else []:
        if not isinstance(task, dict):
            continue
        if task.get("type") == "PlaceholderActivity":
            names.append(str(task.get("name")))
        for key in _NESTED_TASK_KEYS:
            names.extend(_placeholder_task_names(task.get(key)))
        for case in task.get("cases") or []:
            if isinstance(case, dict):
                names.extend(_placeholder_task_names(case.get("activities")))
    return names


def _authored_placeholder_violations(authored_pipelines: list[dict[str, Any]]) -> list[str]:
    """Reports each authored pipeline that still holds a placeholder task, which would ship as a stub."""
    violations: list[str] = []
    for index, pipeline in enumerate(authored_pipelines):
        if not isinstance(pipeline, dict):
            continue
        placeholders = _placeholder_task_names(pipeline.get("tasks"))
        if placeholders:
            label = pipeline.get("name") or f"pipeline[{index}]"
            violations.append(
                f"{label}: still has placeholder tasks {placeholders}; convert every task before filling the unit"
            )
    return violations


def _bundle_folder_clashes(report: dict[str, Any], authored_names: list[Any]) -> list[str]:
    """Authored names that are empty, or share a bundle folder with another pipeline once normalised.

    Package writes each pipeline of a multi-pipeline report to the folder ``normalize_task_key(name)``,
    so ``Sales Load`` and ``Sales-Load`` would overwrite each other's bundle.
    """
    from flowx.utils import normalize_task_key

    clashes: list[str] = []
    folders: dict[str, list[str]] = {}
    for pipeline in _report_pipelines(report):
        name = pipeline.get("name")
        if isinstance(name, str) and name.strip():
            folders.setdefault(normalize_task_key(name), []).append(name)
    for name in authored_names:
        if not isinstance(name, str) or not normalize_task_key(name):
            clashes.append(f"authored pipeline name {name!r} is empty; give every pipeline a name")
            continue
        sharing = folders.get(normalize_task_key(name), [])
        if len(sharing) > 1:
            others = sorted(set(sharing) - {name})
            clashes.append(
                f"authored pipeline {name!r} shares the bundle folder {normalize_task_key(name)!r} with {others}; "
                "give each a distinct name"
            )
    return clashes


def apply_agentic_output(
    output_dir: Path,
    unit_members: Iterable[str],
    authored_pipelines: list[dict[str, Any]],
) -> dict[str, Any]:
    """Fill a routed-agentic unit with agent-authored pipeline(s) on disk.

    Validates the unit's membership against the recorded plan, stores the authored pipelines in
    ``metadata/agentic_conversion.json``, and rebuilds (applying them deterministically). The
    membership must exactly match a routed-agentic unit (a component, or an accepted grouping) in the
    plan, and every authored pipeline must carry the correct source tag, hold no placeholder task, and
    have a name of its own: no repeat within the list, no member of another unit, no pipeline another
    unit's output authored, and no bundle folder shared with another pipeline once normalised. A name
    may reuse a member of this unit, since the fill replaces it, or a pipeline the output of a unit
    sharing members with this one authored (a grouping and its own components never apply together).

    Returns ``{"ok", "violations", "error", "component_id", "pipelines", "already_applied", "message"}``.
    ``ok`` is ``False`` (and nothing written) on a plan/membership error, source tag violation, a
    placeholder left in, a name clash, structural validation failure, or any other error. Empty
    ``authored_pipelines`` is refused.

    The stored entry carries the canonical hash of the complete authored pipelines. When that hash
    matches the stored one and the live report already holds those pipelines, this returns
    ``ok: true, already_applied: true, message: 'already applied, unchanged'`` and writes nothing.
    Otherwise it replaces that unit's entry only -- adding ``{"from", "to"}`` to the entry's
    ``replaced`` history when a different output was stored before -- and rebuilds. Nothing is written
    until structural validation passes (all-or-nothing). Modify's configured copy is never written; the
    routing review page is rewritten.
    """
    members = {str(member) for member in unit_members}
    if not authored_pipelines:
        return {"ok": False, "error": "pipelines list cannot be empty", "violations": [], "pipelines": 0}

    unit_id, plan, source, error = _resolve_agentic_unit(output_dir, members)
    if error is not None:
        return {"ok": False, "error": error, "violations": [], "pipelines": 0}
    assert unit_id is not None and plan is not None and source is not None

    authoring_violations = _authored_source_tag_violations(authored_pipelines, source)
    authoring_violations += _authored_placeholder_violations(authored_pipelines)
    if authoring_violations:
        return {"ok": False, "error": None, "violations": authoring_violations, "pipelines": 0}

    work = Path(output_dir) / WORK_DIRNAME
    report_path = work / REPORT_FILENAME
    if not report_path.exists():
        raise FileNotFoundError(f"No {REPORT_FILENAME} under {work}; run the convert phase first.")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    record = routing_record(report)
    if record is None:
        error = f"{REPORT_FILENAME} carries no routing record; run `route` to apply the plan before filling."
        return {"ok": False, "error": error, "violations": [], "pipelines": 0}
    if routing_record_mismatches(record, plan):
        error = "the report was routed under a different plan; re-run route, then fill-agentic again"
        return {"ok": False, "error": error, "violations": [], "pipelines": 0}

    output_sha256 = canonical_sha256(authored_pipelines)
    stored = load_agentic_output(output_dir)
    authored_names = [pipeline.get("name") for pipeline in authored_pipelines]
    taken_names = {
        member for other_id, unit in routing_units(plan).items() if other_id != unit_id for member in unit["members"]
    } | {
        pipeline.get("name")
        for other_id, entry in stored.outputs.items()
        if other_id != unit_id and members.isdisjoint(entry["members"])
        for pipeline in entry["pipelines"]
        if isinstance(pipeline, dict)
    }
    clashes = sorted({str(name) for name in authored_names if name in taken_names or authored_names.count(name) > 1})
    if clashes:
        error = (
            f"authored pipeline names {clashes} repeat or clash with a pipeline of another component; "
            "give each a unique name"
        )
        return {"ok": False, "error": error, "violations": [], "pipelines": 0}
    in_report = {pipeline.get("name"): pipeline for pipeline in _report_pipelines(report)}
    previous = stored.outputs.get(unit_id)
    if (previous or {}).get("output_sha256") == output_sha256 and all(
        in_report.get(pipeline.get("name")) == pipeline for pipeline in authored_pipelines
    ):
        return {
            "ok": True,
            "error": None,
            "violations": [],
            "component_id": unit_id,
            "pipelines": len(_report_pipelines(report)),
            "already_applied": True,
            "message": "already applied, unchanged",
        }

    raw_previous = stored.document.get("components", {}).get(unit_id)
    replaced = list(raw_previous.get("replaced") or []) if isinstance(raw_previous, dict) else []
    if previous is not None and previous["output_sha256"] != output_sha256:
        replaced.append({"from": previous["output_sha256"], "to": output_sha256})
    entry = {
        "members": sorted(members),
        "pipelines": authored_pipelines,
        "output_sha256": output_sha256,
        "replaced": replaced,
    }
    baseline_report, baseline_gaps, baseline_report_bytes, baseline_gaps_bytes = load_baseline(output_dir, fresh=False)
    new_report, new_gaps, new_record = rebuild(
        baseline_report,
        baseline_gaps,
        plan,
        {**stored.outputs, unit_id: entry},
        stored.gap_fills,
        baseline_report_bytes,
        baseline_gaps_bytes,
    )
    folder_clashes = _bundle_folder_clashes(new_report, authored_names)
    if folder_clashes:
        return {"ok": False, "error": None, "violations": folder_clashes, "pipelines": 0}
    new_report[ROUTING_RECORD_KEY] = new_record

    result = validate_report_structurally(new_report)
    if not result.ok:
        violations = [f"[{finding.code}] {finding.location}: {finding.message}" for finding in result.violations]
        return {"ok": False, "error": None, "violations": violations, "pipelines": 0}

    document = {
        "components": {**stored.document.get("components", {}), unit_id: entry},
        "gap_fills": stored.document.get("gap_fills", []),
    }
    save_agentic_output(output_dir, document)
    _write_json_atomic(report_path, new_report)
    _write_json_atomic(work / GAPS_FILENAME, new_gaps)

    from flowx.reporting.routing_review import write_routing_review

    write_routing_review(Path(output_dir))
    return {
        "ok": True,
        "error": None,
        "violations": [],
        "component_id": unit_id,
        "pipelines": len(new_report["pipelines"]),
        "already_applied": False,
    }


# --------------------------------------------------------------------------- #
# Structural validation via the existing bundle invariants.
# --------------------------------------------------------------------------- #


def validate_report_structurally(report: dict[str, Any]) -> Any:
    """Package the report to a throwaway bundle and run the existing structural invariants over it.

    Uses the same ``prepare_workflow -> write_bundle`` path as the package phase, then aggregates
    :func:`flowx.validate.bundle_invariants.check_bundle_dir` findings across every emitted bundle so
    duplicate keys, dangling dependencies, cycles, and dangling pipeline/run_job references are all
    caught before the merged report is trusted. Returns a
    :class:`~flowx.validate.bundle_invariants.BundleInvariantResult`.
    """
    from flowx.bundler.dab_writer import pipeline_dict_to_ir, write_bundle
    from flowx.preparer.workflow_preparer import prepare_workflow
    from flowx.utils import normalize_task_key
    from flowx.validate.bundle_invariants import BundleInvariantResult, check_bundle_dir

    pipelines = _report_pipelines(report)
    findings: list[Any] = []
    with tempfile.TemporaryDirectory(prefix="flowx-fill-validate-") as temporary:
        root = Path(temporary)
        for pipeline_dict in pipelines:
            pipeline, _skipped = pipeline_dict_to_ir(pipeline_dict)
            workflow = prepare_workflow(pipeline)
            bundle_dir = root / normalize_task_key(pipeline.name) if len(pipelines) > 1 else root
            write_bundle(workflow, bundle_dir)
            findings.extend(check_bundle_dir(bundle_dir).findings)
    return BundleInvariantResult(findings=findings)
