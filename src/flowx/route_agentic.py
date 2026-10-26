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
  and records their hashes; that copy is never written again, and later routes, fills and merges
  rebuild from it only while it still has those hashes.
* The agent's conversion output lives in ``metadata/agentic_conversion.json``: one entry per agentic
  routing unit (the authored pipelines, their canonical hash and the history of earlier outputs it
  replaced), plus the fills of convert's own gaps merged after routing. Each entry and each fill also
  carries a hash over every one of its fields (an entry's with its unit id), so an entry moved to
  another unit, a fill pointed at another activity, or an edited history is never applied. Filling a
  unit again with the same pipelines is a no-op; different pipelines replace the stored entry and add
  to its history.
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
import secrets
import tempfile
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from flowx.discovery_insights import _inventory_pipelines
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

# Held in metadata/ while route, fill-agentic or a merge into the live report reads and rewrites the plan, the
# report, its gaps and the agent's output, so two of them never interleave on one output folder.
ROUTE_LOCK_FILENAME = ".route.lock"
# A hosted agent cannot delete files in output_dir, but discover moves metadata/, the lock with it, into the archive.
ROUTE_LOCK_HELD = (
    f"another route, fill-agentic or merge is writing to this output folder (metadata/{ROUTE_LOCK_FILENAME} "
    "exists); run this again once it finishes. If none is running, one was stopped mid-write: locally, delete "
    f"metadata/{ROUTE_LOCK_FILENAME} and run route again; on the hosted MCP server, run discover again (it moves "
    "metadata/ and .work/, the lock with them, into metadata/archive/), then enrich if you used it, convert, "
    "route and fill again"
)

# Top-level report key holding the routing record. It lives in the report, so a fresh convert (which
# rewrites the report) clears it; the package phase and ir_serde read only the pipelines beside it.
ROUTING_RECORD_KEY = "_routing_record"

# What became of each routing unit in the record: a deterministic unit is left as convert wrote it;
# an agentic one stays "not viable" until fill-agentic fills it, and package refuses until then.
OUTCOME_DETERMINISTIC = "deterministic"
OUTCOME_AGENTIC_APPLIED = "agentic-applied"
OUTCOME_AGENTIC_NOT_VIABLE = "agentic-not-viable"

BASELINE_CHANGED = "the deterministic baseline is missing or changed; re-run convert, then route"

# The store's two top-level keys; every save writes only these.
_STORE_KEYS = frozenset({"components", "gap_fills"})
# Every field a stored unit output and a stored gap fill hold; any other key means it was edited.
_OUTPUT_KEYS = frozenset({"members", "pipelines", "output_sha256", "replaced", "entry_sha256"})
_GAP_FILL_KEYS = frozenset(
    {"pipeline", "activity_name", "task", "task_sha256", "baseline_report_sha256", "fill_sha256"}
)

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
# Where an ADF activity's typeProperties hold nested activities (ForEach / Until / If / Switch).
_ADF_NESTED_ACTIVITY_KEYS = ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities")


# --------------------------------------------------------------------------- #
# Reading the recorded / authored plan.
# --------------------------------------------------------------------------- #


def agentic_pipeline_names(plan: dict[str, Any]) -> set[str]:
    """Return the member pipelines of every routing unit the plan routes ``"agentic"``.

    Read from :func:`routing_units`, so an accepted grouping's members count, and this always agrees
    with the units the record, the fill and package work from. Takes either an authored plan (the
    agent-supplied ``{"components": [...]}``) or the recorded ``conversion_plan.json``.
    """
    return {
        member
        for unit in routing_units(plan).values()
        if unit["decision"] == DECISION_AGENTIC
        for member in unit["members"]
    }


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


def missing_report_message(output_dir: Path) -> str:
    """Say why convert's report is missing and what to run, for route and fill-agentic.

    Package prunes ``.work/`` once it has built the bundle, so after a package the report is gone even
    though convert ran. A ``databricks.yml`` at the top or one folder down shows a package has run.
    """
    root = Path(output_dir)
    work = root / WORK_DIRNAME
    if (root / "databricks.yml").is_file() or any(root.glob("*/databricks.yml")):
        return (
            f"No {REPORT_FILENAME} under {work}: package pruned .work/ when it built the bundle. Re-run convert "
            "with the same source, then route, which re-applies the plan and the stored fills; then run "
            "fill-agentic, modify and package as needed."
        )
    return f"No {REPORT_FILENAME} under {work}; run the convert phase first."


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
        if unit_id not in planned:
            mismatches.append(f"component {unit_id!r} is in the routing record but not in the plan")
            continue
        mismatch = unit_record_mismatch(unit_id, recorded.get(unit_id), planned[unit_id])
        if mismatch is not None:
            mismatches.append(mismatch)
    return mismatches


def unit_record_mismatch(unit_id: str, entry: Any, unit: dict[str, Any]) -> str | None:
    """Why a unit's routing-record entry does not show route applied the plan's unit, or ``None`` when it does.

    Args:
        unit_id: The unit's id.
        entry: The unit's entry in the routing record, or ``None`` when it has none.
        unit: The unit as :func:`routing_units` reads it from the plan.
    """
    if not isinstance(entry, dict):
        return f"component {unit_id!r} is in the plan but not in the routing record"
    members = entry.get("members")
    if not isinstance(members, list) or not all(isinstance(member, str) for member in members):
        return f"component {unit_id!r} has no list of member names in the routing record"
    if sorted(members) != unit["members"]:
        return f"component {unit_id!r} was routed with different members than the plan lists"
    if entry.get("decision") != unit["decision"]:
        return f"component {unit_id!r} was routed {entry.get('decision')!r} but the plan decides {unit['decision']!r}"
    if unit["decision"] != DECISION_AGENTIC and entry.get("outcome") != OUTCOME_DETERMINISTIC:
        return f"a fill was applied to component {unit_id!r}, which the plan routes deterministic"
    return None


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


def load_routed_baseline(
    output_dir: Path, record: dict[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]], bytes, bytes]:
    """Read the saved baseline a routed report was rebuilt from, as :func:`load_baseline` returns it.

    The routing record names the hashes of the baseline files route saved. A copy whose bytes no
    longer have those hashes was changed after route saved it, so it is refused rather than rebuilt
    from, which would stamp a fresh record over the change.

    Raises:
        ValueError: The saved baseline is missing, is not valid JSON, or no longer has the record's hashes.
    """
    baseline = load_baseline(output_dir, fresh=False)
    recorded = (record.get("baseline_report_sha256"), record.get("baseline_gaps_sha256"))
    if recorded != (hashlib.sha256(baseline[2]).hexdigest(), hashlib.sha256(baseline[3]).hexdigest()):
        raise ValueError(BASELINE_CHANGED)
    return baseline


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
        outputs: The unit entries that check out (sorted members, a hash matching their pipelines, and
            an entry hash matching their unit id and every field), keyed by unit id. Only these are
            ever applied.
        gap_fills: The fills of convert's own gaps that check out (a task hash and a fill hash matching
            every field), in stored order.
        edited: A label for every entry that no longer checks out because it was edited by hand.
    """

    document: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    gap_fills: list[dict[str, Any]] = field(default_factory=list)
    edited: list[str] = field(default_factory=list)


def _entry_sha256(unit_id: str, entry: dict[str, Any]) -> str:
    """The hash of a stored unit output: its unit id and every field but this hash."""
    return canonical_sha256(
        {"unit_id": unit_id, **{key: value for key, value in entry.items() if key != "entry_sha256"}}
    )


def _fill_sha256(fill: dict[str, Any]) -> str:
    """The hash of a stored gap fill: every field but this hash."""
    return canonical_sha256({key: value for key, value in fill.items() if key != "fill_sha256"})


def _output_checks_out(unit_id: str, entry: Any) -> bool:
    """Whether a stored unit output is as fill-agentic wrote it for this unit.

    That is: exactly the fields it writes, sorted members, a hash matching its pipelines, a history of
    ``{"from", "to"}`` hashes, and an entry hash matching the unit id and every field.
    """
    if not isinstance(entry, dict) or set(entry) != _OUTPUT_KEYS:
        return False
    members = entry["members"]
    pipelines = entry["pipelines"]
    replaced = entry["replaced"]
    return (
        isinstance(members, list)
        and all(isinstance(member, str) for member in members)
        and members == sorted(set(members))
        and isinstance(pipelines, list)
        and bool(pipelines)
        and entry["output_sha256"] == canonical_sha256(pipelines)
        and isinstance(replaced, list)
        and all(
            isinstance(item, dict) and set(item) == {"from", "to"} and all(isinstance(v, str) for v in item.values())
            for item in replaced
        )
        and entry["entry_sha256"] == _entry_sha256(unit_id, entry)
    )


def _gap_fill_checks_out(entry: Any) -> bool:
    """Whether a stored gap fill is as the merge wrote it: its fields, a matching task hash and fill hash."""
    return (
        isinstance(entry, dict)
        and set(entry) == _GAP_FILL_KEYS
        and isinstance(entry["pipeline"], str)
        and isinstance(entry["activity_name"], str)
        and bool(entry["activity_name"])
        and isinstance(entry["task"], dict)
        and entry["task_sha256"] == canonical_sha256(entry["task"])
        and isinstance(entry["baseline_report_sha256"], str)
        and entry["fill_sha256"] == _fill_sha256(entry)
    )


def _holds_route_lock(lock_path: Path, token: str) -> bool:
    """Whether the lock file at *lock_path* is still the one this command created (holding *token*)."""
    try:
        return lock_path.read_text(encoding="utf-8") == token
    except OSError:
        return False


@contextmanager
def route_lock(output_dir: Path) -> Iterator[bool]:
    """Hold the output folder's route lock, yielding ``False`` when another command already holds it.

    Route, fill-agentic and a merge into the live report each read the plan, the report, its gaps and
    the agent's output and then write some of them, so each takes this lock first and refuses with
    :data:`ROUTE_LOCK_HELD` while another holds it. Like enrich's lock it is a file created with
    ``O_EXCL`` holding a random token, so a command never removes a lock a later one created after
    discover moved ``metadata/`` away. It is removed when the command finishes or raises; only a killed
    process leaves it behind. Discover and convert take no lock. An output folder with no ``metadata/``
    yet gets one, so a merge there is still held; a folder that does not exist holds no files to
    guard, and the command then reports what it is missing.
    """
    if not Path(output_dir).is_dir():
        yield True
        return
    metadata_dir = Path(output_dir) / METADATA_DIRNAME
    metadata_dir.mkdir(exist_ok=True)
    lock_path = metadata_dir / ROUTE_LOCK_FILENAME
    token = secrets.token_hex(16)
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        yield False
        return
    try:
        written = os.write(descriptor, token.encode("ascii"))
    finally:
        os.close(descriptor)
    if written != len(token):
        lock_path.unlink(missing_ok=True)
        raise OSError(f"could not write the route lock {lock_path}")
    try:
        yield True
    finally:
        if _holds_route_lock(lock_path, token):
            lock_path.unlink(missing_ok=True)


def load_agentic_output(output_dir: Path) -> AgenticOutput:
    """Read ``metadata/agentic_conversion.json``; empty when the agent has written nothing yet.

    Route, fill-agentic, the merge and package all read the store here. A rebuild applies only the
    entries that check out, so an edited entry is never applied: package refuses while one exists,
    and filling (or merging) that entry again replaces it.

    Raises:
        ValueError: The file is not valid JSON or not shaped as ``{"components": {...}, "gap_fills": [...]}``,
            with no other top-level key (every save writes only those two, so another would be dropped).
    """
    path = Path(output_dir) / METADATA_DIRNAME / AGENTIC_OUTPUT_FILENAME
    if not path.exists():
        return AgenticOutput()
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"{AGENTIC_OUTPUT_FILENAME} is not valid JSON: {error}") from error
    unknown = sorted(set(document) - _STORE_KEYS) if isinstance(document, dict) else []
    if unknown:
        raise ValueError(f"{AGENTIC_OUTPUT_FILENAME} has unknown top-level key(s) {unknown}; remove them")
    components = document.get("components", {}) if isinstance(document, dict) else None
    gap_fills = document.get("gap_fills", []) if isinstance(document, dict) else None
    if not isinstance(components, dict) or not isinstance(gap_fills, list):
        raise ValueError(f"{AGENTIC_OUTPUT_FILENAME} must hold a 'components' object and a 'gap_fills' list")
    outputs = {unit_id: entry for unit_id, entry in components.items() if _output_checks_out(unit_id, entry)}
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

    Each fill is ``{"pipeline", "activity_name", "task"}``; it is stored with its task's hash, the
    baseline it was merged against and a hash over all of those, replacing an earlier fill of the same
    pipeline and activity. The baseline itself is never changed.
    """
    stored = load_agentic_output(output_dir).document or {"components": {}, "gap_fills": []}
    by_key = {
        (fill.get("pipeline"), fill.get("activity_name")): fill
        for fill in stored.get("gap_fills", [])
        if isinstance(fill, dict)
    }
    for fill in fills:
        task = copy.deepcopy(fill["task"])
        stored_fill = {
            "pipeline": fill["pipeline"],
            "activity_name": fill["activity_name"],
            "task": task,
            "task_sha256": canonical_sha256(task),
            "baseline_report_sha256": baseline_report_sha256,
        }
        by_key[(fill["pipeline"], fill["activity_name"])] = {**stored_fill, "fill_sha256": _fill_sha256(stored_fill)}
    save_agentic_output(output_dir, {"components": stored.get("components", {}), "gap_fills": list(by_key.values())})


def applied_gap_fills(gap_fills: list[dict[str, Any]] | None, baseline_report_sha256: str) -> list[dict[str, Any]]:
    """The stored gap fills a rebuild from this baseline applies, in stored order."""
    return [fill for fill in gap_fills or [] if fill.get("baseline_report_sha256") == baseline_report_sha256]


def drop_filled_gaps(
    gaps: list[Any],
    report: dict[str, Any],
    routed: set[str],
    baseline_report: dict[str, Any],
    fills: list[dict[str, Any]],
) -> list[Any]:
    """Take convert's gaps for the work the gap fills did out of ``gaps``, so the list shows only what's left.

    A fill replaces one task, so convert's gap for it, and for every activity nested in it, is done.
    Convert's gaps carry no pipeline, so one is dropped only when no pipeline of ``report`` outside
    ``routed`` still has that name open: a placeholder, an activity in a placeholder's source definition,
    or an activity a motif collapsed. A same-named gap still open in another pipeline may be that
    pipeline's own, so it is kept. A routed pipeline's work is listed in its own pipeline-tagged gaps,
    which are never dropped here.

    Args:
        gaps: The gaps list to filter.
        report: The report the fills have been applied to.
        routed: The pipelines routed agentic, or authored by fill-agentic.
        baseline_report: Route's deterministic baseline, which holds each task as convert wrote it.
        fills: The gap fills applied, each naming its ``pipeline`` and ``activity_name``.
    """
    baseline_pipelines = {pipeline.get("name"): pipeline for pipeline in _report_pipelines(baseline_report)}
    filled: set[str] = set()
    for fill in fills:
        filled.add(str(fill["activity_name"]))
        pipeline = baseline_pipelines.get(fill["pipeline"])
        replaced = next(
            (task for task in _walk_tasks((pipeline or {}).get("tasks")) if task.get("name") == fill["activity_name"]),
            None,
        )
        if replaced is not None:
            filled.update(_task_names([replaced]))
    still_open: set[str] = set()
    for pipeline in _report_pipelines(report):
        if pipeline.get("name") not in routed:
            still_open.update(_open_task_names(pipeline.get("tasks")))
    done = filled - still_open
    return [
        gap
        for gap in gaps
        if not (isinstance(gap, dict) and gap.get("pipeline") is None and gap.get("activity_name") in done)
    ]


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
    *,
    inventory: dict[str, Any] | None,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Pure rebuild: derive the routed report, gaps, and record from the baseline, fills and plan.

    Returns ``(report, gaps, record)``, all deterministically derived from the inputs. It starts from
    a copy of the baseline, applies the stored fills of convert's own gaps that were merged against this
    baseline, then turns each agentic unit's tasks into placeholders and gaps, and finally, per unit:

    * **deterministic:** the baseline pipelines stay; outcome is ``deterministic``.
    * **agentic with a stored output whose members match:** the output's pipelines replace the members,
      and the gaps the unit owns are dropped: its routed gaps, and convert's gaps for the activities
      its pipelines hold -- tasks, activities collapsed into a motif, and activities the inventory
      lists for them that the report no longer has as tasks -- unless a pipeline that stays
      deterministic also holds that name. Outcome is ``agentic-applied``.
    * **agentic without a matching output:** placeholders and tagged gaps as ``alter_report`` does.
      Outcome is ``agentic-not-viable``, which package refuses.

    Convert's gaps for the work the applied gap fills did are then dropped (see :func:`drop_filled_gaps`),
    so the gaps list shows only what is left to convert.

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
        inventory: The discover ``inventory.json``, whose per-pipeline activities a filled unit also
            owns. Required, so no caller drops it by accident; ``None`` (no inventory to read) means a
            filled unit owns only what the report shows.
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

    applied = _applied_outputs(plan, outputs)
    units: dict[str, dict[str, Any]] = {}
    for unit_id, unit in routing_units(plan).items():
        members = set(unit["members"])
        output = applied.get(unit_id)
        output_sha256 = output.get("output_sha256") if output else None
        if output:
            outcome = OUTCOME_AGENTIC_APPLIED
            new_report = replace_unit_pipelines(new_report, members, output.get("pipelines", []))
            owned = _unit_owned_activity_names(report, members, agentic_names, inventory)
            new_gaps = [gap for gap in new_gaps if not _owned_gap(gap, members, owned)]
        elif unit["decision"] == DECISION_AGENTIC:
            outcome = OUTCOME_AGENTIC_NOT_VIABLE
        else:
            outcome = OUTCOME_DETERMINISTIC
        fingerprint = canonical_sha256(
            {"members": unit["members"], "decision": unit["decision"], "output_sha256": output_sha256}
        )
        units[unit_id] = {**unit, "outcome": outcome, "output_sha256": output_sha256, "fingerprint": fingerprint}
    authored = {
        str(pipeline.get("name"))
        for output in applied.values()
        for pipeline in output.get("pipelines", [])
        if isinstance(pipeline, dict) and isinstance(pipeline.get("name"), str)
    }
    new_gaps = drop_filled_gaps(new_gaps, new_report, agentic_names | authored, baseline_report, applicable_fills)
    record = {
        "conversion_plan_sha256": canonical_sha256(plan),
        "baseline_report_sha256": baseline_report_sha256,
        "baseline_gaps_sha256": hashlib.sha256(baseline_gaps_bytes).hexdigest(),
        "gap_fills_sha256": canonical_sha256(applicable_fills),
        "components": units,
    }
    return new_report, new_gaps, record


def _applied_outputs(plan: dict[str, Any], outputs: dict[str, dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    """The stored unit outputs a rebuild under ``plan`` applies: each agentic unit's, while its members match."""
    applied: dict[str, dict[str, Any]] = {}
    for unit_id, unit in routing_units(plan).items():
        stored = (outputs or {}).get(unit_id)
        if unit["decision"] == DECISION_AGENTIC and stored and set(stored.get("members", [])) == set(unit["members"]):
            applied[unit_id] = stored
    return applied


# --------------------------------------------------------------------------- #
# The edit step: rewrite the report for routed-agentic groups.
# --------------------------------------------------------------------------- #


def _unit_owned_activity_names(
    report: dict[str, Any], members: set[str], agentic_names: set[str], inventory: dict[str, Any] | None
) -> set[str]:
    """The activity names whose convert gaps a filled unit owns.

    That is every name the unit's pipelines hold in the report (tasks, containers' children and the
    activities a motif collapsed) or in the inventory (activities the report no longer has as tasks),
    less any name a pipeline that stays deterministic also holds, whose gap may be that pipeline's.
    Other agentic pipelines do not keep a name: their gaps go once they are filled too.
    """
    owned: set[str] = set()
    elsewhere: set[str] = set()
    for pipeline in _report_pipelines(report):
        name = pipeline.get("name")
        if name in members:
            owned.update(_task_names(pipeline.get("tasks")))
        elif name not in agentic_names:
            elsewhere.update(_task_names(pipeline.get("tasks")))
    for pipeline in _inventory_pipelines(inventory or {}):
        names = {
            str(activity.get("name"))
            for activity in pipeline.get("activities") or []
            if isinstance(activity, dict) and activity.get("name") is not None
        }
        if pipeline.get("name") in members:
            owned.update(names)
        elif pipeline.get("name") not in agentic_names:
            elsewhere.update(names)
    return owned - elsewhere


def _owned_gap(gap: Any, members: set[str], owned: set[str]) -> bool:
    """Whether a gap belongs to a filled unit: tagged with one of its pipelines, or an untagged convert gap it owns."""
    if not isinstance(gap, dict):
        return False
    if gap.get("pipeline") is None:
        return gap.get("activity_name") in owned
    return gap.get("pipeline") in members


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
        if pipeline.get("name") not in agentic:
            nonrouted_task_names.update(_task_names(pipeline.get("tasks")))

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
        # Convert also records gaps for activities nested in a container, and the container's tagged
        # gap now supersedes them, so nested names count as routed too.
        routed_task_names.update(_task_names(pipeline.get("tasks")))
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
        str(pipeline.get("name")) for pipeline in _inventory_pipelines(inventory) if pipeline.get("name") is not None
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


def apply_plan_to_report(output_dir: Path, plan: dict[str, Any], *, inventory: dict[str, Any] | None) -> dict[str, Any]:
    """Apply a routing decision to the report on disk by rebuilding it from the deterministic baseline.

    A report without a routing record is a fresh convert, so it becomes the baseline: the first time
    the rebuild changes anything, route saves it and its gaps to ``.work/route_baseline/``; gap fills
    merged against an earlier baseline are then dropped (merge them again). A routed report is rebuilt
    from the saved copy, refused when it no longer has the hashes the record names. The rebuild applies
    the stored gap fills, the plan and the stored unit outputs from ``metadata/agentic_conversion.json``,
    and the report + gaps are written atomically. When the rebuild changes nothing the baseline bytes
    are written back, so switching back to deterministic restores convert's output exactly and a
    never-routed report is left untouched. Route never writes modify's configured copy; package asks
    for ``modify`` again when it is out of date.

    ``inventory`` is required so no caller skips it by accident. With an inventory and an agentic unit
    in the plan, the baseline's pipelines must be the inventory's (see :func:`baseline_pipeline_mismatch`),
    and a filled unit also owns the convert gaps of the activities the inventory lists for its pipelines
    (see :func:`rebuild`); ``None`` is only for a caller with no inventory to check against.

    Returns a summary dict with the altered pipeline names, the resulting gap count, each unit's
    outcome and how many stale gap fills were dropped.

    Raises:
        FileNotFoundError: when the translation report is missing (run convert first).
        ValueError: when a routed report's saved baseline or the agent's output cannot be read, the
            saved baseline no longer has the record's hashes, or it no longer matches the inventory.
    """
    work = Path(output_dir) / WORK_DIRNAME
    report_path = work / REPORT_FILENAME
    gaps_path = work / GAPS_FILENAME
    if not report_path.exists():
        raise FileNotFoundError(missing_report_message(Path(output_dir)))

    report = json.loads(report_path.read_bytes())
    record = routing_record(report)
    baseline_report, baseline_gaps, baseline_report_bytes, baseline_gaps_bytes = (
        load_baseline(output_dir, fresh=True) if record is None else load_routed_baseline(output_dir, record)
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
        inventory=inventory,
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
    accepted groupings of them).

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
) -> tuple[str | None, dict[str, Any] | None, dict[str, Any] | None, str | None]:
    """Bind ``members`` to a routed-**agentic** unit in the recorded, fingerprint-bound plan.

    Reads ``metadata/conversion_plan.json`` and ``metadata/inventory.json`` and returns
    ``(unit_id, plan, inventory, error)``, where ``plan`` is the recorded plan document and
    ``inventory`` the inventory it was checked against, read once so the fill rebuilds against that
    same one; every authored pipeline must carry its source. The error is set (and the other
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
            return unit_id, plan, inventory, None
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


def _walk_tasks(tasks: Any) -> Iterator[dict[str, Any]]:
    """Every task among ``tasks``, followed by the tasks nested in its ForEach / If / Switch containers."""
    for task in tasks if isinstance(tasks, list) else []:
        if not isinstance(task, dict):
            continue
        yield task
        for key in _NESTED_TASK_KEYS:
            yield from _walk_tasks(task.get(key))
        for case in task.get("cases") or []:
            if isinstance(case, dict):
                yield from _walk_tasks(case.get("activities"))


def _adf_activity_names(activities: Any) -> Iterator[str]:
    """The names of the ADF activities among ``activities`` and of every activity nested inside them."""
    for activity in activities if isinstance(activities, list) else []:
        if not isinstance(activity, dict):
            continue
        yield str(activity.get("name"))
        type_properties = activity.get("typeProperties")
        if not isinstance(type_properties, dict):
            continue
        for key in _ADF_NESTED_ACTIVITY_KEYS:
            yield from _adf_activity_names(type_properties.get(key))
        for case in type_properties.get("cases") or []:
            if isinstance(case, dict):
                yield from _adf_activity_names(case.get("activities"))


def _task_names(tasks: Any) -> set[str]:
    """The names of every task among ``tasks``, looking inside containers too.

    Convert keeps an agentic container such as an ``Until`` as one placeholder, but still records a gap
    for each activity inside it, so the names nested in a placeholder's ADF definition count as well.
    So do the activities a motif collapsed, which survive only in its ``matched_activity_names``.
    """
    names: set[str] = set()
    for task in _walk_tasks(tasks):
        names.add(str(task.get("name")))
        if task.get("type") == "PlaceholderActivity":
            names.update(_adf_activity_names([task.get("raw_definition")]))
        if task.get("type") == "MotifActivity":
            names.update(str(name) for name in task.get("matched_activity_names") or [])
    return names


def _open_task_names(tasks: Any) -> set[str]:
    """The names among ``tasks`` that still need converting, the only ones a convert gap can belong to.

    That is each placeholder and the activities nested in its source definition, and the activities a
    motif collapsed, looking inside containers too.
    """
    names: set[str] = set()
    for task in _walk_tasks(tasks):
        if task.get("type") == "PlaceholderActivity":
            names.add(str(task.get("name")))
            names.update(_adf_activity_names([task.get("raw_definition")]))
        if task.get("type") == "MotifActivity":
            names.update(str(name) for name in task.get("matched_activity_names") or [])
    return names


def _placeholder_task_names(tasks: Any) -> list[str]:
    """The names of every ``PlaceholderActivity`` among ``tasks``, looking inside containers too."""
    return [str(task.get("name")) for task in _walk_tasks(tasks) if task.get("type") == "PlaceholderActivity"]


def _authored_placeholder_violations(authored_pipelines: list[dict[str, Any]]) -> list[str]:
    """Reports each authored pipeline that still holds a placeholder task, which would ship as a stub."""
    violations: list[str] = []
    for index, pipeline in enumerate(authored_pipelines):
        if not isinstance(pipeline, dict) or not isinstance(pipeline.get("tasks"), list):
            continue
        placeholders = _placeholder_task_names(pipeline.get("tasks"))
        if placeholders:
            label = pipeline.get("name") or f"pipeline[{index}]"
            violations.append(
                f"{label}: still has placeholder tasks {placeholders}; convert every task before filling the unit"
            )
    return violations


def _authored_name_violations(authored_pipelines: list[dict[str, Any]]) -> list[str]:
    """Reports each authored pipeline whose name isn't a non-empty string, before any name is compared."""
    violations: list[str] = []
    for index, pipeline in enumerate(authored_pipelines):
        if not isinstance(pipeline, dict):
            continue
        name = pipeline.get("name")
        if not isinstance(name, str) or not name.strip():
            violations.append(
                f"pipeline[{index}]: authored pipeline name {name!r} is not a non-empty string; "
                "give every pipeline a name"
            )
    return violations


def _authored_buildable_violations(authored_pipelines: list[dict[str, Any]]) -> list[str]:
    """Reports each authored pipeline package wouldn't build: no tasks list to run, or marked excluded."""
    violations: list[str] = []
    for index, pipeline in enumerate(authored_pipelines):
        if not isinstance(pipeline, dict):
            continue
        label = pipeline.get("name") if isinstance(pipeline.get("name"), str) else f"pipeline[{index}]"
        tasks = pipeline.get("tasks")
        if not isinstance(tasks, list) or not tasks or not all(isinstance(task, dict) for task in tasks):
            violations.append(f"{label}: authored pipeline needs a non-empty 'tasks' list; give it the tasks to run")
        if pipeline.get("migration_status") == "excluded":
            violations.append(
                f"{label}: authored pipeline is marked migration_status 'excluded', which package skips; "
                "leave it out of the fill or drop the status"
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
    plan, and every authored pipeline must carry the correct source tag, hold a non-empty ``tasks`` list
    of task objects with no placeholder task, not be marked ``migration_status`` ``excluded``, and have a
    non-empty string name of its own: no repeat within the list, no member of another unit, no pipeline
    another unit's applied output authored, and no bundle folder shared with another pipeline once
    normalised. A name may reuse a member of this unit, since the fill replaces it. A stored output the
    rebuild no longer applies (its unit routed deterministic since, or joined into a grouping) holds no
    name.

    Returns ``{"ok", "violations", "error", "component_id", "pipelines", "already_applied", "message"}``.
    ``ok`` is ``False`` (and nothing written) on a plan/membership error, source tag violation, a
    placeholder left in, a missing tasks list, an excluded pipeline, a bad or clashing name, structural
    validation failure, or any other error. Empty
    ``authored_pipelines`` is refused, and so is a fill while another route, fill-agentic or merge holds
    the output folder's route lock (see :func:`route_lock`).

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
    with route_lock(output_dir) as held:
        if not held:
            return {"ok": False, "error": None, "violations": [ROUTE_LOCK_HELD], "pipelines": 0}
        return _fill_unit(output_dir, members, authored_pipelines)


def _fill_unit(output_dir: Path, members: set[str], authored_pipelines: list[dict[str, Any]]) -> dict[str, Any]:
    """The body of :func:`apply_agentic_output`, run while holding the output folder's route lock."""
    from flowx.routing import inventory_source

    unit_id, plan, inventory, error = _resolve_agentic_unit(output_dir, members)
    if error is not None:
        return {"ok": False, "error": error, "violations": [], "pipelines": 0}
    assert unit_id is not None and plan is not None and inventory is not None
    source = inventory_source(inventory)
    assert source is not None

    authoring_violations = _authored_name_violations(authored_pipelines)
    authoring_violations += _authored_buildable_violations(authored_pipelines)
    authoring_violations += _authored_source_tag_violations(authored_pipelines, source)
    authoring_violations += _authored_placeholder_violations(authored_pipelines)
    if authoring_violations:
        return {"ok": False, "error": None, "violations": authoring_violations, "pipelines": 0}

    work = Path(output_dir) / WORK_DIRNAME
    report_path = work / REPORT_FILENAME
    if not report_path.exists():
        raise FileNotFoundError(missing_report_message(Path(output_dir)))

    report = json.loads(report_path.read_text(encoding="utf-8"))
    record = routing_record(report)
    if record is None:
        error = f"{REPORT_FILENAME} carries no routing record; run `route` to apply the plan before filling."
        return {"ok": False, "error": error, "violations": [], "pipelines": 0}
    if routing_record_mismatches(record, plan):
        error = "the report was routed under a different plan; re-run route, then fill-agentic again"
        return {"ok": False, "error": error, "violations": [], "pipelines": 0}
    try:
        baseline_report, baseline_gaps, baseline_report_bytes, baseline_gaps_bytes = load_routed_baseline(
            output_dir, record
        )
    except ValueError as baseline_error:
        return {"ok": False, "error": str(baseline_error), "violations": [], "pipelines": 0}

    output_sha256 = canonical_sha256(authored_pipelines)
    stored = load_agentic_output(output_dir)
    authored_names = [pipeline.get("name") for pipeline in authored_pipelines]
    taken_names = {
        member for other_id, unit in routing_units(plan).items() if other_id != unit_id for member in unit["members"]
    } | {
        pipeline.get("name")
        for other_id, entry in _applied_outputs(plan, stored.outputs).items()
        if other_id != unit_id
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

    # Only an entry that still checks out carries its history on; an edited one's history can't be trusted.
    replaced = list(previous["replaced"]) if previous is not None else []
    if previous is not None and previous["output_sha256"] != output_sha256:
        replaced.append({"from": previous["output_sha256"], "to": output_sha256})
    entry: dict[str, Any] = {
        "members": sorted(members),
        "pipelines": authored_pipelines,
        "output_sha256": output_sha256,
        "replaced": replaced,
    }
    entry["entry_sha256"] = _entry_sha256(unit_id, entry)
    new_report, new_gaps, new_record = rebuild(
        baseline_report,
        baseline_gaps,
        plan,
        {**stored.outputs, unit_id: entry},
        stored.gap_fills,
        baseline_report_bytes,
        baseline_gaps_bytes,
        inventory=inventory,
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
