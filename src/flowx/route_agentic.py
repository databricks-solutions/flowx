"""Phase-1 in-engine agentic conversion: apply a routing decision, then fill the gaps.

``convert`` always produces a deterministic ``.work/translation_report.json`` -- that translation is
**unchanged** by this module. Routing then decides, per connected component, whether each part
converts deterministically or agentically (:mod:`flowx.routing` computes the recommendation and
records the fingerprint-bound ``metadata/conversion_plan.json``). This module carries out the
**post-convert alteration and fill** the decision implies, using an immutable-baseline and pure-rebuild
pattern similar to :mod:`flowx.agentic`, always keeping the work in-engine so the package phase,
structural validation, and provenance all still apply:

* When route runs with at least one agentic component on a fresh convert (no routing record), it saves
  an immutable copy of ``.work/translation_report.json`` and ``.work/gaps.json`` to
  ``.work/route_baseline/``, records their hashes, and never modifies them. Later routes verify and
  reuse the baseline. Combines are stored in ``metadata/agentic_combines.json`` and are idempotent:
  the same combine is a no-op, a different combine replaces the stored entry.
* :func:`rebuild` is a pure function that takes the immutable baseline, the routing plan, and stored
  combines, and produces edited report + gaps + routing record. It implements both re-routing and
  re-applying combines deterministically: every rebuild call with the same inputs produces identical
  bytes. The routing record tracks each component's outcome (deterministic, agentic-applied, or
  agentic-not-viable) and fingerprint changes.
* The agent authors the fill. A routed-agentic component is filled **only** by the cross-pipeline
  COMBINE (N pipelines -> M, e.g. one Lakeflow Connect pipeline, or one same-named pipeline to keep
  it 1:1): stored in ``metadata/agentic_combines.json`` and applied via rebuild. The name-matched
  :func:`flowx.ir_serde.merge_agentic_results` stays for convert's own gaps (after routing, merges
  update both the live report and the stored baseline so the next rebuild keeps them) and refuses
  routed-agentic pipelines.
* :func:`validate_report_structurally` packages the merged report through the same
  ``prepare -> write_bundle`` path the package phase uses and runs the existing
  :func:`flowx.validate.bundle_invariants.check_bundle_dir` over the output, so a fill can never
  introduce duplicate keys, dangling dependencies, a cycle, or a dangling pipeline/run_job reference.

Route stamps one routing record onto the report (:data:`ROUTING_RECORD_KEY`), keyed by the plan's
component ids, holding each component's members, decision, outcome, and a fingerprint. The routing
record also tracks the baseline hashes and combine hashes. Every rebuild carries it forward with
updated outcomes and fingerprints.

There is no LLM here: the tool edits the report and validates the authored fill; the fill itself is
supplied by the agent/harness, following the same ask -> author -> continue pattern as the existing
agentic flow.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import sys
import tempfile
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from flowx.discovery_serde import canonical_sha256
from flowx.models.conversion_plan import DECISION_AGENTIC, ConversionPlan

# The report + gaps live under the shared output dir's transient .work/ folder, beside the pipeline IR.
WORK_DIRNAME = ".work"
REPORT_FILENAME = "translation_report.json"
# The configuration-stamped copy modify writes beside the report; package reads it when present.
STAMPED_REPORT_FILENAME = "translation_report.stamped.json"
GAPS_FILENAME = "gaps.json"
# The immutable baseline copies, saved on first route with agentic components.
BASELINE_DIRNAME = "route_baseline"
BASELINE_REPORT_FILENAME = "translation_report.json"
BASELINE_GAPS_FILENAME = "gaps.json"
# The recorded plan + inventory the combine fill binds against live under metadata/.
METADATA_DIRNAME = "metadata"
INVENTORY_FILENAME = "inventory.json"
COMBINES_FILENAME = "agentic_combines.json"

# Top-level report key holding the routing record. It lives in the report, so a fresh convert (which
# rewrites the report) clears it; the package phase and ir_serde read only the pipelines beside it.
ROUTING_RECORD_KEY = "_routing_record"

# What became of each component in the record: a deterministic component is left as convert wrote it;
# an agentic one stays "not viable" until combine fills it.
OUTCOME_DETERMINISTIC = "deterministic"
OUTCOME_AGENTIC_APPLIED = "agentic-applied"
OUTCOME_AGENTIC_NOT_VIABLE = "agentic-not-viable"

# Legacy: kept for backward compatibility with tests, but no longer used by route.
REROUTED_UNDER_DIFFERENT_PLAN = "the report was routed under a different plan; re-run convert, then route"

COMPONENT_ALREADY_FILLED = "component already filled; re-run convert and route to start again"
ROUTED_AGENTIC_MERGE_REFUSED = (
    "this pipeline is routed agentic; fill it with fill-agentic combine "
    "(to change the approach, change the plan, re-run convert and route, then combine again)"
)

# Guidance stamped onto every placeholder the alteration produces.
_PLACEHOLDER_COMMENT = (
    "Routed agentic by the conversion plan; replace the whole group with agent-authored pipeline(s) "
    "using fill-agentic combine."
)


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


# --------------------------------------------------------------------------- #
# The routing record stamped onto the report.
# --------------------------------------------------------------------------- #


def _planned_components(plan: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Each plan component's sorted members and decision, keyed by component id."""
    return {
        str(component.get("component_id")): {
            "members": sorted(str(member) for member in component.get("members", [])),
            "decision": component.get("decision"),
        }
        for component in plan.get("components", [])
        if isinstance(component, dict)
    }


def build_routing_record(
    plan: dict[str, Any],
    baseline_report_bytes: bytes,
    baseline_gaps_bytes: bytes,
    combines: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Build a routing record for a plan applied to a fresh convert.

    Records the plan hash, baseline file hashes, and per-component information (members, decision,
    outcome, combine hash, and fingerprint).
    """
    planned = _planned_components(plan)
    components: dict[str, dict[str, Any]] = {}
    for component_id, entry in planned.items():
        combine_data = combines.get(component_id)
        combine_sha256 = combine_data.get("combine_sha256") if combine_data else None
        decision = entry["decision"]
        outcome = OUTCOME_AGENTIC_NOT_VIABLE if decision == DECISION_AGENTIC else OUTCOME_DETERMINISTIC
        fingerprint_input = {
            "members": entry["members"],
            "decision": decision,
            "combine_sha256": combine_sha256,
        }
        components[component_id] = {
            **entry,
            "outcome": outcome,
            "combine_sha256": combine_sha256,
            "fingerprint": canonical_sha256(fingerprint_input),
        }
    return {
        "conversion_plan_sha256": canonical_sha256(plan),
        "baseline_report_sha256": hashlib.sha256(baseline_report_bytes).hexdigest(),
        "baseline_gaps_sha256": hashlib.sha256(baseline_gaps_bytes).hexdigest(),
        "components": components,
    }


def routing_record(report: Any) -> dict[str, Any] | None:
    """The routing record a report carries, or ``None`` when route has not edited it."""
    record = report.get(ROUTING_RECORD_KEY) if isinstance(report, dict) else None
    return record if isinstance(record, dict) else None


def routing_record_mismatches(record: dict[str, Any], plan: dict[str, Any]) -> list[str]:
    """Where a report's routing record disagrees with a plan's components, members or decisions.

    Returns one message per component that differs; an empty list means the record was routed under
    these same decisions.
    """
    recorded = record.get("components")
    if not isinstance(recorded, dict):
        recorded = {}
    planned = _planned_components(plan)
    mismatches: list[str] = []
    for component_id in sorted(set(recorded) | set(planned)):
        entry = recorded.get(component_id)
        decided = planned.get(component_id)
        if not isinstance(entry, dict):
            mismatches.append(f"component {component_id!r} is in the plan but not in the routing record")
        elif decided is None:
            mismatches.append(f"component {component_id!r} is in the routing record but not in the plan")
        elif sorted(entry.get("members") or []) != decided["members"]:
            mismatches.append(f"component {component_id!r} was routed with different members than the plan lists")
        elif entry.get("decision") != decided["decision"]:
            mismatches.append(
                f"component {component_id!r} was routed {entry.get('decision')!r} but the plan decides "
                f"{decided['decision']!r}"
            )
        elif decided["decision"] != DECISION_AGENTIC and entry.get("outcome") != OUTCOME_DETERMINISTIC:
            mismatches.append(f"a fill was applied to component {component_id!r}, which the plan routes deterministic")
    return mismatches


def routed_agentic_pipelines(report: Any) -> set[str]:
    """The pipelines only combine may fill: routed-agentic members and the pipelines combine authored.

    Every pipeline convert wrote is a member of some recorded component, so a pipeline in the report
    that belongs to none was authored by a combine. Empty when the report has no routing record.
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
# Baseline management and combines store.
# --------------------------------------------------------------------------- #


def _baseline_exists(output_dir: Path) -> bool:
    """Check whether a baseline has been saved."""
    baseline_dir = Path(output_dir) / WORK_DIRNAME / BASELINE_DIRNAME
    return (baseline_dir / BASELINE_REPORT_FILENAME).exists() or (baseline_dir / BASELINE_GAPS_FILENAME).exists()


def _load_baseline(output_dir: Path) -> tuple[dict[str, Any], list[dict[str, Any]], bytes, bytes, str | None]:
    """Load the immutable baseline report and gaps, creating it from current live ones if needed.

    Returns ``(report, gaps, report_bytes, gaps_bytes, error)`` where error is set when trying to
    load an existing baseline and it fails to read. The bytes are used to compute baseline hashes
    for the routing record.
    """
    baseline_dir = Path(output_dir) / WORK_DIRNAME / BASELINE_DIRNAME
    baseline_report_path = baseline_dir / BASELINE_REPORT_FILENAME
    baseline_gaps_path = baseline_dir / BASELINE_GAPS_FILENAME
    if baseline_report_path.exists() or baseline_gaps_path.exists():
        try:
            report_bytes = baseline_report_path.read_bytes() if baseline_report_path.exists() else b"{}"
            gaps_bytes = baseline_gaps_path.read_bytes() if baseline_gaps_path.exists() else b"[]"
            report = json.loads(report_bytes) if report_bytes else {}
            gaps = json.loads(gaps_bytes) if gaps_bytes else []
            return report, gaps if isinstance(gaps, list) else [], report_bytes, gaps_bytes, None
        except (OSError, json.JSONDecodeError) as error:
            return {}, [], b"{}", b"[]", str(error)
    work_dir = Path(output_dir) / WORK_DIRNAME
    live_report_path = work_dir / REPORT_FILENAME
    live_gaps_path = work_dir / GAPS_FILENAME
    try:
        report_bytes = live_report_path.read_bytes() if live_report_path.exists() else b"{}"
        gaps_bytes = live_gaps_path.read_bytes() if live_gaps_path.exists() else b"[]"
        report = json.loads(report_bytes) if report_bytes else {}
        gaps = json.loads(gaps_bytes) if gaps_bytes else []
        return report, gaps if isinstance(gaps, list) else [], report_bytes, gaps_bytes, None
    except (OSError, json.JSONDecodeError):
        return {}, [], b"{}", b"[]", None


def _save_baseline_bytes(output_dir: Path, report_bytes: bytes, gaps_bytes: bytes) -> None:
    """Save an immutable copy of the baseline files as exact bytes (byte-for-byte)."""
    baseline_dir = Path(output_dir) / WORK_DIRNAME / BASELINE_DIRNAME
    baseline_dir.mkdir(parents=True, exist_ok=True)
    baseline_report_path = baseline_dir / BASELINE_REPORT_FILENAME
    baseline_gaps_path = baseline_dir / BASELINE_GAPS_FILENAME
    baseline_report_path.write_bytes(report_bytes)
    baseline_gaps_path.write_bytes(gaps_bytes)


def _load_combines(output_dir: Path) -> dict[str, dict[str, Any]] | None:
    """Load the agentic combines store, or ``None`` when it doesn't exist."""
    combines_path = Path(output_dir) / METADATA_DIRNAME / COMBINES_FILENAME
    if not combines_path.exists():
        return None
    try:
        raw = json.loads(combines_path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _save_combines(output_dir: Path, combines: dict[str, dict[str, Any]]) -> None:
    """Save the agentic combines store."""
    combines_path = Path(output_dir) / METADATA_DIRNAME / COMBINES_FILENAME
    combines_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(combines_path, combines)


# --------------------------------------------------------------------------- #
# Interactive decision prompt (one route from the user's seat).
# --------------------------------------------------------------------------- #


def _stderr(line: str) -> None:
    """Default sink for the interactive prompt's narration -- stderr keeps stdout clean for JSON."""
    print(line, file=sys.stderr)


def prompt_for_decisions(
    recommendation: dict[str, Any],
    *,
    input_fn: Callable[[str], str] | None = None,
    output_fn: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Ask the user, per connected component, whether to convert deterministically or agentically.

    Shows each component's members, the library's recommendation, and -- when the agentic option
    carries a re-architecture (e.g. a multi-pipeline -> Lakeflow Connect simplification) -- flags it
    prominently. An empty answer accepts the recommendation; ``d``/``a`` (or the full words) choose
    a route. Returns an authored plan dict (``{"components": [{component_id, members, decision}]}``)
    ready to hand to :func:`flowx.routing.record_plan`.

    ``input_fn`` and ``output_fn`` are injectable so the prompt is exercised without a real TTY; both
    are resolved at call time (defaulting to the builtin ``input`` and a stderr sink) so a test that
    patches ``builtins.input`` is honoured.
    """
    ask = input_fn if input_fn is not None else input
    say = output_fn if output_fn is not None else _stderr
    components_out: list[dict[str, Any]] = []
    for component in recommendation.get("components", []):
        component_id = str(component.get("component_id"))
        members = list(component.get("members", []))
        recommended = str(component.get("recommended", "deterministic"))
        options = component.get("options") or {}
        has_simplification = bool((options.get("agentic") or {}).get("has_simplification"))

        say(f"\nComponent {component_id}: {', '.join(members) or '(none)'}")
        say(f"  recommended: {recommended}")
        if has_simplification:
            say("  agentic option includes a simplification re-architecture (e.g. Lakeflow Connect)")
        answer = ask(f"  route [d]eterministic / [a]gentic (default={recommended}): ").strip().lower()
        if answer in ("a", "agentic"):
            decision = DECISION_AGENTIC
        elif answer in ("d", "deterministic"):
            decision = "deterministic"
        else:
            decision = recommended
        components_out.append({"component_id": component_id, "members": members, "decision": decision})
    return {"components": components_out}


# --------------------------------------------------------------------------- #
# The pure rebuild step: deterministic, idempotent report transformation.
# --------------------------------------------------------------------------- #


def rebuild(
    baseline_report: dict[str, Any],
    baseline_gaps: list[dict[str, Any]],
    plan: dict[str, Any],
    combines: dict[str, dict[str, Any]] | None,
    previous_record: dict[str, Any] | None,
    baseline_report_bytes: bytes | None = None,
    baseline_gaps_bytes: bytes | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Pure rebuild: derive the routed report, gaps, and record from baseline and plan.

    Returns ``(report, gaps, record)``, all deterministically derived from inputs. For each component:
    * **deterministic:** the baseline pipelines stay; outcome is ``deterministic``.
    * **agentic with a stored combine whose members match:** replace members with combine's pipelines,
      reuse the combine_sha256. Outcome is ``agentic-applied``.
    * **agentic without a matching combine:** placeholders and tagged gaps as ``alter_report`` does.
      Outcome is ``agentic-not-viable``.

    The record holds per-component members, decision, outcome, combine_sha256, and fingerprint. When
    fingerprint differs from previous_record's, a replacement entry is appended to track the change.
    An all-deterministic plan returns no record (byte-identical to baseline).

    Args:
        baseline_report: Parsed baseline report JSON.
        baseline_gaps: Parsed baseline gaps JSON.
        plan: Routing plan dict.
        combines: Stored combines from metadata/agentic_combines.json.
        previous_record: Prior routing record for change tracking.
        baseline_report_bytes: Raw bytes of baseline report file for hashing. If not provided,
            computed from baseline_report as compact JSON.
        baseline_gaps_bytes: Raw bytes of baseline gaps file for hashing. If not provided,
            computed from baseline_gaps as compact JSON.
    """
    combines_store = combines or {}
    planned = _planned_components(plan)
    agentic_names = agentic_pipeline_names(plan)
    if not agentic_names and previous_record is None:
        return baseline_report, baseline_gaps, {}
    report = copy.deepcopy(baseline_report)
    gaps = copy.deepcopy(baseline_gaps)
    new_report, new_gaps = alter_report(report, gaps, agentic_names)
    if not isinstance(new_report.get("pipelines"), list):
        new_report = {"pipelines": [new_report]}
    if baseline_report_bytes is None:
        baseline_report_bytes = json.dumps(baseline_report, separators=(",", ":")).encode("utf-8")
    if baseline_gaps_bytes is None:
        baseline_gaps_bytes = json.dumps(baseline_gaps, separators=(",", ":")).encode("utf-8")
    components: dict[str, dict[str, Any]] = {}
    for component_id, planned_entry in planned.items():
        decision = planned_entry["decision"]
        members_set = set(planned_entry["members"])
        combine_data = combines_store.get(component_id)
        combine_sha256 = combine_data.get("combine_sha256") if combine_data else None
        fingerprint_input = {
            "members": planned_entry["members"],
            "decision": decision,
            "combine_sha256": combine_sha256,
        }
        fingerprint = canonical_sha256(fingerprint_input)
        if decision == DECISION_AGENTIC and combine_data and set(combine_data.get("members", [])) == members_set:
            outcome = OUTCOME_AGENTIC_APPLIED
            new_report = combine_group_fill(new_report, members_set, combine_data.get("pipelines", []))
        elif decision == DECISION_AGENTIC:
            outcome = OUTCOME_AGENTIC_NOT_VIABLE
        else:
            outcome = OUTCOME_DETERMINISTIC
        entry: dict[str, Any] = {
            **planned_entry,
            "outcome": outcome,
            "combine_sha256": combine_sha256,
            "fingerprint": fingerprint,
        }
        if previous_record and component_id in previous_record.get("components", {}):
            old_fingerprint = previous_record["components"][component_id].get("fingerprint")
            if old_fingerprint and old_fingerprint != fingerprint:
                old_record = previous_record["components"][component_id]
                entry["replacements"] = (old_record.get("replacements") or []) + [
                    {"from": old_fingerprint, "to": fingerprint}
                ]
            else:
                entry["replacements"] = previous_record["components"][component_id].get("replacements") or []
        else:
            entry["replacements"] = []
        components[component_id] = entry
    if all(entry["decision"] != DECISION_AGENTIC for entry in components.values()):
        return baseline_report, baseline_gaps, {}
    record = {
        "conversion_plan_sha256": canonical_sha256(plan),
        "baseline_report_sha256": hashlib.sha256(baseline_report_bytes).hexdigest(),
        "baseline_gaps_sha256": hashlib.sha256(baseline_gaps_bytes).hexdigest(),
        "components": components,
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


def _write_json_atomic(path: Path, document: Any) -> None:
    """Write JSON atomically (temp file + ``os.replace``), matching the report's 2-space indentation."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(document, indent=2, default=str), encoding="utf-8")
    os.replace(temporary, path)


def _refresh_record_plan_hash(report_path: Path, plan: dict[str, Any]) -> None:
    """Bring a report copy's recorded plan hash up to date when it was routed under these decisions.

    A copy without a record, or routed under other decisions, is left for package to refuse.
    """
    if not report_path.exists():
        return
    report = json.loads(report_path.read_text(encoding="utf-8"))
    record = routing_record(report)
    plan_sha256 = canonical_sha256(plan)
    if record is None or routing_record_mismatches(record, plan) or record.get("conversion_plan_sha256") == plan_sha256:
        return
    record["conversion_plan_sha256"] = plan_sha256
    _write_json_atomic(report_path, report)


def apply_plan_to_report(output_dir: Path, plan: dict[str, Any]) -> dict[str, Any]:
    """Apply a routing decision to the report on disk by rebuilding from baseline.

    Reads or creates the immutable baseline at ``.work/route_baseline/``, loads stored combines from
    ``metadata/agentic_combines.json``, rebuilds the report + gaps + record deterministically, and
    writes them atomically. When no component is routed agentic and no record exists, files are left
    untouched (non-breaking guarantee). Re-routing with different decisions rebuilds (not refused).

    Returns a summary dict with the altered pipeline names and the resulting gap count.

    Raises:
        FileNotFoundError: when the translation report is missing (run convert first).
    """
    work = Path(output_dir) / WORK_DIRNAME
    report_path = work / REPORT_FILENAME
    if not report_path.exists():
        raise FileNotFoundError(f"No {REPORT_FILENAME} under {work}; run the convert phase first.")

    baseline_report, baseline_gaps, baseline_report_bytes, baseline_gaps_bytes, baseline_error = _load_baseline(
        output_dir
    )
    if baseline_error is not None:
        raise ValueError(f"Failed to load baseline: {baseline_error}")
    old_report_bytes = report_path.read_bytes()
    report = json.loads(old_report_bytes)
    old_gaps_bytes = (work / GAPS_FILENAME).read_bytes() if (work / GAPS_FILENAME).exists() else b"[]"
    record = routing_record(report)
    combines = _load_combines(output_dir) or {}
    agentic = agentic_pipeline_names(plan)
    new_report, new_gaps, new_record = rebuild(
        baseline_report,
        baseline_gaps,
        plan,
        combines,
        record,
        baseline_report_bytes,
        baseline_gaps_bytes,
    )
    if not new_record:
        if record is not None:
            report_without_record = copy.deepcopy(report)
            report_without_record.pop(ROUTING_RECORD_KEY, None)
            _write_json_atomic(report_path, report_without_record)
            _write_json_atomic(work / GAPS_FILENAME, baseline_gaps)
            if (work / STAMPED_REPORT_FILENAME).exists():
                _write_json_atomic(work / STAMPED_REPORT_FILENAME, report_without_record)
        return {"agentic_pipelines": sorted(agentic), "gaps": len(baseline_gaps), "altered": record is not None}
    if not _baseline_exists(output_dir):
        _save_baseline_bytes(output_dir, baseline_report_bytes, baseline_gaps_bytes)
    new_report[ROUTING_RECORD_KEY] = new_record
    old_report_without_record = copy.deepcopy(report)
    old_report_without_record.pop(ROUTING_RECORD_KEY, None)
    new_gaps_bytes = json.dumps(new_gaps, indent=2, default=str).encode("utf-8")
    altered = (
        json.dumps(old_report_without_record, separators=(",", ":"), sort_keys=True, default=str)
        != json.dumps(
            {k: v for k, v in new_report.items() if k != ROUTING_RECORD_KEY},
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        )
    ) or (old_gaps_bytes != new_gaps_bytes)
    _write_json_atomic(report_path, new_report)
    _write_json_atomic(work / GAPS_FILENAME, new_gaps)
    if (work / STAMPED_REPORT_FILENAME).exists():
        _write_json_atomic(work / STAMPED_REPORT_FILENAME, new_report)
    return {"agentic_pipelines": sorted(agentic), "gaps": len(new_gaps), "altered": altered}


def apply_plan(output_dir: Path, plan: ConversionPlan) -> dict[str, Any]:
    """Apply a recorded, typed plan to the IR after convert: the library's one routing entry point.

    Checks the plan still matches the inventory, source graphs and source insights it was decided
    on, then placeholders the routed-agentic components in the translation report (see
    :func:`apply_plan_to_report`); the fills (``convert --merge-agentic`` per pipeline,
    ``fill-agentic combine`` across pipelines) then replace those placeholders. Phase 1 decides whole
    components, so the reserved per-node assignments are not read here.

    Raises:
        FileNotFoundError: The inventory or translation report is missing.
        ValueError: The plan is stale against the current discovery outputs.
    """
    from flowx.routing import plan_binding_violations

    inventory_path = Path(output_dir) / METADATA_DIRNAME / INVENTORY_FILENAME
    if not inventory_path.exists():
        raise FileNotFoundError(f"No {INVENTORY_FILENAME} under {inventory_path.parent}; run the discover phase first.")
    stale = plan_binding_violations(plan, json.loads(inventory_path.read_text(encoding="utf-8")))
    if stale:
        raise ValueError("; ".join(stale))
    return apply_plan_to_report(output_dir, plan.to_dict())


# --------------------------------------------------------------------------- #
# Cross-pipeline COMBINE: the pipeline-grain fill.
# --------------------------------------------------------------------------- #


def combine_group_fill(
    report: dict[str, Any],
    group_members: Iterable[str],
    authored_pipelines: list[dict[str, Any]],
) -> dict[str, Any]:
    """Replace a routed group's pipelines with the agent-authored pipeline(s).

    Drops every pipeline whose name is in ``group_members`` and appends the ``authored_pipelines``
    (each a pipeline IR dict, typically carrying ``AgenticComponentActivity`` nodes). Pipelines
    outside the group are preserved in order. Returns a ``{"pipelines": [...]}`` report; the input is
    not mutated.
    """
    members = {name for name in group_members}
    kept = [copy.deepcopy(pipeline) for pipeline in _report_pipelines(report) if pipeline.get("name") not in members]
    kept.extend(copy.deepcopy(pipeline) for pipeline in authored_pipelines)
    return {"pipelines": kept}


def _resolve_agentic_component(
    output_dir: Path, members: set[str]
) -> tuple[str | None, dict[str, Any] | None, str | None, str | None]:
    """Bind ``members`` to a routed-**agentic** component in the recorded, fingerprint-bound plan.

    Reads ``metadata/conversion_plan.json`` and ``metadata/inventory.json`` and returns
    ``(component_id, plan, source, error)``, where ``plan`` is the recorded plan document and
    ``source`` the inventory's source, which every authored pipeline must carry. The error is set
    (and the other three are ``None``) when: the inventory's source cannot be routed agentic
    (:data:`flowx.routing.AGENTIC_ROUTING_SOURCES`); the plan or inventory is missing; the plan's
    ``inventory_sha256`` no longer matches the current inventory (a stale plan); ``members`` do not
    exactly equal one component's members (a partial, superset, or mistyped group); or the
    exactly-matching component is routed deterministic rather than agentic. Requiring an exact match
    to a routed-agentic component stops a caller from swapping deterministic pipelines or a partial
    group. The plan is returned so the combine can check the report's routing record against it.
    """
    from flowx.routing import agentic_routing_supported, inventory_source, plan_binding_violations

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
        return None, None, None, f"inventory source {inventory_source(inventory)!r} cannot be routed agentic."
    stale = plan_binding_violations(recorded, inventory)
    if stale:
        return None, None, None, f"conversion_plan.json is stale: {stale[0]}; re-run `route` before filling."
    plan = recorded.to_dict()

    for component in plan.get("components", []):
        if not isinstance(component, dict):
            continue
        component_members = {str(member) for member in component.get("members", [])}
        if component_members != members:
            continue
        if component.get("decision") == DECISION_AGENTIC:
            return str(component.get("component_id")), plan, inventory_source(inventory), None
        return (
            None,
            None,
            None,
            (
                f"members {sorted(members)} match component {component.get('component_id')!r}, which is "
                f"routed {component.get('decision')!r}, not agentic; only routed-agentic components can be combined."
            ),
        )
    return (
        None,
        None,
        None,
        (
            f"members {sorted(members)} do not exactly match any component in the recorded plan "
            "(partial, superset, or mistyped); pass the exact member set of one routed-agentic component."
        ),
    )


def _authored_source_tag_violations(authored_pipelines: list[dict[str, Any]], source: str) -> list[str]:
    """Reports each authored combine pipeline that does not carry ``tags.source`` equal to ``source``.

    The authored pipelines replace pipelines of the routed inventory, so one that omits or mis-sets
    the source tag is an authoring error. Catching it here fails the combine closed (nothing
    written) with a clear message, rather than letting the mis-tagged pipeline reach the package
    preflight where it is only rejected much later.
    """
    violations: list[str] = []
    for index, pipeline in enumerate(authored_pipelines):
        label = pipeline.get("name") if isinstance(pipeline, dict) and pipeline.get("name") else f"pipeline[{index}]"
        tags = pipeline.get("tags") if isinstance(pipeline, dict) else None
        authored_source = tags.get("source") if isinstance(tags, dict) else None
        if authored_source != source:
            violations.append(
                f"{label}: authored combine pipeline must carry tags.source == {source!r}, got {authored_source!r}"
            )
    return violations


def apply_combine_fill(
    output_dir: Path,
    group_members: Iterable[str],
    authored_pipelines: list[dict[str, Any]],
) -> dict[str, Any]:
    """Combine a routed-agentic group into agent-authored pipeline(s) on disk.

    Validates the group's membership against the recorded plan, stores the authored pipelines in
    ``metadata/agentic_combines.json``, and rebuilds (applying it deterministically). The group's
    membership must exactly match a routed-agentic component in the plan, and every authored pipeline
    must carry the correct source tag. The merged report is always validated structurally before
    writing.

    Returns ``{"ok", "violations", "error", "component_id", "pipelines", "already_combined"}``. ``ok``
    is ``False`` (and nothing written) on a plan/membership error, source tag violation, or structural
    issue. Empty ``authored_pipelines`` is refused. Same combine_sha256 is a no-op; different hash
    replaces the stored entry and rebuilds. Different authored pipelines after applying are refused.
    """
    members = {str(member) for member in group_members}
    if not authored_pipelines:
        return {"ok": False, "error": "pipelines list cannot be empty", "violations": [], "pipelines": 0}

    component_id, plan, source, error = _resolve_agentic_component(output_dir, members)
    if error is not None:
        return {"ok": False, "error": error, "violations": [], "pipelines": 0}
    assert component_id is not None and plan is not None and source is not None

    tag_violations = _authored_source_tag_violations(authored_pipelines, source)
    if tag_violations:
        return {"ok": False, "error": None, "violations": tag_violations, "pipelines": 0}

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
        error = "the report was routed under a different plan; re-run convert, then route, then combine again"
        return {"ok": False, "error": error, "violations": [], "pipelines": 0}

    new_combine_sha256 = canonical_sha256([pipeline_to_store(p) for p in authored_pipelines])
    combines = _load_combines(output_dir) or {}
    stored_combine = combines.get(component_id, {})
    stored_sha256 = stored_combine.get("combine_sha256")
    in_report = {pipeline.get("name"): pipeline for pipeline in _report_pipelines(report)}
    if stored_sha256 == new_combine_sha256:
        if all(in_report.get(pipeline.get("name")) == pipeline for pipeline in authored_pipelines):
            return {
                "ok": True,
                "error": None,
                "violations": [],
                "component_id": component_id,
                "pipelines": len(_report_pipelines(report)),
                "already_combined": True,
            }
        return {
            "ok": False,
            "error": COMPONENT_ALREADY_FILLED,
            "violations": [],
            "pipelines": 0,
        }
    combines[component_id] = {
        "members": list(members),
        "pipelines": authored_pipelines,
        "combine_sha256": new_combine_sha256,
    }
    _save_combines(output_dir, combines)
    baseline_report, baseline_gaps, baseline_report_bytes, baseline_gaps_bytes, _ = _load_baseline(output_dir)
    new_report, new_gaps, new_record = rebuild(
        baseline_report,
        baseline_gaps,
        plan,
        combines,
        record,
        baseline_report_bytes,
        baseline_gaps_bytes,
    )
    if not isinstance(new_report.get("pipelines"), list):
        new_report = {"pipelines": [new_report]}
    new_report[ROUTING_RECORD_KEY] = new_record
    result = validate_report_structurally(new_report)
    if not result.ok:
        violations = [f"[{finding.code}] {finding.location}: {finding.message}" for finding in result.violations]
        return {"ok": False, "error": None, "violations": violations, "pipelines": 0}

    _write_json_atomic(report_path, new_report)
    _write_json_atomic(work / GAPS_FILENAME, new_gaps)
    if (work / STAMPED_REPORT_FILENAME).exists():
        _write_json_atomic(work / STAMPED_REPORT_FILENAME, new_report)
    return {
        "ok": True,
        "error": None,
        "violations": [],
        "component_id": component_id,
        "pipelines": len(new_report["pipelines"]),
        "already_combined": False,
    }


def pipeline_to_store(pipeline: dict[str, Any]) -> dict[str, Any]:
    """Extract the storable parts of a pipeline IR dict for hashing (excluding dynamic fields)."""
    return {
        "name": pipeline.get("name"),
        "tags": pipeline.get("tags"),
        "tasks": pipeline.get("tasks"),
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
