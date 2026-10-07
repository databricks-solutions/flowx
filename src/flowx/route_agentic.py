"""Phase-1 in-engine agentic conversion: apply a routing decision, then fill the gaps.

``convert`` always produces a deterministic ``.work/translation_report.json`` -- that translation is
**unchanged** by this module. Routing then decides, per connected component, whether each part
converts deterministically or agentically (:mod:`flowx.routing` computes the recommendation and
records the fingerprint-bound ``metadata/conversion_plan.json``). This module carries out the
**post-convert alteration and fill** the decision implies, always keeping the work in-engine so the
package phase, structural validation, and provenance all still apply:

* :func:`alter_report` rewrites the report for the routed-**agentic** groups only: every task in an
  agentic-routed pipeline is removed and replaced by a :class:`~flowx.models.ir.PlaceholderActivity`,
  and exactly one pipeline-tagged :class:`~flowx.models.ir.AgenticGap` is emitted per routed task,
  replacing (never appending to) any prior gap for those pipelines' tasks, so the standard gap-fill
  path handles them without duplicates and the edit is idempotent. Pipelines in deterministic groups
  are left byte-identical, and when nothing is routed agentic the report and gaps are returned
  unchanged -- the non-breaking guarantee.
* the agent authors the fill. **Per-pipeline** agentic reuses the existing name-matched
  :func:`flowx.ir_serde.merge_agentic_results`. **Cross-pipeline COMBINE**
  (N pipelines -> M, e.g. one Lakeflow Connect pipeline) is the one net-new capability:
  :func:`combine_group_fill` swaps the routed group's pipelines for the agent-authored pipeline(s),
  which carry :class:`~flowx.models.ir.AgenticComponentActivity` nodes (the escape hatch).
* :func:`validate_report_structurally` packages the merged report through the same
  ``prepare -> write_bundle`` path the package phase uses and runs the existing
  :func:`flowx.validate.bundle_invariants.check_bundle_dir` over the output, so a fill can never
  introduce duplicate keys, dangling dependencies, a cycle, or a dangling pipeline/run_job reference.

Route stamps one routing record onto the report (:data:`ROUTING_RECORD_KEY`), keyed by the plan's
component ids, holding each component's members, decision and outcome plus the plan hash and the hash
of the report route started from. Every later rewrite of the report carries it forward, the fills
update the outcomes, and package refuses a report whose record no longer matches the recorded plan.

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
# The recorded plan + inventory the combine fill binds against live under metadata/.
METADATA_DIRNAME = "metadata"
INVENTORY_FILENAME = "inventory.json"

# Routing / in-engine agentic conversion is ADF-only, so every agent-authored combine pipeline must
# carry this source tag. The package preflight enforces it too, but combine asserts it up front so a
# mis-tagged authored pipeline fails closed here (nothing written) instead of surviving to package.
REQUIRED_COMBINE_SOURCE_TAG = "adf"

# Top-level report key holding the routing record. It lives in the report, so a fresh convert (which
# rewrites the report) clears it; the package phase and ir_serde read only the pipelines beside it.
ROUTING_RECORD_KEY = "_routing_record"

# What became of each component in the record: a deterministic component is left as convert wrote it;
# an agentic one stays "not viable" until a fill leaves none of its members with a routed placeholder.
OUTCOME_DETERMINISTIC = "deterministic"
OUTCOME_AGENTIC_APPLIED = "agentic-applied"
OUTCOME_AGENTIC_NOT_VIABLE = "agentic-not-viable"

REROUTED_UNDER_DIFFERENT_PLAN = "the report was routed under a different plan; re-run convert, then route"

# Each routed-agentic component takes exactly one kind of fill: a cross-pipeline combine or
# per-pipeline merges, never both.
COMPONENT_ALREADY_FILLED = "component already filled; re-run convert and route to start again"
FILL_COMBINE = "combine"
FILL_MERGE = "merge"

# Guidance stamped onto every placeholder the alteration produces.
_PLACEHOLDER_COMMENT = (
    "Routed agentic by the conversion plan; author a replacement task (per-pipeline fill) or replace "
    "the whole group with agent-authored pipeline(s) (cross-pipeline combine)."
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


def build_routing_record(plan: dict[str, Any], baseline_bytes: bytes) -> dict[str, Any]:
    """The record route stamps onto the report when it first applies a plan with an agentic component."""
    components = {
        component_id: {
            **entry,
            "outcome": OUTCOME_AGENTIC_NOT_VIABLE if entry["decision"] == DECISION_AGENTIC else OUTCOME_DETERMINISTIC,
        }
        for component_id, entry in _planned_components(plan).items()
    }
    return {
        "conversion_plan_sha256": canonical_sha256(plan),
        "baseline_report_sha256": hashlib.sha256(baseline_bytes).hexdigest(),
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


def refresh_routing_outcomes(report: dict[str, Any]) -> None:
    """Update each agentic component's outcome in the report's record after a fill, in place.

    A component is applied once none of its member pipelines left in the report still holds a routed
    placeholder; a combine removes the members entirely. A report without a record is left alone.
    """
    record = routing_record(report)
    if record is None or not isinstance(record.get("components"), dict):
        return
    with_placeholders = {
        pipeline.get("name")
        for pipeline in _report_pipelines(report)
        for task in pipeline.get("tasks", [])
        if isinstance(task, dict) and task.get("type") == "PlaceholderActivity"
    }
    for entry in record["components"].values():
        if isinstance(entry, dict) and entry.get("decision") == DECISION_AGENTIC:
            unfilled = any(member in with_placeholders for member in entry.get("members") or [])
            entry["outcome"] = OUTCOME_AGENTIC_NOT_VIABLE if unfilled else OUTCOME_AGENTIC_APPLIED


def load_gaps(work_dir: Path) -> list[dict[str, Any]]:
    """The gaps recorded beside a translation report in ``work_dir``, or an empty list when none were."""
    gaps_path = Path(work_dir) / GAPS_FILENAME
    gaps = json.loads(gaps_path.read_text(encoding="utf-8")) if gaps_path.exists() else []
    return [gap for gap in gaps if isinstance(gap, dict)] if isinstance(gaps, list) else []


def _routed_task_names(gaps: list[dict[str, Any]]) -> dict[str, set[str]]:
    """The task names route turned into placeholders, per pipeline, from its pipeline-tagged gaps."""
    names: dict[str, set[str]] = {}
    for gap in gaps:
        if gap.get("pipeline") is not None:
            names.setdefault(str(gap["pipeline"]), set()).add(str(gap.get("activity_name")))
    return names


def component_fill(report: dict[str, Any], members: Iterable[str], gaps: list[dict[str, Any]]) -> str | None:
    """Which kind of fill has touched a routed-agentic component, read from the report and its gaps.

    Route turns every top-level task of a member into a placeholder and records one gap per task. A
    per-pipeline merge replaces those placeholders by name, so a merged member keeps the routed task
    names. A combine replaces the members with authored pipelines, so a member is combined when it
    is gone from the report, or when an authored pipeline reusing its name holds no placeholder and
    none of the routed task names. Returns :data:`FILL_COMBINE`, :data:`FILL_MERGE`, or ``None``
    when neither has run.
    """
    by_name = {pipeline.get("name"): pipeline for pipeline in _report_pipelines(report)}
    routed = _routed_task_names(gaps)
    filled = False
    for member in members:
        pipeline = by_name.get(member)
        if pipeline is None:
            return FILL_COMBINE
        tasks = [task for task in pipeline.get("tasks", []) if isinstance(task, dict)]
        placeholders = [task for task in tasks if task.get("type") == "PlaceholderActivity"]
        if tasks and not placeholders and not {str(task.get("name")) for task in tasks} & routed.get(member, set()):
            return FILL_COMBINE
        filled = filled or len(placeholders) < len(tasks)
    return FILL_MERGE if filled else None


def combined_pipelines(report: dict[str, Any], gaps: list[dict[str, Any]]) -> set[str]:
    """The pipelines a combine has filled: combined members, and the authored pipelines beside them.

    A pipeline that is no member of any routed component was authored by a combine, so once any
    component is combined those pipelines count too. Empty when the report has no routing record.
    """
    record = routing_record(report)
    components = record.get("components") if record is not None else None
    if not isinstance(components, dict):
        return set()
    names: set[str] = set()
    all_members: set[str] = set()
    for entry in components.values():
        if not isinstance(entry, dict):
            continue
        members = [str(member) for member in entry.get("members") or []]
        all_members.update(members)
        if entry.get("decision") == DECISION_AGENTIC and component_fill(report, members, gaps) == FILL_COMBINE:
            names.update(members)
    if names:
        authored = {str(pipeline.get("name")) for pipeline in _report_pipelines(report)} - all_members
        names.update(authored)
    return names


def reroute_conflict(output_dir: Path, plan: dict[str, Any]) -> str | None:
    """Explain why route must not apply ``plan`` to the report on disk, or ``None`` when it may.

    Route refuses once the report carries a record routed under different components or decisions:
    the earlier edit cannot be undone in place, so the report has to be converted again first.
    """
    report_path = Path(output_dir) / WORK_DIRNAME / REPORT_FILENAME
    if not report_path.exists():
        return None
    record = routing_record(json.loads(report_path.read_text(encoding="utf-8")))
    if record is not None and routing_record_mismatches(record, plan):
        return REROUTED_UNDER_DIFFERENT_PLAN
    return None


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
    """Apply a routing decision to the report on disk: placeholder the routed-agentic groups.

    Reads ``<output_dir>/.work/translation_report.json`` (and ``gaps.json`` when present), rewrites
    them for the plan's agentic components, stamps the routing record and writes them back atomically.
    When no component is routed agentic and the report carries no record the files are left
    untouched, so the non-breaking guarantee holds. A report already routed under the same decisions
    is not edited again; only the record's plan hash is brought up to date, in the report and in the
    configuration-stamped copy package reads when ``modify`` wrote one.

    Returns a summary dict with the altered pipeline names and the resulting gap count.

    Raises:
        FileNotFoundError: when the translation report is missing (run convert first).
        ValueError: when the report was routed under different components or decisions.
    """
    work = Path(output_dir) / WORK_DIRNAME
    report_path = work / REPORT_FILENAME
    if not report_path.exists():
        raise FileNotFoundError(f"No {REPORT_FILENAME} under {work}; run the convert phase first.")

    agentic = agentic_pipeline_names(plan)
    baseline_bytes = report_path.read_bytes()
    report = json.loads(baseline_bytes)
    record = routing_record(report)
    if record is None and not agentic:
        return {"agentic_pipelines": [], "gaps": 0, "altered": False}

    gaps = load_gaps(work)

    if record is not None:
        if routing_record_mismatches(record, plan):
            raise ValueError(REROUTED_UNDER_DIFFERENT_PLAN)
        for path in (report_path, work / STAMPED_REPORT_FILENAME):
            _refresh_record_plan_hash(path, plan)
        return {"agentic_pipelines": sorted(agentic), "gaps": len(gaps), "altered": False}

    new_report, new_gaps = alter_report(report, gaps, agentic)
    if not isinstance(new_report.get("pipelines"), list):
        new_report = {"pipelines": [new_report]}
    new_report[ROUTING_RECORD_KEY] = build_routing_record(plan, baseline_bytes)
    _write_json_atomic(report_path, new_report)
    _write_json_atomic(work / GAPS_FILENAME, new_gaps)
    return {"agentic_pipelines": sorted(agentic), "gaps": len(new_gaps), "altered": True}


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
) -> tuple[str | None, dict[str, Any] | None, str | None]:
    """Bind ``members`` to a routed-**agentic** component in the recorded, fingerprint-bound plan.

    Reads ``metadata/conversion_plan.json`` and ``metadata/inventory.json`` and returns
    ``(component_id, plan, error)``, where ``plan`` is the recorded plan document. The error is set
    (and the other two are ``None``) when: the plan or inventory is missing; the plan's
    ``inventory_sha256`` no longer matches the current inventory (a stale plan); ``members`` do not
    exactly equal one component's members (a partial, superset, or mistyped group); or the
    exactly-matching component is routed deterministic rather than agentic. Requiring an exact match
    to a routed-agentic component stops a caller from swapping deterministic pipelines or a partial
    group. The plan is returned so the combine can check the report's routing record against it.
    """
    from flowx.routing import plan_binding_violations

    metadata = Path(output_dir) / METADATA_DIRNAME
    inventory_path = metadata / INVENTORY_FILENAME
    try:
        recorded = ConversionPlan.load(Path(output_dir))
    except ValueError as error:
        return None, None, str(error)
    if recorded is None:
        return None, None, "No metadata/conversion_plan.json; record a routing decision with `route` first."
    if not inventory_path.exists():
        return None, None, "No metadata/inventory.json; run the discover phase first."

    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    stale = plan_binding_violations(recorded, inventory)
    if stale:
        return None, None, f"conversion_plan.json is stale: {stale[0]}; re-run `route` before filling."
    plan = recorded.to_dict()

    for component in plan.get("components", []):
        if not isinstance(component, dict):
            continue
        component_members = {str(member) for member in component.get("members", [])}
        if component_members != members:
            continue
        if component.get("decision") == DECISION_AGENTIC:
            return str(component.get("component_id")), plan, None
        return (
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
        (
            f"members {sorted(members)} do not exactly match any component in the recorded plan "
            "(partial, superset, or mistyped); pass the exact member set of one routed-agentic component."
        ),
    )


def _authored_source_tag_violations(authored_pipelines: list[dict[str, Any]]) -> list[str]:
    """Reports each authored combine pipeline that is missing the required ``tags.source == 'adf'``.

    Routing / in-engine agentic conversion is ADF-only, so an authored combine pipeline that omits or
    mis-sets the source tag is an authoring error. Catching it here fails the combine closed (nothing
    written) with a clear message, rather than letting the mis-tagged pipeline reach the package
    preflight where it is only rejected much later.
    """
    violations: list[str] = []
    for index, pipeline in enumerate(authored_pipelines):
        label = pipeline.get("name") if isinstance(pipeline, dict) and pipeline.get("name") else f"pipeline[{index}]"
        tags = pipeline.get("tags") if isinstance(pipeline, dict) else None
        source = tags.get("source") if isinstance(tags, dict) else None
        if source != REQUIRED_COMBINE_SOURCE_TAG:
            violations.append(
                f"{label}: authored combine pipeline must carry tags.source == "
                f"{REQUIRED_COMBINE_SOURCE_TAG!r}, got {source!r}"
            )
    return violations


def apply_combine_fill(
    output_dir: Path,
    group_members: Iterable[str],
    authored_pipelines: list[dict[str, Any]],
) -> dict[str, Any]:
    """Combine a routed-agentic group into agent-authored pipeline(s) on disk.

    The group's membership is **bound to the recorded plan**: ``group_members`` must exactly match a
    routed-agentic component in ``metadata/conversion_plan.json`` (whose fingerprint must still match
    the current inventory), so a caller cannot swap deterministic pipelines or a partial/typoed group.
    The report must carry a routing record that matches that plan, so route has to have applied it.
    Every authored pipeline must carry ``tags.source == 'adf'`` (routing/agentic is ADF-only); a
    mis-tagged pipeline fails the combine closed here rather than surviving to the package preflight.
    The merged report is then **always** validated with the structural bundle invariants (a real
    ``prepare -> write_bundle`` pass over :func:`validate_report_structurally`) -- there is no bypass --
    and written back only when it passes, so a dangling reference or duplicate key never lands on disk.

    Each component takes one kind of fill: a component a per-pipeline merge has already filled is
    refused with :data:`COMPONENT_ALREADY_FILLED`. The combine is **idempotent**: once a combine has
    replaced the component's members (see :func:`component_fill`) a re-run no-ops (``already_combined`` true)
    instead of collapsing/appending again, so it never duplicates the authored pipeline(s) even when
    the authored replacement is renamed or an authored name collides with a former member. A fresh
    ``convert`` rewrites the report without the record, so the combine applies again after a
    re-convert and route.

    Returns ``{"ok", "violations", "error", "component_id", "pipelines", "already_combined"}``. ``ok``
    is ``False`` (and nothing written) on a plan/membership error (``error`` set), a missing source tag,
    or any structural violation (``violations`` set).

    Raises:
        FileNotFoundError: when the translation report is missing (run convert first).
    """
    members = {str(member) for member in group_members}
    component_id, plan, error = _resolve_agentic_component(output_dir, members)
    if error is not None:
        return {"ok": False, "error": error, "violations": [], "pipelines": 0}
    assert component_id is not None and plan is not None  # guaranteed when error is None

    tag_violations = _authored_source_tag_violations(authored_pipelines)
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
        return {"ok": False, "error": REROUTED_UNDER_DIFFERENT_PLAN, "violations": [], "pipelines": 0}

    fill = component_fill(report, members, load_gaps(work))
    if fill == FILL_MERGE:
        return {"ok": False, "error": COMPONENT_ALREADY_FILLED, "violations": [], "pipelines": 0}
    if fill == FILL_COMBINE:
        return {
            "ok": True,
            "error": None,
            "violations": [],
            "component_id": component_id,
            "pipelines": len(_report_pipelines(report)),
            "already_combined": True,
        }

    merged = combine_group_fill(report, members, authored_pipelines)
    merged[ROUTING_RECORD_KEY] = copy.deepcopy(record)
    refresh_routing_outcomes(merged)

    result = validate_report_structurally(merged)
    if not result.ok:
        violations = [f"[{finding.code}] {finding.location}: {finding.message}" for finding in result.violations]
        return {"ok": False, "error": None, "violations": violations, "pipelines": 0}

    _write_json_atomic(report_path, merged)
    return {
        "ok": True,
        "error": None,
        "violations": [],
        "component_id": component_id,
        "pipelines": len(merged["pipelines"]),
        "already_combined": False,
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
