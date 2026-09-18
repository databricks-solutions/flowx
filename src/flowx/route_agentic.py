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
  :func:`flowx.ir_serde.merge_agentic_results` (no new code here). **Cross-pipeline COMBINE**
  (N pipelines -> M, e.g. one Lakeflow Connect pipeline) is the one net-new capability:
  :func:`combine_group_fill` swaps the routed group's pipelines for the agent-authored pipeline(s),
  which carry :class:`~flowx.models.ir.AgenticComponentActivity` nodes (the escape hatch).
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
import json
import os
import sys
import tempfile
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from flowx.models.conversion_plan import DECISION_AGENTIC

# The report + gaps live under the shared output dir's transient .work/ folder, beside the pipeline IR.
WORK_DIRNAME = ".work"
REPORT_FILENAME = "translation_report.json"
GAPS_FILENAME = "gaps.json"
# The recorded plan + inventory the combine fill binds against live under metadata/.
METADATA_DIRNAME = "metadata"
INVENTORY_FILENAME = "inventory.json"

# Routing / in-engine agentic conversion is ADF-only, so every agent-authored combine pipeline must
# carry this source tag. The package preflight enforces it too, but combine asserts it up front so a
# mis-tagged authored pipeline fails closed here (nothing written) instead of surviving to package.
REQUIRED_COMBINE_SOURCE_TAG = "adf"

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


def apply_plan_to_report(output_dir: Path, plan: dict[str, Any]) -> dict[str, Any]:
    """Apply a routing decision to the report on disk: placeholder the routed-agentic groups.

    Reads ``<output_dir>/.work/translation_report.json`` (and ``gaps.json`` when present), rewrites
    them for the plan's agentic components, and writes them back atomically. When no component is
    routed agentic the files are left untouched, so the non-breaking guarantee holds.

    Returns a summary dict with the altered pipeline names and the resulting gap count.

    Raises:
        FileNotFoundError: when the translation report is missing (run convert first).
    """
    work = Path(output_dir) / WORK_DIRNAME
    report_path = work / REPORT_FILENAME
    if not report_path.exists():
        raise FileNotFoundError(f"No {REPORT_FILENAME} under {work}; run the convert phase first.")
    gaps_path = work / GAPS_FILENAME

    agentic = agentic_pipeline_names(plan)
    if not agentic:
        return {"agentic_pipelines": [], "gaps": 0, "altered": False}

    report = json.loads(report_path.read_text(encoding="utf-8"))
    gaps = json.loads(gaps_path.read_text(encoding="utf-8")) if gaps_path.exists() else []
    if not isinstance(gaps, list):
        gaps = []

    new_report, new_gaps = alter_report(report, gaps, agentic)
    _write_json_atomic(report_path, new_report)
    _write_json_atomic(gaps_path, new_gaps)
    return {"agentic_pipelines": sorted(agentic), "gaps": len(new_gaps), "altered": True}


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


def _resolve_agentic_component(output_dir: Path, members: set[str]) -> tuple[str | None, str | None]:
    """Bind ``members`` to a routed-**agentic** component in the recorded, fingerprint-bound plan.

    Reads ``metadata/conversion_plan.json`` and ``metadata/inventory.json`` and returns
    ``(component_id, error)``. The error is set (and ``component_id`` is ``None``) when: the plan or
    inventory is missing; the plan's ``inventory_sha256`` no longer matches the current inventory (a
    stale plan); ``members`` do not exactly equal one component's members (a partial, superset, or
    mistyped group); or the exactly-matching component is routed deterministic rather than agentic.
    Requiring an exact match to a routed-agentic component stops a caller from swapping deterministic
    pipelines or a partial group.
    """
    from flowx.discovery_insights import inventory_fingerprint

    metadata = Path(output_dir) / METADATA_DIRNAME
    plan_path = metadata / "conversion_plan.json"
    inventory_path = metadata / INVENTORY_FILENAME
    if not plan_path.exists():
        return None, "No metadata/conversion_plan.json; record a routing decision with `route` first."
    if not inventory_path.exists():
        return None, "No metadata/inventory.json; run the discover phase first."

    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    recorded_fingerprint = plan.get("inventory_sha256")
    current_fingerprint = inventory_fingerprint(inventory)
    if recorded_fingerprint != current_fingerprint:
        return None, (
            "conversion_plan.json is stale: it was recorded against a different inventory "
            f"({recorded_fingerprint!r} != {current_fingerprint!r}); re-run `route` before filling."
        )

    for component in plan.get("components", []):
        if not isinstance(component, dict):
            continue
        component_members = {str(member) for member in component.get("members", [])}
        if component_members != members:
            continue
        if component.get("decision") == DECISION_AGENTIC:
            return str(component.get("component_id")), None
        return None, (
            f"members {sorted(members)} match component {component.get('component_id')!r}, which is "
            f"routed {component.get('decision')!r}, not agentic; only routed-agentic components can be combined."
        )
    return None, (
        f"members {sorted(members)} do not exactly match any component in the recorded plan "
        "(partial, superset, or mistyped); pass the exact member set of one routed-agentic component."
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
    Every authored pipeline must carry ``tags.source == 'adf'`` (routing/agentic is ADF-only); a
    mis-tagged pipeline fails the combine closed here rather than surviving to the package preflight.
    The merged report is then **always** validated with the structural bundle invariants (a real
    ``prepare -> write_bundle`` pass over :func:`validate_report_structurally`) -- there is no bypass --
    and written back only when it passes, so a dangling reference or duplicate key never lands on disk.

    The combine is **idempotent**: because the merged report no longer contains the collapsed members
    (only the recorded plan still lists them), re-running with the same ``group_members`` and authored
    pipeline(s) detects the already-combined state -- the members are gone from the report and the
    authored pipeline(s) are already present -- and no-ops (``already_combined`` true) instead of
    appending the authored pipeline(s) a second time.

    Returns ``{"ok", "violations", "error", "component_id", "pipelines", "already_combined"}``. ``ok``
    is ``False`` (and nothing written) on a plan/membership error (``error`` set), a missing source tag,
    or any structural violation (``violations`` set).

    Raises:
        FileNotFoundError: when the translation report is missing (run convert first).
    """
    members = {str(member) for member in group_members}
    component_id, error = _resolve_agentic_component(output_dir, members)
    if error is not None:
        return {"ok": False, "error": error, "violations": [], "pipelines": 0}

    tag_violations = _authored_source_tag_violations(authored_pipelines)
    if tag_violations:
        return {"ok": False, "error": None, "violations": tag_violations, "pipelines": 0}

    work = Path(output_dir) / WORK_DIRNAME
    report_path = work / REPORT_FILENAME
    if not report_path.exists():
        raise FileNotFoundError(f"No {REPORT_FILENAME} under {work}; run the convert phase first.")

    report = json.loads(report_path.read_text(encoding="utf-8"))

    # Idempotency: the recorded plan still lists the members even after a prior combine collapsed them
    # out of the report, so the plan/membership check above passes on a re-run. Detect the
    # already-combined state (members gone from the report, authored pipeline(s) already present) and
    # no-op, so a second run cannot append a duplicate authored pipeline.
    report_pipelines = _report_pipelines(report)
    report_names = {pipeline.get("name") for pipeline in report_pipelines}
    authored_names = {pipeline.get("name") for pipeline in authored_pipelines}
    if authored_names and not (members & report_names) and authored_names <= report_names:
        return {
            "ok": True,
            "error": None,
            "violations": [],
            "component_id": component_id,
            "pipelines": len(report_pipelines),
            "already_combined": True,
        }

    merged = combine_group_fill(report, members, authored_pipelines)

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
