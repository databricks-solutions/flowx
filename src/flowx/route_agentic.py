"""Phase-1 in-engine agentic conversion: apply a routing decision, then fill the gaps.

``convert`` always produces a deterministic ``.work/translation_report.json`` -- that translation is
**unchanged** by this module. Routing then decides, per connected component, whether each part
converts deterministically or agentically (:mod:`flowx.routing` computes the recommendation and
records the fingerprint-bound ``metadata/conversion_plan.json``). This module carries out the
**post-convert alteration and fill** the decision implies, always keeping the work in-engine so the
package phase, structural validation, and provenance all still apply:

* :func:`alter_report` rewrites the report for the routed-**agentic** groups only: every task in an
  agentic-routed pipeline is removed and replaced by a :class:`~flowx.models.ir.PlaceholderActivity`,
  and one :class:`~flowx.models.ir.AgenticGap` is appended per task, so the standard gap-fill path
  handles them. Pipelines in deterministic groups are left byte-identical, and when nothing is routed
  agentic the report and gaps are returned unchanged -- the non-breaking guarantee.
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
    """Turn one deterministic task into a placeholder + gap, preserving its identity and edges."""
    original_type = str(task.get("type", "unknown"))
    name = task.get("name")
    task_key = task.get("task_key")
    placeholder: dict[str, Any] = {
        "name": name,
        "task_key": task_key,
        "type": "PlaceholderActivity",
        "original_type": original_type,
        "comment": _PLACEHOLDER_COMMENT,
        # Keep the deterministic translation as context so the agent can author against it.
        "raw_definition": task,
    }
    if task.get("depends_on"):
        placeholder["depends_on"] = task["depends_on"]
    gap: dict[str, Any] = {
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
    gaps = list(gaps)
    for pipeline in _report_pipelines(report):
        if pipeline.get("name") not in agentic:
            continue
        placeholders: list[dict[str, Any]] = []
        for task in pipeline.get("tasks", []):
            if not isinstance(task, dict):
                continue
            placeholder, gap = _placeholder_and_gap(task, str(pipeline.get("name")))
            placeholders.append(placeholder)
            gaps.append(gap)
        pipeline["tasks"] = placeholders
    return report, gaps


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


def apply_combine_fill(
    output_dir: Path,
    group_members: Iterable[str],
    authored_pipelines: list[dict[str, Any]],
    *,
    validate: bool = True,
) -> dict[str, Any]:
    """Combine a routed group into agent-authored pipeline(s) on disk, validating before writing.

    Reads ``<output_dir>/.work/translation_report.json``, swaps the group's pipelines for
    ``authored_pipelines``, and -- when ``validate`` -- runs the structural bundle invariants over the
    merged report. The report is written back only when validation passes, so a dangling reference or
    duplicate key never lands on disk.

    Returns ``{"ok", "violations", "pipelines"}``. ``ok`` is ``False`` (and nothing written) on any
    structural violation.

    Raises:
        FileNotFoundError: when the translation report is missing (run convert first).
    """
    work = Path(output_dir) / WORK_DIRNAME
    report_path = work / REPORT_FILENAME
    if not report_path.exists():
        raise FileNotFoundError(f"No {REPORT_FILENAME} under {work}; run the convert phase first.")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    merged = combine_group_fill(report, group_members, authored_pipelines)

    if validate:
        result = validate_report_structurally(merged)
        if not result.ok:
            violations = [f"[{finding.code}] {finding.location}: {finding.message}" for finding in result.violations]
            return {"ok": False, "violations": violations, "pipelines": 0}

    _write_json_atomic(report_path, merged)
    return {"ok": True, "violations": [], "pipelines": len(merged["pipelines"])}


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
