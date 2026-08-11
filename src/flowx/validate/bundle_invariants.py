"""Structural-invariant checks for a generated Databricks Asset Bundle.

These guard against output that is valid YAML / valid Python but invalid as a
Databricks job -- e.g. a job parameter declared twice (the duplicate-``region``
regression), a duplicate task key, a ``{{job.parameters.X}}`` reference to an
undeclared parameter, a ``depends_on`` edge to a missing task, a dependency
cycle, or a leaked YAML anchor/alias (the fingerprint of a shared mutable object
reaching serialization).

Run :func:`check_bundle_dir` over a generated bundle in tests (and optionally as
a Tier-0 prepare step) so these never ship silently.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

# PyYAML emits anchors/aliases as ``&id001`` / ``*id001`` when the same object
# appears more than once in the tree.  flowx never intends to emit these.
_ANCHOR_RE = re.compile(r"[&*]id\d+\b")
_JOB_PARAM_REF_RE = re.compile(r"\{\{\s*job\.parameters\.([A-Za-z0-9_]+)\s*\}\}")
_JOB_RESOURCE_ID_RE = re.compile(r"\$\{resources\.jobs\.([^.}]+)\.id\}")
_PYDABS_JOB_RE = re.compile(r"resources\.add_job\(\s*['\"]([^'\"]+)['\"]")


@dataclass(slots=True, kw_only=True)
class BundleFinding:
    """A single invariant violation.

    Attributes:
        code: Stable machine-readable identifier.
        message: Human-readable explanation.
        location: File / job / task the finding concerns.
    """

    code: str
    message: str
    location: str = ""
    severity: str = "violation"


@dataclass(slots=True, kw_only=True)
class BundleInvariantResult:
    """Outcome of :func:`check_bundle_dir` / :func:`check_job`."""

    findings: list[BundleFinding] = field(default_factory=list)

    @property
    def violations(self) -> list[BundleFinding]:
        """Hard, always-invalid findings (these fail a bundle)."""
        return [finding for finding in self.findings if finding.severity == "violation"]

    @property
    def warnings(self) -> list[BundleFinding]:
        """Soft findings worth surfacing but not build-failing."""
        return [finding for finding in self.findings if finding.severity == "warning"]

    @property
    def ok(self) -> bool:
        """True when no hard invariant was violated (warnings are allowed)."""
        return not self.violations


def _collect_task_keys(tasks: list[dict[str, Any]]) -> list[str]:
    """Top-level task keys plus any single nested ``for_each_task.task`` key."""
    keys: list[str] = []
    for task in tasks or []:
        if "task_key" in task:
            keys.append(task["task_key"])
        nested = (task.get("for_each_task") or {}).get("task")
        if isinstance(nested, dict) and "task_key" in nested:
            keys.append(nested["task_key"])
    return keys


def _iter_tasks(tasks: list[dict[str, Any]]):
    """Yields top-level tasks and nested ``for_each_task.task`` bodies."""
    for task in tasks:
        if not isinstance(task, dict):
            continue
        yield task
        nested = (task.get("for_each_task") or {}).get("task")
        if isinstance(nested, dict):
            yield from _iter_tasks([nested])


def _dump(obj: Any) -> str:
    """Serialise a structure to a string for reference scanning."""
    return yaml.safe_dump(obj, default_flow_style=False)


def check_job(job_key: str, job: dict[str, Any]) -> list[BundleFinding]:
    """Check the structural invariants of a single job resource dict."""
    findings: list[BundleFinding] = []
    where = f"job '{job_key}'"

    if not job.get("tasks"):
        findings.append(
            BundleFinding(
                code="empty_job",
                location=where,
                message="A Lakeflow Job must contain at least one executable task.",
            )
        )

    # 1. No duplicate job-parameter names.
    param_names = [param.get("name") for param in (job.get("parameters") or []) if isinstance(param, dict)]
    duplicate_params = sorted({name for name in param_names if name is not None and param_names.count(name) > 1})
    for name in duplicate_params:
        findings.append(
            BundleFinding(
                code="duplicate_job_parameter",
                location=where,
                message=f"Job parameter '{name}' is declared more than once.",
            )
        )

    # 2. No duplicate task keys.
    task_keys = _collect_task_keys(job.get("tasks") or [])
    duplicate_keys = sorted({key for key in task_keys if task_keys.count(key) > 1})
    for key in duplicate_keys:
        findings.append(
            BundleFinding(
                code="duplicate_task_key", location=where, message=f"Task key '{key}' is used more than once."
            )
        )

    # 3. Every {{job.parameters.X}} reference is declared.
    declared = {name for name in param_names if name is not None}
    referenced = set(_JOB_PARAM_REF_RE.findall(_dump(job)))
    for name in sorted(referenced - declared):
        findings.append(
            BundleFinding(
                code="undeclared_job_parameter",
                severity="warning",
                location=where,
                message=f"'{{{{job.parameters.{name}}}}}' is referenced but '{name}' is not a declared job parameter.",
            )
        )

    # 4. Every top-level depends_on target exists.
    top_level_keys = {task.get("task_key") for task in (job.get("tasks") or []) if isinstance(task, dict)}
    for task in job.get("tasks") or []:
        for dep in task.get("depends_on") or []:
            target = dep.get("task_key")
            if target and target not in top_level_keys:
                findings.append(
                    BundleFinding(
                        code="dangling_depends_on",
                        location=f"{where}, task '{task.get('task_key')}'",
                        message=f"depends_on references unknown task '{target}'.",
                    )
                )

    # 5. The task dependency graph is acyclic (a cycle fails `databricks bundle validate`).
    if _has_dependency_cycle(job.get("tasks") or []):
        findings.append(
            BundleFinding(
                code="dependency_cycle",
                location=where,
                message="The job's task dependency graph contains a cycle.",
            )
        )
    return findings


def _has_dependency_cycle(tasks: list[dict[str, Any]]) -> bool:
    """Returns True when the top-level ``depends_on`` graph has a cycle (Kahn's algorithm).

    Source-agnostic: operates on the emitted job's task keys and depends_on edges, so it
    guards every source's output. Edges to unknown tasks are ignored here (surfaced
    separately as ``dangling_depends_on``).
    """
    keys: list[str] = [
        task["task_key"] for task in tasks if isinstance(task, dict) and isinstance(task.get("task_key"), str)
    ]
    key_set = set(keys)
    in_degree: dict[str, int] = {key: 0 for key in keys}
    adjacency: dict[str, set[str]] = {key: set() for key in keys}
    for task in tasks:
        if not isinstance(task, dict):
            continue
        downstream = task.get("task_key")
        if not isinstance(downstream, str) or downstream not in key_set:
            continue
        for dep in task.get("depends_on") or []:
            upstream = dep.get("task_key") if isinstance(dep, dict) else None
            if isinstance(upstream, str) and upstream in key_set and downstream not in adjacency[upstream]:
                adjacency[upstream].add(downstream)
                in_degree[downstream] += 1
    queue = [key for key in keys if in_degree[key] == 0]
    visited = 0
    while queue:
        node = queue.pop()
        visited += 1
        for successor in adjacency[node]:
            in_degree[successor] -= 1
            if in_degree[successor] == 0:
                queue.append(successor)
    return visited != len(keys)


def check_resource_text(text: str, *, filename: str = "") -> list[BundleFinding]:
    """Check one resource YAML document (raw text): anchors + per-job invariants."""
    findings: list[BundleFinding] = []
    if _ANCHOR_RE.search(text):
        findings.append(
            BundleFinding(
                code="yaml_anchor",
                location=filename,
                message=(
                    "Emitted YAML contains an anchor/alias (&idN/*idN); a shared mutable object "
                    "leaked into the bundle structure. This usually means a value was added twice."
                ),
            )
        )
    doc = yaml.safe_load(text) or {}
    jobs = ((doc.get("resources") or {}).get("jobs") or {}) if isinstance(doc, dict) else {}
    for job_key, job in jobs.items():
        if isinstance(job, dict):
            findings.extend(check_job(job_key, job))
    return findings


def check_bundle_dir(bundle_dir: Path) -> BundleInvariantResult:
    """Run all structural invariants over every resource YAML in a bundle directory."""
    bundle_dir = Path(bundle_dir)
    findings: list[BundleFinding] = []
    resources_dir = bundle_dir / "resources"
    yaml_files = sorted(resources_dir.glob("*.yml")) if resources_dir.exists() else []
    databricks_yml = bundle_dir / "databricks.yml"
    if databricks_yml.exists():
        yaml_files.append(databricks_yml)

    documents: list[tuple[Path, dict[str, Any]]] = []
    for path in yaml_files:
        text = path.read_text(encoding="utf-8")
        findings.extend(check_resource_text(text, filename=path.name))
        document = yaml.safe_load(text) or {}
        if isinstance(document, dict):
            documents.append((path, document))

    known_jobs: set[str] = set()
    for _path, document in documents:
        jobs = (document.get("resources") or {}).get("jobs") or {}
        if isinstance(jobs, dict):
            known_jobs.update(str(job_key) for job_key in jobs)
        python_resources = (document.get("python") or {}).get("resources") or []
        for resource in python_resources:
            if not isinstance(resource, str):
                continue
            module = resource.split(":", 1)[0]
            if module.startswith("resources."):
                known_jobs.add(module.rsplit(".", 1)[-1])
    for hook_path in sorted(resources_dir.glob("*.py")) if resources_dir.exists() else []:
        known_jobs.update(_PYDABS_JOB_RE.findall(hook_path.read_text(encoding="utf-8")))

    for path, document in documents:
        jobs = (document.get("resources") or {}).get("jobs") or {}
        if not isinstance(jobs, dict):
            continue
        for job_key, job in jobs.items():
            if not isinstance(job, dict):
                continue
            for task in _iter_tasks(job.get("tasks") or []):
                run_job = task.get("run_job_task") or {}
                job_id = run_job.get("job_id") if isinstance(run_job, dict) else None
                match = _JOB_RESOURCE_ID_RE.fullmatch(job_id) if isinstance(job_id, str) else None
                if match is None or match.group(1) in known_jobs:
                    continue
                findings.append(
                    BundleFinding(
                        code="dangling_run_job_reference",
                        location=f"{path.name}, job '{job_key}', task '{task.get('task_key', '')}'",
                        message=(
                            f"run_job_task references bundle job '{match.group(1)}', which is not declared "
                            "in static resource YAML or registered as a Python resource."
                        ),
                    )
                )
    return BundleInvariantResult(findings=findings)


def format_result(result: BundleInvariantResult) -> str:
    """Render a result as a compact human-readable report."""
    if not result.findings:
        return "Bundle invariants: OK"
    lines = ["Bundle invariants: FAILED" if result.violations else "Bundle invariants: WARNINGS"]
    lines.extend(f"  - [{finding.code}] {finding.location}: {finding.message}" for finding in result.findings)
    return "\n".join(lines)
