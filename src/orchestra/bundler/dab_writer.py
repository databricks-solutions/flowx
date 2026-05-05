"""Writes a PreparedWorkflow to Databricks Declarative Automation Bundle files."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml

from flowx.bundler.inner_job_params import normalize_value
from flowx.bundler.notebook_writer import write_notebooks
from flowx.bundler.prereqs_writer import ManualParameter, build_prereqs, render_setup_md
from flowx.bundler.setup_generator import generate_setup_tasks
from flowx.models.dab import DabNotebook
from flowx.models.ir import (
    Activity,
    AppendVariableActivity,
    CopyActivity,
    DeleteActivity,
    Dependency,
    ExecutePipelineActivity,
    FilterActivity,
    ForEachActivity,
    IfConditionActivity,
    LookupActivity,
    MotifActivity,
    NotebookActivity,
    Pipeline,
    PlaceholderActivity,
    RunJobActivity,
    SetVariableActivity,
    SparkJarActivity,
    SparkPythonActivity,
    SwitchActivity,
    SwitchCase,
    UnsupportedActivity,
    WaitActivity,
    WebActivity,
)
from flowx.preparer.workflow_preparer import PreparedWorkflow, prepare_workflow
from flowx.utils import normalize_task_key


class _BundleYamlDumper(yaml.SafeDumper):
    """YAML dumper that leaves keys unquoted and only quotes values when needed."""


# Module-level warnings collector — reset per write_bundle call.
_bundle_warnings: list[str] = []

# Cross-bundle ExecutePipeline refs seen while translating: variable_name →
# target pipeline name.  Reset per write_bundle call and surfaced via the
# bundle's ``variables`` block + SETUP.md.
_cross_bundle_variables: dict[str, str] = {}

_WIDGET_REFERENCE = re.compile(r"""dbutils\.widgets\.get\(\s*["']([^"']+)["']\s*\)""")


def write_bundle(
    workflow: PreparedWorkflow,
    output_dir: Path,
    catalog: str = "main",
    schema: str = "default",
    bundle_name: str | None = None,
) -> list[Path]:
    """Writes all DAB files to output_dir.

    Args:
        workflow: The PreparedWorkflow to serialize.
        output_dir: Root directory for the bundle output.
        catalog: Default target catalog name.
        schema: Default target schema name.
        bundle_name: Optional bundle name (defaults to workflow name).

    Returns:
        List of absolute paths to all created files.
    """
    # Reset module-level accumulators so successive ``write_bundle`` calls
    # (CLI loops, library users, integration tests) don't carry warnings or
    # cross-bundle variables from one bundle into the next.
    _bundle_warnings.clear()
    _cross_bundle_variables.clear()

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    created_files: list[Path] = []
    resource_key = normalize_task_key(workflow.name)
    effective_name = bundle_name or resource_key

    # Bind clusters across the parent workflow and any inner workflows up
    # front so we can decide whether the bundle needs cluster-related
    # tunables in ``databricks.yml`` at all.  Binding is idempotent, so the
    # subsequent ``_build_job_resource`` calls re-checking the same tasks is
    # harmless.
    _bind_cluster_to_notebook_tasks(workflow.tasks)
    for inner in workflow.inner_workflows:
        _bind_cluster_to_notebook_tasks(inner.tasks)
    bundle_uses_classic_cluster = _any_task_uses_classic_cluster(workflow.tasks) or any(
        _any_task_uses_classic_cluster(inner.tasks) for inner in workflow.inner_workflows
    )

    # 1. Write databricks.yml.  When at least one task runs on classic
    #    compute, defaults for spark_version / node_type_id come from the
    #    ADF linked service configs on the tasks so the emitted cluster
    #    matches the source-of-truth runtime.  When every task is
    #    serverless, those variables are omitted entirely.
    databricks_yml_path = output_dir / "databricks.yml"
    inferred_spark_version, inferred_node_type_id = _infer_bundle_cluster_defaults(workflow)
    databricks_yml_dict = _build_databricks_yml(
        effective_name,
        catalog,
        schema,
        spark_version=inferred_spark_version,
        node_type_id=inferred_node_type_id,
        include_cluster_variables=bundle_uses_classic_cluster,
    )
    databricks_yml_path.write_text(
        yaml.dump(
            databricks_yml_dict,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            Dumper=_BundleYamlDumper,
        ),
        encoding="utf-8",
    )
    created_files.append(databricks_yml_path.resolve())

    # 2. Write job resource YAML.  Strip broken base_parameters from
    #    existing-notebook tasks before serialising — these are surfaced
    #    in SETUP.md (§Existing-notebook parameter handling) further down
    #    and shouldn't ship in the YAML as malformed widget values.
    manual_parameters: list[ManualParameter] = _extract_manual_parameters_from_existing_notebook_tasks(workflow.tasks)
    for inner in workflow.inner_workflows:
        manual_parameters.extend(_extract_manual_parameters_from_existing_notebook_tasks(inner.tasks))

    resources_dir = output_dir / "resources"
    resources_dir.mkdir(parents=True, exist_ok=True)
    job_yml_path = resources_dir / f"{resource_key}.yml"
    job_resource = _build_job_resource(workflow, resource_key)
    job_yml_path.write_text(
        yaml.dump(
            job_resource, default_flow_style=False, sort_keys=False, allow_unicode=True, Dumper=_BundleYamlDumper
        ),
        encoding="utf-8",
    )
    created_files.append(job_yml_path.resolve())

    # Write inner workflows as additional resource files.  Inner tasks reuse
    # notebooks that live in the parent workflow's notebooks list, so pass
    # those in so the inner job's widget auto-augmentation can see them.
    for inner in workflow.inner_workflows:
        inner_key = normalize_task_key(inner.name)
        inner_yml_path = resources_dir / f"{inner_key}.yml"
        inner_resource = _build_job_resource(inner, inner_key, extra_notebooks_for_augment=workflow.notebooks)
        inner_yml_path.write_text(
            yaml.dump(
                inner_resource,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
                Dumper=_BundleYamlDumper,
            ),
            encoding="utf-8",
        )
        created_files.append(inner_yml_path.resolve())

    # 3. Write generated notebooks
    src_dir = output_dir / "src"
    if workflow.notebooks:
        created_files.extend(write_notebooks(workflow.notebooks, src_dir))

    # 4. Generate and write setup notebooks (create-scope, create-volume, etc.).
    #    These are the *executable* provisioning artifacts; SETUP.md (below)
    #    is the human-readable companion.
    setup_notebooks: list[DabNotebook] = generate_setup_tasks(
        secrets=workflow.secrets,
        setup_tasks=workflow.setup_tasks,
        catalog=catalog,
        schema=schema,
    )
    if setup_notebooks:
        created_files.extend(write_notebooks(setup_notebooks, src_dir))

    # Collect notebooks from inner workflows
    for inner in workflow.inner_workflows:
        if inner.notebooks:
            created_files.extend(write_notebooks(inner.notebooks, src_dir))
        inner_setup = generate_setup_tasks(
            secrets=inner.secrets,
            setup_tasks=inner.setup_tasks,
            catalog=catalog,
            schema=schema,
        )
        if inner_setup:
            created_files.extend(write_notebooks(inner_setup, src_dir))

    # 5. Build SETUP.md — a root-level, human-readable summary of every
    #    external step the user must take before ``bundle run``.  This is
    #    additive to the setup/ notebooks above; the setup notebooks are
    #    the executable path, SETUP.md is the checklist.
    all_notebooks = list(workflow.notebooks)
    for inner in workflow.inner_workflows:
        all_notebooks.extend(inner.notebooks)
    all_tasks = list(workflow.tasks)
    for inner in workflow.inner_workflows:
        all_tasks.extend(inner.tasks)
    known_bundle_jobs = {resource_key} | {normalize_task_key(inner.name) for inner in workflow.inner_workflows}
    # ``manual_parameters`` was collected above (before YAML emission) so
    # the broken values are also stripped from the on-disk YAML.
    prereqs = build_prereqs(
        notebooks=all_notebooks,
        tasks=all_tasks,
        known_bundle_jobs=known_bundle_jobs,
        cross_bundle_variables=dict(_cross_bundle_variables),
        manual_parameters=manual_parameters,
    )
    setup_path = output_dir / "SETUP.md"
    setup_path.write_text(render_setup_md(prereqs, bundle_name=effective_name), encoding="utf-8")
    created_files.append(setup_path.resolve())

    # 5. Write warnings file if any warnings were collected
    if _bundle_warnings:
        warnings_path = output_dir / "WARNINGS.md"
        lines = [
            "# Translation Warnings\n",
            "",
            "The following items require manual review or modification:\n",
            "",
        ]
        lines.extend(_bundle_warnings)
        lines.append("")
        warnings_path.write_text("\n".join(lines), encoding="utf-8")
        created_files.append(warnings_path.resolve())

    return created_files


def main() -> None:
    """CLI entry point for DAB bundle generation."""
    parser = argparse.ArgumentParser(
        description="Generate a Databricks Declarative Automation Bundle from a translation report.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        required=True,
        help="Path to the translation report or pipeline IR JSON produced by the translate phase.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./orchestra_output/bundle"),
        help="Output directory for the DAB bundle (default: ./orchestra_output/bundle).",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default="main",
        help="Target Unity Catalog name (default: main).",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default="default",
        help="Target schema name (default: default).",
    )
    parser.add_argument(
        "--bundle-name",
        type=str,
        default=None,
        help="Override the bundle name (defaults to the workflow name).",
    )
    args = parser.parse_args()

    if not args.report.exists():
        print(f"Error: Report file not found: {args.report}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading translation report: {args.report}")
    workflows = _load_report(args.report)

    if not workflows:
        print("No translated pipelines found in the report.", file=sys.stderr)
        sys.exit(1)

    all_created: list[Path] = []
    for index, workflow in enumerate(workflows):
        if len(workflows) > 1:
            workflow_dir = args.output_dir / normalize_task_key(workflow.name)
        else:
            workflow_dir = args.output_dir

        effective_bundle_name = args.bundle_name if len(workflows) == 1 else None
        created = write_bundle(
            workflow=workflow,
            output_dir=workflow_dir,
            catalog=args.catalog,
            schema=args.schema,
            bundle_name=effective_bundle_name,
        )
        all_created.extend(created)
        print(f"  [{index + 1}/{len(workflows)}] {workflow.name}: {len(created)} files")

    print(f"\nBundle generation complete: {len(all_created)} files written to {args.output_dir}")
    print("\nNext steps:")
    print("  1. Review the generated notebooks in src/")
    print("  2. Run the setup notebooks to create secrets and volumes")
    print("  3. Validate the bundle: databricks bundle validate")
    print("  4. Deploy: databricks bundle deploy -t dev")


def _warn(task_key: str, message: str) -> None:
    """Record a translation warning for the current bundle."""
    _bundle_warnings.append(f"- **{task_key}**: {message}")


_DEFAULT_SPARK_VERSION = "15.4.x-scala2.12"
_DEFAULT_NODE_TYPE_ID = "Standard_DS3_v2"


def _infer_bundle_cluster_defaults(workflow: PreparedWorkflow) -> tuple[str, str]:
    """Derive ``spark_version`` and ``node_type_id`` defaults from task clusters.

    Args:
        workflow: The prepared workflow being written.

    Returns:
        ``(spark_version, node_type_id)`` strings.
    """
    from collections import Counter

    spark_versions = [hint["spark_version"] for hint in workflow.cluster_hints if hint.get("spark_version")]
    node_types = [hint["node_type_id"] for hint in workflow.cluster_hints if hint.get("node_type_id")]

    spark_version = Counter(spark_versions).most_common(1)[0][0] if spark_versions else _DEFAULT_SPARK_VERSION
    node_type_id = Counter(node_types).most_common(1)[0][0] if node_types else _DEFAULT_NODE_TYPE_ID
    return spark_version, node_type_id


def _build_databricks_yml(
    bundle_name: str,
    catalog: str,
    schema: str,
    *,
    spark_version: str = _DEFAULT_SPARK_VERSION,
    node_type_id: str = _DEFAULT_NODE_TYPE_ID,
    include_cluster_variables: bool = True,
) -> dict[str, Any]:
    """Builds the root ``databricks.yml`` configuration as a dict.

    Args:
        bundle_name: Name for the bundle.
        catalog: Default target catalog.
        schema: Default target schema.
        spark_version: DBR version for the default job_cluster.  Callers
            typically derive this from :func:`_infer_bundle_cluster_defaults`.
        node_type_id: Instance type for the default job_cluster.
        include_cluster_variables: When True, declares ``spark_version`` and
            ``node_type_id`` variables for the default job_cluster.  Set to
            False when no task in the bundle uses classic compute (every
            generated notebook runs on serverless), so the bundle stays
            free of unused tunables.

    Returns:
        Dict ready for YAML serialization.
    """
    variables: dict[str, Any] = {
        "catalog": {
            "description": "Target catalog",
            "default": catalog,
        },
        "schema": {
            "description": "Target schema",
            "default": schema,
        },
    }
    if include_cluster_variables:
        variables["node_type_id"] = {
            "description": (
                "Instance type for the default job_cluster — override per cloud "
                "(e.g. i3.xlarge on AWS, n1-standard-4 on GCP)."
            ),
            "default": node_type_id,
        }
        variables["spark_version"] = {
            "description": "Databricks Runtime for the default job_cluster.",
            "default": spark_version,
        }
    # Declare a variable for each cross-bundle ExecutePipeline reference so
    # `${var.X_job_id}` resolves and `bundle validate` passes.  Users fill in
    # the numeric job ID per SETUP.md.
    for variable_name, target_pipeline in sorted(_cross_bundle_variables.items()):
        variables[variable_name] = {
            "description": (
                f"Numeric job ID for pipeline '{target_pipeline}' (defined in a sibling bundle). "
                f'Populate via `databricks bundle deploy --var "{variable_name}=<job_id>"` or set '
                "the default here."
            ),
        }
    return {
        "bundle": {
            "name": bundle_name,
        },
        "variables": variables,
        "include": [
            "resources/*.yml",
        ],
        "targets": {
            "dev": {
                "mode": "development",
            },
            "staging": {
                "mode": "production",
            },
            "prod": {
                "mode": "production",
            },
        },
    }


_DEFAULT_JOB_CLUSTER_KEY = "default_cluster"


def _build_default_job_clusters() -> list[dict[str, Any]]:
    """Return a job_clusters stanza that binds notebook tasks to a real cluster."""
    return [
        {
            "job_cluster_key": _DEFAULT_JOB_CLUSTER_KEY,
            "new_cluster": {
                "spark_version": "${var.spark_version}",
                "node_type_id": "${var.node_type_id}",
                "num_workers": 1,
                "data_security_mode": "SINGLE_USER",
            },
        }
    ]


# Patterns that signal a base_parameter value couldn't be evaluated cleanly.
# When any task references an *existing* notebook (absolute workspace path),
# flowx can't inject the runtime computation, so these end up as manual
# work for the user.
_HYBRID_ADF_FN_RE = re.compile(r"@[a-zA-Z][a-zA-Z0-9]*\(")
_PYTHON_CODE_HINTS = ("dbutils.widgets.get(", "datetime.now(", "datetime.fromisoformat(")


def _value_needs_manual_handling(value: Any) -> bool:
    """Return True when *value* is too dynamic for DAB to substitute at deploy time."""
    if not isinstance(value, str):
        return False
    if _HYBRID_ADF_FN_RE.search(value):
        return True
    return any(hint in value for hint in _PYTHON_CODE_HINTS)


def _extract_manual_parameters_from_existing_notebook_tasks(
    tasks: list[dict[str, Any]],
) -> list[ManualParameter]:
    """Finds base_parameters flowx couldn't evaluate for existing-notebook tasks."""
    manual_parameters: list[ManualParameter] = []
    for task in _iter_tasks_recursively(tasks):
        notebook_task = task.get("notebook_task") or {}
        notebook_path = notebook_task.get("notebook_path", "")
        base_params = notebook_task.get("base_parameters")
        # Bundle-relative paths (``../src/...``) can have their notebook
        # bodies patched to inline the runtime computation; absolute paths
        # belong to the user's existing notebooks and must be surfaced.
        if not notebook_path.startswith("/") or not isinstance(base_params, dict):
            continue
        keys_to_drop: list[str] = []
        for key, value in base_params.items():
            if not _value_needs_manual_handling(value):
                continue
            manual_parameters.append(
                ManualParameter(
                    task_key=task.get("task_key", ""),
                    widget_name=key,
                    notebook_path=notebook_path,
                    raw_expression=str(value),
                )
            )
            keys_to_drop.append(key)
        for key in keys_to_drop:
            del base_params[key]
        if not base_params:
            notebook_task.pop("base_parameters", None)
    return manual_parameters


def _iter_tasks_recursively(tasks: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    """Yields every task in *tasks*, descending into ``for_each_task.task``."""
    for task in tasks:
        yield task
        for_each = task.get("for_each_task") or {}
        inner = for_each.get("task")
        if isinstance(inner, dict):
            yield from _iter_tasks_recursively([inner])


_CLUSTER_BINDING_KEYS = ("existing_cluster_id", "new_cluster", "job_cluster_key")


def _any_task_uses_classic_cluster(tasks: list[dict[str, Any]]) -> bool:
    """Return True if any task (recursively) is bound to a job_cluster_key."""
    return any(task.get("job_cluster_key") for task in _iter_tasks_recursively(tasks))


def _bind_cluster_to_notebook_tasks(tasks: list[dict[str, Any]]) -> None:
    """Attaches the default job_cluster_key to existing-notebook tasks."""
    for task in _iter_tasks_recursively(tasks):
        notebook_task = task.get("notebook_task")
        if notebook_task is None:
            continue
        notebook_path = notebook_task.get("notebook_path", "")
        if notebook_path.startswith("../src/"):
            continue
        if any(key in task for key in _CLUSTER_BINDING_KEYS):
            continue
        task["job_cluster_key"] = _DEFAULT_JOB_CLUSTER_KEY


def _rewrite_post_branch_dependencies(tasks: list[dict[str, Any]]) -> None:
    """Rewrites ``depends_on`` edges that target a condition_task to target its branches.

    Args:
        tasks: Top-level task list, mutated in place.
    """
    condition_keys = {task["task_key"] for task in tasks if "condition_task" in task}
    if not condition_keys:
        return

    direct_outcome_children: dict[str, list[str]] = {key: [] for key in condition_keys}
    for task in tasks:
        for dep in task.get("depends_on") or []:
            if dep.get("task_key") in condition_keys and dep.get("outcome") in ("true", "false"):
                direct_outcome_children[dep["task_key"]].append(task["task_key"])

    def expand_terminals(condition_key: str, seen: set[str]) -> list[str]:
        """Return branch-terminal task keys for a condition, transitively."""
        terminals: list[str] = []
        for child_key in direct_outcome_children.get(condition_key, []):
            if child_key in seen:
                continue
            seen.add(child_key)
            if child_key in condition_keys:
                terminals.extend(expand_terminals(child_key, seen))
            else:
                terminals.append(child_key)
        return terminals

    for task in tasks:
        depends_on = task.get("depends_on") or []
        if not depends_on:
            continue
        rewritten: list[dict[str, Any]] = []
        touched_condition = False
        for dep in depends_on:
            dep_task_key = dep.get("task_key")
            if dep_task_key in condition_keys and "outcome" not in dep:
                replacement_keys = expand_terminals(dep_task_key, set())
                if replacement_keys:
                    rewritten.extend({"task_key": branch_key} for branch_key in replacement_keys)
                    touched_condition = True
                else:
                    rewritten.append(dep)
            else:
                rewritten.append(dep)
        if touched_condition:
            # Drop duplicates — a diamond-shaped join could hit the same
            # terminal via more than one branch.
            seen_keys: set[str] = set()
            deduped: list[dict[str, Any]] = []
            for dep in rewritten:
                key = dep.get("task_key", "")
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                deduped.append(dep)
            task["depends_on"] = deduped
            task.setdefault("run_if", "AT_LEAST_ONE_SUCCESS")


_TASK_VALUE_REF = re.compile(r"\{\{tasks\.([^.]+)\.values\.[^}]+\}\}")


def _strip_dangling_task_value_refs(tasks: list[dict[str, Any]], all_task_keys: set[str]) -> None:
    """Replaces ``{{tasks.X.values.Y}}`` refs whose ``X`` is not in the bundle.

    Args:
        tasks: Top-level tasks for one job (mutated in place).
        all_task_keys: Task keys that do exist in this job (including those
            inside ``for_each_task.task`` bodies).
    """

    def visit(task: dict[str, Any]) -> None:
        notebook_task = task.get("notebook_task") or {}
        base_parameters = notebook_task.get("base_parameters") or {}
        for widget_name, value in list(base_parameters.items()):
            if not isinstance(value, str):
                continue
            match = _TASK_VALUE_REF.search(value)
            if match and match.group(1) not in all_task_keys:
                base_parameters[widget_name] = ""
        for_each = task.get("for_each_task")
        if for_each and isinstance(for_each.get("task"), dict):
            visit(for_each["task"])

    for task in tasks:
        visit(task)


def _collect_all_task_keys(tasks: list[dict[str, Any]]) -> set[str]:
    """Collects every task_key reachable from the job's top-level task list."""
    keys: set[str] = set()
    for task in tasks:
        keys.add(task.get("task_key", ""))
        for_each = task.get("for_each_task")
        if for_each and isinstance(for_each.get("task"), dict):
            keys.update(_collect_all_task_keys([for_each["task"]]))
    keys.discard("")
    return keys


def _augment_base_parameters(tasks: list[dict[str, Any]], notebooks: list[DabNotebook]) -> None:
    """Ensure every widget a notebook reads is declared in its base_parameters.

    Args:
        tasks: Top-level task dicts (mutated in place).
        notebooks: Generated notebooks to scan.
    """
    notebook_by_relpath = {notebook.relative_path: notebook for notebook in notebooks}

    def visit(task: dict[str, Any]) -> None:
        notebook_task = task.get("notebook_task")
        if notebook_task:
            notebook_path = notebook_task.get("notebook_path", "")
            relative = notebook_path[len("../src/") :] if notebook_path.startswith("../src/") else ""
            notebook = notebook_by_relpath.get(relative)
            if notebook:
                widgets = set(_WIDGET_REFERENCE.findall(notebook.content))
                base_parameters = notebook_task.setdefault("base_parameters", {})
                for widget_name in sorted(widgets):
                    base_parameters.setdefault(widget_name, "")
        for_each = task.get("for_each_task")
        if for_each and isinstance(for_each.get("task"), dict):
            visit(for_each["task"])

    for task in tasks:
        visit(task)


def _build_job_resource(
    workflow: PreparedWorkflow,
    resource_key: str,
    *,
    attach_clusters: bool = True,
    extra_notebooks_for_augment: list[DabNotebook] | None = None,
) -> dict[str, Any]:
    """Builds a job resource dict for a single workflow.

    Args:
        workflow: The prepared workflow to serialize.
        resource_key: The sanitised resource key for this job.
        attach_clusters: When ``True`` (default) emits a ``job_clusters`` block
            and binds every notebook task to it.  Set to ``False`` for inner
            jobs that are invoked via ``run_job_task`` from another bundle
            job — they inherit compute from the caller.

    Returns:
        Dict ready for YAML serialization.
    """
    _rewrite_post_branch_dependencies(workflow.tasks)
    # For inner jobs (invoked via run_job_task), notebooks live in the parent
    # workflow's notebooks list — pass them in so widget auto-augment can
    # still find the bound notebook and populate base_parameters.
    augment_scope = list(workflow.notebooks) + list(extra_notebooks_for_augment or [])
    _augment_base_parameters(workflow.tasks, augment_scope)
    # Task values don't cross ``run_job_task`` boundaries; any such
    # reference in this job resolves to an empty string at runtime.  Emit
    # the empty string now so SETUP.md §4 flags it.
    _strip_dangling_task_value_refs(workflow.tasks, _collect_all_task_keys(workflow.tasks))

    job_def: dict[str, Any] = {
        "name": workflow.name,
        "tasks": workflow.tasks,
    }

    if attach_clusters:
        _bind_cluster_to_notebook_tasks(workflow.tasks)
        # Only emit the ``job_clusters`` block when at least one task is
        # actually bound to it.  When every task runs on serverless (the
        # generated-notebook case), the job stays cluster-free and inherits
        # the workspace's serverless defaults.
        if _any_task_uses_classic_cluster(workflow.tasks):
            job_def["job_clusters"] = _build_default_job_clusters()

    if workflow.parameters:
        job_def["parameters"] = workflow.parameters

    return {
        "resources": {
            "jobs": {
                resource_key: job_def,
            },
        },
    }


def _normalize_base_parameters(
    params: dict[str, Any],
    *,
    task_key: str = "",
) -> dict[str, str]:
    """Normalise raw ADF expression dicts in base_parameters to resolved strings.

    Args:
        params: Raw base_parameters dict from the IR.
        task_key: Task key for warning attribution.

    Returns:
        Dict with all values resolved to strings.
    """
    resolved: dict[str, str] = {}
    for key, value in params.items():
        normalized = normalize_value(value)
        if "dbutils.widgets.get" in normalized or "dbutils.jobs.taskValues" in normalized:
            _warn(
                task_key,
                f"Parameter `{key}` contains a computed expression that cannot be "
                f"expressed as a DAB dynamic value reference. The task's notebook "
                f"or entry point must handle this parameter at runtime. "
                f"Value: `{normalized}`",
            )
        resolved[key] = normalized
    return resolved


def _load_report(report_path: Path) -> list[PreparedWorkflow]:
    """Loads a translation report and reconstruct PreparedWorkflow objects.

    Args:
        report_path: Path to the translation report JSON file.

    Returns:
        List of PreparedWorkflow objects, one per pipeline.
    """
    with open(report_path, encoding="utf-8") as report_file:
        report = json.load(report_file)

    workflows: list[PreparedWorkflow] = []

    if "tasks" in report and "name" in report:
        workflow = _pipeline_dict_to_workflow(report)
        workflows.append(workflow)
        return workflows

    if "translations" in report:
        # Aggregated translation_report.json format: ``translations`` is a
        # flat list of ``{pipeline, ir, status, ...}`` entries.  Group by
        # pipeline name and route each group through the same
        # ``_pipeline_dict_to_workflow`` machinery as the single-pipeline IR
        # format, so secret discovery / setup tasks / control-flow handling
        # all match.
        pipelines: dict[str, list[dict]] = {}
        for translation in report.get("translations", []):
            pipeline_name = translation.get("pipeline", "unknown")
            if translation.get("status") != "translated":
                continue
            ir = translation.get("ir") or {}
            if not ir:
                continue
            pipelines.setdefault(pipeline_name, []).append(ir)

        for pipeline_name, task_irs in pipelines.items():
            workflow = _pipeline_dict_to_workflow({"name": pipeline_name, "tasks": task_irs})
            workflows.append(workflow)
        return workflows

    # Empty or unrecognised report shape — nothing to do.
    return workflows


def _pipeline_dict_to_workflow(pipeline_dict: dict[str, Any]) -> PreparedWorkflow:
    """Converts a serialised pipeline IR dict to a PreparedWorkflow.

    Rehydrates every task into a typed Activity via :func:`_reconstruct_ir`
    and routes through :func:`prepare_workflow` so the JSON-reload path
    shares one code path with the in-process translator.  This guarantees
    feature parity for secrets, setup tasks, manual parameters,
    expression resolution, and motif handling without duplicating the
    per-activity preparer logic.
    """
    activities = [_reconstruct_ir(task_ir) for task_ir in pipeline_dict.get("tasks", [])]

    parameters: list[dict[str, Any]] = []
    for param in pipeline_dict.get("parameters") or []:
        entry: dict[str, Any] = {"name": param["name"]}
        if "default" in param and param["default"] is not None:
            entry["default"] = normalize_value(str(param["default"]))
        parameters.append(entry)

    pipeline = Pipeline(
        name=pipeline_dict.get("name", "unknown"),
        tasks=activities,
        parameters=parameters or None,
    )

    workflow = prepare_workflow(pipeline)
    if parameters:
        workflow.parameters.extend(parameters)
    return workflow


def _reconstruct_ir(task_ir: dict[str, Any]) -> Activity:
    """Rehydrates a typed Activity from its serialised IR dict.

    Recurses into control-flow inner activities (ForEach, IfCondition,
    Switch).  Unknown ``type`` strings fall back to PlaceholderActivity
    so the rest of the pipeline can still be prepared.
    """
    task_type = task_ir.get("type", "")
    base = _common_activity_kwargs(task_ir)

    if task_type == "LookupActivity":
        return LookupActivity(
            **base,
            source_type=task_ir.get("source_type"),
            source_properties=task_ir.get("source_properties"),
            first_row_only=task_ir.get("first_row_only", True),
            source_query=task_ir.get("source_query"),
        )
    if task_type == "CopyActivity":
        return CopyActivity(
            **base,
            source_type=task_ir.get("source_type"),
            sink_type=task_ir.get("sink_type"),
            source_properties=task_ir.get("source_properties"),
            sink_properties=task_ir.get("sink_properties"),
            sink_dataset_type=task_ir.get("sink_dataset_type"),
            sink_format=task_ir.get("sink_format"),
            sink_resolved_path=task_ir.get("sink_resolved_path"),
            column_mapping=task_ir.get("column_mapping"),
        )
    if task_type == "WebActivity":
        return WebActivity(
            **base,
            url=task_ir.get("url", ""),
            method=task_ir.get("method", "GET"),
            body=task_ir.get("body"),
            headers=task_ir.get("headers"),
            authentication=task_ir.get("authentication"),
        )
    if task_type == "SetVariableActivity":
        return SetVariableActivity(
            **base,
            variable_name=task_ir.get("variable_name", ""),
            variable_value=task_ir.get("variable_value", ""),
            value_kind=task_ir.get("value_kind", "literal"),
            notebook_code=task_ir.get("notebook_code"),
            notebook_imports=task_ir.get("notebook_imports", []),
        )
    if task_type == "WaitActivity":
        return WaitActivity(
            **base,
            wait_time_seconds=task_ir.get("wait_time_seconds", 0),
        )
    if task_type == "DeleteActivity":
        return DeleteActivity(
            **base,
            dataset_name=task_ir.get("dataset_name", ""),
            folder_path=task_ir.get("folder_path"),
            recursive=task_ir.get("recursive", True),
        )
    if task_type == "FilterActivity":
        return FilterActivity(
            **base,
            items_expression=task_ir.get("items_expression", ""),
            condition_expression=task_ir.get("condition_expression", ""),
            condition_code=task_ir.get("condition_code"),
            condition_imports=list(task_ir.get("condition_imports") or []),
        )
    if task_type == "AppendVariableActivity":
        return AppendVariableActivity(
            **base,
            variable_name=task_ir.get("variable_name", ""),
            append_value=task_ir.get("append_value", ""),
            value_kind=task_ir.get("value_kind", "literal"),
            notebook_code=task_ir.get("notebook_code"),
            notebook_imports=task_ir.get("notebook_imports", []),
        )
    if task_type == "NotebookActivity":
        return NotebookActivity(
            **base,
            notebook_path=task_ir.get("notebook_path", ""),
            base_parameters=task_ir.get("base_parameters"),
        )
    if task_type == "SparkJarActivity":
        return SparkJarActivity(
            **base,
            main_class_name=task_ir.get("main_class_name", ""),
            parameters=task_ir.get("parameters"),
            libraries=task_ir.get("libraries"),
        )
    if task_type == "SparkPythonActivity":
        return SparkPythonActivity(
            **base,
            python_file=task_ir.get("python_file", ""),
            parameters=task_ir.get("parameters"),
        )
    if task_type == "ExecutePipelineActivity":
        return ExecutePipelineActivity(
            **base,
            pipeline_name=task_ir.get("pipeline_name", ""),
            parameters=task_ir.get("parameters"),
            wait_on_completion=task_ir.get("wait_on_completion", True),
        )
    if task_type == "RunJobActivity":
        return RunJobActivity(
            **base,
            job_name=task_ir.get("job_name", ""),
            existing_job_id=task_ir.get("existing_job_id"),
            job_parameters=task_ir.get("job_parameters") or task_ir.get("parameters"),
        )
    if task_type == "ForEachActivity":
        return ForEachActivity(
            **base,
            items_expression=task_ir.get("items_expression", ""),
            inner_activities=[_reconstruct_ir(child) for child in task_ir.get("inner_activities") or []],
            concurrency=task_ir.get("concurrency"),
        )
    if task_type == "IfConditionActivity":
        return IfConditionActivity(
            **base,
            op=task_ir.get("op", "EQUAL_TO"),
            left=task_ir.get("left", ""),
            right=task_ir.get("right", ""),
            if_true_activities=[_reconstruct_ir(child) for child in task_ir.get("if_true_activities") or []],
            if_false_activities=[_reconstruct_ir(child) for child in task_ir.get("if_false_activities") or []],
        )
    if task_type == "SwitchActivity":
        return SwitchActivity(
            **base,
            on_expression=task_ir.get("on_expression", ""),
            cases=[
                SwitchCase(
                    value=case.get("value", ""),
                    activities=[_reconstruct_ir(child) for child in case.get("activities") or []],
                )
                for case in task_ir.get("cases") or []
            ],
            default_activities=[_reconstruct_ir(child) for child in task_ir.get("default_activities") or []],
        )
    if task_type == "MotifActivity":
        return MotifActivity(
            **base,
            motif_id=task_ir.get("motif_id", "unknown"),
            display_name=task_ir.get("display_name", base["name"]),
            databricks_replacement=task_ir.get("databricks_replacement", "notebook"),
            matched_activity_names=list(task_ir.get("matched_activity_names", [])),
            source_type_hint=task_ir.get("source_type_hint"),
            confidence_notes=list(task_ir.get("confidence_notes", [])),
            original_activities=[],
            notebook_template=task_ir.get("notebook_template"),
            motif_config=task_ir.get("motif_config") or {},
        )
    if task_type == "UnsupportedActivity":
        return UnsupportedActivity(
            **base,
            original_type=task_ir.get("original_type", "unknown"),
            reason=task_ir.get("reason"),
        )
    if task_type == "PlaceholderActivity":
        return PlaceholderActivity(
            **base,
            original_type=task_ir.get("original_type", task_type),
            notebook_path=task_ir.get("notebook_path", "/UNSUPPORTED_ADF_ACTIVITY"),
            comment=task_ir.get("comment"),
        )
    return PlaceholderActivity(
        **base,
        original_type=task_type or "unknown",
        comment=f"Unknown activity type {task_type!r}; produced as placeholder during JSON-reload.",
    )


def _common_activity_kwargs(task_ir: dict[str, Any]) -> dict[str, Any]:
    """Extracts the base Activity fields shared by every IR class."""
    task_key = task_ir.get("task_key", "")
    return {
        "name": task_ir.get("name", task_key),
        "task_key": task_key,
        "description": task_ir.get("description"),
        "timeout_seconds": task_ir.get("timeout_seconds"),
        "max_retries": task_ir.get("max_retries"),
        "min_retry_interval_millis": task_ir.get("min_retry_interval_millis"),
        "depends_on": _reconstruct_dependencies(task_ir.get("depends_on")),
        "cluster": task_ir.get("cluster"),
        "required_parameters": dict(task_ir.get("required_parameters") or {}),
    }


def _reconstruct_dependencies(raw: list[dict[str, Any]] | None) -> list[Dependency] | None:
    if not raw:
        return None
    return [Dependency(task_key=dep.get("task_key", ""), outcome=dep.get("outcome")) for dep in raw]


# Notebook content generators


if __name__ == "__main__":
    main()
