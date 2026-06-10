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

from orchestra.adapter.operations import collect_workspace_artifact_paths
from orchestra.bundler.constants import (
    COMPUTE_MODE_TO_CLUSTER_KEY,
    DEFAULT_JOB_CLUSTER_KEY,
    MULTI_NODE_CLUSTER_NODE_TYPE_ID,
    MULTI_NODE_JOB_CLUSTER_KEY,
    SINGLE_NODE_JOB_CLUSTER_KEY,
)
from orchestra.bundler.inner_job_params import normalize_value
from orchestra.bundler.notebook_writer import write_notebooks
from orchestra.bundler.prereqs_writer import ManualParameter, build_prereqs, render_setup_md
from orchestra.bundler.setup_generator import generate_setup_tasks
from orchestra.models.dab import DabNotebook
from orchestra.models.ir import (
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
from orchestra.preparer.workflow_preparer import PreparedWorkflow, prepare_workflow
from orchestra.preparer.workspace_downloader import (
    enable_workspace_downloads,
    prompt_for_auth_if_missing,
    set_profile,
)
from orchestra.utils import normalize_task_key


class _BundleYamlDumper(yaml.SafeDumper):
    """YAML dumper that leaves keys unquoted and only quotes values when needed."""


# Module-level warnings collector — reset per write_bundle call.
_bundle_warnings: list[str] = []

# Cross-bundle ExecutePipeline refs seen while translating: variable_name →
# target pipeline name.  Reset per write_bundle call and surfaced via the
# bundle's ``variables`` block + SETUP.md.
_cross_bundle_variables: dict[str, str] = {}

# C-43 (CF5-001 / CF5-002): condition_task operands the dangling-ref safety
# net had to blank.  Each entry is {task_key, field, original_ref}.  Reset
# per write_bundle call and surfaced as a SETUP.md section so a neutralised
# branch predicate (always-true) is never silent.
_neutralized_conditions: list[dict[str, str]] = []

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
    _neutralized_conditions.clear()

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

    # 2b. Write Lakeflow pipeline resources (Lakeflow Connect ingestion
    # definitions emitted by the Copy preparer's LFC branch).  Each
    # resource lives in its own YAML so the bundle parser merges them
    # alongside the job resources via the ``include`` glob.
    pipelines_dir = resources_dir / "pipelines"
    for resource in _collect_pipeline_resources(workflow):
        pipelines_dir.mkdir(parents=True, exist_ok=True)
        resource_yml_path = pipelines_dir / f"{resource['resource_key']}.yml"
        resource_yml_path.write_text(
            yaml.dump(
                _wrap_pipeline_resource(resource),
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
                Dumper=_BundleYamlDumper,
            ),
            encoding="utf-8",
        )
        created_files.append(resource_yml_path.resolve())

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
    parameter_approximations = list(workflow.parameter_approximations)
    for inner in workflow.inner_workflows:
        parameter_approximations.extend(inner.parameter_approximations)
    known_bundle_jobs = {resource_key} | {normalize_task_key(inner.name) for inner in workflow.inner_workflows}
    # ``manual_parameters`` was collected above (before YAML emission) so
    # the broken values are also stripped from the on-disk YAML.
    # VAREX3-003: manual_variable_rollup SetupTasks emitted by
    # workflow_preparer surface in SETUP.md so the user knows where to add
    # a roll-up notebook.
    rollup_configs = [st.config for st in workflow.setup_tasks if st.type == "manual_variable_rollup"]
    for inner in workflow.inner_workflows:
        rollup_configs.extend(st.config for st in inner.setup_tasks if st.type == "manual_variable_rollup")
    dynamic_dispatch_configs = [st.config for st in workflow.setup_tasks if st.type == "dynamic_notebook_dispatch"]
    unresolved_library_configs = [st.config for st in workflow.setup_tasks if st.type == "unresolved_library"]
    manual_variable_init_configs = [st.config for st in workflow.setup_tasks if st.type == "manual_variable_init"]
    manual_schedule_time_of_day_configs = [
        st.config for st in workflow.setup_tasks if st.type == "manual_schedule_time_of_day"
    ]
    manual_credential_configs = [st.config for st in workflow.setup_tasks if st.type == "manual_credential"]
    for inner in workflow.inner_workflows:
        dynamic_dispatch_configs.extend(st.config for st in inner.setup_tasks if st.type == "dynamic_notebook_dispatch")
        unresolved_library_configs.extend(st.config for st in inner.setup_tasks if st.type == "unresolved_library")
        manual_variable_init_configs.extend(st.config for st in inner.setup_tasks if st.type == "manual_variable_init")
        manual_schedule_time_of_day_configs.extend(
            st.config for st in inner.setup_tasks if st.type == "manual_schedule_time_of_day"
        )
        manual_credential_configs.extend(st.config for st in inner.setup_tasks if st.type == "manual_credential")
    # LSC3-006: union typed SecretInstructions from the workflow (and
    # inner workflows) with the notebook-scanned scopes so SETUP.md and
    # create_secrets.py reference the same set of (scope, key) pairs.
    all_secret_instructions = list(workflow.secrets)
    for inner in workflow.inner_workflows:
        all_secret_instructions.extend(inner.secrets)
    prereqs = build_prereqs(
        notebooks=all_notebooks,
        tasks=all_tasks,
        known_bundle_jobs=known_bundle_jobs,
        cross_bundle_variables=dict(_cross_bundle_variables),
        manual_parameters=manual_parameters,
        parameter_approximations=parameter_approximations,
        manual_variable_rollups=rollup_configs,
        secret_instructions=all_secret_instructions,
        dynamic_notebook_dispatches=dynamic_dispatch_configs,
        unresolved_libraries=unresolved_library_configs,
        manual_variable_inits=manual_variable_init_configs,
        manual_schedule_time_of_day=manual_schedule_time_of_day_configs,
        manual_credentials=manual_credential_configs,
        neutralized_conditions=list(_neutralized_conditions),
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
    parser.add_argument(
        "--profile",
        type=str,
        default=None,
        help="Databricks CLI profile to use when downloading workspace artifacts.",
    )
    parser.add_argument(
        "--no-vendor-workspace-files",
        action="store_true",
        help=(
            "Skip downloading workspace-resident notebooks / Python files / JARs. "
            "Tasks keep their original workspace paths and the bundle is not self-contained."
        ),
    )
    args = parser.parse_args()

    if not args.report.exists():
        print(f"Error: Report file not found: {args.report}", file=sys.stderr)
        sys.exit(1)

    if args.profile:
        set_profile(args.profile)

    if not args.no_vendor_workspace_files:
        workspace_paths = collect_workspace_artifact_paths(args.report)
        if workspace_paths:
            if not prompt_for_auth_if_missing(workspace_paths):
                print(
                    "Aborted. Run `databricks auth login --host <workspace-url>` and retry.",
                    file=sys.stderr,
                )
                sys.exit(2)
            enable_workspace_downloads(True)

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

# C-29 (NB-ITER4-002): a real DBR version string matches e.g.
# "15.4.x-scala2.12" / "15.4.x-photon-scala2.12".  ADF expressions like
# ``@if(equals(item()?.photon,true),...)`` slip through unfiltered today
# and land in ``databricks.yml`` as the spark_version variable default,
# which bundle deploy rejects.  The regex anchors on the canonical
# Databricks Runtime shape so unrecognised strings fall through to the
# safe default.
_DBR_VERSION_RE = re.compile(r"^\d+\.\d+\.x(-[a-z0-9.]+)*$")


def _is_valid_spark_version(value: Any) -> bool:
    """Return True when *value* parses as a real DBR runtime version string."""
    if not isinstance(value, str) or not value:
        return False
    return _DBR_VERSION_RE.match(value) is not None


def _is_valid_node_type_id(value: Any) -> bool:
    """Return True when *value* looks like a real cloud instance type.

    Conservatively rejects anything that starts with ``@`` (an unresolved
    ADF expression) or contains spaces; otherwise accepts the value
    verbatim so we don't gate out cloud-specific instance families.
    """
    if not isinstance(value, str) or not value:
        return False
    if value.startswith("@"):
        return False
    if any(ch.isspace() for ch in value):
        return False
    return True


def _infer_bundle_cluster_defaults(workflow: PreparedWorkflow) -> tuple[str, str]:
    """Derive ``spark_version`` and ``node_type_id`` defaults from task clusters.

    Args:
        workflow: The prepared workflow being written.

    Returns:
        ``(spark_version, node_type_id)`` strings.
    """
    from collections import Counter

    # C-29 (NB-ITER4-002): filter out unparseable spark_version /
    # node_type_id hints before Counter so unresolved ADF expressions
    # (e.g. ``@if(equals(item()?.photon,true),...)``) don't land as the
    # bundle's default and break ``databricks bundle deploy``.
    spark_versions = [
        hint["spark_version"] for hint in workflow.cluster_hints if _is_valid_spark_version(hint.get("spark_version"))
    ]
    node_types = [
        hint["node_type_id"] for hint in workflow.cluster_hints if _is_valid_node_type_id(hint.get("node_type_id"))
    ]

    spark_version = Counter(spark_versions).most_common(1)[0][0] if spark_versions else _DEFAULT_SPARK_VERSION
    node_type_id = Counter(node_types).most_common(1)[0][0] if node_types else _DEFAULT_NODE_TYPE_ID
    return spark_version, node_type_id


def _infer_bundle_cluster_extras(workflow: PreparedWorkflow) -> dict[str, Any]:
    """Surface non-default cluster fields shared across the workflow's tasks.

    Mines :attr:`PreparedWorkflow.cluster_hints` for cluster fields beyond
    spark_version / node_type_id (including num_workers) and returns the
    consensus values so the default job_cluster reflects ADF settings
    end-to-end.

    Args:
        workflow: The prepared workflow being written.

    Returns:
        Dict of cluster fields ready to merge under ``new_cluster``.  Only
        the most common value across hints is propagated for each field;
        ties are broken by first occurrence.
    """
    from collections import Counter

    extras: dict[str, Any] = {}
    extra_keys = (
        "num_workers",
        "driver_node_type_id",
        "data_security_mode",
        "spark_env_vars",
        "custom_tags",
        "init_scripts",
        "cluster_log_conf",
        "spark_conf",
    )
    for key in extra_keys:
        values = [hint[key] for hint in workflow.cluster_hints if hint.get(key)]
        if not values:
            continue
        # Use string repr to dedupe non-hashable dict entries while still
        # picking the most common.
        rep_counter: Counter[str] = Counter()
        rep_to_value: dict[str, Any] = {}
        for value in values:
            rep = repr(value)
            rep_counter[rep] += 1
            rep_to_value.setdefault(rep, value)
        top_rep, _count = rep_counter.most_common(1)[0]
        extras[key] = rep_to_value[top_rep]
    return extras


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


def _build_default_job_clusters(
    needed_keys: set[str],
    *,
    extras: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Builds the job_clusters stanza, emitting only the clusters in use.

    Args:
        needed_keys: Set of job_cluster_key strings referenced by any task
            in the workflow.
        extras: Optional cluster fields surfaced from per-task hints
            (driver_node_type_id, spark_env_vars, custom_tags, ...).  When
            present these override the corresponding fields of the
            multi-purpose default cluster so ADF-derived settings flow
            into the emitted YAML.

    Returns:
        Ordered list of cluster definitions for inclusion under the job's
        ``job_clusters`` block.
    """
    builders: tuple[tuple[str, Any], ...] = (
        (DEFAULT_JOB_CLUSTER_KEY, lambda: _build_default_cluster(extras)),
        (SINGLE_NODE_JOB_CLUSTER_KEY, _build_single_node_cluster),
        (MULTI_NODE_JOB_CLUSTER_KEY, _build_multi_node_cluster),
    )
    return [builder() for key, builder in builders if key in needed_keys]


def _build_default_cluster(extras: dict[str, Any] | None = None) -> dict[str, Any]:
    """Builds the multi-purpose default job_cluster used for legacy bindings.

    Args:
        extras: Optional cluster fields lifted from per-task hints to
            merge into ``new_cluster`` (num_workers, driver_node_type_id,
            spark_env_vars, custom_tags, init_scripts, cluster_log_conf,
            spark_conf, data_security_mode).  ``num_workers`` overrides the
            default single-worker value and ``data_security_mode`` overrides
            the default ``SINGLE_USER`` value when supplied.

    Returns:
        Cluster definition with the mined (or default single) worker count
        and bundle-variable knobs for spark_version and node_type_id, plus
        any merged extras.
    """
    new_cluster: dict[str, Any] = {
        "spark_version": "${var.spark_version}",
        "node_type_id": "${var.node_type_id}",
        "num_workers": 1,
        "data_security_mode": "SINGLE_USER",
        "single_user_name": "${workspace.current_user.userName}",
    }
    if extras:
        for key, value in extras.items():
            new_cluster[key] = value
    return {
        "job_cluster_key": DEFAULT_JOB_CLUSTER_KEY,
        "new_cluster": new_cluster,
    }


def _build_single_node_cluster() -> dict[str, Any]:
    """Builds the single-node job_cluster used for non-Databricks tasks under classic compute.

    Returns:
        Cluster definition using ``is_single_node`` so Databricks
        configures the cluster for single-node execution without
        requiring ``num_workers``, custom Spark conf, or tags.
    """
    return {
        "job_cluster_key": SINGLE_NODE_JOB_CLUSTER_KEY,
        "new_cluster": {
            "spark_version": "${var.spark_version}",
            "node_type_id": "${var.node_type_id}",
            "is_single_node": True,
            "data_security_mode": "SINGLE_USER",
            "single_user_name": "${workspace.current_user.userName}",
        },
    }


def _build_multi_node_cluster() -> dict[str, Any]:
    """Builds the fixed two-node job_cluster used for Copy Data tasks under classic compute.

    Returns:
        Cluster definition with two workers on the Copy Data instance
        type and the bundle-variable spark_version knob.
    """
    return {
        "job_cluster_key": MULTI_NODE_JOB_CLUSTER_KEY,
        "new_cluster": {
            "spark_version": "${var.spark_version}",
            "node_type_id": MULTI_NODE_CLUSTER_NODE_TYPE_ID,
            "num_workers": 2,
            "data_security_mode": "SINGLE_USER",
            "single_user_name": "${workspace.current_user.userName}",
        },
    }


def _collect_pipeline_resources(workflow: PreparedWorkflow) -> list[dict[str, Any]]:
    """Returns every Lakeflow pipeline resource carried by *workflow* and its inner jobs.

    Args:
        workflow: The prepared workflow being written.

    Returns:
        Flat list of pipeline-resource dicts (each with ``resource_key``
        and ``definition``), including entries from inner workflows.
    """
    resources = list(workflow.pipeline_resources)
    for inner in workflow.inner_workflows:
        resources.extend(inner.pipeline_resources)
    return resources


def _wrap_pipeline_resource(resource: dict[str, Any]) -> dict[str, Any]:
    """Wraps a pipeline definition in the DAB ``resources.pipelines`` envelope.

    Args:
        resource: Dict with ``resource_key`` and ``definition`` keys as
            produced by the Copy preparer's Lakeflow Connect branch.

    Returns:
        A dict shaped for direct YAML serialisation under a bundle
        resource file.
    """
    return {"resources": {"pipelines": {resource["resource_key"]: resource["definition"]}}}


def _collect_required_cluster_keys(tasks: list[dict[str, Any]]) -> set[str]:
    """Walks every task and returns the set of job_cluster keys actually bound.

    Args:
        tasks: Top-level task dicts after cluster binding has run.

    Returns:
        Set of ``job_cluster_key`` values present anywhere in the task
        tree (including bodies under ``for_each_task.task``).
    """
    return {task["job_cluster_key"] for task in _iter_tasks_recursively(tasks) if task.get("job_cluster_key")}


def _strip_compute_mode_markers(tasks: list[dict[str, Any]]) -> None:
    """Removes the private ``_compute_mode`` marker from every task before YAML output.

    Args:
        tasks: Top-level task dicts (mutated in place).
    """
    for task in _iter_tasks_recursively(tasks):
        task.pop("_compute_mode", None)


# Patterns that signal a base_parameter value couldn't be evaluated cleanly.
# When any task references an *existing* notebook (absolute workspace path),
# orchestra can't inject the runtime computation, so these end up as manual
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
    """Finds base_parameters orchestra couldn't evaluate for notebook tasks.

    Previously this scan skipped stub notebooks under ``../src/`` because
    their bodies could (in principle) be patched to inline the runtime
    computation.  In practice stub tasks for activities that have no
    deterministic translation are emitted with raw ADF expression values
    that ``dbutils.widgets.get`` returns verbatim, which fails at runtime.
    Walking both the absolute-path and bundle-relative cases drops the
    broken values and surfaces them as a SETUP.md row instead.
    """
    manual_parameters: list[ManualParameter] = []
    for task in _iter_tasks_recursively(tasks):
        notebook_task = task.get("notebook_task") or {}
        notebook_path = notebook_task.get("notebook_path", "")
        base_params = notebook_task.get("base_parameters")
        if not isinstance(base_params, dict):
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
    """Binds notebook tasks to the cluster their compute_mode marker dictates.

    Tasks that the pipeline modifier marked ``serverless`` are left
    unbound so they run on serverless compute.  Tasks marked
    ``classic_single_node`` or ``classic_multi_node`` bind to the
    matching job_cluster.  Tasks without a marker fall back to the
    legacy behaviour: existing-workspace notebooks bind to
    ``default_cluster`` and orchestra-generated notebooks stay unbound.

    Args:
        tasks: Top-level task dicts (mutated in place).
    """
    for task in _iter_tasks_recursively(tasks):
        notebook_task = task.get("notebook_task")
        if notebook_task is None:
            continue
        if any(key in task for key in _CLUSTER_BINDING_KEYS):
            continue
        compute_mode = task.get("_compute_mode")
        if compute_mode == "serverless":
            # Serverless cannot host jar/whl libraries.  When the task
            # ships libraries we must still bind a classic cluster so the
            # Jobs API accepts the libraries block.
            if _task_has_jar_or_whl_libraries(task):
                task["job_cluster_key"] = DEFAULT_JOB_CLUSTER_KEY
            continue
        cluster_key = COMPUTE_MODE_TO_CLUSTER_KEY.get(compute_mode or "")
        if cluster_key is not None:
            task["job_cluster_key"] = cluster_key
            continue
        notebook_path = notebook_task.get("notebook_path", "")
        # Stub notebooks (../src/...) are normally left unbound for
        # serverless compute.  But when libraries are attached we must
        # bind to a real cluster (NB-2) -- serverless cannot install
        # jar / whl libraries.
        if notebook_path.startswith("../src/"):
            if _task_has_jar_or_whl_libraries(task):
                task["job_cluster_key"] = DEFAULT_JOB_CLUSTER_KEY
            continue
        task["job_cluster_key"] = DEFAULT_JOB_CLUSTER_KEY


def _task_has_jar_or_whl_libraries(task: dict[str, Any]) -> bool:
    """Return True when *task* references a library shape that needs a cluster.

    JAR / EGG / whl / PyPI / Maven / CRAN entries all require a classic
    cluster — they cannot be installed on serverless.  Requirements files
    are treated the same way to be safe.
    """
    libs = task.get("libraries")
    if not isinstance(libs, list):
        return False
    cluster_required = {"jar", "egg", "whl", "maven", "pypi", "cran", "requirements"}
    for entry in libs:
        if isinstance(entry, dict) and any(key in entry for key in cluster_required):
            return True
    return False


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


def _apply_schedule_to_job(job_def: dict[str, Any], spec: dict[str, Any]) -> None:
    """Renders a workflow schedule spec onto a DAB job definition.

    C-10 (SCHED-001): translates the structured schedule dict produced by
    ``engine._adf_trigger_to_schedule`` into either ``schedule:`` or
    ``trigger:`` keys on the job YAML.  Best-effort schedule shapes
    (Tumbling / CustomEvents) fall back to a comment-style placeholder
    so SETUP.md can capture them.
    """
    kind = spec.get("kind")
    if kind == "schedule":
        if "quartz_cron_expression" in spec:
            schedule_block: dict[str, Any] = {
                "quartz_cron_expression": spec["quartz_cron_expression"],
                "timezone_id": spec.get("timezone_id", "UTC"),
            }
            if spec.get("pause_status"):
                schedule_block["pause_status"] = spec["pause_status"]
            job_def["schedule"] = schedule_block
            return
        # Tumbling fallback -- attach a hint rather than emitting a
        # malformed schedule.  SETUP.md picks it up downstream.
        job_def["schedule_setup_note"] = spec
        return
    if kind == "periodic":
        # SCHED3-002: Day/Week/Month with interval > 1 maps to trigger.periodic.
        trigger_block: dict[str, Any] = {
            "periodic": {
                "interval": spec.get("interval", 1),
                "unit": spec.get("unit", "DAYS"),
            }
        }
        if spec.get("pause_status"):
            trigger_block["pause_status"] = spec["pause_status"]
        job_def["trigger"] = trigger_block
        return
    if kind == "file_arrival":
        trigger_block = {
            "file_arrival": {"url": spec.get("url", "")},
        }
        if spec.get("pause_status"):
            trigger_block["pause_status"] = spec["pause_status"]
        job_def["trigger"] = trigger_block
        return
    if kind == "manual_setup":
        # No DAB primitive -- surface the raw spec so SETUP.md can flag it.
        job_def["schedule_setup_note"] = spec
        return


def _strip_dangling_task_value_refs(
    tasks: list[dict[str, Any]],
    all_task_keys: set[str],
) -> list[dict[str, str]]:
    """Replaces ``{{tasks.X.values.Y}}`` refs whose ``X`` is not in the bundle.

    C-12 (VAREX-005): in addition to ``notebook_task.base_parameters``,
    walk ``run_job_task.job_parameters``, ``condition_task.left`` and
    ``condition_task.right``, plus the nested for_each body so cross-job
    parameter passing surfaces are caught.  C-05 fixes most variable
    defaults; this safety net catches the residual cases (renames,
    scoped-out variables) by emitting an empty string in place of the
    dangling ref so SETUP.md §4 can flag it.

    C-43 (CF5-001 / CF5-002): blanking a *condition_task* operand silently
    turns ``NOT_EQUAL('', '0')`` into an always-true predicate, so the
    branch runs unconditionally with no signal.  This function now records
    each condition operand it neutralises and returns them so the caller
    can surface a 'conditions neutralized — manual re-wiring required'
    section in SETUP.md instead of failing silently.

    Args:
        tasks: Top-level tasks for one job (mutated in place).
        all_task_keys: Task keys that do exist in this job (including those
            inside ``for_each_task.task`` bodies).

    Returns:
        List of ``{task_key, field, original_ref}`` dicts for every
        condition operand that was blanked.
    """
    neutralized: list[dict[str, str]] = []

    def _is_dangling(value: Any) -> bool:
        if not isinstance(value, str):
            return False
        match = _TASK_VALUE_REF.search(value)
        return bool(match and match.group(1) not in all_task_keys)

    def visit(task: dict[str, Any]) -> None:
        notebook_task = task.get("notebook_task") or {}
        base_parameters = notebook_task.get("base_parameters") or {}
        for widget_name, value in list(base_parameters.items()):
            if _is_dangling(value):
                base_parameters[widget_name] = ""

        # C-12: run_job_task.job_parameters references the parent's task
        # values when crossing into an inner job.  Strip dangling refs.
        run_job_task = task.get("run_job_task") or {}
        job_parameters = run_job_task.get("job_parameters") or {}
        if isinstance(job_parameters, dict):
            for param_name, value in list(job_parameters.items()):
                if _is_dangling(value):
                    job_parameters[param_name] = ""

        # C-12: condition_task operands can also carry dangling refs
        # when an upstream renamed task disappeared between rewrite
        # passes.  C-43: record each neutralised operand for SETUP.md.
        condition_task = task.get("condition_task") or {}
        if condition_task:
            task_key = task.get("task_key", "")
            for field_name in ("left", "right"):
                operand = condition_task.get(field_name)
                if _is_dangling(operand):
                    neutralized.append(
                        {
                            "task_key": str(task_key),
                            "field": field_name,
                            "original_ref": str(operand),
                        }
                    )
                    condition_task[field_name] = ""

        for_each = task.get("for_each_task")
        if for_each and isinstance(for_each.get("task"), dict):
            visit(for_each["task"])

    for task in tasks:
        visit(task)

    return neutralized


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
    # the empty string now so SETUP.md §4 flags it.  C-43: a blanked
    # condition operand silently makes the predicate always-true, so record
    # each neutralised condition for the SETUP.md re-wiring section.
    _neutralized_conditions.extend(
        _strip_dangling_task_value_refs(workflow.tasks, _collect_all_task_keys(workflow.tasks))
    )

    job_def: dict[str, Any] = {
        "name": workflow.name,
        "tasks": workflow.tasks,
    }

    if attach_clusters:
        _bind_cluster_to_notebook_tasks(workflow.tasks)
        needed_keys = _collect_required_cluster_keys(workflow.tasks)
        if needed_keys:
            cluster_extras = _infer_bundle_cluster_extras(workflow)
            job_def["job_clusters"] = _build_default_job_clusters(
                needed_keys,
                extras=cluster_extras or None,
            )

    _strip_compute_mode_markers(workflow.tasks)

    if workflow.parameters:
        job_def["parameters"] = workflow.parameters

    # C-10 (SCHED-001): render the workflow schedule / trigger spec.
    schedule_spec = getattr(workflow, "schedule", None)
    if schedule_spec:
        _apply_schedule_to_job(job_def, schedule_spec)
        # SCHED3-003: trigger-supplied per-pipeline parameter overrides
        # update the matching job.parameter defaults so scheduled runs
        # receive the trigger's pinned values instead of the bare pipeline
        # default.  Overrides only mutate existing declared parameters;
        # unknown names are silently ignored to keep job_def well-formed.
        overrides = schedule_spec.get("parameter_overrides") or {}
        if overrides and job_def.get("parameters"):
            for entry in job_def["parameters"]:
                if entry.get("name") in overrides:
                    entry["default"] = overrides[entry["name"]]

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
        pipeline_params: dict[str, list[dict[str, Any]]] = {}
        pipeline_schedules: dict[str, dict[str, Any]] = {}
        for translation in report.get("translations", []):
            pipeline_name = translation.get("pipeline", "unknown")
            if translation.get("status") != "translated":
                continue
            ir = translation.get("ir") or {}
            if not ir:
                continue
            pipelines.setdefault(pipeline_name, []).append(ir)
            # Round-trip pipeline-level parameters either supplied per-
            # translation (newer report shape) or alongside the ir under
            # an ``ir.parameters`` key (older single-pipeline serialisations
            # roundtripped through this aggregator).
            params = translation.get("parameters") or ir.get("parameters")
            if params and pipeline_name not in pipeline_params:
                pipeline_params[pipeline_name] = list(params)
            # Likewise carry pipeline-level ``schedule`` through to the
            # rehydrated pipeline_dict so trigger-derived schedule / trigger
            # blocks survive the aggregated report shape.
            schedule = translation.get("schedule") or ir.get("schedule")
            if schedule and pipeline_name not in pipeline_schedules:
                pipeline_schedules[pipeline_name] = dict(schedule)

        for pipeline_name, task_irs in pipelines.items():
            pipeline_dict: dict[str, Any] = {"name": pipeline_name, "tasks": task_irs}
            if pipeline_params.get(pipeline_name):
                pipeline_dict["parameters"] = pipeline_params[pipeline_name]
            if pipeline_schedules.get(pipeline_name):
                pipeline_dict["schedule"] = pipeline_schedules[pipeline_name]
            workflow = _pipeline_dict_to_workflow(pipeline_dict)
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
    pipeline, parameters = pipeline_dict_to_ir(pipeline_dict)
    workflow = prepare_workflow(pipeline)
    if parameters:
        workflow.parameters.extend(parameters)
    return workflow


def pipeline_dict_to_ir(pipeline_dict: dict[str, Any]) -> tuple[Pipeline, list[dict[str, Any]]]:
    """Rehydrates a serialised pipeline IR dict into a typed :class:`Pipeline`.

    Args:
        pipeline_dict: Dict produced by ``engine._pipeline_to_dict`` (or
            the equivalent shape emitted by the adapter CLI bridge).

    Returns:
        Tuple of ``(pipeline, parameters)`` where ``pipeline`` is the
        rehydrated :class:`Pipeline` and ``parameters`` is the normalised
        list of pipeline-level parameter definitions (empty when the
        report carries no parameters).
    """
    activities = [_reconstruct_ir(task_ir) for task_ir in pipeline_dict.get("tasks", [])]
    parameters: list[dict[str, Any]] = []
    for param in pipeline_dict.get("parameters") or []:
        entry: dict[str, Any] = {"name": param["name"]}
        if "default" in param and param["default"] is not None:
            default_value = param["default"]
            # Bool / int / float defaults must survive the JSON round-trip
            # as their declared type so the emitted YAML carries a real
            # boolean / number, not a quoted string.  String defaults go
            # through normalize_value to resolve embedded ADF refs.
            if isinstance(default_value, bool):
                entry["default"] = default_value
            elif isinstance(default_value, (int, float)):
                entry["default"] = default_value
            else:
                entry["default"] = normalize_value(str(default_value))
        parameters.append(entry)
    pipeline = Pipeline(
        name=pipeline_dict.get("name", "unknown"),
        tasks=activities,
        parameters=parameters or None,
        translation_preferences=_reconstruct_preferences(pipeline_dict.get("translation_preferences")),
        schedule=pipeline_dict.get("schedule"),
    )
    return pipeline, parameters


def _reconstruct_preferences(raw: dict[str, Any] | None) -> Any:
    """Rebuilds a :class:`TranslationPreferences` from its serialised form.

    Args:
        raw: Dict emitted by ``engine._preferences_to_dict``, or ``None``
            when the report carries no preferences.

    Returns:
        A :class:`TranslationPreferences` instance, or ``None`` when
        *raw* is falsy.
    """
    if not raw:
        return None
    from orchestra.adapter.models import TranslationPreferences

    # Reports authored before the databricks_task_compute option was
    # removed may still carry that key; drop it silently so old reports
    # remain rehydratable.
    return TranslationPreferences(
        copy_activity_paradigm=raw.get("copy_activity_paradigm", "notebook"),
        non_databricks_task_compute=raw.get("non_databricks_task_compute", "serverless"),
        use_lakeflow_connectors=raw.get("use_lakeflow_connectors", "existing"),
        lakeflow_connector_type=raw.get("lakeflow_connector_type", "cdc"),
        motif_consolidations=dict(raw.get("motif_consolidations") or {}),
        per_task=dict(raw.get("per_task") or {}),
    )


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
            target_format=task_ir.get("target_format"),
            use_lakeflow_connector=bool(task_ir.get("use_lakeflow_connector", False)),
            lakeflow_connector_type=task_ir.get("lakeflow_connector_type"),
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
            raw_expression=task_ir.get("raw_expression"),
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
            notebook_path_unresolved=bool(task_ir.get("notebook_path_unresolved", False)),
            notebook_path_expression=task_ir.get("notebook_path_expression"),
            unresolved_libraries=list(task_ir.get("unresolved_libraries") or []),
        )
    if task_type == "SparkJarActivity":
        return SparkJarActivity(
            **base,
            main_class_name=task_ir.get("main_class_name", ""),
            parameters=task_ir.get("parameters"),
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
            # C-31 (CF4-001): preserve bridge fields so the preparer can
            # synthesise the inputs bridge after a JSON roundtrip.
            inputs_bridge_notebook_code=task_ir.get("inputs_bridge_notebook_code"),
            inputs_bridge_notebook_imports=list(task_ir.get("inputs_bridge_notebook_imports") or []),
            inputs_bridge_required_parameters=dict(task_ir.get("inputs_bridge_required_parameters") or {}),
        )
    if task_type == "IfConditionActivity":
        return IfConditionActivity(
            **base,
            op=task_ir.get("op", "EQUAL_TO"),
            left=task_ir.get("left", ""),
            right=task_ir.get("right", ""),
            if_true_activities=[_reconstruct_ir(child) for child in task_ir.get("if_true_activities") or []],
            if_false_activities=[_reconstruct_ir(child) for child in task_ir.get("if_false_activities") or []],
            # C-14 (CF3-001 / VAREX3-001): preserve bridge fields so the
            # preparer can re-synthesise the hidden _bridge SetVariable task
            # after a JSON roundtrip.
            bridge_notebook_code=task_ir.get("bridge_notebook_code"),
            bridge_notebook_imports=list(task_ir.get("bridge_notebook_imports") or []),
            bridge_required_parameters=dict(task_ir.get("bridge_required_parameters") or {}),
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
            # C-14 (CF3-001 / VAREX3-001): preserve bridge fields for Switch
            # so the preparer can re-synthesise the bridge task after a
            # JSON roundtrip.
            bridge_notebook_code=task_ir.get("bridge_notebook_code"),
            bridge_notebook_imports=list(task_ir.get("bridge_notebook_imports") or []),
            bridge_required_parameters=dict(task_ir.get("bridge_required_parameters") or {}),
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
            consolidate_metadata_driven=bool(task_ir.get("consolidate_metadata_driven", False)),
            lookup_values=list(task_ir.get("lookup_values") or []),
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
        "existing_cluster_id": task_ir.get("existing_cluster_id"),
        "libraries": task_ir.get("libraries"),
        "parameter_approximations": list(task_ir.get("parameter_approximations") or []),
        "required_parameters": dict(task_ir.get("required_parameters") or {}),
        "compute_mode": task_ir.get("compute_mode"),
    }


def _reconstruct_dependencies(raw: list[dict[str, Any]] | None) -> list[Dependency] | None:
    if not raw:
        return None
    return [Dependency(task_key=dep.get("task_key", ""), outcome=dep.get("outcome")) for dep in raw]


# Notebook content generators


if __name__ == "__main__":
    main()
