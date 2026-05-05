"""Generates a SETUP.md file listing steps required before a bundle can run."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from flowx.models.dab import DabNotebook

# Regexes used to mine the generated artifacts for external dependencies.
# Kept as compiled patterns so :func:`build_prereqs` is cheap to call.
_SECRET_REFERENCE = re.compile(
    r"""dbutils\.secrets\.get\(\s*scope\s*=\s*["']([^"']+)["']\s*,\s*key\s*=\s*["']([^"']+)["']""",
)
_WORKSPACE_PATH_HEADER = re.compile(
    r"\*\*Source workspace path\*\*:\s*`([^`]+)`",
)
_NOT_IMPLEMENTED_STUB = re.compile(r"\braise\s+NotImplementedError\b")
_WIDGET_REFERENCE = re.compile(r"""dbutils\.widgets\.get\(\s*["']([^"']+)["']\s*\)""")


@dataclass(slots=True, kw_only=True)
class MissingNotebook:
    """A notebook that must be authored or imported before the bundle can run.

    Attributes:
        task_key: DAB task key that references the notebook.
        workspace_path: Original ADF/workspace path (empty if unknown).
        bundle_path: Relative path within the bundle where the stub lives.
        widget_names: Widget names the task passes via ``base_parameters``
            — useful for the author of the replacement notebook.
    """

    task_key: str
    workspace_path: str
    bundle_path: str
    widget_names: list[str] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class CrossBundleReference:
    """A ``run_job_task`` pointing at a job defined in a different bundle.

    Attributes:
        task_key: DAB task key of the caller.
        target_pipeline: Name of the referenced pipeline/job.
    """

    task_key: str
    target_pipeline: str


@dataclass(slots=True, kw_only=True)
class EmptyParameter:
    """A task widget the notebook reads but the translator couldn't populate.

    Attributes:
        task_key: DAB task key.
        widget_name: Widget name with an empty default.
    """

    task_key: str
    widget_name: str


@dataclass(slots=True, kw_only=True)
class ManualParameter:
    """A base_parameter that the user must compute inside an existing notebook.

    Attributes:
        task_key: DAB task key.
        widget_name: The base_parameter name.
        notebook_path: Workspace path of the existing notebook.
        raw_expression: The original ADF expression (verbatim) so the user
            knows what runtime value they need to compute.
    """

    task_key: str
    widget_name: str
    notebook_path: str
    raw_expression: str


@dataclass(slots=True, kw_only=True)
class NetworkEndpoint:
    """A piece of network connectivity the bundle's notebooks expect.

    Attributes:
        kind: One of ``"jdbc"`` (database server), ``"http"`` (REST endpoint),
            or ``"storage"`` (object storage / UC volume).
        target: A short identifier for the endpoint — the JDBC scope name,
            the URL host, or the storage account.
        notes: Free-form guidance shown next to the endpoint in SETUP.md.
    """

    kind: str
    target: str
    notes: str = ""


@dataclass(slots=True, kw_only=True)
class Prereqs:
    """Collected external dependencies for one bundle."""

    secrets: dict[str, set[str]] = field(default_factory=dict)  # scope -> {keys}
    missing_notebooks: list[MissingNotebook] = field(default_factory=list)
    cross_bundle_refs: list[CrossBundleReference] = field(default_factory=list)
    empty_parameters: list[EmptyParameter] = field(default_factory=list)
    compute_notes: list[str] = field(default_factory=list)
    network_endpoints: list[NetworkEndpoint] = field(default_factory=list)
    manual_parameters: list[ManualParameter] = field(default_factory=list)

    def is_empty(self) -> bool:
        """Return ``True`` when nothing needs to happen before ``bundle run``."""
        return (
            not self.secrets
            and not self.missing_notebooks
            and not self.cross_bundle_refs
            and not self.empty_parameters
            and not self.compute_notes
            and not self.network_endpoints
            and not self.manual_parameters
        )


def _walk_tasks(tasks: list[dict[str, Any]]):
    """Yields every task dict including nested ``for_each_task.task`` bodies."""
    for task in tasks:
        yield task
        for_each = task.get("for_each_task")
        if for_each and isinstance(for_each.get("task"), dict):
            yield from _walk_tasks([for_each["task"]])


def scan_notebooks_for_secrets(notebooks: list[DabNotebook]) -> dict[str, set[str]]:
    """Extracts all ``dbutils.secrets.get(scope=..., key=...)`` references.

    Args:
        notebooks: Generated notebooks in the bundle.

    Returns:
        Mapping of scope name to the set of keys referenced within that scope.
    """
    scopes: dict[str, set[str]] = {}
    for notebook in notebooks:
        for scope_name, key in _SECRET_REFERENCE.findall(notebook.content):
            scopes.setdefault(scope_name, set()).add(key)
    return scopes


def collect_missing_notebooks(
    notebooks: list[DabNotebook],
    tasks: list[dict[str, Any]],
) -> list[MissingNotebook]:
    """Identify notebook stubs the user must replace before running the bundle.

    Args:
        notebooks: Generated notebooks in the bundle.
        tasks: Top-level task dicts (used to correlate widgets and paths).

    Returns:
        List of :class:`MissingNotebook` entries sorted by task_key.
    """
    task_by_path: dict[str, dict[str, Any]] = {}
    for task in _walk_tasks(tasks):
        notebook_task = task.get("notebook_task") or {}
        path = notebook_task.get("notebook_path", "")
        if path:
            task_by_path[path] = task

    missing: list[MissingNotebook] = []
    for notebook in notebooks:
        if not _NOT_IMPLEMENTED_STUB.search(notebook.content):
            continue

        workspace_match = _WORKSPACE_PATH_HEADER.search(notebook.content)
        workspace_path = workspace_match.group(1) if workspace_match else ""

        bundle_relative_notebook_path = f"../src/{notebook.relative_path}"
        task = task_by_path.get(bundle_relative_notebook_path)
        task_key = task.get("task_key", notebook.relative_path) if task else notebook.relative_path
        base_parameters: dict[str, str] = {}
        if task and "notebook_task" in task:
            base_parameters = task["notebook_task"].get("base_parameters") or {}
        widget_names = sorted(base_parameters.keys())

        missing.append(
            MissingNotebook(
                task_key=task_key,
                workspace_path=workspace_path,
                bundle_path=f"src/{notebook.relative_path}",
                widget_names=widget_names,
            )
        )

    missing.sort(key=lambda n: n.task_key)
    return missing


def collect_cross_bundle_refs(tasks: list[dict[str, Any]], known_bundle_jobs: set[str]) -> list[CrossBundleReference]:
    """Finds ``run_job_task`` entries pointing outside this bundle.

    Args:
        tasks: Top-level task dicts.
        known_bundle_jobs: Resource keys of jobs defined in this bundle.

    Returns:
        List of :class:`CrossBundleReference` entries.
    """
    pattern = re.compile(r"\$\{resources\.jobs\.([^.]+)\.id\}")
    refs: list[CrossBundleReference] = []
    for task in _walk_tasks(tasks):
        run_job = task.get("run_job_task")
        if not run_job:
            continue
        job_id_reference = run_job.get("job_id", "")
        match = pattern.search(str(job_id_reference))
        if not match:
            continue
        target = match.group(1)
        if target in known_bundle_jobs:
            continue
        refs.append(CrossBundleReference(task_key=task.get("task_key", ""), target_pipeline=target))
    return refs


def collect_empty_parameters(tasks: list[dict[str, Any]]) -> list[EmptyParameter]:
    """Finds base_parameters whose values are empty strings.

    Args:
        tasks: All top-level task dicts in the bundle.

    Returns:
        One :class:`EmptyParameter` per empty widget, sorted for stability.
    """
    empty: list[EmptyParameter] = []
    for task in _walk_tasks(tasks):
        notebook_task = task.get("notebook_task") or {}
        for widget_name, value in (notebook_task.get("base_parameters") or {}).items():
            if isinstance(value, str) and value == "":
                empty.append(EmptyParameter(task_key=task.get("task_key", ""), widget_name=widget_name))
    empty.sort(key=lambda parameter: (parameter.task_key, parameter.widget_name))
    return empty


def collect_network_endpoints(notebooks: list[DabNotebook]) -> list[NetworkEndpoint]:
    """Scans generated notebook content for network-dependent endpoints.

    Args:
        notebooks: All generated notebooks in the bundle.

    Returns:
        Sorted, deduplicated list of :class:`NetworkEndpoint` records.
    """
    seen: set[tuple[str, str]] = set()
    endpoints: list[NetworkEndpoint] = []

    jdbc_scope_re = re.compile(
        r"""dbutils\.secrets\.get\(\s*scope\s*=\s*["']([^"']+)["']\s*,\s*key\s*=\s*["']jdbc-url["']""",
    )
    requests_re = re.compile(r"\brequests\.(?:get|post|put|patch|delete|request)\(")
    https_url_re = re.compile(r"https?://[A-Za-z0-9.\-]+(?::\d+)?/?")
    storage_url_re = re.compile(r"(?:abfss|wasbs)://[A-Za-z0-9_.\-@/]+")

    for notebook in notebooks:
        content = notebook.content
        for scope_name in jdbc_scope_re.findall(content):
            key = ("jdbc", scope_name)
            if key not in seen:
                seen.add(key)
                endpoints.append(
                    NetworkEndpoint(
                        kind="jdbc",
                        target=scope_name,
                        notes=(
                            "JDBC database read by the generated notebook. "
                            "Confirm the workspace has network reach to the database "
                            "(VNet peering, private endpoint, or firewall allowlist) "
                            "before running the job."
                        ),
                    )
                )

        if requests_re.search(content):
            for url in https_url_re.findall(content):
                # Trim placeholders like ``https://example.com/...`` that we
                # emit in fallback bodies — they are not real endpoints.
                if "example.com" in url:
                    continue
                key = ("http", url)
                if key not in seen:
                    seen.add(key)
                    endpoints.append(
                        NetworkEndpoint(
                            kind="http",
                            target=url,
                            notes="HTTP/S endpoint reached via `requests`. Verify outbound HTTPS is allowed.",
                        )
                    )

        for storage_url in storage_url_re.findall(content):
            key = ("storage", storage_url)
            if key not in seen:
                seen.add(key)
                endpoints.append(
                    NetworkEndpoint(
                        kind="storage",
                        target=storage_url,
                        notes="Cloud storage path. Best reached through a Unity Catalog external volume.",
                    )
                )

    endpoints.sort(key=lambda endpoint: (endpoint.kind, endpoint.target))
    return endpoints


def build_prereqs(
    *,
    notebooks: list[DabNotebook],
    tasks: list[dict[str, Any]],
    known_bundle_jobs: set[str],
    cross_bundle_variables: dict[str, str] | None = None,
    compute_notes: list[str] | None = None,
    manual_parameters: list[ManualParameter] | None = None,
) -> Prereqs:
    """Assemble a :class:`Prereqs` from the bundle's generated artifacts.

    Args:
        notebooks: All generated notebooks (including inner-workflow notebooks).
        tasks: All task dicts in the bundle (including inner-workflow tasks).
        known_bundle_jobs: Resource keys for every job defined in this bundle.
        cross_bundle_variables: Map of bundle-variable name → target pipeline
            name for every ExecutePipeline reference the bundler translated
            as ``${var.<name>}``.  The user must supply a numeric job ID for
            each one before running.
        compute_notes: Free-form compute/configuration notes to surface.

    Returns:
        A :class:`Prereqs` aggregating everything the user must do before
        ``databricks bundle run``.
    """
    cross_bundle = [
        CrossBundleReference(task_key=variable_name, target_pipeline=target_pipeline)
        for variable_name, target_pipeline in sorted((cross_bundle_variables or {}).items())
    ]
    # Also fold in any residual ${resources.jobs.X.id} refs we see directly in
    # the tasks (in case upstream still emits them).
    cross_bundle.extend(collect_cross_bundle_refs(tasks, known_bundle_jobs))

    return Prereqs(
        secrets=scan_notebooks_for_secrets(notebooks),
        missing_notebooks=collect_missing_notebooks(notebooks, tasks),
        cross_bundle_refs=cross_bundle,
        empty_parameters=collect_empty_parameters(tasks),
        compute_notes=list(compute_notes or []),
        network_endpoints=collect_network_endpoints(notebooks),
        manual_parameters=list(manual_parameters or []),
    )


def render_setup_md(prereqs: Prereqs, *, bundle_name: str) -> str:
    """Renders a :class:`Prereqs` into a human-readable ``SETUP.md``.

    Args:
        prereqs: Collected dependencies for this bundle.
        bundle_name: Bundle name (used in the header).

    Returns:
        Markdown source as a string.  Always begins with a header so the
        file is consistent even when nothing needs to happen.
    """
    lines: list[str] = [
        f"# Setup for bundle `{bundle_name}`",
        "",
    ]

    if prereqs.is_empty():
        lines.extend(
            [
                "This bundle has no external prerequisites. Deploy it and run the job:",
                "",
                "```bash",
                "databricks bundle validate",
                "databricks bundle deploy -t dev",
                "```",
                "",
            ]
        )
        return "\n".join(lines)

    lines.append(
        "Complete every step below before running the bundle. "
        "Deployment itself is **not** listed — it is the step that comes *after* everything here."
    )
    lines.append("")

    if prereqs.secrets:
        lines.append("## Secret scopes and values")
        lines.append("")
        lines.append(
            "The generated notebooks read credentials via `dbutils.secrets.get(...)`. "
            "You have two equivalent ways to provision the scopes and keys:"
        )
        lines.append("")
        lines.append(
            "**Option A — run `src/setup/create_secrets.py`** in the target workspace. "
            "It creates every scope and populates each key with a `PLACEHOLDER` value "
            "that you then replace with a real credential."
        )
        lines.append("")
        lines.append("**Option B — use the CLI directly:**")
        lines.append("")
        lines.append("```bash")
        for scope_name in sorted(prereqs.secrets):
            lines.append(f"databricks secrets create-scope {scope_name}")
            for key in sorted(prereqs.secrets[scope_name]):
                lines.append(f"databricks secrets put-secret {scope_name} {key}")
        lines.append("```")
        lines.append("")
        lines.append(
            '`put-secret` opens an editor by default; pass `--json \'{"string_value": "…"}\'` '
            "for a non-interactive flow."
        )
        lines.append("")

    if prereqs.missing_notebooks:
        lines.append("## Notebooks to author")
        lines.append("")
        lines.append(
            "The following notebooks are stubs that raise `NotImplementedError`. "
            "Flowx could not download the source (either no workspace path was "
            "supplied in ADF, or the path did not resolve against the "
            "authenticated workspace). Replace each stub with the real logic."
        )
        lines.append("")
        lines.append("| Task | Workspace path (ADF) | Stub in bundle | Widgets available |")
        lines.append("|---|---|---|---|")
        for missing_notebook in prereqs.missing_notebooks:
            source_cell = f"`{missing_notebook.workspace_path}`" if missing_notebook.workspace_path else "*(none)*"
            widgets_cell = (
                ", ".join(f"`{name}`" for name in missing_notebook.widget_names)
                if missing_notebook.widget_names
                else "*(none)*"
            )
            lines.append(
                f"| `{missing_notebook.task_key}` | {source_cell} | `{missing_notebook.bundle_path}` | {widgets_cell} |"
            )
        lines.append("")
        lines.append(
            "If the workspace path exists in a reachable Databricks workspace, you can "
            "have Flowx re-ingest it by running `databricks workspace export` and "
            "placing the result at the indicated bundle path."
        )
        lines.append("")

    if prereqs.cross_bundle_refs:
        lines.append("## Cross-bundle job references")
        lines.append("")
        lines.append(
            "Each row below describes a `run_job_task` that invokes a job **not** "
            "defined in this bundle. Flowx emitted a bundle variable for each "
            "one (`${var.<name>}`) so `databricks bundle validate` passes. "
            "Before running, populate the variable with the numeric job ID the "
            "target pipeline was deployed under — either set a `default:` in "
            '`databricks.yml` or pass `--var "<name>=<job_id>"` at deploy time.'
        )
        lines.append("")
        lines.append("| Variable | Target pipeline |")
        lines.append("|---|---|")
        for ref in prereqs.cross_bundle_refs:
            lines.append(f"| `{ref.task_key}` | `{ref.target_pipeline}` |")
        lines.append("")

    if prereqs.empty_parameters:
        lines.append("## Unresolved task parameters")
        lines.append("")
        lines.append(
            "The translator left the base_parameters below with empty-string "
            "defaults — either the source ADF activity carried a value that "
            "couldn't be resolved, or it depended on pipeline state (variables, "
            "activity outputs) that doesn't cross the bundle boundary.  Review "
            "each entry and either set a real default in the job YAML or pass "
            "a value via `databricks bundle run ... --params '{<widget>:<value>}'`."
        )
        lines.append("")
        lines.append("| Task | Widget |")
        lines.append("|---|---|")
        for empty_parameter in prereqs.empty_parameters:
            lines.append(f"| `{empty_parameter.task_key}` | `{empty_parameter.widget_name}` |")
        lines.append("")

    if prereqs.compute_notes:
        lines.append("## Compute configuration")
        lines.append("")
        for note in prereqs.compute_notes:
            lines.append(f"- {note}")
        lines.append("")

    if prereqs.manual_parameters:
        lines.append("## Existing-notebook parameter handling")
        lines.append("")
        lines.append(
            "The translator could not evaluate the following base_parameters into DAB-compatible "
            "values.  These come from ADF expressions that need runtime context (e.g. `utcnow()`, "
            "`activity().output...`) which DAB does not evaluate.  The tasks below already point "
            "at existing workspace notebooks, so flowx cannot inject the computation; **update "
            "the listed notebooks to compute each value in-line** (the original ADF expression is "
            "shown so you know what runtime value to produce)."
        )
        lines.append("")
        lines.append("| Task | Existing notebook | Widget | Original ADF expression |")
        lines.append("|---|---|---|---|")
        for manual_parameter in prereqs.manual_parameters:
            lines.append(
                f"| `{manual_parameter.task_key}` | `{manual_parameter.notebook_path}` "
                f"| `{manual_parameter.widget_name}` | `{manual_parameter.raw_expression}` |"
            )
        lines.append("")

    if prereqs.network_endpoints:
        lines.append("## Networking")
        lines.append("")
        lines.append(
            "The generated notebooks reach the following endpoints. Confirm the workspace's "
            "network profile permits each one before running the bundle. Private endpoints, "
            "VNet peering, or storage credentials may be required."
        )
        lines.append("")
        lines.append("| Type | Target | Notes |")
        lines.append("|---|---|---|")
        kind_label = {"jdbc": "Database (JDBC)", "http": "HTTP/S", "storage": "Cloud storage"}
        for endpoint in prereqs.network_endpoints:
            label = kind_label.get(endpoint.kind, endpoint.kind)
            lines.append(f"| {label} | `{endpoint.target}` | {endpoint.notes} |")
        lines.append("")

    return "\n".join(lines)
