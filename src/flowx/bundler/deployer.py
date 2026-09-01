"""Ordered deploy of the per-pipeline bundles a multi-pipeline migration produces.

flowx emits **one Databricks Asset Bundle per ADF pipeline**. When pipeline A calls pipeline B via
``ExecutePipeline``, the generated ``run_job_task.job_id`` references B — a job that lives in B's own
bundle. ``_rewrite_cross_bundle_run_job_refs`` (see :mod:`flowx.bundler.dab_writer`) rewrites those
out-of-bundle references to ``${var.<B>}`` and declares a matching bundle variable, so each bundle is
deploy-valid on its own; the operator otherwise has to discover B's numeric job id and pass it by hand.

This module automates that. It **discovers** the bundles under an output directory (no manifest
needed), reads each bundle's job resource keys and its ``${var.<callee>}`` cross-bundle dependencies
straight from the generated YAML, topologically orders them (callees first), and deploys each with
``databricks bundle deploy``. After every deploy it reads the deployed job id from
``databricks bundle summary -o json`` and injects it into callers via ``--var "<callee>=<id>"``.

Numeric ids (not names) are captured and injected, so dev-mode ``[dev <user>]`` job-name prefixes are
irrelevant — this works identically for ``dev`` and ``prod`` targets.

This is a **local CLI** operation: it shells out to the ``databricks`` CLI and needs a real profile.
``databricks bundle deploy`` is not available on Databricks serverless / Genie Code, so it does not
run on the hosted MCP server.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

# The cross-bundle variable form _rewrite_cross_bundle_run_job_refs writes: ${var.<callee_resource_key>}.
_VAR_REF = re.compile(r"\$\{var\.([A-Za-z0-9_]+)\}")


class CycleError(Exception):
    """Raised when the bundle dependency graph contains a cycle (cannot be ordered)."""


class MissingDependencyError(Exception):
    """Raised when a bundle depends on a callee no discovered bundle provides."""


class DiscoveredBundle:
    """One per-pipeline bundle found under the output directory.

    Attributes:
        bundle_dir: Directory name (relative to the output dir), e.g. ``ingest_sales``.
        resource_keys: Job resource keys this bundle defines (parent + inner ForEach jobs).
        depends_on: Callee resource keys referenced via ``${var.<callee>}`` in a ``run_job_task``.
    """

    __slots__ = ("bundle_dir", "resource_keys", "depends_on")

    def __init__(self, bundle_dir: str, resource_keys: set[str], depends_on: set[str]) -> None:
        self.bundle_dir = bundle_dir
        self.resource_keys = resource_keys
        self.depends_on = depends_on


def _iter_run_job_ids(node: Any) -> Any:
    """Yields every ``run_job_task.job_id`` string anywhere in a parsed resource YAML tree."""
    if isinstance(node, dict):
        run_job = node.get("run_job_task")
        if isinstance(run_job, dict) and "job_id" in run_job:
            yield str(run_job["job_id"])
        for value in node.values():
            yield from _iter_run_job_ids(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_run_job_ids(item)


def _read_bundle_dir(bundle_dir: Path, bundle_name: str) -> DiscoveredBundle:
    """Reads one bundle's job resource keys and ``${var.<callee>}`` deps from its ``resources/*.yml``."""
    resource_keys: set[str] = set()
    depends_on: set[str] = set()
    resources_dir = bundle_dir / "resources"
    if resources_dir.is_dir():
        for resource_yml in sorted(resources_dir.glob("*.yml")):
            try:
                doc = yaml.safe_load(resource_yml.read_text()) or {}
            except yaml.YAMLError:
                continue
            jobs = ((doc.get("resources") or {}).get("jobs")) or {}
            resource_keys.update(jobs.keys())
            for job_id in _iter_run_job_ids(doc):
                match = _VAR_REF.fullmatch(job_id)
                if match:
                    depends_on.add(match.group(1))
    return DiscoveredBundle(bundle_name, resource_keys, depends_on)


class AmbiguousLayoutError(Exception):
    """Raised when the output dir holds both a root bundle and subdirectory bundles."""


def _discover_bundles(output_dir: Path) -> list[DiscoveredBundle]:
    """Finds every bundle under *output_dir* and reads its jobs + cross-bundle deps.

    A bundle is any immediate subdirectory containing a ``databricks.yml`` (the ``per-pipeline`` /
    ``per-group`` layout). When *output_dir* itself holds a ``databricks.yml`` (the ``single`` mode /
    single-pipeline layout, where the sole bundle sits at the root), that root bundle is returned
    instead — a single root bundle has no siblings to order against, so it is deployed directly. Its
    ``bundle_dir`` is ``"."`` so ``run`` shells out in *output_dir* itself.

    Raises:
        AmbiguousLayoutError: when *both* a root ``databricks.yml`` and subdirectory bundles are
            present. ``package`` never clears the output dir (it only prunes ``.work/``), so
            re-packaging into the same dir with a different ``--packaging-mode`` leaves both layouts on
            disk. Silently deploying the root bundle would ignore the freshly-written subdirectory
            bundles (or vice versa). Refuse and tell the operator to clear the dir, rather than deploy a
            stale layout.
    """
    subdir_bundles = [
        child for child in sorted(output_dir.iterdir()) if child.is_dir() and (child / "databricks.yml").exists()
    ]

    if (output_dir / "databricks.yml").exists():
        if subdir_bundles:
            names = ", ".join(child.name for child in subdir_bundles)
            raise AmbiguousLayoutError(
                f"{output_dir} holds both a root-level databricks.yml (a 'single'-mode bundle) and "
                f"subdirectory bundle(s): {names}. This usually means the dir was packaged more than "
                "once with different --packaging-mode values (package does not clear the output dir). "
                "Deploying would use only one layout and silently ignore the other. Delete the stale "
                "layout (or re-package into a clean directory) and retry."
            )
        return [_read_bundle_dir(output_dir, ".")]

    return [_read_bundle_dir(child, child.name) for child in subdir_bundles]


def _build_graph(bundles: list[DiscoveredBundle], *, allow_missing_deps: bool = False) -> dict[str, list[str]]:
    """Builds a ``bundle_dir -> [dependency bundle_dirs]`` adjacency map.

    A ``${var.<callee>}`` dependency names a job resource key, which may be a bundle's parent job or an
    inner ForEach job — resolve it to the owning bundle's directory.

    Raises:
        MissingDependencyError: when a callee resource key belongs to no discovered bundle and
            ``allow_missing_deps`` is False.
    """
    key_to_dir: dict[str, str] = {}
    for bundle in bundles:
        for key in bundle.resource_keys:
            key_to_dir[key] = bundle.bundle_dir

    graph: dict[str, list[str]] = {}
    for bundle in bundles:
        deps: list[str] = []
        for callee in sorted(bundle.depends_on):
            owner = key_to_dir.get(callee)
            if owner is None:
                if not allow_missing_deps:
                    raise MissingDependencyError(
                        f"Bundle '{bundle.bundle_dir}' references ${{var.{callee}}}, but no bundle under "
                        "the output directory defines a job named '"
                        f"{callee}'. Pass --allow-missing-deps to deploy anyway (set ${{var.{callee}}} "
                        "manually per SETUP.md)."
                    )
                continue
            if owner != bundle.bundle_dir and owner not in deps:
                deps.append(owner)
        graph[bundle.bundle_dir] = deps
    return graph


def _topo_sort(graph: dict[str, list[str]]) -> list[str]:
    """Returns bundle dirs in dependency-first order (Kahn's algorithm).

    Raises:
        CycleError: when the graph has a cycle.
    """
    in_degree = {node: len(deps) for node, deps in graph.items()}
    dependents: dict[str, list[str]] = {node: [] for node in graph}
    for node, deps in graph.items():
        for dep in deps:
            dependents[dep].append(node)

    queue = sorted(node for node, deg in in_degree.items() if deg == 0)
    ordered: list[str] = []
    while queue:
        node = queue.pop(0)
        ordered.append(node)
        for dependent in sorted(dependents[node]):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)
        queue.sort()

    if len(ordered) != len(graph):
        remaining = sorted(node for node in graph if node not in ordered)
        raise CycleError(
            "Cyclic dependency between bundles: " + ", ".join(remaining) + ". "
            "Cyclic factories cannot be deployed as ordered separate bundles."
        )
    return ordered


def _run_cli(cmd: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    """Runs a CLI command, capturing output. Isolated so tests can monkeypatch it."""
    return subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)


def _capture_job_ids(
    bundle_dir: Path,
    resource_keys: set[str],
    *,
    target: str,
    profile: str | None,
    var_pairs: dict[str, int],
) -> dict[str, int]:
    """Reads deployed job ids for *resource_keys* from ``databricks bundle summary -o json``.

    Args:
        var_pairs: The same ``{"<callee>": <id>}`` cross-bundle vars this bundle was deployed with.
            ``bundle summary`` re-resolves the config and errors on any unset required variable, so
            these must be re-passed.

    Returns:
        ``{"<resource_key>": <numeric id>}`` for every key whose id was found. Keys without a deployed
        id (e.g. a pipeline resource, not a job) are skipped so no empty ``--var`` is ever injected.
    """
    cmd = ["databricks", "bundle", "summary", "-o", "json", "-t", target]
    if profile:
        cmd += ["-p", profile]
    for var_name, job_id in sorted(var_pairs.items()):
        cmd += ["--var", f"{var_name}={job_id}"]
    result = _run_cli(cmd, cwd=bundle_dir)
    if result.returncode != 0:
        print(f"    warning: `bundle summary` failed for {bundle_dir.name}: {result.stderr.strip()}", file=sys.stderr)
        return {}
    try:
        summary = json.loads(result.stdout)
    except json.JSONDecodeError:
        print(f"    warning: could not parse `bundle summary` JSON for {bundle_dir.name}", file=sys.stderr)
        return {}

    jobs = ((summary.get("resources") or {}).get("jobs")) or {}
    captured: dict[str, int] = {}
    for key in resource_keys:
        job: dict[str, Any] = jobs.get(key) or {}
        deployed_id: Any = job.get("id")
        if deployed_id is not None:
            captured[key] = int(deployed_id)
    return captured


def run(
    output_dir: Path,
    *,
    target: str = "dev",
    profile: str | None = None,
    dry_run: bool = False,
    allow_missing_deps: bool = False,
) -> int:
    """Deploys every bundle under *output_dir* in dependency order. Returns a process exit code."""
    output_dir = Path(output_dir)
    if not output_dir.is_dir():
        print(f"Error: output directory not found: {output_dir}", file=sys.stderr)
        return 1

    try:
        bundles = _discover_bundles(output_dir)
    except AmbiguousLayoutError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    if not bundles:
        print(
            f"No bundles found under {output_dir} (looked for immediate subdirectories with a "
            "databricks.yml). Package with the multi-pipeline output first.",
            file=sys.stderr,
        )
        return 1

    by_dir = {bundle.bundle_dir: bundle for bundle in bundles}
    try:
        graph = _build_graph(bundles, allow_missing_deps=allow_missing_deps)
        order = _topo_sort(graph)
    except (CycleError, MissingDependencyError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Deploy order ({len(order)} bundle(s), target={target}): {' -> '.join(order)}")

    # Every captured "<resource_key>": <id> from bundles deployed so far.
    deployed_ids: dict[str, int] = {}

    for position, bundle_dir in enumerate(order, start=1):
        bundle = by_dir[bundle_dir]
        # Only pass the vars this bundle references (its resolved dependencies' job keys).
        needed = {callee: deployed_ids[callee] for callee in sorted(bundle.depends_on) if callee in deployed_ids}

        # Every in-migration callee this bundle depends on was deployed earlier in topological order, so
        # its id should be in deployed_ids. A missing one means the callee's `bundle summary` id-capture
        # failed; deploying now would abort cryptically on an unset required ${var.<callee>}. Fail here
        # with an actionable message instead. (allow_missing_deps skips callees that no bundle provides;
        # those were already dropped from bundle.depends_on's resolved deps in _build_graph.)
        if not dry_run:
            uncaptured = sorted(
                callee
                for callee in bundle.depends_on
                if callee not in deployed_ids and any(callee in b.resource_keys for b in bundles)
            )
            if uncaptured:
                print(f"  [{position}/{len(order)}] {bundle_dir}: BLOCKED", file=sys.stderr)
                print(
                    f"Cannot deploy '{bundle_dir}': its cross-bundle job id(s) {', '.join(uncaptured)} were "
                    "not captured from the callee bundle's `databricks bundle summary` (deploy may have "
                    "succeeded but the summary read failed). Re-run the deploy, or set the "
                    f"${{var.<callee>}} value(s) manually and deploy '{bundle_dir}' with "
                    "`databricks bundle deploy --var <callee>=<job_id>`.",
                    file=sys.stderr,
                )
                return 1

        cmd = ["databricks", "bundle", "deploy", "-t", target]
        if profile:
            cmd += ["-p", profile]
        for var_name, job_id in sorted(needed.items()):
            cmd += ["--var", f"{var_name}={job_id}"]

        if dry_run:
            shown = " ".join(cmd)
            if bundle.depends_on and not needed:
                shown += "  # + --var <callee>=<captured at deploy time>"
            print(f"  [{position}/{len(order)}] {bundle_dir}: {shown}")
            continue

        result = _run_cli(cmd, cwd=output_dir / bundle_dir)
        if result.returncode != 0:
            print(f"  [{position}/{len(order)}] {bundle_dir}: FAILED", file=sys.stderr)
            print(result.stderr.strip(), file=sys.stderr)
            print(f"Stopped: {bundle_dir} failed to deploy; dependent bundles were not deployed.", file=sys.stderr)
            return 1

        captured = _capture_job_ids(
            output_dir / bundle_dir,
            bundle.resource_keys,
            target=target,
            profile=profile,
            var_pairs=needed,
        )
        deployed_ids.update(captured)
        ids_note = ", ".join(f"{k}={v}" for k, v in sorted(captured.items())) or "no job ids captured"
        print(f"  [{position}/{len(order)}] {bundle_dir}: deployed ({ids_note})")

    if dry_run:
        print("\nDry run — no bundles were deployed.")
    else:
        print(f"\nDeployed {len(order)} bundle(s) to target '{target}'.")
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for ordered multi-bundle deploy."""
    parser = argparse.ArgumentParser(
        description="Deploy per-pipeline flowx bundles in dependency order, wiring cross-bundle job ids.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./flowx_output"),
        help="Directory holding the per-pipeline bundle subdirectories.",
    )
    parser.add_argument("--target", type=str, default="dev", help="Bundle target to deploy (default: dev).")
    parser.add_argument("--profile", type=str, default=None, help="Databricks CLI profile for deploy and summary.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the dependency order and deploy commands without deploying.",
    )
    parser.add_argument(
        "--allow-missing-deps",
        action="store_true",
        help=(
            "Order and attempt to deploy even when a bundle references a callee absent from the output "
            "dir. The missing ${var.<callee>} is declared without a default, so that bundle's deploy "
            "still fails until you supply the value manually (edit its databricks.yml default or "
            "`databricks bundle deploy --var <callee>=<job_id>` per SETUP.md); this flag only unblocks "
            "the ordering, not the deploy."
        ),
    )
    args = parser.parse_args(argv)

    return run(
        args.output_dir,
        target=args.target,
        profile=args.profile,
        dry_run=args.dry_run,
        allow_missing_deps=args.allow_missing_deps,
    )


if __name__ == "__main__":
    raise SystemExit(main())
