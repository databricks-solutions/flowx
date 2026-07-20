"""Read a dbt ``manifest.json`` and explode it into per-node task specs.

This is the deterministic core of dbt-factory mode: it turns the stable
dbt-core manifest artifact into an ordered list of :class:`DbtNode` objects, one
per dbt model / seed / snapshot / test, with the dependency edges between them
pruned to the exploded set.  It performs no I/O beyond reading the manifest file
and needs no dbt install, so it is unit-testable against a synthetic manifest.

Both renderers (static explosion and the PyDABs deploy-time hook) consume the
same :class:`DbtNode` list, so the "one IR node, two renderers" contract holds.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

# dbt resource_types that dbt-factory turns into their own orchestrator task. deps/docs and source
# definitions are not runnable nodes; snapshots/seeds/models/tests are.
_RUNNABLE_RESOURCE_TYPES: frozenset[str] = frozenset({"model", "seed", "snapshot", "test"})

# The dbt command each runnable resource_type maps to (dbt-factory's model->run, seed->seed, etc.).
_RESOURCE_TYPE_TO_COMMAND: dict[str, str] = {
    "model": "run",
    "seed": "seed",
    "snapshot": "snapshot",
    "test": "test",
}

# FQN components go into a `--select fqn:a.b.c` selector; restrict to characters dbt's own selector
# grammar accepts so a crafted node name can't inject extra selector syntax.
_FQN_COMPONENT = re.compile(r"[A-Za-z0-9_.-]+")


@dataclass(slots=True, kw_only=True)
class DbtNode:
    """One runnable dbt node exploded from the manifest.

    Attributes:
        unique_id: dbt manifest unique_id (e.g. ``model.pkg.stg_orders``).
        resource_type: ``model`` / ``seed`` / ``snapshot`` / ``test``.
        name: dbt node name.
        command: dbt subcommand for this node (``run`` / ``seed`` / ...).
        selector: The ``fqn:`` selector that resolves to exactly this node.
        task_key: Databricks task key (``<resource_type>_<name>`` sanitized).
        depends_on: Task keys of upstream exploded nodes (pruned to the set).
    """

    unique_id: str
    resource_type: str
    name: str
    command: str
    selector: str
    task_key: str
    depends_on: list[str] = field(default_factory=list)


def _sanitize_task_key(resource_type: str, name: str) -> str:
    """Builds a Databricks task key from a dbt node's type and name."""
    raw = f"{resource_type}_{name}"
    key = re.sub(r"[^a-zA-Z0-9_-]", "_", raw)
    key = re.sub(r"_+", "_", key).strip("_")
    return key or "dbt_node"


def _fqn_selector(fqn: list[str]) -> str:
    """Builds a ``fqn:`` selector string from a node's fqn components.

    Raises:
        ValueError: When a component contains characters outside dbt's
            selector grammar, so a crafted node name cannot inject extra
            selector syntax into the generated ``--select`` argument.
    """
    for component in fqn:
        if not _FQN_COMPONENT.fullmatch(component):
            raise ValueError(f"Unsafe fqn component {component!r} in {'.'.join(fqn)!r}")
    return "fqn:" + ".".join(fqn)


def load_dbt_nodes(manifest_path: Path) -> list[DbtNode]:
    """Reads a dbt manifest and returns its runnable nodes as task specs.

    Args:
        manifest_path: Path to a dbt ``manifest.json``.

    Returns:
        Ordered list of :class:`DbtNode`, one per runnable node, with
        ``depends_on`` pruned to the exploded set (edges to sources,
        macros, or filtered-out nodes are dropped).

    Raises:
        ValueError: When the test factory would be enabled but the
            manifest carries unit tests (dbt-factory 0.2.1 silently drops
            them), or when a node's fqn contains unsafe characters.
    """
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    return explode_manifest(manifest)


def explode_manifest(manifest: dict) -> list[DbtNode]:
    """Explodes an in-memory dbt manifest dict into runnable task specs.

    Split out from :func:`load_dbt_nodes` so tests can pass a synthetic
    manifest dict without touching the filesystem.
    """
    nodes: dict[str, dict] = manifest.get("nodes", {})

    runnable: dict[str, DbtNode] = {}
    for unique_id, node in nodes.items():
        resource_type = node.get("resource_type", "")
        if resource_type not in _RUNNABLE_RESOURCE_TYPES:
            continue
        fqn = node.get("fqn") or [node.get("name", unique_id)]
        runnable[unique_id] = DbtNode(
            unique_id=unique_id,
            resource_type=resource_type,
            name=node.get("name", unique_id),
            command=_RESOURCE_TYPE_TO_COMMAND[resource_type],
            selector=_fqn_selector(fqn),
            task_key=_sanitize_task_key(resource_type, node.get("name", unique_id)),
        )

    # Fail closed on unit tests: dbt-factory 0.2.1 does not emit unit-test tasks, so a manifest that
    # carries them would silently lose coverage if any test task is exploded.
    if manifest.get("unit_tests") and any(n.resource_type == "test" for n in runnable.values()):
        raise ValueError(
            "Manifest declares unit_tests, which dbt-factory 0.2.1 does not explode into tasks. "
            "Refusing to emit an incomplete dbt job (fail-closed)."
        )

    # Prune dependency edges to the exploded set. dbt nodes depend on sources, macros, and each other;
    # only edges between two runnable nodes become task dependencies.
    task_key_by_uid = {uid: dbt_node.task_key for uid, dbt_node in runnable.items()}
    for uid, dbt_node in runnable.items():
        upstream_uids = nodes[uid].get("depends_on", {}).get("nodes") or []
        dbt_node.depends_on = [task_key_by_uid[up] for up in upstream_uids if up in task_key_by_uid]

    _assert_unique_task_keys(list(runnable.values()))
    # Deterministic order: manifest iteration order is stable, but sort by task_key so the emitted
    # job is byte-identical across runs regardless of dict ordering.
    return sorted(runnable.values(), key=lambda n: n.task_key)


def _assert_unique_task_keys(nodes: list[DbtNode]) -> None:
    """Raises when two distinct dbt nodes sanitize to the same task key."""
    seen: dict[str, str] = {}
    for node in nodes:
        if node.task_key in seen:
            raise ValueError(
                f"dbt nodes {seen[node.task_key]!r} and {node.unique_id!r} collide on task key "
                f"{node.task_key!r}; refusing to emit a job with a duplicate task."
            )
        seen[node.task_key] = node.unique_id
