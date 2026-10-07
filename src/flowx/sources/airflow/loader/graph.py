"""Dependency-graph helpers: group-edge expansion, node rewiring, root-sensor triggers."""

from __future__ import annotations

import ast

from flowx.sources.airflow import operators as ops


def _expand_group_edges(
    edges: list[tuple[str, str]],
    groups: dict[str, str],
    group_vars: dict[str, str],
) -> list[tuple[str, str]]:
    """Rewrites edges whose endpoint is a ``TaskGroup`` var into task-to-task edges.

    A group endpoint expands to its boundary tasks: as an upstream, the group's *leaves* (members
    with no downstream inside the group); as a downstream, the group's *roots* (members with no
    upstream inside the group). Airflow connects leaves(upstream) -> roots(downstream). A non-group
    var resolves to itself. Membership includes nested subgroups (prefix match).
    """
    if not group_vars:
        return edges

    # Group prefix -> member task vars (a member's group prefix equals or nests under the group's).
    def _members(prefix: str) -> list[str]:
        return [var for var, gp in groups.items() if gp == prefix or gp.startswith(prefix + "__")]

    # Intra-group edges decide which members are roots (no in-group upstream) / leaves (no
    # in-group downstream). Edges here are still in var terms.
    def _roots_leaves(prefix: str) -> tuple[list[str], list[str]]:
        members = set(_members(prefix))
        has_in_up = {v: False for v in members}
        has_in_down = {v: False for v in members}
        for up, down in edges:
            if up in members and down in members:
                has_in_down[up] = True
                has_in_up[down] = True
        roots = [v for v in members if not has_in_up[v]]
        leaves = [v for v in members if not has_in_down[v]]
        return roots or list(members), leaves or list(members)

    def _resolve(var: str, *, as_upstream: bool) -> list[str]:
        prefix = group_vars.get(var)
        if prefix is None:
            return [var]
        roots, leaves = _roots_leaves(prefix)
        return leaves if as_upstream else roots

    expanded: list[tuple[str, str]] = []
    for up, down in edges:
        if up not in group_vars and down not in group_vars:
            expanded.append((up, down))
            continue
        for u in _resolve(up, as_upstream=True):
            for d in _resolve(down, as_upstream=False):
                if u != d:
                    expanded.append((u, d))
    return expanded


def _rewire_dropped(upstreams: dict[str, list[str]], dropped: set[str]) -> dict[str, list[str]]:
    """Returns upstream edges with *dropped* vars removed and their edges bridged.

    A downstream of a dropped node inherits the dropped node's (transitive)
    non-dropped upstreams, so the DAG stays connected after Dummy/Empty and
    lifted sensors are removed.
    """

    def resolve(var: str, seen: set[str]) -> list[str]:
        result: list[str] = []
        for up in upstreams.get(var, []):
            if up in dropped:
                if up not in seen:
                    result.extend(resolve(up, seen | {up}))
            else:
                result.append(up)
        # De-dup while preserving order.
        return list(dict.fromkeys(result))

    return {var: resolve(var, {var}) for var in upstreams if var not in dropped}


def _root_trigger_sensor(
    operators: dict[str, tuple[str, str, dict[str, ast.expr]]],
    upstreams: dict[str, list[str]],
    all_task_vars: set[str],
) -> tuple[str, set[str]] | None:
    """Returns a root sensor and its proven descendant set, or None.

    Only a sensor with no upstreams (the DAG's entry gate) can lift to a file_arrival /
    table_update trigger: mid-DAG sensors are ordering gates within the run and must stay
    as tasks. The sensor must reach every non-sensor task; otherwise lifting it would gate
    independent work that Airflow did not gate. File sensors win over table sensors when both
    qualify. A table/SQL sensor lifts only when it names a literal table.
    """
    adjacency: dict[str, set[str]] = {var: set() for var in all_task_vars}
    for downstream, dependencies in upstreams.items():
        for upstream in dependencies:
            adjacency.setdefault(upstream, set()).add(downstream)

    def _descendants(root: str) -> set[str]:
        descendants: set[str] = set()
        stack = list(adjacency.get(root, ()))
        while stack:
            current = stack.pop()
            if current in descendants:
                continue
            descendants.add(current)
            stack.extend(adjacency.get(current, ()))
        return descendants

    sensor_vars = {var for var, (_id, operator, _kwargs) in operators.items() if operator.endswith("Sensor")}
    required = all_task_vars - sensor_vars
    candidates = [
        var
        for var, (_id, operator, _kwargs) in operators.items()
        if operator in ops.FILE_SENSORS and not upstreams.get(var)
    ]
    candidates.extend(
        var
        for var, (_id, operator, kwargs) in operators.items()
        if operator in ops.TABLE_SENSORS
        and not upstreams.get(var)
        and ops.literal_str(kwargs.get("table_name")) is not None
    )
    for candidate in candidates:
        descendants = _descendants(candidate)
        if required <= descendants:
            return candidate, descendants
    return None


def _trigger_from_sensor(operator: str, kwargs: dict[str, ast.expr]) -> dict[str, object] | None:
    """Builds a job-level trigger dict from a single sensor's operator + kwargs.

    File sensors (S3/GCS/File/HDFS) -> ``trigger.file_arrival``; table sensors with a literal
    ``table_name`` -> ``trigger.table_update``. Returns None when the sensor can't lift.
    """
    if operator in ops.FILE_SENSORS:
        url = ops.file_sensor_path(kwargs) or "<file_arrival_url>"
        return {"kind": "file_arrival", "url": url, "pause_status": "UNPAUSED"}
    if operator in ops.TABLE_SENSORS:
        table_name = ops.literal_str(kwargs.get("table_name"))
        if table_name is not None:
            return {
                "kind": "table_update",
                "table_names": [table_name],
                "condition": "ANY_UPDATED",
                "pause_status": "UNPAUSED",
            }
    return None
