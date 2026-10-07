"""dbt-factory and Cosmos DbtDag/DbtTaskGroup construction."""

from __future__ import annotations

import ast
from typing import Any

from flowx.models.ir import (
    DbtFactoryActivity,
    Dependency,
)
from flowx.sources.airflow import operators as ops


def _build_dbt_factory(
    task_id: str,
    task_key: str,
    kwargs_list: list[dict[str, ast.expr]],
    depends_on: list[Dependency] | None,
    dbt_mode: str = "static",
    operator_types: list[str] | None = None,
) -> DbtFactoryActivity:
    """Builds a DbtFactoryActivity from cosmos config or a set of dbt CLI operators.

    Extracts project_dir / profiles_dir / target from cosmos ProjectConfig/ProfileConfig
    args or dbt operator kwargs. ``dbt_mode`` selects the render mode (static | pydabs);
    the manifest is read at package time from project_dir/target/manifest.json.
    """
    project_dir = "."
    profiles_dir = "dbt_profiles"
    target = "dev"
    manifest_path: str | None = None
    selectors: list[str] = []
    exclude_selectors: list[str] = []
    variables: dict[str, Any] | str | None = None
    full_refresh = False
    for kwargs in kwargs_list:
        # dbt CLI operators pass project_dir/target directly as kwargs.
        project_dir = ops.literal_str(kwargs.get("project_dir")) or ops.literal_str(kwargs.get("dir")) or project_dir
        profiles_dir = ops.literal_str(kwargs.get("profiles_dir")) or profiles_dir
        target = ops.literal_str(kwargs.get("target")) or ops.literal_str(kwargs.get("target_name")) or target
        # Cosmos nests config in ProjectConfig(...) / ProfileConfig(...) calls.
        project_dir = _cosmos_project_dir(kwargs.get("project_config")) or project_dir
        target = _cosmos_target(kwargs.get("profile_config")) or target
        manifest_path = _cosmos_manifest_path(kwargs.get("project_config")) or manifest_path
        selectors.extend(_dbt_selector_list(ops.literal_value(kwargs.get("select") or kwargs.get("models"))))
        exclude_selectors.extend(_dbt_selector_list(ops.literal_value(kwargs.get("exclude"))))
        dbt_variables = ops.literal_value(kwargs.get("vars"))
        if isinstance(dbt_variables, (dict, str)):
            variables = dbt_variables
        full_refresh = full_refresh or ops.literal_value(kwargs.get("full_refresh")) is True
    # The static preparer needs the standard manifest produced under target/ unless Cosmos supplied
    # an explicit manifest path. Without this the child job would be empty.
    if manifest_path is None:
        base = project_dir.rstrip("/") if project_dir not in ("", ".") else "."
        manifest_path = f"{base}/target/manifest.json" if base != "." else "target/manifest.json"
    commands = {
        ops.DBT_OPERATOR_COMMAND[operator] for operator in operator_types or [] if operator in ops.DBT_OPERATOR_COMMAND
    }
    resource_types: set[str] = set()
    for command in commands:
        if command == "build":
            resource_types.update(("model", "seed", "snapshot", "test"))
        elif command == "deps":
            resource_types.add("dependency")
        else:
            resource_types.add({"run": "model", "seed": "seed", "snapshot": "snapshot", "test": "test"}[command])
    if not operator_types or any(operator in ops.COSMOS_CONSTRUCTS for operator in operator_types):
        resource_types.update(("model", "seed", "snapshot", "test"))
    return DbtFactoryActivity(
        name=task_id,
        task_key=task_key,
        depends_on=depends_on,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target=target,
        manifest_path=manifest_path,
        render_mode="pydabs" if dbt_mode == "pydabs" else "static",
        selectors=list(dict.fromkeys(selectors)),
        exclude_selectors=list(dict.fromkeys(exclude_selectors)),
        variables=variables,
        full_refresh=full_refresh,
        resource_types=sorted(resource_types),
    )


def _dbt_selector_list(value: Any) -> list[str]:
    """Returns literal dbt selectors as a normalized string list."""
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        return [selector for selector in value if isinstance(selector, str)]
    return []


def _cosmos_project_dir(node: ast.expr | None) -> str | None:
    """Extracts the dbt project path from a cosmos ``ProjectConfig(...)`` call.

    Accepts the path as the first positional arg or as ``dbt_project_path=`` /
    ``project_dir=``. Returns None when *node* is not such a call.
    """
    if not isinstance(node, ast.Call):
        return None
    if node.args:
        positional = ops.literal_str(node.args[0])
        if positional:
            return positional
    kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
    return ops.literal_str(kwargs.get("dbt_project_path")) or ops.literal_str(kwargs.get("project_dir"))


def _cosmos_target(node: ast.expr | None) -> str | None:
    """Extracts ``target_name`` from a cosmos ``ProfileConfig(...)`` call."""
    if not isinstance(node, ast.Call):
        return None
    kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
    return ops.literal_str(kwargs.get("target_name"))


def _cosmos_manifest_path(node: ast.expr | None) -> str | None:
    """Extracts an explicit ``manifest_path`` from a cosmos ``ProjectConfig(...)`` call, if any."""
    if not isinstance(node, ast.Call):
        return None
    kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
    return ops.literal_str(kwargs.get("manifest_path"))
