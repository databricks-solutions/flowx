"""Shared helpers used by every activity preparer."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

from flowx.bundler.constants import DEFAULT_JOB_CLUSTER_KEY
from flowx.models.dab import DabNotebook, SecretInstruction
from flowx.models.ir import TranslationContext
from flowx.parser.expression_parser import resolve_expression, resolve_interpolated_string
from flowx.preparer.workflow_preparer import build_common_task_fields

if TYPE_CHECKING:
    from flowx.models.ir import Activity


def resolve_param_value(value: str) -> str:
    """Resolves an ADF expression in a parameter value to its DAB form."""
    if "@{" in value:
        return resolve_interpolated_string(value, TranslationContext())
    if value.startswith("@"):
        result = resolve_expression(value, TranslationContext())
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value
    return value


def build_notebook_task_artifacts(
    *,
    notebook_relative_path: str,
    notebook_content: str,
    base_parameters: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[DabNotebook]]:
    """Builds the ``notebook_task`` dict and the matching DabNotebook artifact."""
    notebook_task: dict[str, Any] = {
        "notebook_path": f"../src/{notebook_relative_path}",
    }
    if base_parameters is not None:
        notebook_task["base_parameters"] = base_parameters

    notebooks = [
        DabNotebook(
            relative_path=notebook_relative_path,
            content=notebook_content,
        )
    ]
    return notebook_task, notebooks


def build_notebook_activity_task(
    activity: Activity,
    *,
    notebook_relative_path: str,
    notebook_content: str,
    base_parameters: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[DabNotebook]]:
    """Builds the common task fields and notebook_task scaffolding for *activity*."""
    task = build_common_task_fields(activity)
    notebook_task, notebooks = build_notebook_task_artifacts(
        notebook_relative_path=notebook_relative_path,
        notebook_content=notebook_content,
        base_parameters=base_parameters,
    )
    task["notebook_task"] = notebook_task
    return task, notebooks


def make_jdbc_secrets(
    *,
    scope_name: str,
    source_type: str,
    activity_name: str,
    role: str = "source",
) -> list[SecretInstruction]:
    """Returns the ``jdbc-url`` / ``jdbc-password`` secret pair for a JDBC connector."""
    return [
        SecretInstruction(
            scope=scope_name,
            key="jdbc-url",
            value_source=f"JDBC URL for {source_type} {role} in activity '{activity_name}'",
        ),
        SecretInstruction(
            scope=scope_name,
            key="jdbc-password",
            value_source=f"JDBC password for {source_type} {role} in activity '{activity_name}'",
        ),
    ]


def bind_requested_job_cluster(task: dict[str, Any], activity: Activity) -> None:
    """Binds *task* to the default job cluster when its source asked for dedicated Jobs compute.

    A source front-end requests this by setting ``_bind_default_cluster`` in the activity's cluster
    hint. The job has one shared default cluster, whose worker count and Spark configuration are the
    most common values across the tasks' hints, so a task whose hint differs runs on those values.
    """
    if (activity.cluster or {}).get("_bind_default_cluster") and not task.get("existing_cluster_id"):
        task["job_cluster_key"] = DEFAULT_JOB_CLUSTER_KEY


def bind_spec_job_cluster(task: dict[str, Any], activity: Activity) -> None:
    """Binds *task* to a job cluster built from its own spec, sharing it with tasks whose spec is identical.

    A source front-end that knows each task's exact compute (an Oozie spark action's spark-submit options)
    sets ``_job_cluster_spec`` in the activity's cluster hint. Keying by a hash of the spec keeps identical
    tasks on one cluster without flattening different ones onto the most common settings.
    """
    spec = (activity.cluster or {}).get("_job_cluster_spec")
    if not spec or task.get("existing_cluster_id"):
        return
    digest = hashlib.sha1(json.dumps(spec, sort_keys=True).encode("utf-8")).hexdigest()[:10]
    task["job_cluster_key"] = f"spec_{digest}"
    task["_job_cluster_spec"] = spec


def is_remote_artifact_uri(path: str) -> bool:
    """Returns True for a cloud-storage URI such as ``gs://`` or ``s3://`` that a job reads in place."""
    scheme, separator, _ = path.partition("://")
    return bool(separator) and scheme.lower() not in ("dbfs", "file")
