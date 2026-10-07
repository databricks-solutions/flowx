"""Public entry points: load DAGs into Pipeline IR, discover DAG files, detect hosts."""

from __future__ import annotations

import ast
import re
from pathlib import Path

from flowx.models.ir import (
    Pipeline,
    PlaceholderActivity,
    RunJobActivity,
)
from flowx.sources.airflow import audit as source_audit
from flowx.sources.airflow.loader.ast_utils import _expand_top_level_loops
from flowx.sources.airflow.loader.dag_discovery import (
    _failed_dag_declaration_pipeline,
    _module_for_dag,
    _top_level_dag_declarations,
)
from flowx.sources.airflow.loader.lowering import _load_airflow_module
from flowx.utils import normalize_task_key

_HOST_PATTERN = re.compile(r"https://([A-Za-z0-9._-]*(?:azuredatabricks\.net|databricks\.com|cloud\.databricks\.com))")


def load_airflow_dag(dag_path: Path, *, dbt_mode: str = "static") -> Pipeline:
    """Parses the first Airflow DAG in a file into a flowx Pipeline IR."""
    pipelines = load_airflow_dags(dag_path, dbt_mode=dbt_mode)
    if not pipelines:
        raise ValueError(f"No Airflow DAG found in {dag_path}")
    return pipelines[0]


def load_airflow_dags(
    dag_path: Path,
    *,
    dbt_mode: str = "static",
    source_file: str | None = None,
) -> list[Pipeline]:
    """Parses every independently declared Airflow DAG in a Python file."""
    source = Path(dag_path).read_text(encoding="utf-8")
    module = _expand_top_level_loops(ast.parse(source))
    declarations = _top_level_dag_declarations(module)
    pipelines: list[Pipeline] = []
    for declaration in declarations:
        if declaration.unsupported_reason is not None:
            pipelines.append(
                _failed_dag_declaration_pipeline(
                    dag_path,
                    declaration,
                    source_file=source_file or dag_path.name,
                )
            )
            continue
        pipelines.append(
            _load_airflow_module(
                dag_path,
                source,
                _module_for_dag(module, declaration, declarations),
                dbt_mode=dbt_mode,
                target_dag_variable=declaration.target_dag_variable,
                source_file=source_file or dag_path.name,
            )
        )
    return pipelines


def load_pipelines(
    source_path: Path,
    pipeline: str | None = None,
    *,
    dbt_mode: str = "static",
    exclude_dags: set[str] | None = None,
) -> list[Pipeline]:
    """Loads every DAG under *source_path* into Pipeline IR.

    Args:
        source_path: A DAG ``.py`` file or a directory of them.
        pipeline: When set, keep only the pipeline whose name (dag_id) matches.
        dbt_mode: dbt-factory render mode -- ``"static"`` (default) or ``"pydabs"``.

    Returns:
        One :class:`~flowx.models.ir.Pipeline` per discovered DAG, filtered to
        *pipeline* when provided.
    """
    root = source_path if source_path.is_dir() else source_path.parent
    pipelines = [
        loaded
        for dag_path in discover_dags(source_path)
        for loaded in load_airflow_dags(
            dag_path,
            dbt_mode=dbt_mode,
            source_file=source_audit.source_label(dag_path, root),
        )
    ]
    if pipeline is not None:
        pipelines = [p for p in pipelines if p.name == pipeline]
    excluded = set(exclude_dags or ())
    for loaded in pipelines:
        if loaded.name in excluded:
            loaded.migration_status = "excluded"
            count = int(loaded.audit.get("audited_activity_count", 0))
            loaded.audit.update(
                {
                    "deterministic_count": 0,
                    "agentic_count": 0,
                    "failed_count": 0,
                    "excluded_count": count,
                }
            )
    if excluded:
        _replace_excluded_dag_references(pipelines, excluded)
    return pipelines


def _replace_excluded_dag_references(pipelines: list[Pipeline], excluded: set[str]) -> None:
    """Replaces included-to-excluded run-job references with explicit placeholders."""
    excluded_by_key = {normalize_task_key(name): name for name in excluded}
    for pipeline in pipelines:
        if pipeline.migration_status == "excluded":
            continue
        for index, task in enumerate(pipeline.tasks):
            if isinstance(task, RunJobActivity) and task.job_name in excluded_by_key:
                excluded_name = excluded_by_key[task.job_name]
                placeholder = PlaceholderActivity(
                    name=task.name,
                    task_key=task.task_key,
                    depends_on=task.depends_on,
                    original_type="ExcludedDagReference",
                    comment=f"Referenced Airflow DAG {excluded_name!r} was excluded from this migration.",
                    raw_definition={"excluded_dag": excluded_name},
                )
                pipeline.tasks[index] = placeholder
                entry = source_audit.finding(
                    source_file=str(pipeline.audit.get("source_file", "")),
                    code="excluded_dag_reference",
                    severity="gap",
                    message=f"Task {task.task_key!r} references excluded DAG {excluded_name!r}.",
                    details={"task_key": task.task_key, "excluded_dag": excluded_name},
                )
                pipeline.not_translatable.append(entry)
                if pipeline.reconciliation_status != "failed":
                    pipeline.reconciliation_status = "verified_with_gaps"
                pipeline.audit["agentic_count"] = int(pipeline.audit.get("agentic_count", 0)) + 1
                pipeline.audit["deterministic_count"] = max(0, int(pipeline.audit.get("deterministic_count", 0)) - 1)


def discover_dags(source_path: Path) -> list[Path]:
    """Returns the DAG ``.py`` files under *source_path*.

    Accepts either a single ``.py`` file or a directory (scanned recursively).
    Discovery uses the same static declaration model as loading, including
    import aliases and qualified TaskFlow decorators.
    """
    source_path = Path(source_path)
    candidates = [source_path] if source_path.is_file() else sorted(source_path.rglob("*.py"))
    dags: list[Path] = []
    for candidate in candidates:
        if candidate.suffix != ".py":
            continue
        try:
            module = _expand_top_level_loops(ast.parse(candidate.read_text(encoding="utf-8")))
        except (OSError, SyntaxError):
            continue
        if _top_level_dag_declarations(module):
            dags.append(candidate)
    return dags


def detect_hosts(source_path: Path) -> list[str]:
    """Returns Databricks workspace hosts referenced by the DAG files under *source_path*.

    Scans DAG source text for ``https://<workspace>.azuredatabricks.net`` /
    ``.databricks.com`` URLs (e.g. in a DatabricksNotebook/RunNow operator's host or a
    connection default). Returns a sorted, de-duplicated list; empty when none are found.
    """
    hosts: set[str] = set()
    for dag_path in discover_dags(source_path):
        try:
            text = dag_path.read_text(encoding="utf-8")
        except OSError:
            continue
        hosts.update(match.rstrip("/") for match in _HOST_PATTERN.findall(text))
    return sorted(hosts)
