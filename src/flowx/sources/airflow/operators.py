"""Airflow operator -> flowx IR builders and the dispatch registry.

Each builder maps one Airflow operator family to an :class:`~flowx.models.ir.Activity`
subclass the flowx bundler can render.  flowx emits ``notebook_task`` /
``spark_python_task`` / ``spark_jar_task`` / ``run_job_task`` / ``condition_task`` /
``for_each_task`` today (no ``sql_task``), so SQL operators map to a NotebookActivity
that runs ``spark.sql(...)`` on cluster/serverless compute -- the cluster-backed path.

Sensors and structural operators (Dummy/Empty) are classified here but handled by
the loader: file sensors lift to a job-level ``file_arrival`` trigger, time sensors
are absorbed into the schedule, and Dummy/Empty are dropped with dependency rewiring.
Operators with no deterministic mapping become a PlaceholderActivity carrying guidance.
"""

from __future__ import annotations

import ast
import shlex
import textwrap
from dataclasses import dataclass, field
from typing import Any, Callable

from flowx.models.ir import (
    Activity,
    NotebookActivity,
    PlaceholderActivity,
    RunJobActivity,
    SparkJarActivity,
    SparkPythonActivity,
    SqlActivity,
)

# --------------------------------------------------------------------------------------
# Operator classification (handled specially by the loader, not via a task builder)
# --------------------------------------------------------------------------------------

# Removed from the graph; downstream dependencies rewired to the dropped node's upstreams.
DUMMY_OPERATORS: frozenset[str] = frozenset({"DummyOperator", "EmptyOperator"})

# Lift to a job-level file_arrival trigger; the sensor task itself is dropped.
FILE_SENSORS: frozenset[str] = frozenset(
    {"S3KeySensor", "GCSObjectExistenceSensor", "FileSensor", "HdfsSensor", "WebHdfsSensor"}
)

# Absorbed into the job schedule (a start-of-DAG delay); dropped with a migration note.
TIME_SENSORS: frozenset[str] = frozenset({"TimeSensor", "TimeDeltaSensor"})

# Lift to a job-level table_update trigger; the sensor task itself is dropped. The table
# name is read from the sensor's table_name kwarg (SQL-condition sensors without one fall
# through to a placeholder so their arbitrary condition isn't silently lost).
TABLE_SENSORS: frozenset[str] = frozenset(
    {"DatabricksPartitionSensor", "DatabricksSqlSensor", "DatabricksSQLStatementsSensor", "SqlSensor"}
)

# dbt CLI operators -> a single DbtFactoryActivity (built by the loader, which collapses
# a seed>>run>>test chain into one factory job).
DBT_CLI_OPERATORS: frozenset[str] = frozenset(
    {
        "DbtOperator",
        "DbtRunOperator",
        "DbtTestOperator",
        "DbtSeedOperator",
        "DbtSnapshotOperator",
        "DbtBuildOperator",
        "DbtDepsOperator",
    }
)

# Cosmos constructs -> DbtFactoryActivity (runtime-rendered, statically unparseable task-by-task).
COSMOS_CONSTRUCTS: frozenset[str] = frozenset({"DbtDag", "DbtTaskGroup"})

# dbt CLI command each dbt operator issues (for the factory's enabled types).
DBT_OPERATOR_COMMAND: dict[str, str] = {
    "DbtRunOperator": "run",
    "DbtTestOperator": "test",
    "DbtSeedOperator": "seed",
    "DbtSnapshotOperator": "snapshot",
    "DbtBuildOperator": "build",
}


# --------------------------------------------------------------------------------------
# AST kwarg extraction helpers
# --------------------------------------------------------------------------------------


def literal_str(node: ast.expr | None) -> str | None:
    """Returns the string value of a constant AST node, else None."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def literal_value(node: ast.expr | None) -> Any:
    """Best-effort evaluation of a literal AST node (str/num/bool/list/dict/None).

    Returns ``None`` when the node is not a compile-time literal (e.g. a name or
    call), so callers treat "not a literal" and "literal None" the same way --
    acceptable for the kwargs we read.
    """
    if node is None:
        return None
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError):
        return None


def callable_name(node: ast.expr | None) -> str | None:
    """Returns the referenced function name for ``python_callable=fn``."""
    if isinstance(node, ast.Name):
        return node.id
    return None


@dataclass(slots=True, kw_only=True)
class OperatorContext:
    """Everything a builder needs to translate one operator call.

    Attributes:
        task_id: The Airflow task_id.
        task_key: Sanitized Databricks task key.
        operator: Operator class name (e.g. ``KubernetesPodOperator``).
        kwargs: The operator call's keyword arguments as AST nodes.
        functions: Module-level functions (for resolving python_callable).
        source: Full module source text.
        call_source: The verbatim source of this operator call, embedded in a
            PlaceholderActivity so the agentic-gap round can reason from it (the
            Airflow analog of ADF's raw ARM JSON).
    """

    task_id: str
    task_key: str
    operator: str
    kwargs: dict[str, ast.expr]
    functions: dict[str, ast.FunctionDef]
    source: str
    call_source: str = ""
    default_args: dict[str, ast.expr] = field(default_factory=dict)


# --------------------------------------------------------------------------------------
# Notebook body generators
# --------------------------------------------------------------------------------------


def _notebook_header(task_id: str, note: str) -> str:
    return f"# Databricks notebook source\n# Migrated from Airflow {note} '{task_id}'.\n\n"


def notebook_from_callable(func: ast.FunctionDef, source: str) -> str:
    """Renders a PythonOperator callable body as a notebook (dedented body statements).

    Airflow ``Variable.get(...)`` / ``BaseHook.get_connection(...)`` calls in the body are
    rewritten to ``dbutils.widgets.get`` / ``dbutils.secrets.get`` so the notebook does not
    reference a nonexistent Airflow metastore at runtime.
    """
    from flowx.sources.airflow import templating

    segments = [ast.get_source_segment(source, stmt) for stmt in func.body]
    body = textwrap.dedent("\n\n".join(seg for seg in segments if seg))
    body, _params, _notes = templating.rewrite_airflow_calls(body)
    return f"# Databricks notebook source\n# Migrated from Airflow PythonOperator '{func.name}'.\n\n{body}\n"


def _sh_notebook(task_id: str, command: str) -> str:
    lines = "".join(f"# MAGIC {line}\n" for line in command.splitlines())
    return _notebook_header(task_id, "BashOperator") + "# MAGIC %sh\n" + lines


# --------------------------------------------------------------------------------------
# spark-submit parsing (BashOperator / SSHOperator wrapping spark-submit)
# --------------------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class _SparkSubmit:
    application: str | None
    java_class: str | None
    app_args: list[str]


def parse_spark_submit(command: str) -> _SparkSubmit | None:
    """Parses a ``spark-submit ...`` command line into its application + args.

    Returns ``None`` when the command is not a spark-submit invocation.
    """
    try:
        tokens = shlex.split(command)
    except ValueError:
        return None
    if "spark-submit" not in tokens:
        return None
    tokens = tokens[tokens.index("spark-submit") + 1 :]

    java_class: str | None = None
    application: str | None = None
    app_args: list[str] = []
    index = 0
    # spark-submit flags that take a value we skip over (cluster-side config, not app args).
    valued_flags = {"--master", "--deploy-mode", "--conf", "--name", "--jars", "--packages", "--files", "--py-files"}
    while index < len(tokens):
        token = tokens[index]
        if token == "--class" and index + 1 < len(tokens):
            java_class = tokens[index + 1]
            index += 2
            continue
        if token in valued_flags and index + 1 < len(tokens):
            index += 2
            continue
        if token.startswith("--"):
            index += 1
            continue
        # First bare token is the application; the rest are application args.
        application = token
        app_args = tokens[index + 1 :]
        break
    return _SparkSubmit(application=application, java_class=java_class, app_args=app_args)


def _spark_activity_from_submit(ctx: OperatorContext, submit: _SparkSubmit, note: str) -> Activity:
    """Builds a Spark JAR/Python activity from a parsed spark-submit."""
    app = submit.application or ""
    if submit.java_class or app.endswith(".jar"):
        activity: Activity = SparkJarActivity(
            name=ctx.task_id,
            task_key=ctx.task_key,
            main_class_name=submit.java_class or "UNKNOWN_MAIN_CLASS",
            parameters=submit.app_args or None,
            libraries=[{"jar": app}] if app else None,
        )
    else:
        activity = SparkPythonActivity(
            name=ctx.task_id,
            task_key=ctx.task_key,
            python_file=app or f"../src/{ctx.task_key}.py",
            parameters=submit.app_args or None,
        )
    return activity


# --------------------------------------------------------------------------------------
# Tier 1 builders
# --------------------------------------------------------------------------------------


def _build_python(ctx: OperatorContext) -> Activity:
    func = ctx.functions.get(callable_name(ctx.kwargs.get("python_callable")) or "")
    generated = notebook_from_callable(func, ctx.source) if func is not None else None
    params = literal_value(ctx.kwargs.get("op_kwargs"))
    return NotebookActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        notebook_path=f"notebooks/{ctx.task_key}.py",
        generated_source=generated,
        base_parameters={k: str(v) for k, v in params.items()} if isinstance(params, dict) else None,
    )


def _build_bash(ctx: OperatorContext) -> Activity:
    command = literal_str(ctx.kwargs.get("bash_command"))
    if command is not None:
        submit = parse_spark_submit(command)
        if submit is not None:
            return _spark_activity_from_submit(ctx, submit, "BashOperator spark-submit")
        return NotebookActivity(
            name=ctx.task_id,
            task_key=ctx.task_key,
            notebook_path=f"notebooks/{ctx.task_key}.py",
            generated_source=_sh_notebook(ctx.task_id, command),
        )
    return _placeholder(ctx, "BashOperator command is not a string literal; supply the command manually.")


def _build_ssh(ctx: OperatorContext) -> Activity:
    command = literal_str(ctx.kwargs.get("command"))
    if command is not None:
        submit = parse_spark_submit(command)
        if submit is not None:
            # The SSH hop is eliminated -- Databricks runs Spark natively.
            return _spark_activity_from_submit(ctx, submit, "SSHOperator spark-submit")
        return NotebookActivity(
            name=ctx.task_id,
            task_key=ctx.task_key,
            notebook_path=f"notebooks/{ctx.task_key}.py",
            generated_source=_sh_notebook(ctx.task_id, command),
        )
    return _placeholder(ctx, "SSHOperator command is not a string literal; supply the command manually.")


def _build_spark_submit(ctx: OperatorContext) -> Activity:
    application = literal_str(ctx.kwargs.get("application")) or ""
    java_class = literal_str(ctx.kwargs.get("java_class")) or literal_str(ctx.kwargs.get("conf"))
    app_args = literal_value(ctx.kwargs.get("application_args"))
    args = [str(a) for a in app_args] if isinstance(app_args, list) else None
    if application.endswith(".jar") or java_class:
        return SparkJarActivity(
            name=ctx.task_id,
            task_key=ctx.task_key,
            main_class_name=java_class or "UNKNOWN_MAIN_CLASS",
            parameters=args,
            libraries=[{"jar": application}] if application else None,
        )
    return SparkPythonActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        python_file=application or f"../src/{ctx.task_key}.py",
        parameters=args,
    )


def _build_databricks_notebook(ctx: OperatorContext) -> Activity:
    path = literal_str(ctx.kwargs.get("notebook_path")) or f"notebooks/{ctx.task_key}.py"
    params = literal_value(ctx.kwargs.get("notebook_params"))
    return NotebookActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        notebook_path=path,
        base_parameters={k: str(v) for k, v in params.items()} if isinstance(params, dict) else None,
    )


def _build_run_now(ctx: OperatorContext) -> Activity:
    job_id = literal_value(ctx.kwargs.get("job_id"))
    params = (
        literal_value(ctx.kwargs.get("notebook_params"))
        or literal_value(ctx.kwargs.get("python_params"))
        or literal_value(ctx.kwargs.get("jar_params"))
    )
    return RunJobActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        job_name=ctx.task_key,
        existing_job_id=str(job_id) if job_id is not None else None,
        job_parameters={k: str(v) for k, v in params.items()} if isinstance(params, dict) else None,
    )


def _build_trigger_dag_run(ctx: OperatorContext) -> Activity:
    target = literal_str(ctx.kwargs.get("trigger_dag_id")) or ctx.task_key
    conf = literal_value(ctx.kwargs.get("conf"))
    return RunJobActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        job_name=_sanitize_job_name(target),
        job_parameters={k: str(v) for k, v in conf.items()} if isinstance(conf, dict) else None,
    )


def _build_databricks_submit_run(ctx: OperatorContext) -> Activity:
    """DatabricksSubmitRunOperator: read the notebook_task path out of the json payload."""
    payload = literal_value(ctx.kwargs.get("json"))
    if isinstance(payload, dict):
        notebook_task = payload.get("notebook_task")
        if isinstance(notebook_task, dict) and notebook_task.get("notebook_path"):
            base = notebook_task.get("base_parameters")
            return NotebookActivity(
                name=ctx.task_id,
                task_key=ctx.task_key,
                notebook_path=str(notebook_task["notebook_path"]),
                base_parameters={k: str(v) for k, v in base.items()} if isinstance(base, dict) else None,
            )
    return _placeholder(
        ctx, "DatabricksSubmitRunOperator json payload could not be read statically; translate the run spec manually."
    )


def _sql_builder(note: str, sql_kwarg: str = "sql") -> Callable[[OperatorContext], Activity]:
    """Factory: build a warehouse-backed SqlActivity (sql_task) from an operator's inline SQL."""

    def build(ctx: OperatorContext) -> Activity:
        sql = literal_str(ctx.kwargs.get(sql_kwarg)) or literal_str(ctx.kwargs.get("hql"))
        if sql is None:
            return _placeholder(ctx, f"{ctx.operator} SQL is not a string literal; extract it manually.")
        return SqlActivity(name=ctx.task_id, task_key=ctx.task_key, sql=sql)

    return build


def _build_copy_into(ctx: OperatorContext) -> Activity:
    table = literal_str(ctx.kwargs.get("table_name")) or "<target_table>"
    location = literal_str(ctx.kwargs.get("file_location")) or "<file_location>"
    file_format = literal_str(ctx.kwargs.get("file_format")) or "CSV"
    sql = f"COPY INTO {table}\nFROM '{location}'\nFILEFORMAT = {file_format}"
    return SqlActivity(name=ctx.task_id, task_key=ctx.task_key, sql=sql)


# --------------------------------------------------------------------------------------
# Tier 2 builders
# --------------------------------------------------------------------------------------


def _build_branch(ctx: OperatorContext) -> Activity:
    # The branch condition lives in a Python callable we can't reduce to left/op/right, so emit
    # the evaluation as a notebook that should set a task value; wiring a condition_task on that
    # value is a manual follow-up (surfaced in the placeholder comment).
    func = ctx.functions.get(callable_name(ctx.kwargs.get("python_callable")) or "")
    generated = notebook_from_callable(func, ctx.source) if func is not None else None
    activity = NotebookActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        notebook_path=f"notebooks/{ctx.task_key}.py",
        generated_source=generated,
    )
    return activity


def _build_virtualenv(ctx: OperatorContext) -> Activity:
    func = ctx.functions.get(callable_name(ctx.kwargs.get("python_callable")) or "")
    requirements = literal_value(ctx.kwargs.get("requirements"))
    body = notebook_from_callable(func, ctx.source) if func is not None else _notebook_header(ctx.task_id, ctx.operator)
    if isinstance(requirements, list) and requirements:
        pip = " ".join(str(r) for r in requirements)
        # Insert a %pip install cell after the notebook-source header.
        header, _, rest = body.partition("\n\n")
        body = f"{header}\n\n# MAGIC %pip install {pip}\n\n{rest}"
    return NotebookActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        notebook_path=f"notebooks/{ctx.task_key}.py",
        generated_source=body,
    )


def _build_email(ctx: OperatorContext) -> Activity:
    return _placeholder(
        ctx,
        "EmailOperator: prefer job-level email_notifications on the job/task instead of a task. "
        "If a mid-DAG email is required, implement it in a notebook (smtplib) or a webhook notification.",
    )


# --------------------------------------------------------------------------------------
# Fallback
# --------------------------------------------------------------------------------------


def _placeholder(ctx: OperatorContext, comment: str) -> Activity:
    # Carry the operator's raw source so the agentic-gap round can reason from it
    # (the Airflow analog of ADF's raw ARM JSON), mirroring the ADF placeholder path.
    raw_definition = {"operator": ctx.operator, "source": ctx.call_source} if ctx.call_source else None
    return PlaceholderActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        original_type=ctx.operator,
        comment=comment,
        raw_definition=raw_definition,
    )


def build_placeholder(ctx: OperatorContext) -> Activity:
    """Tier 4 fallback: an unmapped operator becomes a placeholder notebook with guidance."""
    return _placeholder(
        ctx, f"Airflow operator '{ctx.operator}' has no deterministic flowx mapping; translate manually."
    )


def _sanitize_job_name(name: str) -> str:
    import re

    key = re.sub(r"[^a-zA-Z0-9_-]", "_", name)
    return re.sub(r"_+", "_", key).strip("_") or "job"


# --------------------------------------------------------------------------------------
# Registry: operator name -> builder
# --------------------------------------------------------------------------------------

OPERATOR_REGISTRY: dict[str, Callable[[OperatorContext], Activity]] = {
    # Tier 1
    "PythonOperator": _build_python,
    "BranchPythonOperator": _build_branch,
    "ShortCircuitOperator": _build_branch,
    "BashOperator": _build_bash,
    "SSHOperator": _build_ssh,
    "SparkSubmitOperator": _build_spark_submit,
    "DatabricksSubmitRunOperator": _build_databricks_submit_run,
    "DatabricksSubmitRunDeferrableOperator": _build_databricks_submit_run,
    "DatabricksRunNowOperator": _build_run_now,
    "DatabricksRunNowDeferrableOperator": _build_run_now,
    "DatabricksNotebookOperator": _build_databricks_notebook,
    "DatabricksSqlOperator": _sql_builder("DatabricksSqlOperator"),
    "DatabricksSQLStatementsOperator": _sql_builder("DatabricksSQLStatementsOperator"),
    "DatabricksCopyIntoOperator": _build_copy_into,
    "SQLExecuteQueryOperator": _sql_builder("SQLExecuteQueryOperator"),
    "PostgresOperator": _sql_builder("PostgresOperator"),
    "MySqlOperator": _sql_builder("MySqlOperator"),
    "HiveOperator": _sql_builder("HiveOperator", sql_kwarg="hql"),
    "TriggerDagRunOperator": _build_trigger_dag_run,
    # Tier 2
    "PythonVirtualenvOperator": _build_virtualenv,
    "ExternalPythonOperator": _build_virtualenv,
    "EmailOperator": _build_email,
}
