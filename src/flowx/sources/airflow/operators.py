"""Airflow operator -> flowx IR builders and the dispatch registry.

Each builder maps one Airflow operator family to an :class:`~flowx.models.ir.Activity`
subclass the flowx bundler can render (``notebook_task`` / ``spark_python_task`` /
``spark_jar_task`` / ``sql_task`` / ``run_job_task`` / ``condition_task`` / ``for_each_task``).

Structural operators (Dummy/Empty) are dropped by the loader with dependency rewiring. Time
sensors remain explicit placeholders because a job schedule cannot preserve their per-run wait
semantics. A file or table sensor at the DAG root with no schedule lifts to a job-level ``file_arrival`` /
``table_update`` trigger; otherwise (mid-DAG, or under a schedule) it is retained as a polling
notebook task via :func:`_build_file_sensor` / :func:`_build_table_sensor`. Operators with no
deterministic mapping become a PlaceholderActivity carrying guidance.
"""

from __future__ import annotations

import ast
import json as _json
import shlex
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
from flowx.sources.airflow import callable_notebook
from flowx.utils import normalize_task_key

# --------------------------------------------------------------------------------------
# Operator classification (handled specially by the loader, not via a task builder)
# --------------------------------------------------------------------------------------

# Removed from the graph; downstream dependencies rewired to the dropped node's upstreams.
DUMMY_OPERATORS: frozenset[str] = frozenset({"DummyOperator", "EmptyOperator"})

# File sensors: a root file sensor with no schedule lifts to a job-level file_arrival trigger;
# otherwise it is retained as a dbutils.fs polling task (_build_file_sensor).
FILE_SENSORS: frozenset[str] = frozenset(
    {"S3KeySensor", "GCSObjectExistenceSensor", "FileSensor", "HdfsSensor", "WebHdfsSensor"}
)

# Table/SQL sensors: a root table sensor naming a literal table with no schedule lifts to a
# job-level table_update trigger; otherwise it is retained as a spark.sql polling task
# (_build_table_sensor). A sensor with no literal sql/table_name becomes a placeholder.
TABLE_SENSORS: frozenset[str] = frozenset(
    {"DatabricksPartitionSensor", "DatabricksSqlSensor", "DatabricksSQLStatementsSensor", "SqlSensor"}
)

# dbt CLI operators -> a single DbtFactoryActivity (built by the loader, which collapses
# a seed>>run>>test chain into one factory job).
DBT_CLI_OPERATORS: frozenset[str] = frozenset(
    {
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
    "DbtDepsOperator": "deps",
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


def notebook_from_callable(
    func: ast.FunctionDef, source: str, *, op_args: bool = False, op_kwargs: bool = False
) -> str:
    """Renders a PythonOperator callable as a valid, runnable Databricks notebook.

    Preserves the callable's full ``def`` (early returns stay legal), carries its transitive
    module-level dependencies (helpers / constants / non-Airflow imports), and invokes it with
    ``op_args`` / ``op_kwargs`` read from JSON widgets. Airflow variable access is rewritten.
    """
    return callable_notebook.render(func, source, op_args=op_args, op_kwargs=op_kwargs)


def _sh_notebook(task_id: str, command: str) -> str:
    lines = "".join(f"# MAGIC {line}\n" for line in command.splitlines())
    return _notebook_header(task_id, "BashOperator") + "# MAGIC %sh\n" + lines


# Airflow sensor defaults (seconds): poke every 60s, give up after 7 days.
_DEFAULT_POKE_INTERVAL = 60
_DEFAULT_SENSOR_TIMEOUT = 604800


def _poke_settings(kwargs: dict[str, ast.expr]) -> tuple[int, int]:
    """Reads ``poke_interval`` / ``timeout`` (seconds) from a sensor's kwargs, with Airflow defaults."""
    interval = literal_value(kwargs.get("poke_interval"))
    timeout = literal_value(kwargs.get("timeout"))
    poke = int(interval) if isinstance(interval, (int, float)) and interval > 0 else _DEFAULT_POKE_INTERVAL
    limit = int(timeout) if isinstance(timeout, (int, float)) and timeout > 0 else _DEFAULT_SENSOR_TIMEOUT
    return poke, limit


def _poll_body(operator: str, check_expr: str, description: str, poke: int, timeout: int) -> str:
    """The polling loop for a retained sensor (no notebook header / imports; callers add those).

    ``check_expr`` is a Python expression (evaluated each poke) that returns truthy when the
    awaited condition holds. The loop honours the sensor's poke_interval / timeout and raises on
    expiry so the task fails rather than passing silently.
    """
    return (
        f"POKE_INTERVAL = {poke}  # seconds\n"
        + f"TIMEOUT = {timeout}  # seconds\n"
        + f"DESCRIPTION = {description!r}\n\n"
        + "def _condition_met():\n"
        + f"    # {operator} poke: returns truthy once the awaited condition holds.\n"
        + f"    return {check_expr}\n\n"
        + "deadline = time.monotonic() + TIMEOUT\n"
        + "while not _condition_met():\n"
        + "    if time.monotonic() >= deadline:\n"
        + '        raise TimeoutError(f"Sensor timed out after {TIMEOUT}s waiting for: {DESCRIPTION}")\n'
        + "    time.sleep(POKE_INTERVAL)\n"
        + 'print(f"Condition met: {DESCRIPTION}")\n'
    )


def file_sensor_path(kwargs: dict[str, ast.expr]) -> str | None:
    """Best-effort literal storage path a file sensor waits on (S3/GCS/File/HDFS)."""
    bucket_key = literal_str(kwargs.get("bucket_key"))
    bucket_name = literal_str(kwargs.get("bucket_name"))
    if bucket_key is not None:
        if "://" in bucket_key or bucket_name is None:
            return bucket_key
        return f"s3://{bucket_name}/{bucket_key.lstrip('/')}"
    obj = literal_str(kwargs.get("object"))
    bucket = literal_str(kwargs.get("bucket"))
    if obj is not None and bucket is not None:
        return f"gs://{bucket}/{obj.lstrip('/')}"
    return literal_str(kwargs.get("filepath")) or literal_str(kwargs.get("filepath_"))


def _build_file_sensor(ctx: OperatorContext) -> Activity:
    """A retained file sensor -> a notebook that polls dbutils.fs for the awaited path."""
    path = file_sensor_path(ctx.kwargs)
    if path is None:
        return _placeholder(
            ctx,
            f"{ctx.operator} path is not a string literal; implement the wait (poll dbutils.fs.ls "
            "for the awaited object) manually, or lift it to a file_arrival trigger if it gates the DAG.",
        )
    poke, timeout = _poke_settings(ctx.kwargs)
    header = _notebook_header(ctx.task_id, ctx.operator) + (
        "import time\n\n"
        "def _path_exists(path):\n"
        "    try:\n"
        "        dbutils.fs.ls(path)\n"
        "        return True\n"
        "    except Exception:\n"
        "        return False\n\n"
    )
    loop = _poll_body(ctx.operator, f"_path_exists({path!r})", f"file at {path}", poke, timeout)
    return NotebookActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        notebook_path=f"notebooks/{ctx.task_key}.py",
        generated_source=header + loop,
    )


def _build_table_sensor(ctx: OperatorContext) -> Activity:
    """A retained table/SQL sensor -> a notebook that polls a spark.sql condition."""
    poke, timeout = _poke_settings(ctx.kwargs)
    sql = literal_str(ctx.kwargs.get("sql"))
    table_name = literal_str(ctx.kwargs.get("table_name"))
    if sql is not None:
        # SqlSensor semantics: run the query, take the first row; ready unless there are no rows or
        # the first cell is falsy (0 / "0" / "" / None), matching Airflow's default success criteria.
        check = "_sql_sensor_ready(SENSOR_SQL)"
        header = (
            _notebook_header(ctx.task_id, ctx.operator)
            + "import time\n\n"
            + f"SENSOR_SQL = {sql!r}\n\n"
            + "def _sql_sensor_ready(query):\n"
            + "    rows = spark.sql(query).take(1)\n"
            + "    if not rows:\n"
            + "        return False\n"
            + "    first = rows[0][0]\n"
            + '    return first not in (0, "0", "", None, False)\n\n'
        )
        desc = "SQL sensor condition"
    elif table_name is not None:
        check = f"spark.catalog.tableExists({table_name!r})"
        header = _notebook_header(ctx.task_id, ctx.operator) + "import time\n\n"
        desc = f"table {table_name}"
    else:
        return _placeholder(
            ctx,
            f"{ctx.operator} has no literal sql/table_name; implement the wait (poll spark.sql for the "
            "awaited condition) manually.",
        )
    loop = _poll_body(ctx.operator, check, desc, poke, timeout)
    return NotebookActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        notebook_path=f"notebooks/{ctx.task_key}.py",
        generated_source=header + loop,
    )


def _build_external_task_sensor(ctx: OperatorContext) -> Activity:
    """Routes ExternalTaskSensor to manual translation preserving logical-run semantics."""
    external_dag = literal_str(ctx.kwargs.get("external_dag_id"))
    if external_dag is None:
        return _placeholder(
            ctx,
            f"{ctx.operator} external_dag_id is not a string literal; implement the cross-DAG wait "
            "(poll the upstream job's run state) manually.",
        )
    external_task = literal_str(ctx.kwargs.get("external_task_id"))
    target = f" task '{external_task}'" if external_task else ""
    return _placeholder(
        ctx,
        f"ExternalTaskSensor waits for the matching logical run of DAG '{external_dag}'{target}. Databricks has "
        "no cross-job task dependency primitive; translate this to upstream run_job_task orchestration, a table "
        "update trigger, or a logical-time-aware polling implementation.",
    )


def _build_http_sensor(ctx: OperatorContext) -> Activity:
    """HttpSensor -> a notebook that polls an HTTP endpoint until it returns 2xx."""
    endpoint = literal_str(ctx.kwargs.get("endpoint")) or ""
    if not endpoint:
        return _placeholder(
            ctx,
            f"{ctx.operator} endpoint is not a string literal; implement the HTTP poll manually "
            "(the http_conn_id base URL also needs wiring).",
        )
    if not endpoint.startswith(("http://", "https://")):
        return _placeholder(
            ctx,
            f"{ctx.operator} endpoint '{endpoint}' depends on http_conn_id for its base URL; map the Airflow "
            "connection to a complete URL before generating a polling task.",
        )
    poke, timeout = _poke_settings(ctx.kwargs)
    header = _notebook_header(ctx.task_id, ctx.operator) + (
        "import time\n\n"
        "import requests\n\n"
        f"ENDPOINT = {endpoint!r}\n\n"
        "def _endpoint_ready():\n"
        "    try:\n"
        "        return requests.get(ENDPOINT, timeout=30).ok\n"
        "    except requests.RequestException:\n"
        "        return False\n\n"
    )
    loop = _poll_body(ctx.operator, "_endpoint_ready()", f"HTTP endpoint {endpoint}", poke, timeout)
    return NotebookActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        notebook_path=f"notebooks/{ctx.task_key}.py",
        generated_source=header + loop,
    )


def _build_python_sensor(ctx: OperatorContext) -> Activity:
    """PythonSensor -> a notebook that polls its python_callable until it returns truthy."""
    func = ctx.functions.get(callable_name(ctx.kwargs.get("python_callable")) or "")
    if func is None:
        return _placeholder(
            ctx,
            f"{ctx.operator} python_callable could not be resolved; implement the poll manually.",
        )
    reason = callable_notebook.airflow_runtime_reason(func, ctx.source)
    if reason is not None:
        return _placeholder(
            ctx,
            f"Airflow {ctx.operator} {reason}. flowx has no Airflow runtime to supply it; implement "
            "the poll condition manually.",
        )
    poke, timeout = _poke_settings(ctx.kwargs)
    # Emit the callable's def + deps, then poll its return value (no eager one-shot invocation).
    prelude = callable_notebook.render_definitions(func, ctx.source, note=ctx.operator)
    loop = _poll_body(ctx.operator, f"{func.name}()", f"{func.name}() condition", poke, timeout)
    return NotebookActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        notebook_path=f"notebooks/{ctx.task_key}.py",
        generated_source=prelude + "import time\n\n" + loop,
    )


def _build_datetime_sensor(ctx: OperatorContext) -> Activity:
    """DateTimeSensor -> a notebook that sleeps until a target datetime."""
    target = literal_str(ctx.kwargs.get("target_time"))
    if target is None:
        return _placeholder(
            ctx,
            f"{ctx.operator} target_time is not a string literal; implement the wait-until manually.",
        )
    _poke, timeout = _poke_settings(ctx.kwargs)
    header = _notebook_header(ctx.task_id, ctx.operator) + (
        "import time\n"
        "from datetime import datetime, timezone\n\n"
        f"TARGET_TIME = {target!r}\n\n"
        "def _target_reached():\n"
        "    target = datetime.fromisoformat(TARGET_TIME)\n"
        "    now = datetime.now(target.tzinfo or timezone.utc)\n"
        "    return now >= target\n\n"
    )
    loop = _poll_body(ctx.operator, "_target_reached()", f"datetime {target}", 60, timeout)
    return NotebookActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        notebook_path=f"notebooks/{ctx.task_key}.py",
        generated_source=header + loop,
    )


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
    # A callable that reads Airflow task context (**context / ti) or XCom can't run as a plain
    # notebook -- route it to the agentic-gap round instead of emitting code that fails at runtime.
    if func is not None:
        reason = callable_notebook.airflow_runtime_reason(func, ctx.source)
        if reason is not None:
            return _placeholder(
                ctx,
                f"Airflow {ctx.operator} {reason}. flowx has no Airflow runtime to supply it; "
                "translate manually -- pass upstream data via job parameters or map XCom to "
                "dbutils.jobs.taskValues (set in the producer, get in the consumer).",
            )
    op_kwargs_node = ctx.kwargs.get("op_kwargs")
    op_args_node = ctx.kwargs.get("op_args")
    op_kwargs = literal_value(op_kwargs_node)
    op_args = literal_value(op_args_node)
    if op_kwargs_node is not None and not isinstance(op_kwargs, dict):
        return _placeholder(ctx, "PythonOperator op_kwargs is not a static dictionary; bind its arguments manually.")
    if op_args_node is not None and not isinstance(op_args, (list, tuple)):
        return _placeholder(ctx, "PythonOperator op_args is not a static sequence; bind its arguments manually.")
    if isinstance(op_args, tuple):
        op_args = list(op_args)
    has_kwargs = isinstance(op_kwargs, dict)
    has_args = isinstance(op_args, list)
    generated = (
        notebook_from_callable(func, ctx.source, op_args=has_args, op_kwargs=has_kwargs) if func is not None else None
    )
    # op_args/op_kwargs pass as JSON widgets so lists/numbers/nested objects survive; the notebook
    # json.loads() them and splats into the call.
    base_parameters: dict[str, str] = {}
    if has_args:
        base_parameters["__flowx_op_args"] = _json.dumps(op_args)
    if has_kwargs:
        base_parameters["__flowx_op_kwargs"] = _json.dumps(op_kwargs)
    return NotebookActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        notebook_path=f"notebooks/{ctx.task_key}.py",
        generated_source=generated,
        base_parameters=base_parameters or None,
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
    # job_name becomes ${resources.jobs.<job_name>.id}; it must match the target DAG's job resource
    # key, which write_bundle derives with normalize_task_key(dag_id). Using the same sanitizer keeps
    # a cross-DAG TriggerDagRunOperator ref resolvable for hyphenated / mixed-case dag_ids.
    return RunJobActivity(
        name=ctx.task_id,
        task_key=ctx.task_key,
        job_name=normalize_task_key(target),
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
    # Airflow Branch/ShortCircuit gate *sibling* tasks on a Python callable's return, which flowx
    # can't statically lower to a condition_task's left/op/right plus per-branch true/false outcome
    # wiring. Emitting it as an ordinary notebook would silently let every downstream branch run, so
    # route it to the agentic-gap round with the callable source instead.
    return _placeholder(
        ctx,
        f"Airflow {ctx.operator} selects downstream tasks at runtime. Translate to a Databricks "
        "condition_task (or a task that sets a task value read by a condition_task) and gate each "
        "downstream branch with a true/false outcome dependency; do NOT run all branches.",
    )


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


def build_placeholder_with_comment(ctx: OperatorContext, comment: str) -> Activity:
    """Builds a placeholder carrying a caller-supplied migration explanation."""
    return _placeholder(ctx, comment)


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

# File/table sensors retained as tasks (mid-DAG, or under a schedule) poll for their condition. Root
# instances without a schedule are lifted to a job trigger by the loader before dispatch reaches here.
OPERATOR_REGISTRY.update({name: _build_file_sensor for name in FILE_SENSORS})
OPERATOR_REGISTRY.update({name: _build_table_sensor for name in TABLE_SENSORS})

# Sensors that always become polling tasks (never triggers): cross-DAG, HTTP, arbitrary-callable,
# and wait-until-datetime.
OPERATOR_REGISTRY.update(
    {
        "ExternalTaskSensor": _build_external_task_sensor,
        "ExternalTaskSensorAsync": _build_external_task_sensor,
        "HttpSensor": _build_http_sensor,
        "HttpSensorAsync": _build_http_sensor,
        "PythonSensor": _build_python_sensor,
        "DateTimeSensor": _build_datetime_sensor,
        "DateTimeSensorAsync": _build_datetime_sensor,
    }
)
