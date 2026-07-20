"""Airflow DAG parser: parse a DAG file into a flowx Pipeline IR.

Core parser for the Airflow source (``flowx.sources.airflow``).  It reads a DAG
module statically with :mod:`ast` (no Airflow install or DAG execution) and
produces the same :class:`~flowx.models.ir.Pipeline` IR the ADF path emits, so
the shared downstream half -- ``prepare_workflow`` -> ``write_bundle`` -> DABs --
is reused unchanged.  The ``discover`` and ``convert`` phase entry points in
this package wrap :func:`load_airflow_dag`.

Coverage spans the four tiers (per-operator builders live in
:mod:`flowx.sources.airflow.operators`): Tier 1 direct mappings (Python/Bash,
Spark-submit, Databricks provider, SQL, dbt CLI), Tier 2 semantic
(branch/virtualenv, cosmos ``DbtTaskGroup`` -> DbtFactoryActivity, Dummy/Empty
dropped + rewired), Tier 3 sensors (file sensors -> ``file_arrival`` trigger,
time sensors -> schedule), and Tier 4 (unmapped -> PlaceholderActivity).
``>>`` / ``<<`` dependencies and cron ``schedule_interval`` -> Quartz are
handled here.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

from flowx.models.ir import (
    Activity,
    DbtFactoryActivity,
    Dependency,
    ForEachActivity,
    Pipeline,
    SqlActivity,
)
from flowx.sources.airflow import operators as ops
from flowx.sources.airflow import templating


def _sanitize_task_key(name: str) -> str:
    """Converts an Airflow task_id into a valid Databricks task key."""
    import re

    key = re.sub(r"[^a-zA-Z0-9_-]", "_", name)
    key = re.sub(r"_+", "_", key).strip("_")
    return key or "unnamed"


def _cron_to_quartz(cron: str) -> str | None:
    """Converts a 5-field Unix cron to a 6-field Quartz expression.

    Quartz is ``second minute hour day-of-month month day-of-week``; Unix cron
    is ``minute hour day-of-month month day-of-week``. Prepend the seconds
    field and reconcile the day-of-month / day-of-week wildcard (Quartz rejects
    ``*`` in both simultaneously -- one must be ``?``).
    """
    fields = cron.split()
    if len(fields) != 5:
        return None
    minute, hour, dom, month, dow = fields
    if dow == "*" and dom != "*":
        dow = "?"
    elif dom == "*":
        dom = "?"
    return f"0 {minute} {hour} {dom} {month} {dow}"


_CRON_PRESETS: dict[str, str] = {
    "@hourly": "0 0 * * * ?",
    "@daily": "0 0 0 * * ?",
    "@midnight": "0 0 0 * * ?",
    "@weekly": "0 0 0 ? * SUN",
    "@monthly": "0 0 0 1 * ?",
    "@yearly": "0 0 0 1 1 ?",
    "@annually": "0 0 0 1 1 ?",
}


_TIMEDELTA_UNIT_SECONDS: dict[str, int] = {
    "weeks": 604800,
    "days": 86400,
    "hours": 3600,
    "minutes": 60,
    "seconds": 1,
}


def _extract_timezone(node: ast.expr | None) -> str | None:
    """Extracts an IANA timezone from a ``pendulum.timezone("…")`` call or a tz string kwarg.

    Handles ``start_date=datetime(..., tzinfo=pendulum.timezone("Europe/Madrid"))``,
    ``timezone="Europe/Madrid"``, and ``pendulum.timezone("…")`` directly. Returns None
    when no literal timezone is present (caller falls back to UTC).
    """
    if node is None:
        return None
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Call):
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else (func.id if isinstance(func, ast.Name) else "")
        if name in ("timezone", "timezone_") and node.args:
            return ops.literal_str(node.args[0])
        # datetime(..., tzinfo=pendulum.timezone("…")) / tz=...
        for kw in node.keywords:
            if kw.arg in ("tzinfo", "tz"):
                return _extract_timezone(kw.value)
    return None


def _timedelta_to_periodic(node: ast.expr | None) -> dict[str, object] | None:
    """Maps a ``timedelta(...)`` schedule to a ``trigger.periodic`` spec.

    Databricks periodic units are DAYS/HOURS/WEEKS. A timedelta of whole weeks/days/hours
    maps to the largest exact unit; anything finer (minutes/seconds) is expressed as a
    cron in the caller, so this returns None for those.
    """
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    name = func.attr if isinstance(func, ast.Attribute) else (func.id if isinstance(func, ast.Name) else "")
    if name != "timedelta":
        return None
    total = 0
    for kw in node.keywords:
        if kw.arg in _TIMEDELTA_UNIT_SECONDS and isinstance(kw.value, ast.Constant):
            if isinstance(kw.value.value, int):
                total += kw.value.value * _TIMEDELTA_UNIT_SECONDS[kw.arg]
    if total <= 0:
        return None
    for unit, unit_seconds in (("WEEKS", 604800), ("DAYS", 86400), ("HOURS", 3600)):
        if total % unit_seconds == 0:
            return {"kind": "periodic", "interval": total // unit_seconds, "unit": unit, "pause_status": "UNPAUSED"}
    return None


def _schedule_from_interval(
    interval: str | None,
    *,
    node: ast.expr | None = None,
    timezone: str | None = None,
) -> dict[str, object] | None:
    """Builds a Pipeline.schedule spec from an Airflow schedule.

    A string cron / preset -> ``kind: schedule`` (Quartz) with the DAG timezone;
    a ``timedelta(...)`` -> ``kind: periodic``. Returns None when neither applies.
    """
    if interval:
        quartz: str | None = _CRON_PRESETS.get(interval) or _cron_to_quartz(interval)
        if quartz is not None:
            return {
                "kind": "schedule",
                "quartz_cron_expression": quartz,
                "timezone_id": timezone or "UTC",
                "pause_status": "UNPAUSED",
            }
    return _timedelta_to_periodic(node)


class _DagVisitor(ast.NodeVisitor):
    """Collects operator calls, dependency edges, and the DAG's schedule."""

    def __init__(self, module: ast.Module) -> None:
        self._functions: dict[str, ast.FunctionDef] = {
            node.name: node for node in module.body if isinstance(node, ast.FunctionDef)
        }
        # task variable name -> (task_id, operator, kwargs)
        self.operators: dict[str, tuple[str, str, dict[str, ast.expr]]] = {}
        # task variable name -> the operator's ast.Call node (for source-slicing placeholders)
        self.calls: dict[str, ast.Call] = {}
        self.edges: list[tuple[str, str]] = []  # (upstream_var, downstream_var)
        self.dag_id: str | None = None
        self.schedule_interval: str | None = None
        self.schedule_node: ast.expr | None = None
        self.timezone: str | None = None
        self.default_args: dict[str, ast.expr] = {}
        # task variable name -> TaskGroup id prefix (for task-key namespacing)
        self.groups: dict[str, str] = {}
        self._group_stack: list[str] = []
        # task variable names defined via dynamic mapping (.expand()) -> wrapped in a for_each
        self.mapped: set[str] = set()

    def functions(self) -> dict[str, ast.FunctionDef]:
        return self._functions

    def visit_Assign(self, node: ast.Assign) -> None:
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and isinstance(node.value, ast.Call):
            var = node.targets[0].id
            direct = _direct_operator_call(node.value)
            mapped = None if direct is not None else _mapped_operator_call(node.value)
            call = direct or mapped
            if call is not None and isinstance(call.func, ast.Name):
                construct = call.func.id
                kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
                task_id = ops.literal_str(kwargs.get("task_id")) or ops.literal_str(kwargs.get("group_id")) or var
                self.operators[var] = (task_id, construct, kwargs)
                self.calls[var] = call
                if mapped is not None:
                    self.mapped.add(var)
                if self._group_stack:
                    self.groups[var] = "__".join(self._group_stack)
        self.generic_visit(node)

    def visit_With(self, node: ast.With) -> None:
        pushed_group = False
        for item in node.items:
            call = item.context_expr
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Name):
                if call.func.id == "DAG":
                    self._read_dag_kwargs(call)
                elif call.func.id == "TaskGroup":
                    # `with TaskGroup("etl") as tg:` — namespace the member tasks by group id.
                    kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
                    group_id = (
                        ops.literal_str(kwargs.get("group_id"))
                        or (ops.literal_str(call.args[0]) if call.args else None)
                        or "group"
                    )
                    self._group_stack.append(_sanitize_task_key(group_id))
                    pushed_group = True
                elif _is_task_construct(call.func.id) and item.optional_vars is not None:
                    # `with DbtTaskGroup(...) as g:` — a cosmos group bound to a name.
                    if isinstance(item.optional_vars, ast.Name):
                        var = item.optional_vars.id
                        kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
                        task_id = ops.literal_str(kwargs.get("group_id")) or var
                        self.operators[var] = (task_id, call.func.id, kwargs)
                        self.calls[var] = call
        self.generic_visit(node)
        if pushed_group:
            self._group_stack.pop()

    def _read_dag_kwargs(self, call: ast.Call) -> None:
        kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
        self.dag_id = ops.literal_str(kwargs.get("dag_id"))
        self.schedule_node = kwargs.get("schedule_interval") or kwargs.get("schedule")
        self.schedule_interval = ops.literal_str(kwargs.get("schedule_interval")) or ops.literal_str(
            kwargs.get("schedule")
        )
        self.timezone = _extract_timezone(kwargs.get("start_date")) or _extract_timezone(kwargs.get("timezone"))
        # default_args is a dict literal of DAG-wide task settings (retries, timeouts, email).
        default_args = kwargs.get("default_args")
        if isinstance(default_args, ast.Dict):
            self.default_args = {
                key.value: val
                for key, val in zip(default_args.keys, default_args.values)
                if isinstance(key, ast.Constant) and isinstance(key.value, str)
            }

    def visit_Expr(self, node: ast.Expr) -> None:
        # Capture `a >> b >> c` and `a << b` dependency chains.
        if isinstance(node.value, ast.BinOp) and isinstance(node.value.op, (ast.RShift, ast.LShift)):
            self._collect_shift_chain(node.value)
        self.generic_visit(node)

    def _collect_shift_chain(self, binop: ast.BinOp) -> None:
        names = _flatten_shift(binop)
        if not names:
            return
        pairs = zip(names, names[1:])
        for left, right in pairs:
            if isinstance(binop.op, ast.RShift):
                self.edges.append((left, right))
            else:
                self.edges.append((right, left))


def _flatten_shift(node: ast.expr) -> list[str]:
    """Flattens a chain of ``>>`` / ``<<`` Name nodes into an ordered list."""
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
        return _flatten_shift(node.left) + _flatten_shift(node.right)
    return []


def _direct_operator_call(node: ast.Call) -> ast.Call | None:
    """Returns *node* if it is a direct ``SomeOperator(...)`` / ``SomeSensor(...)`` call."""
    if isinstance(node.func, ast.Name) and _is_task_construct(node.func.id):
        return node
    return None


def _mapped_operator_call(node: ast.Call) -> ast.Call | None:
    """Returns the underlying operator call for a dynamic-mapping ``.expand(...)`` chain.

    Handles ``Op(...).expand(...)`` and ``Op.partial(...).expand(...)``. The returned
    Call's keywords are the merged operator kwargs (partial args + expand args), and its
    ``.func`` is the operator Name, so the caller treats it like a direct operator call.
    The mapped kwargs let the loader wrap the operator in a for_each_task.
    """
    if not (isinstance(node.func, ast.Attribute) and node.func.attr == "expand"):
        return None
    inner = node.func.value  # the Op(...) or Op.partial(...) call
    if not isinstance(inner, ast.Call):
        return None
    if isinstance(inner.func, ast.Name) and _is_task_construct(inner.func.id):
        operator_name = inner.func.id  # Op(...).expand(...)
    elif (
        isinstance(inner.func, ast.Attribute)
        and inner.func.attr == "partial"
        and isinstance(inner.func.value, ast.Name)
        and _is_task_construct(inner.func.value.id)
    ):
        operator_name = inner.func.value.id  # Op.partial(...).expand(...)
    else:
        return None
    merged = ast.Call(
        func=ast.Name(id=operator_name, ctx=ast.Load()),
        args=[],
        keywords=list(inner.keywords) + list(node.keywords),
    )
    return merged


def _is_task_construct(name: str) -> bool:
    """True when a call name is an Airflow task-defining construct we should capture.

    Covers operators (``*Operator``), sensors (``*Sensor``), and the cosmos
    constructs (``DbtDag`` / ``DbtTaskGroup``) that don't follow either suffix.
    """
    return name.endswith("Operator") or name.endswith("Sensor") or name in ops.COSMOS_CONSTRUCTS


def load_airflow_dag(dag_path: Path) -> Pipeline:
    """Parses an Airflow DAG file into a flowx Pipeline IR.

    Args:
        dag_path: Path to a ``.py`` DAG module.

    Returns:
        A :class:`~flowx.models.ir.Pipeline`. Mapped operators become their IR
        node (NotebookActivity, SparkPython/JarActivity, RunJobActivity,
        DbtFactoryActivity, ...); Dummy/Empty are dropped with dependency
        rewiring; file sensors lift to a job-level file_arrival trigger; time
        sensors are absorbed into the schedule; unmapped operators become a
        PlaceholderActivity.
    """
    source = Path(dag_path).read_text(encoding="utf-8")
    module = ast.parse(source)
    visitor = _DagVisitor(module)
    visitor.visit(module)
    functions = visitor.functions()

    # Prefix TaskGroup member keys with the group id (e.g. extract__run) so two tasks named
    # `run` in different groups don't collide.
    def _task_key(var: str, task_id: str) -> str:
        key = _sanitize_task_key(task_id)
        return f"{visitor.groups[var]}__{key}" if var in visitor.groups else key

    var_to_task_key = {var: _task_key(var, task_id) for var, (task_id, _, _) in visitor.operators.items()}

    # Build the upstream adjacency in dependency terms, then drop structural nodes
    # (Dummy/Empty, file/time sensors) by rewiring their downstreams to their upstreams.
    upstreams: dict[str, list[str]] = {var: [] for var in visitor.operators}
    for upstream_var, downstream_var in visitor.edges:
        if downstream_var in upstreams and upstream_var in var_to_task_key:
            upstreams[downstream_var].append(upstream_var)

    dropped = {var for var, (_, op, kw) in visitor.operators.items() if _is_dropped_construct(op, kw)}
    upstreams = _rewire_dropped(upstreams, dropped)

    # Lift file/time sensors to a job-level trigger / schedule note (they don't become tasks).
    schedule = _schedule_from_interval(visitor.schedule_interval, node=visitor.schedule_node, timezone=visitor.timezone)
    trigger = _trigger_from_sensors(visitor.operators)
    if trigger is not None and schedule is None:
        schedule = trigger

    # Collapse all dbt CLI operators over the one project into a single DbtFactoryActivity.
    dbt_vars = [var for var, (_, op, _) in visitor.operators.items() if op in ops.DBT_CLI_OPERATORS]

    tasks: list[Activity] = []
    referenced_params: set[str] = set()
    emitted_dbt = False
    for var, (task_id, operator, kwargs) in visitor.operators.items():
        if var in dropped:
            continue
        task_key = var_to_task_key[var]
        outcome = templating.trigger_rule_outcome(kwargs)
        depends_on = [Dependency(task_key=var_to_task_key[u], outcome=outcome) for u in upstreams[var]] or None

        if operator in ops.COSMOS_CONSTRUCTS:
            tasks.append(_build_dbt_factory(task_id, task_key, [kwargs], depends_on))
            continue
        if operator in ops.DBT_CLI_OPERATORS:
            # Emit one factory job for the whole dbt chain, at the first dbt task's position.
            if emitted_dbt:
                continue
            emitted_dbt = True
            dbt_kwargs = [visitor.operators[v][2] for v in dbt_vars]
            tasks.append(_build_dbt_factory(task_id, task_key, dbt_kwargs, depends_on))
            continue

        call_node = visitor.calls.get(var)
        call_source = ast.get_source_segment(source, call_node) or "" if call_node is not None else ""
        ctx = ops.OperatorContext(
            task_id=task_id,
            task_key=task_key,
            operator=operator,
            kwargs=kwargs,
            functions=functions,
            source=source,
            call_source=call_source,
            default_args=visitor.default_args,
        )
        builder = ops.OPERATOR_REGISTRY.get(operator, ops.build_placeholder)
        activity = builder(ctx)
        activity.depends_on = depends_on
        # Stamp DAG/task retry + timeout policy (per-task kwargs override default_args).
        policy = templating.retry_policy(visitor.default_args, kwargs)
        activity.max_retries = policy.get("max_retries")
        activity.timeout_seconds = policy.get("timeout_seconds")
        activity.min_retry_interval_millis = policy.get("min_retry_interval_millis")
        # Convert Airflow Jinja in the activity's parameter fields to DAB refs; collect params.
        referenced_params |= _convert_activity_templates(activity)

        if var in visitor.mapped:
            # Dynamic mapping (.expand()) -> a for_each_task iterating the mapped operator.
            tasks.append(_wrap_in_for_each(activity, task_id, task_key, depends_on, kwargs))
        else:
            tasks.append(activity)

    parameters = [{"name": name} for name in sorted(referenced_params)] or None
    return Pipeline(
        name=visitor.dag_id or Path(dag_path).stem,
        tasks=tasks,
        parameters=parameters,
        schedule=schedule,
        tags={"source": "airflow", "dag_id": visitor.dag_id or ""},
    )


def _wrap_in_for_each(
    activity: Activity,
    task_id: str,
    task_key: str,
    depends_on: list[Dependency] | None,
    kwargs: dict[str, ast.expr],
) -> ForEachActivity:
    """Wraps a dynamically-mapped operator in a ForEachActivity (-> for_each_task).

    Airflow ``.expand(x=[...])`` fans a task out over an iterable. The for_each's
    ``inputs`` is the first list-valued expand kwarg (rendered as a JSON array literal
    when it is a static list; otherwise ``{{job.parameters...}}`` is left for review).
    The mapped operator becomes the single inner activity, re-keyed so it doesn't
    collide with the for_each task key.
    """
    items = "[]"
    for key, node in kwargs.items():
        if key in ("task_id", "group_id"):
            continue
        value = ops.literal_value(node)
        if isinstance(value, list):
            items = json.dumps(value)
            break
    inner = activity
    inner.task_key = f"{task_key}_iteration"
    inner.name = f"{task_id}_iteration"
    inner.depends_on = None
    return ForEachActivity(
        name=task_id,
        task_key=task_key,
        depends_on=depends_on,
        items_expression=items,
        inner_activities=[inner],
    )


def _convert_activity_templates(activity: Activity) -> set[str]:
    """Converts Airflow Jinja in an activity's parameter fields to DAB refs.

    Mutates ``base_parameters`` (NotebookActivity), ``parameters`` (Spark/Sql/RunJob),
    ``job_parameters`` (RunJob), and ``sql`` (SqlActivity) in place, returning the set
    of ``{{job.parameters.X}}`` names referenced so the pipeline can declare them.
    """
    referenced: set[str] = set()
    for attr in ("base_parameters", "job_parameters", "parameters"):
        value = getattr(activity, attr, None)
        if value:
            converted, refs = templating.convert_params(value)
            setattr(activity, attr, converted)
            referenced |= refs
    if isinstance(activity, SqlActivity):
        converted_sql, refs = templating.convert_template(activity.sql)
        activity.sql = converted_sql
        referenced |= refs
    # generated_source was already rewritten (Variable.get -> dbutils.widgets.get); collect the
    # widget names so the pipeline declares them as job parameters.
    generated = getattr(activity, "generated_source", None)
    if isinstance(generated, str):
        referenced |= set(_WIDGET_GET.findall(generated))
    return referenced


_WIDGET_GET = re.compile(r"""dbutils\.widgets\.get\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)""")


def _is_dropped_construct(operator: str, kwargs: dict[str, ast.expr]) -> bool:
    """True for constructs that produce no task (lifted to a trigger/schedule or removed).

    Dummy/Empty and file/time sensors always drop. A table sensor drops only when it
    names a table (it lifts to a table_update trigger); a table/SQL sensor with no
    ``table_name`` is an arbitrary-condition sensor and is kept as a placeholder task
    rather than silently vanishing.
    """
    if operator in ops.DUMMY_OPERATORS or operator in ops.FILE_SENSORS or operator in ops.TIME_SENSORS:
        return True
    if operator in ops.TABLE_SENSORS:
        return ops.literal_str(kwargs.get("table_name")) is not None
    return False


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


def _trigger_from_sensors(operators: dict[str, tuple[str, str, dict[str, ast.expr]]]) -> dict[str, object] | None:
    """Builds a job-level trigger from the first eligible sensor.

    File sensors (S3/GCS/File/HDFS) -> ``trigger.file_arrival``; table sensors with a
    ``table_name`` -> ``trigger.table_update``. File sensors take precedence when both
    are present. Only one trigger is emitted (DABs jobs take one); additional sensors
    are left for MIGRATION_NOTES. Returns None when no eligible sensor is present.
    """
    for _var, (_task_id, operator, kwargs) in operators.items():
        if operator in ops.FILE_SENSORS:
            url = (
                ops.literal_str(kwargs.get("bucket_key"))
                or ops.literal_str(kwargs.get("filepath"))
                or ops.literal_str(kwargs.get("filepath_"))
                or ops.literal_str(kwargs.get("bucket_name"))
                or "<file_arrival_url>"
            )
            return {"kind": "file_arrival", "url": url, "pause_status": "UNPAUSED"}
    for _var, (_task_id, operator, kwargs) in operators.items():
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


def _build_dbt_factory(
    task_id: str,
    task_key: str,
    kwargs_list: list[dict[str, ast.expr]],
    depends_on: list[Dependency] | None,
) -> DbtFactoryActivity:
    """Builds a DbtFactoryActivity from cosmos config or a set of dbt CLI operators.

    Extracts project_dir / profiles_dir / target from cosmos ProjectConfig/ProfileConfig
    args or dbt operator kwargs. render_mode defaults to static (the flowx-native path);
    the manifest is read at package time from project_dir/target/manifest.json.
    """
    project_dir = "."
    profiles_dir = "dbt_profiles"
    target = "dev"
    for kwargs in kwargs_list:
        # dbt CLI operators pass project_dir/target directly as kwargs.
        project_dir = ops.literal_str(kwargs.get("project_dir")) or ops.literal_str(kwargs.get("dir")) or project_dir
        profiles_dir = ops.literal_str(kwargs.get("profiles_dir")) or profiles_dir
        target = ops.literal_str(kwargs.get("target")) or ops.literal_str(kwargs.get("target_name")) or target
        # Cosmos nests config in ProjectConfig(...) / ProfileConfig(...) calls.
        project_dir = _cosmos_project_dir(kwargs.get("project_config")) or project_dir
        target = _cosmos_target(kwargs.get("profile_config")) or target
    return DbtFactoryActivity(
        name=task_id,
        task_key=task_key,
        depends_on=depends_on,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target=target,
        render_mode="static",
    )


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


def discover_dags(source_path: Path) -> list[Path]:
    """Returns the DAG ``.py`` files under *source_path*.

    Accepts either a single ``.py`` file or a directory (scanned recursively).
    Files whose source contains no ``DAG(`` construct are skipped so helper
    modules in a DAGs folder are not mistaken for DAG definitions.
    """
    source_path = Path(source_path)
    candidates = [source_path] if source_path.is_file() else sorted(source_path.rglob("*.py"))
    dags: list[Path] = []
    for candidate in candidates:
        if candidate.suffix != ".py":
            continue
        try:
            text = candidate.read_text(encoding="utf-8")
        except OSError:
            continue
        if "DAG(" in text or "@dag" in text:
            dags.append(candidate)
    return dags


def load_pipelines(source_path: Path, pipeline: str | None = None) -> list[Pipeline]:
    """Loads every DAG under *source_path* into Pipeline IR.

    Args:
        source_path: A DAG ``.py`` file or a directory of them.
        pipeline: When set, keep only the pipeline whose name (dag_id) matches.

    Returns:
        One :class:`~flowx.models.ir.Pipeline` per discovered DAG, filtered to
        *pipeline* when provided.
    """
    pipelines = [load_airflow_dag(dag_path) for dag_path in discover_dags(source_path)]
    if pipeline is not None:
        pipelines = [p for p in pipelines if p.name == pipeline]
    return pipelines


_HOST_PATTERN = re.compile(r"https://([A-Za-z0-9._-]*(?:azuredatabricks\.net|databricks\.com|cloud\.databricks\.com))")


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
