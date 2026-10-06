"""Lower one isolated DAG declaration into a flowx Pipeline IR."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

from flowx.models.ir import (
    Activity,
    Dependency,
    NotebookActivity,
    Pipeline,
    PlaceholderActivity,
)
from flowx.sources.airflow import audit as source_audit
from flowx.sources.airflow import operators as ops
from flowx.sources.airflow import templating
from flowx.sources.airflow.loader import reconcile
from flowx.sources.airflow.loader.activity_templates import (
    _convert_activity_templates,
    _declared_param_default,
    _unresolved_activity_templates,
)
from flowx.sources.airflow.loader.ast_utils import _sanitize_task_key, _span
from flowx.sources.airflow.loader.dbt import _build_dbt_factory
from flowx.sources.airflow.loader.graph import (
    _expand_group_edges,
    _rewire_dropped,
    _root_trigger_sensor,
    _trigger_from_sensor,
)
from flowx.sources.airflow.loader.policy import _job_email_notifications, _job_timeout_seconds
from flowx.sources.airflow.loader.reconcile import _iter_placeholders, _semantic_finding
from flowx.sources.airflow.loader.schedule import _asset_schedule_from_node, _schedule_from_interval
from flowx.sources.airflow.loader.taskflow import _build_taskflow_task, _wrap_in_for_each, _wrap_taskflow_in_for_each
from flowx.sources.airflow.loader.visitor import _DagVisitor

_DATABRICKS_JOB_TAG_LIMIT = 25


def _load_airflow_module(
    dag_path: Path,
    source: str,
    module: ast.Module,
    *,
    dbt_mode: str = "static",
    target_dag_variable: str | None = None,
    source_file: str | None = None,
) -> Pipeline:
    """Parses one isolated DAG declaration into a flowx Pipeline IR.

    Args:
        dag_path: Path to a ``.py`` DAG module.
        dbt_mode: dbt-factory render mode for any dbt workload -- ``"static"`` (default,
            an inner job of per-node tasks) or ``"pydabs"`` (a deploy-time hook module).

    Returns:
        A :class:`~flowx.models.ir.Pipeline`. Mapped operators become their IR
        node (NotebookActivity, SparkPython/JarActivity, RunJobActivity,
        DbtFactoryActivity, ...); Dummy/Empty are dropped with dependency
        rewiring; file sensors lift to a job-level file_arrival trigger; time
        sensors remain explicit placeholders; unmapped operators become a
        PlaceholderActivity.
    """
    audit = source_audit.audit_module(module, target_dag_variable=target_dag_variable)
    visitor = _DagVisitor(module, target_dag_variable=target_dag_variable)
    visitor.visit(module)
    functions = visitor.functions()

    # Prefix TaskGroup member keys with the group id (e.g. extract__run) so two tasks named
    # `run` in different groups don't collide.
    def _task_key(var: str, task_id: str) -> str:
        key = _sanitize_task_key(task_id)
        return f"{visitor.groups[var]}__{key}" if var in visitor.groups else key

    # TaskFlow @task instances share the task table with classic operators (both are just tasks with
    # a task_key and dependency edges downstream).
    var_task_ids: dict[str, str] = {var: tid for var, (tid, _, _) in visitor.operators.items()}
    var_task_ids.update({var: tf.task_id for var, tf in visitor.taskflow_tasks.items()})
    var_task_ids.update({var: task_id for var, (task_id, _, _) in visitor.taskgroup_calls.items()})
    var_to_task_key: dict[str, str] = {}
    used_task_keys: set[str] = set()
    for var, task_id in var_task_ids.items():
        base = _task_key(var, task_id)
        candidate = base
        suffix = 2
        while candidate in used_task_keys:
            candidate = f"{base}__{suffix}"
            suffix += 1
        used_task_keys.add(candidate)
        var_to_task_key[var] = candidate

    # Expand group-level edges (`group_a >> group_b`, `task >> group`, ...) into edges between the
    # groups' boundary tasks: leaves of the upstream group -> roots of the downstream group, matching
    # Airflow's TaskGroup dependency semantics. A non-group var resolves to itself.
    edges = _expand_group_edges(visitor.edges, visitor.groups, visitor.group_vars)

    # Build the upstream adjacency in dependency terms, then drop structural nodes
    # (Dummy/Empty and lifted root sensors) by rewiring their downstreams to their upstreams.
    upstreams: dict[str, list[str]] = {var: [] for var in var_task_ids}
    for upstream_var, downstream_var in edges:
        if downstream_var in upstreams and upstream_var in var_to_task_key:
            upstreams[downstream_var].append(upstream_var)

    # Sensor / schedule precedence. Airflow semantics are "run on schedule, THEN wait for data",
    # and Databricks treats schedule / file_arrival / table_update as mutually-exclusive job trigger
    # types -- so a data sensor lifts to a file_arrival/table_update *trigger* only when it stands at
    # the DAG root AND no cron/timedelta schedule is present. With a schedule (cron AND-THEN wait) or
    # mid-DAG (an ordering gate, not the DAG's entry condition), the sensor is retained as a polling
    # task instead of being silently dropped.
    schedule = _schedule_from_interval(visitor.schedule_interval, node=visitor.schedule_node, timezone=visitor.timezone)
    schedule_proof: dict[str, Any] | None = None
    schedule_node = visitor.schedule_node
    explicit_none_schedule = isinstance(schedule_node, ast.Constant) and schedule_node.value is None
    if schedule is None and schedule_node is not None and not explicit_none_schedule:
        schedule, schedule_gap = _asset_schedule_from_node(schedule_node, visitor._aliases, visitor.asset_definitions)
        if schedule is not None:
            schedule_span = _span(schedule_node)
            table_names = schedule["table_names"]
            condition = schedule["condition"]
            assert isinstance(table_names, list)
            assert isinstance(condition, str)
            schedule_proof = {
                "code": "asset_schedule_lowered",
                "table_names": list(table_names),
                "condition": condition,
                "source_span": {
                    "line": schedule_span.line,
                    "column": schedule_span.column,
                    "end_line": schedule_span.end_line,
                    "end_column": schedule_span.end_column,
                },
            }
        elif schedule_gap is not None:
            visitor.unresolved_constructs.append((schedule_gap, schedule_node))
    has_schedule = schedule is not None

    # Dummy/Empty operators are structural and can be removed after dependency rewiring.
    dropped = {var for var, (_, op, _) in visitor.operators.items() if op in ops.DUMMY_OPERATORS}
    sensor_lift_proof: dict[str, Any] | None = None
    if not has_schedule:
        trigger_candidate = _root_trigger_sensor(visitor.operators, upstreams, set(var_task_ids))
        if trigger_candidate is not None:
            trigger_var, covered_tasks = trigger_candidate
            trigger = _trigger_from_sensor(*visitor.operators[trigger_var][1:])
            if trigger is not None:
                schedule = trigger
                dropped.add(trigger_var)
                sensor_lift_proof = {
                    "code": "sensor_lift_dominates_dag",
                    "capture_id": trigger_var,
                    "task_key": var_to_task_key[trigger_var],
                    "covered_capture_ids": sorted(covered_tasks),
                }
    upstreams = _rewire_dropped(upstreams, dropped)

    # Collapse all dbt CLI operators over the one project into a single DbtFactoryActivity emitted at
    # the first dbt task's position. Every dbt var's task_key remaps to that single key, so a
    # downstream task that depended on a later dbt op (e.g. `dbt_test`) points at the factory task
    # rather than a task_key that was never emitted (which would dangle).
    dbt_vars = [var for var, (_, op, _) in visitor.operators.items() if op in ops.DBT_CLI_OPERATORS]
    dbt_var_set = set(dbt_vars)
    dbt_factory_key = var_to_task_key[dbt_vars[0]] if dbt_vars else None
    dbt_key_remap = {var_to_task_key[v]: dbt_factory_key for v in dbt_vars} if dbt_factory_key else {}

    # Non-dbt tasks reachable *downstream* from the collapsed dbt set. Because every dbt op folds into
    # one factory task, a task that sat between two dbt ops (e.g. `dbt_seed >> task_b >> dbt_run`) is
    # downstream of the factory; the factory therefore cannot depend on it without forming a cycle,
    # but it must still depend on the factory and gate whatever followed it.
    downstream_of_factory: set[str] = set()
    if dbt_factory_key:
        adjacency: dict[str, list[str]] = {v: [] for v in var_task_ids}
        for downstream_var, ups in upstreams.items():
            for upstream_var in ups:
                adjacency.setdefault(upstream_var, []).append(downstream_var)
        stack = list(dbt_vars)
        seen_ds: set[str] = set(dbt_vars)
        while stack:
            for nxt in adjacency.get(stack.pop(), []):
                if nxt not in seen_ds:
                    seen_ds.add(nxt)
                    stack.append(nxt)
        downstream_of_factory = {var_to_task_key[v] for v in seen_ds if v not in dbt_var_set}

    def _sandwiched_before(dbt_var: str) -> set[str]:
        """Non-dbt tasks that fed *dbt_var* (through the collapsed dbt chain) and sit downstream of the
        factory. A task consuming a later dbt op must still wait for these, since the collapse drops
        the intermediate dbt op they fed."""
        result: set[str] = set()
        for upstream_var in upstreams.get(dbt_var, []):
            if upstream_var in dbt_var_set:
                result |= _sandwiched_before(upstream_var)
            elif var_to_task_key[upstream_var] in downstream_of_factory:
                result.add(var_to_task_key[upstream_var])
        return result

    # The factory absorbs every dbt op's external (non-dbt) upstream that is not itself downstream of
    # the factory -- not just the first dbt op's, so a later dbt op's upstream is not silently dropped.
    factory_dep_keys: set[str] = set()
    for dbt_var in dbt_vars:
        for upstream_var in upstreams.get(dbt_var, []):
            if upstream_var in dbt_var_set:
                continue
            key = var_to_task_key[upstream_var]
            if key not in downstream_of_factory:
                factory_dep_keys.add(key)

    def _dep(upstream_var: str, outcome: str | None) -> str:
        key = var_to_task_key[upstream_var]
        return dbt_key_remap.get(key, key)

    tasks: list[Activity] = []
    placeholder_capture_ids: dict[int, str] = {}
    helper_expansion_ids = {str(item["capture_id"]) for item in visitor.helper_expansions}

    def append_task(activity: Activity, capture_id: str) -> None:
        for placeholder in _iter_placeholders([activity]):
            placeholder_capture_ids[id(placeholder)] = capture_id
        tasks.append(activity)

    semantic_findings: list[dict[str, Any]] = []
    argument_proofs = [
        {
            "code": "operator_arguments_classified",
            "capture_id": var,
            "task_key": var_to_task_key[var],
            "operator": operator,
            "arguments": ops.argument_classification(operator, kwargs),
        }
        for var, (_task_id, operator, kwargs) in visitor.operators.items()
    ]
    referenced_params: set[str] = set()
    emitted_dbt = False
    for var, (task_id, operator, kwargs) in visitor.operators.items():
        if var in dropped:
            continue
        task_key = var_to_task_key[var]
        trigger_mapping = templating.trigger_rule_mapping(kwargs)
        outcome = trigger_mapping.outcome
        # Remap dbt-chain upstreams to the single factory key and drop self-edges (a dbt op
        # depending on another dbt op in the same collapsed chain).
        dep_keys = {_dep(u, outcome) for u in upstreams[var]}
        # A task consuming a later dbt op must also wait for any non-dbt task that sat between two dbt
        # ops (the collapse folds away the intermediate dbt op that carried that ordering).
        for upstream_var in upstreams[var]:
            if upstream_var in dbt_var_set:
                dep_keys |= _sandwiched_before(upstream_var)
        dep_keys.discard(task_key if operator not in ops.DBT_CLI_OPERATORS else dbt_factory_key)
        depends_on = [Dependency(task_key=k, outcome=outcome) for k in sorted(dep_keys)] or None

        if operator in ops.COSMOS_CONSTRUCTS:
            append_task(
                _build_dbt_factory(task_id, task_key, [kwargs], depends_on, dbt_mode, operator_types=[operator]),
                var,
            )
            continue
        if operator in ops.DBT_CLI_OPERATORS:
            # Emit one factory job for the whole dbt chain, at the first dbt task's position.
            if emitted_dbt:
                continue
            emitted_dbt = True
            # The factory gates on every dbt op's external upstreams (not just the first op's), minus
            # any that are downstream of the factory itself (a sandwiched task, which would cycle).
            factory_depends_on = [Dependency(task_key=k, outcome=outcome) for k in sorted(factory_dep_keys)] or None
            dbt_kwargs = [visitor.operators[v][2] for v in dbt_vars]
            append_task(
                _build_dbt_factory(
                    task_id,
                    task_key,
                    dbt_kwargs,
                    factory_depends_on,
                    dbt_mode,
                    operator_types=[visitor.operators[dbt_var][1] for dbt_var in dbt_vars],
                ),
                var,
            )
            continue

        call_node = visitor.calls.get(var)
        if call_node is None:
            call_source = ""
        elif var in helper_expansion_ids:
            call_source = ast.unparse(call_node)
        else:
            call_source = ast.get_source_segment(source, call_node) or ""
        ctx = ops.OperatorContext(
            task_id=task_id,
            task_key=task_key,
            operator=operator,
            kwargs=kwargs,
            functions=visitor.functions_for(var),
            source=source,
            call_source=call_source,
            default_args=visitor.default_args,
        )
        builder = ops.OPERATOR_REGISTRY.get(operator, ops.build_placeholder)
        activity = builder(ctx)
        activity.depends_on = depends_on
        if trigger_mapping.status == "unsupported":
            activity = ops.build_placeholder_with_comment(
                ctx,
                f"Airflow trigger_rule {trigger_mapping.rule!r} is unsupported. {trigger_mapping.message}",
            )
            activity.depends_on = depends_on
            semantic_findings.append(
                _semantic_finding(
                    source_file or dag_path.name,
                    visitor.calls.get(var),
                    code="unsupported_trigger_rule",
                    message=(f"Task {task_id!r} uses trigger_rule {trigger_mapping.rule!r}; {trigger_mapping.message}"),
                    task_key=task_key,
                    capture_id=var,
                )
            )
        elif trigger_mapping.status == "approximate":
            semantic_findings.append(
                _semantic_finding(
                    source_file or dag_path.name,
                    visitor.calls.get(var),
                    code="approximated_trigger_rule",
                    message=(
                        f"Task {task_id!r} maps trigger_rule {trigger_mapping.rule!r} to "
                        f"{trigger_mapping.outcome}. {trigger_mapping.message}"
                    ),
                    task_key=task_key,
                    capture_id=var,
                )
            )
        unconsumed = ops.unconsumed_kwargs(operator, kwargs)
        if unconsumed:
            names = ", ".join(sorted(unconsumed))
            activity = ops.build_placeholder_with_comment(
                ctx,
                f"Airflow {operator} argument(s) {names} are not represented by the Databricks task; "
                "translate them explicitly.",
            )
            activity.depends_on = depends_on
            semantic_findings.append(
                _semantic_finding(
                    source_file or dag_path.name,
                    visitor.calls.get(var),
                    code="unconsumed_operator_arguments",
                    message=f"Task {task_id!r} has unconsumed operator argument(s): {names}.",
                    task_key=task_key,
                    capture_id=var,
                    arguments=sorted(unconsumed),
                )
            )
        unrepresented_policy = templating.unrepresented_retry_policy_arguments(visitor.default_args, kwargs)
        if unrepresented_policy:
            names = ", ".join(unrepresented_policy)
            activity = ops.build_placeholder_with_comment(
                ctx,
                f"Airflow task policy argument(s) {names} cannot be represented statically; "
                "resolve the policy before migration.",
            )
            activity.depends_on = depends_on
            semantic_findings.append(
                _semantic_finding(
                    source_file or dag_path.name,
                    visitor.calls.get(var),
                    code="unrepresented_task_policy",
                    message=f"Task {task_id!r} has unrepresented retry/timeout policy argument(s): {names}.",
                    task_key=task_key,
                    capture_id=var,
                    arguments=unrepresented_policy,
                )
            )
        # Convert Airflow Jinja in the activity's parameter fields to DAB refs; collect params.
        referenced_params |= _convert_activity_templates(activity)
        unresolved_templates = _unresolved_activity_templates(activity)
        if unresolved_templates:
            expressions = ", ".join(sorted(unresolved_templates))
            activity = ops.build_placeholder_with_comment(
                ctx,
                f"Airflow template expression(s) {expressions} have no deterministic Databricks mapping; "
                "translate the value manually.",
            )
            activity.depends_on = depends_on
            semantic_findings.append(
                _semantic_finding(
                    source_file or dag_path.name,
                    visitor.calls.get(var),
                    code="unresolved_airflow_template",
                    message=f"Task {task_id!r} contains unresolved Airflow template expression(s): {expressions}.",
                    task_key=task_key,
                    capture_id=var,
                    expressions=sorted(unresolved_templates),
                )
            )

        is_mapped = var in visitor.mapped
        mapped_names: list[str] = []
        if is_mapped:
            mapped_names = visitor.expand_kwargs.get(var) or []
            partial_note = (
                " The mapping also contains .partial() fixed arguments." if var in visitor.partial_mapped else ""
            )
            activity = ops.build_placeholder_with_comment(
                ctx,
                "Classic Airflow dynamic mapping cannot be emitted until every mapped argument is "
                f"bound into the inner task ({', '.join(mapped_names) or 'unknown mapping'}).{partial_note}",
            )
            activity.depends_on = depends_on
            semantic_findings.append(
                _semantic_finding(
                    source_file or dag_path.name,
                    visitor.calls.get(var),
                    code="classic_mapping_arguments_unbound",
                    message=(
                        f"Task {task_id!r} maps argument(s) {', '.join(mapped_names) or '<unknown>'}, "
                        "but the generated inner task cannot bind them safely."
                    ),
                    task_key=task_key,
                    capture_id=var,
                    arguments=mapped_names,
                    has_partial=var in visitor.partial_mapped,
                )
            )

        # Stamp policy only after every semantic guard has selected the final leaf activity.
        policy = templating.retry_policy(visitor.default_args, kwargs)
        activity.max_retries = policy.get("max_retries")
        activity.timeout_seconds = policy.get("timeout_seconds")
        activity.min_retry_interval_millis = policy.get("min_retry_interval_millis")
        if isinstance(activity, PlaceholderActivity) and call_node is not None:
            raw_definition = dict(activity.raw_definition or {})
            raw_definition["bound_source"] = ast.unparse(call_node)
            activity.raw_definition = raw_definition

        if is_mapped:
            append_task(_wrap_in_for_each(activity, task_id, task_key, depends_on, kwargs, mapped_names), var)
        else:
            append_task(activity, var)

    # TaskFlow @task instances: emit each as a notebook that reads upstream return values via
    # dbutils.jobs.taskValues, calls the decorated function, and sets its own return value.
    for var, tf in visitor.taskflow_tasks.items():
        task_key = var_to_task_key[var]
        dep_keys = {var_to_task_key[u] for u in upstreams.get(var, []) if u in var_to_task_key}
        dep_keys.discard(task_key)
        depends_on = [Dependency(task_key=k) for k in sorted(dep_keys)] or None
        definition, _decorator = visitor.taskflow_defs[tf.def_name]
        if tf.is_async:
            mapping_call = visitor.calls.get(var)
            raw_definition = {
                "operator": "@task.async.expand" if var in visitor.mapped else "@task.async",
                "source": ast.get_source_segment(source, definition) or "",
                "invocation": ast.get_source_segment(source, visitor.capture_source_nodes[var]) or "",
            }
            if mapping_call is not None and var in visitor.mapped:
                raw_definition["mapping"] = ast.get_source_segment(source, mapping_call) or ""
            activity = PlaceholderActivity(
                name=tf.task_id,
                task_key=task_key,
                original_type="@task.async.expand" if var in visitor.mapped else "@task.async",
                comment=(
                    f"Native async TaskFlow callable {tf.def_name!r} requires an async-aware Databricks "
                    "implementation; resolve this captured leaf without changing its graph identity."
                ),
                raw_definition=raw_definition,
            )
            activity.depends_on = depends_on
            if var in visitor.mapped and tf.expand_items_json is not None:
                append_task(_wrap_taskflow_in_for_each(activity, tf, task_key, depends_on), var)
            else:
                append_task(activity, var)
            continue
        mapped_output_dependencies = sorted(
            {
                dependency
                for dependency in [*tf.positional_deps.values(), *tf.keyword_deps.values()]
                if dependency in visitor.mapped
            }
        )
        if mapped_output_dependencies:
            placeholder = PlaceholderActivity(
                name=tf.task_id,
                task_key=task_key,
                original_type=f"@{tf.decorator}",
                comment=(
                    "Airflow aggregates mapped TaskFlow return values for downstream XCom consumers, but "
                    "Databricks For each tasks do not expose nested task values to downstream tasks. "
                    "Materialize and aggregate the mapped results explicitly."
                ),
                raw_definition={
                    "operator": f"@{tf.decorator}",
                    "source": ast.get_source_segment(source, definition) or "",
                    "invocation": ast.get_source_segment(source, visitor.capture_source_nodes[var]) or "",
                    "mapped_upstreams": [var_to_task_key[dependency] for dependency in mapped_output_dependencies],
                },
            )
            placeholder.depends_on = depends_on
            append_task(placeholder, var)
            semantic_findings.append(
                _semantic_finding(
                    source_file or dag_path.name,
                    visitor.calls.get(var),
                    code="taskflow_mapped_output_unavailable",
                    message=(
                        f"Task {tf.task_id!r} consumes mapped TaskFlow output that Databricks For each "
                        "tasks cannot expose as an aggregate."
                    ),
                    task_key=task_key,
                    capture_id=var,
                    upstream_task_keys=[var_to_task_key[dependency] for dependency in mapped_output_dependencies],
                )
            )
            continue
        if var in visitor.mapped and tf.expand_items_json is None:
            # .expand over a non-literal iterable (e.g. an upstream task's output) can't be lowered to
            # a static for_each inputs array -- route to the agentic-gap round instead of silently
            # emitting a single-run notebook.
            reason = f"mapped parameter {tf.expand_kwarg!r}" if tf.expand_kwarg else "multiple mapped parameters"
            func = functions.get(tf.def_name)
            # The mapping call carries the .partial(...) fixed args and the mapped iterable, neither of
            # which appears in the callable's own source -- without it the agentic round can't
            # reconstruct the invocation.
            mapping_call = visitor.calls.get(var)
            mapping_source = ast.get_source_segment(source, mapping_call) if mapping_call is not None else None
            placeholder = PlaceholderActivity(
                name=tf.task_id,
                task_key=task_key,
                original_type=f"@{tf.decorator}.expand",
                comment=(
                    f"TaskFlow @{tf.decorator} '{tf.def_name}'.expand() maps over a non-literal iterable "
                    f"({reason}); translate to a Databricks for_each_task whose inputs reference the "
                    "upstream task value, iterating the callable."
                ),
                raw_definition={
                    "operator": f"@{tf.decorator}.expand",
                    "source": ast.get_source_segment(source, func) if func is not None else "",
                    "mapping": mapping_source or "",
                },
            )
            placeholder.depends_on = depends_on
            append_task(placeholder, var)
            continue
        activity = _build_taskflow_task(tf, var_to_task_key, functions, source, task_key)
        activity.depends_on = depends_on
        if isinstance(activity, PlaceholderActivity):
            raw_definition = dict(activity.raw_definition or {})
            raw_definition["invocation"] = ast.get_source_segment(source, visitor.capture_source_nodes[var]) or ""
            activity.raw_definition = raw_definition
        referenced_params |= _convert_activity_templates(activity)
        if var in visitor.mapped and isinstance(activity, NotebookActivity):
            # .expand(param=[literal list]) -> a for_each_task iterating the callable notebook; the
            # inner notebook reads the mapped parameter from the per-iteration `item` widget.
            append_task(_wrap_taskflow_in_for_each(activity, tf, task_key, depends_on), var)
        else:
            append_task(activity, var)

    # @task_group invocations: a group is a sub-pipeline of tasks with no single-task lowering, so
    # emit a placeholder + gap (never silently drop the whole group) for the agentic round to expand.
    for var, (task_id, def_name, mapped) in visitor.taskgroup_calls.items():
        task_key = var_to_task_key[var]
        dep_keys = {var_to_task_key[u] for u in upstreams.get(var, []) if u in var_to_task_key}
        dep_keys.discard(task_key)
        depends_on = [Dependency(task_key=k) for k in sorted(dep_keys)] or None
        group_func = functions.get(def_name)
        detail = (
            "maps the group over an iterable (one group run per element); translate to a for_each_task "
            "whose inner task expands the group's tasks"
            if mapped
            else "bundles multiple tasks; expand it into its member tasks with their dependencies"
        )
        placeholder = PlaceholderActivity(
            name=task_id,
            task_key=task_key,
            original_type="@task_group",
            comment=f"Airflow @task_group '{def_name}' {detail}. flowx does not lower task groups.",
            raw_definition={
                "operator": "@task_group",
                "source": ast.get_source_segment(source, group_func) if group_func is not None else "",
                "invocation": ast.get_source_segment(source, visitor.capture_source_nodes[var]) or "",
            },
        )
        placeholder.depends_on = depends_on
        append_task(placeholder, var)

    # Declare every job parameter -- those referenced in templates plus any from the DAG's
    # params={...} -- each with a default (Databricks requires one): the params={...} default when
    # present; a reserved logical-date parameter its schedule-aware time ref so a native backfill can
    # override it per window; else an empty string so the bundle still validates.
    param_names = referenced_params | set(visitor.dag_params)
    parameters = [
        {"name": name, "default": _declared_param_default(name, visitor.dag_params, schedule)}
        for name in sorted(param_names)
    ] or None
    tags = {"source": "airflow", "dag_id": visitor.dag_id or ""}
    if visitor.catchup:
        # Airflow catchup=True has no DABs schedule setting; it maps to running a native Databricks
        # backfill, which overrides the reserved logical-date parameter per replayed window.
        tags["airflow_catchup"] = "true"
    if visitor.dag_owner:
        tags["airflow_owner"] = visitor.dag_owner
    available_user_tags = _DATABRICKS_JOB_TAG_LIMIT - len(tags)
    tags.update(
        {
            f"airflow_tag_{index}": value
            for index, value in enumerate(visitor.dag_user_tags[:available_user_tags], start=1)
        }
    )
    expected_ir_edges = {(dependency.task_key, task.task_key) for task in tasks for dependency in task.depends_on or []}
    pipeline = Pipeline(
        name=visitor.dag_id or Path(dag_path).stem,
        description=visitor.dag_description,
        tasks=tasks,
        parameters=parameters,
        schedule=schedule,
        timeout_seconds=_job_timeout_seconds(visitor),
        email_notifications=_job_email_notifications(visitor),
        tags=tags,
    )
    return reconcile._reconcile_pipeline(
        pipeline,
        audit=audit,
        visitor=visitor,
        source_file=source_file or dag_path.name,
        var_to_task_key=var_to_task_key,
        dropped=dropped,
        dbt_vars=dbt_vars,
        semantic_findings=semantic_findings,
        sensor_lift_proof=sensor_lift_proof,
        schedule_proof=schedule_proof,
        argument_proofs=argument_proofs,
        expected_ir_edges=expected_ir_edges,
        placeholder_capture_ids=placeholder_capture_ids,
    )
