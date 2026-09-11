"""The DAG-body visitor that collects operator calls, edges, and the schedule."""

from __future__ import annotations

import ast
import json
from typing import Any

from flowx.sources.airflow import operators as ops
from flowx.sources.airflow import templating
from flowx.sources.airflow.loader.ast_utils import (
    _UNRESOLVED,
    _airflow_generation,
    _bind_constants,
    _construct_name,
    _import_aliases,
    _index_lexical_functions,
    _iter_functions,
    _literal_argument_source,
    _param_default,
    _safe_static_value,
    _sanitize_task_key,
    _span,
    _static_iteration_nodes,
)
from flowx.sources.airflow.loader.captures import EdgeCapture, TaskCapture, _TaskFlowTask
from flowx.sources.airflow.loader.dispatch import (
    _DAG_DECORATORS,
    _TASK_DECORATORS,
    _TASK_GROUP_DECORATORS,
    _decorator_kwargs,
    _decorator_name,
    _direct_operator_call,
    _has_decorator,
    _has_partial_call,
    _is_task_construct,
    _mapped_operator_call,
    _mapping_chain_args,
)
from flowx.sources.airflow.loader.schedule import _asset_definitions, _extract_timezone

_EDGE_MODIFIER_CONSTRUCTS = frozenset({"Label"})


class _DagVisitor(ast.NodeVisitor):
    """Collects operator calls, dependency edges, and the DAG's schedule."""

    def __init__(self, module: ast.Module, *, target_dag_variable: str | None = None) -> None:
        self._aliases = _import_aliases(module)
        self.airflow_generation = _airflow_generation(module)
        self.asset_definitions = _asset_definitions(module, self._aliases)
        self._target_dag_variable = target_dag_variable
        # Classic python_callable resolution starts at module scope. Nested functions are only visible
        # from their lexical parent and must never overwrite a same-named module function.
        self._functions: dict[str, ast.FunctionDef] = {
            node.name: node for node in module.body if isinstance(node, ast.FunctionDef)
        }
        self._lexical_functions = _index_lexical_functions(module)
        self._scope_stack: list[ast.Module | ast.FunctionDef] = [module]
        self._resolved_callables: dict[str, tuple[str, ast.FunctionDef | None]] = {}
        self.helper_expansions: list[dict[str, Any]] = []
        self._constants: dict[str, Any] = {}
        self._task_bindings: dict[str, str | list[str]] = {}
        self._list_bindings: dict[str, list[str]] = {}
        self._capture_sequence = 0
        self.task_captures: dict[str, TaskCapture] = {}
        self.capture_source_nodes: dict[str, ast.Call] = {}
        self.edge_captures: list[EdgeCapture] = []
        self.unclaimed_task_calls: list[ast.Call] = []
        self.unclaimed_statements: list[ast.stmt] = []
        self.unresolved_constructs: list[tuple[str, ast.AST]] = []
        self._claimed_task_call_ids: set[int] = set()
        self._claimed_statement_ids: set[int] = set()
        self._dag_scope_depth = 0
        self.captured_dag_settings: set[str] = set()
        self.dag_kwargs: dict[str, ast.expr] = {}
        # task variable name -> (task_id, operator, kwargs)
        self.operators: dict[str, tuple[str, str, dict[str, ast.expr]]] = {}
        # task variable name -> the operator's ast.Call node (for source-slicing placeholders)
        self.calls: dict[str, ast.Call] = {}
        self.edges: list[tuple[str, str]] = []  # (upstream_var, downstream_var)
        self.dag_id: str | None = None
        self.schedule_interval: str | None = None
        self.schedule_node: ast.expr | None = None
        self.timezone: str | None = None
        # DAG catchup= flag: True means Airflow backfills missed intervals, which maps to a native
        # Databricks backfill overriding the reserved Airflow date parameter.
        self.catchup: bool = False
        self.default_args: dict[str, ast.expr] = {}
        # DAG-level params={...} defaults (param name -> literal default), so emitted job parameters
        # carry a Databricks-required default rather than an empty placeholder.
        self.dag_params: dict[str, Any] = {}
        self.dag_description: str | None = None
        self.dag_user_tags: list[str] = []
        self.dag_owner: str | None = None
        # task variable name -> TaskGroup id prefix (for task-key namespacing)
        self.groups: dict[str, str] = {}
        self._group_stack: list[str] = []
        # `with TaskGroup(...) as tg:` binding -> the group's prefix, so a group-level edge
        # (tg >> other) can expand to edges between the groups' boundary tasks.
        self.group_vars: dict[str, str] = {}
        # task variable names defined via dynamic mapping (.expand()) -> wrapped in a for_each
        self.mapped: set[str] = set()
        # mapped var -> the kwarg names passed to .expand(). Only these fan out; a list-valued
        # .partial() arg is a fixed value and must not be mistaken for the mapped iterable.
        self.expand_kwargs: dict[str, list[str]] = {}
        self.partial_mapped: set[str] = set()
        # Disambiguates synthetic vars for operators instantiated without an assignment.
        self._bare_operator_counter = 0
        # TaskFlow: function name -> (definition, decorator dotted-name) for @task-decorated defs.
        # Pre-scanned so a @task def defined after the @dag body that uses it is still resolved.
        self.taskflow_defs: dict[str, tuple[ast.FunctionDef | ast.AsyncFunctionDef, str]] = {}
        # @task_group def names -- a group is a sub-pipeline, not a single renderable task, so an
        # invocation routes to a placeholder + gap rather than being expanded here.
        self.taskgroup_defs: set[str] = set()
        for fn in _iter_functions(module):
            decorator = next(
                (
                    _decorator_name(d, self._aliases)
                    for d in fn.decorator_list
                    if _decorator_name(d, self._aliases) in _TASK_DECORATORS
                ),
                None,
            )
            if decorator is not None:
                self.taskflow_defs[fn.name] = (fn, decorator)
            elif _has_decorator(fn, _TASK_GROUP_DECORATORS, self._aliases):
                self.taskgroup_defs.add(fn.name)
        # TaskFlow task instances: var name -> _TaskFlowTask (id, def-name, decorator, arg bindings).
        self.taskflow_tasks: dict[str, _TaskFlowTask] = {}
        # @task_group invocations: var name -> (task_id, def-name, is_mapped).
        self.taskgroup_calls: dict[str, tuple[str, str, bool]] = {}
        self._taskflow_counter = 0
        self._taskgroup_counter = 0
        # A @dag-decorated function was found (so a bare `@task` file is still recognized as a DAG).
        self.is_taskflow_dag: bool = False

    def functions(self) -> dict[str, ast.FunctionDef]:
        taskflow = {
            name: definition
            for name, (definition, _decorator) in self.taskflow_defs.items()
            if isinstance(definition, ast.FunctionDef)
        }
        return {**self._functions, **taskflow}

    def functions_for(self, task_var: str) -> dict[str, ast.FunctionDef]:
        """Returns module functions with a task's lexically resolved callable overlaid."""
        functions = self.functions()
        resolved = self._resolved_callables.get(task_var)
        if resolved is None:
            return functions
        name, definition = resolved
        functions.pop(name, None)
        if definition is not None:
            functions[name] = definition
        return functions

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        # A @task- or @task_group-decorated function defines a task / sub-pipeline from its body,
        # which is internal logic rather than DAG structure, so don't descend. @dag marks the
        # DAG-defining function: read its config off the decorator, then descend so the body's task
        # instances / edges are collected.
        if _has_decorator(node, _TASK_DECORATORS, self._aliases) or _has_decorator(
            node, _TASK_GROUP_DECORATORS, self._aliases
        ):
            if self._dag_scope_depth:
                self._claimed_statement_ids.add(id(node))
            return
        is_dag_definition = _has_decorator(node, _DAG_DECORATORS, self._aliases)
        if not is_dag_definition:
            if self._dag_scope_depth:
                self._claimed_statement_ids.add(id(node))
            return
        if is_dag_definition:
            self.is_taskflow_dag = True
            dag_kwargs = {
                name: _bind_constants(value, self._constants)
                for name, value in _decorator_kwargs(node.decorator_list, _DAG_DECORATORS, self._aliases).items()
            }
            self._apply_dag_kwargs(dag_kwargs)
            if self.dag_id is None:
                self.dag_id = ops.literal_str(dag_kwargs.get("dag_id")) or node.name
        self._scope_stack.append(node)
        if is_dag_definition:
            self._dag_scope_depth += 1
        try:
            for statement in node.body:
                if is_dag_definition:
                    self._visit_dag_statement(statement)
                else:
                    self.visit(statement)
        finally:
            if is_dag_definition:
                self._dag_scope_depth -= 1
            self._scope_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Claims native async TaskFlow definitions without treating their bodies as DAG structure."""
        if self._dag_scope_depth:
            self._claimed_statement_ids.add(id(node))

    def visit_Assign(self, node: ast.Assign) -> None:
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and isinstance(node.value, ast.Call):
            var = node.targets[0].id
            if _construct_name(node.value.func, self._aliases) == "DAG":
                self._read_dag_kwargs(node.value)
                self.dag_id = self.dag_id or var
                self._claimed_statement_ids.add(id(node))
                return
            internal_var = self._new_task_var(var, node.value)
            if self._register_operator_call(node.value, internal_var, binding=var):
                self._claimed_statement_ids.add(id(node))
                pass  # a `x = SomeOperator(...)` (optionally .expand()-mapped) instantiation
            elif self._register_helper_factory_call(node.value, internal_var, binding=var):
                self._claimed_statement_ids.add(id(node))
                pass
            elif self._register_taskflow_call(node.value, internal_var, source_reference=var):
                self._task_bindings[var] = internal_var
                self._claimed_statement_ids.add(id(node))
                pass  # a `x = mytask(...)` TaskFlow invocation, captured with var as its key
            else:
                if self._register_taskgroup_call(node.value, internal_var):
                    self._task_bindings[var] = internal_var
                    self._claimed_statement_ids.add(id(node))
                    return
        elif len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            target = node.targets[0].id
            if isinstance(node.value, ast.Name):
                resolved = self._resolve_task_names(node.value)
                if resolved:
                    self._task_bindings[target] = resolved[0] if len(resolved) == 1 else resolved
                    self._constants.pop(target, None)
                    self._claimed_statement_ids.add(id(node))
                    return
            value = _safe_static_value(node.value, self._constants)
            if value is not _UNRESOLVED:
                self._constants[target] = value
                self._task_bindings.pop(target, None)
                if isinstance(value, list):
                    self._list_bindings[target] = []
                self._claimed_statement_ids.add(id(node))
                return
        self.generic_visit(node)

    def _new_task_var(self, binding: str, node: ast.AST) -> str:
        """Allocates an internal identity while preserving Python's latest name binding."""
        if binding not in self.operators and binding not in self.taskflow_tasks and binding not in self.taskgroup_calls:
            return binding
        self._capture_sequence += 1
        return f"{binding}__L{getattr(node, 'lineno', 0)}_{self._capture_sequence}"

    def _register_operator_call(self, node: ast.Call, var: str, *, binding: str | None = None) -> bool:
        """Registers a classic operator/sensor instantiation under the task variable *var*.

        Airflow registers a task when the operator is instantiated inside a DAG context; assigning it
        to a name is a Python convenience, not a requirement. So this is shared by the assigned form
        and the bare-statement / bare-chain forms, which synthesise *var* from the task_id.

        Returns True when *node* was a (possibly ``.expand()``-mapped) operator call.
        """
        direct = _direct_operator_call(node, self._aliases)
        mapped = None if direct is not None else _mapped_operator_call(node, self._aliases)
        call = direct or (mapped[0] if mapped is not None else None)
        if call is None:
            return False
        construct = _construct_name(call.func, self._aliases)
        kwargs = {kw.arg: _bind_constants(kw.value, self._constants) for kw in call.keywords if kw.arg}
        dag_node = kwargs.get("dag")
        if self._target_dag_variable is not None and not (
            isinstance(dag_node, ast.Name) and dag_node.id == self._target_dag_variable
        ):
            return False
        call = ast.Call(
            func=ast.Name(id=construct, ctx=ast.Load()),
            args=[],
            keywords=[ast.keyword(arg=key, value=value) for key, value in kwargs.items()],
        )
        ast.copy_location(call, node)
        task_id = ops.literal_str(kwargs.get("task_id")) or ops.literal_str(kwargs.get("group_id")) or var
        self.operators[var] = (task_id, construct, kwargs)
        self.calls[var] = call
        self._task_bindings[binding or var] = var
        self.task_captures[var] = TaskCapture(
            capture_id=var,
            variable=binding or var,
            task_id=task_id,
            operator=construct,
            call=call,
            span=_span(node),
        )
        self.capture_source_nodes[var] = node
        self._claimed_task_call_ids.add(id(node))
        callable_node = kwargs.get("python_callable")
        if isinstance(callable_node, ast.Name):
            self._resolved_callables[var] = (
                callable_node.id,
                self._resolve_lexical_function(callable_node.id, node),
            )
        if mapped is not None:
            self.mapped.add(var)
            self.expand_kwargs[var] = mapped[1]
            if mapped[2]:
                self.partial_mapped.add(var)
        if self._group_stack:
            self.groups[var] = "__".join(self._group_stack)
        return True

    def _register_helper_factory_call(self, node: ast.Call, var: str, *, binding: str) -> bool:
        """Expands the deliberately narrow single-return operator factory shape."""
        helper_return = self._helper_factory_return(node)
        if helper_return is None:
            return False
        helper, return_call = helper_return
        parameters = [*helper.args.posonlyargs, *helper.args.args, *helper.args.kwonlyargs]
        if len(node.args) > len(parameters) or any(keyword.arg is None for keyword in node.keywords):
            return False
        bound: dict[str, Any] = {}
        for parameter, argument in zip(parameters, node.args):
            bound[parameter.arg] = _bind_constants(argument, self._constants)
        for keyword in node.keywords:
            if keyword.arg:
                bound[keyword.arg] = _bind_constants(keyword.value, self._constants)
        missing = [parameter.arg for parameter in parameters if parameter.arg not in bound]
        positional_defaults = [None] * (len(helper.args.args) - len(helper.args.defaults)) + list(helper.args.defaults)
        defaults = {
            parameter.arg: default
            for parameter, default in zip(helper.args.args, positional_defaults)
            if default is not None
        }
        defaults.update(
            {
                parameter.arg: default
                for parameter, default in zip(helper.args.kwonlyargs, helper.args.kw_defaults)
                if default is not None
            }
        )
        for name in missing:
            if name not in defaults:
                return False
            bound[name] = defaults[name]
        constants = dict(self._constants)
        for name, expression in bound.items():
            if isinstance(expression, ast.expr):
                value = _safe_static_value(expression, constants)
                if value is _UNRESOLVED:
                    return False
                constants[name] = value
        factory_call = _bind_constants(return_call, constants)
        registered = isinstance(factory_call, ast.Call) and self._register_operator_call(
            factory_call, var, binding=binding
        )
        if registered:
            self.capture_source_nodes[var] = node
            self._claimed_task_call_ids.add(id(node))
            self.helper_expansions.append(
                {
                    "code": "helper_factory_expanded",
                    "capture_id": var,
                    "helper": helper.name,
                    "helper_line": helper.lineno,
                    "invocation_line": getattr(node, "lineno", 0),
                }
            )
        return registered

    def _helper_factory_return(self, node: ast.Call) -> tuple[ast.FunctionDef, ast.Call] | None:
        """Returns the operator call from a supported single-return helper invocation."""
        if not isinstance(node.func, ast.Name):
            return None
        helper = self._resolve_lexical_function(node.func.id, node)
        if helper is None or helper.decorator_list or helper.args.vararg or helper.args.kwarg:
            return None
        body = list(helper.body)
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            if isinstance(body[0].value.value, str):
                body = body[1:]
        if len(body) != 1 or not isinstance(body[0], ast.Return) or not isinstance(body[0].value, ast.Call):
            return None
        if _direct_operator_call(body[0].value, self._aliases) is None:
            return None
        return helper, body[0].value

    def _resolve_lexical_function(self, name: str, reference: ast.AST) -> ast.FunctionDef | None:
        """Resolves a function name by lexical scope and source-order binding semantics."""
        line = getattr(reference, "lineno", 0)
        for scope in reversed(self._scope_stack):
            events = self._lexical_functions.get(id(scope), {}).get(name, [])
            visible = [event for event in events if event[0] <= line]
            if visible:
                _event_line, conditional, definition = visible[-1]
                return None if conditional else definition
            if events and isinstance(scope, ast.FunctionDef):
                return None
        return None

    def _register_bare_operator_call(self, node: ast.Call) -> str | None:
        """Registers an operator instantiated without an assignment, keyed by a synthetic var.

        The var is derived from the literal ``task_id`` (which is what the emitted task key comes from
        anyway), with a counter suffix if two bare operators somehow share one.
        """
        if _direct_operator_call(node, self._aliases) is None and _mapped_operator_call(node, self._aliases) is None:
            return None
        kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
        base = ops.literal_str(kwargs.get("task_id")) or ops.literal_str(kwargs.get("group_id"))
        if base is None:
            # `.expand()` chains carry task_id on the inner .partial(...) call, not the outer one.
            mapped = _mapped_operator_call(node, self._aliases)
            if mapped is not None:
                inner_kwargs = {kw.arg: kw.value for kw in mapped[0].keywords if kw.arg}
                base = ops.literal_str(inner_kwargs.get("task_id")) or ops.literal_str(inner_kwargs.get("group_id"))
        if base is None:
            self._bare_operator_counter += 1
            base = f"_bare_task{self._bare_operator_counter}"
        var = base
        while var in self.operators:
            self._bare_operator_counter += 1
            var = f"{base}__{self._bare_operator_counter}"
        return var if self._register_operator_call(node, var, binding=var) else None

    def _taskflow_def_name(self, call: ast.Call) -> tuple[str | None, bool, str | None]:
        """Resolves a call's underlying ``@task`` def name, unwrapping the mapping/config chain.

        Handles ``.expand(...)`` / ``.expand_kwargs(...)`` (both set ``is_mapped``) and the
        ``.override(...)`` / ``.partial(...)`` config calls, in any order, so forms like
        ``op.partial(...).expand(...)`` resolve. Returns ``(def_name_or_None, is_mapped, override_id)``.
        """
        func = call.func
        mapped = False
        override_id: str | None = None
        while True:
            if isinstance(func, ast.Attribute):
                if func.attr in ("expand", "expand_kwargs"):
                    mapped = True
                func = func.value
                continue
            if isinstance(func, ast.Call) and isinstance(func.func, ast.Attribute):
                config_call = func.func
                if config_call.attr == "override":
                    arguments = {keyword.arg: keyword.value for keyword in func.keywords if keyword.arg}
                    override_id = ops.literal_str(arguments.get("task_id"))
                    func = config_call.value
                    continue
                if config_call.attr == "partial":
                    func = config_call.value
                    continue
            break
        if isinstance(func, ast.Name) and func.id in self.taskflow_defs:
            return func.id, mapped, override_id
        return None, mapped, override_id

    def _register_taskflow_call(self, call: ast.Call, var: str, *, source_reference: str | None = None) -> bool:
        """Records a TaskFlow ``@task`` invocation as a task instance keyed by *var*.

        Binds each call argument that references (or nests) another ``@task`` to that upstream task
        var -- TaskFlow's implicit XCom data flow (``transform(extract())`` wires extract ->
        transform). Nested calls (``load(transform(extract()))``) register their own instances
        recursively. A ``.override(task_id=...)`` renames the task. Returns True when captured.
        """
        def_name, mapped, override_id = self._taskflow_def_name(call)
        if def_name is None:
            return False
        function, decorator = self.taskflow_defs[def_name]
        task = _TaskFlowTask(
            task_id=override_id or var,
            def_name=def_name,
            decorator=decorator,
            source_reference=source_reference or var,
            is_async=isinstance(function, ast.AsyncFunctionDef),
        )
        self.taskflow_tasks[var] = task
        self.calls[var] = call
        self.capture_source_nodes[var] = call
        self._claimed_task_call_ids.add(id(call))
        if mapped:
            self.mapped.add(var)
            # ``.expand(param=<iterable>)`` args live on the outer call; capture the single mapped
            # parameter + its literal iterable (Tier 1). A non-literal iterable leaves items None,
            # which routes the task to the agentic-gap round in _build_taskflow_task.
            self._capture_expand(task, call)
            # A mapped iterable OR a .partial(...) fixed arg can be an upstream task's output
            # (``process.partial(x=raw).expand(y=vals)``); wire those data-flow edges so the mapped
            # task still depends on its producers, whether it lowers to a for_each or a placeholder.
            for mapped_arg in _mapping_chain_args(call):
                dep = self._resolve_taskflow_arg(mapped_arg)
                if dep is not None and dep != var:
                    self._add_edges([dep], [var], call)
        if self._group_stack:
            self.groups[var] = "__".join(self._group_stack)
        if mapped:
            return True
        # Bind each arg that resolves to an upstream task var, and add the data-flow edge.
        for index, arg in enumerate(call.args):
            dep = self._resolve_taskflow_arg(arg)
            if dep is not None:
                task.positional_deps[index] = dep
                self._add_edges([dep], [var], call)
            else:
                value = _literal_argument_source(arg)
                if value is None:
                    task.unresolved_arguments.append(ast.unparse(arg))
                else:
                    task.positional_values[index] = value
        for kw in call.keywords:
            if kw.arg is None:
                task.unresolved_arguments.append(f"**{ast.unparse(kw.value)}")
                continue
            dep = self._resolve_taskflow_arg(kw.value)
            if dep is not None:
                task.keyword_deps[kw.arg] = dep
                self._add_edges([dep], [var], call)
            else:
                value = _literal_argument_source(kw.value)
                if value is None:
                    task.unresolved_arguments.append(f"{kw.arg}={ast.unparse(kw.value)}")
                else:
                    task.keyword_values[kw.arg] = value
        return True

    def _capture_expand(self, task: _TaskFlowTask, call: ast.Call) -> None:
        """Captures a ``@task.expand(param=<iterable>)`` mapping onto *task*.

        Tier 1 (deterministic -> for_each_task): a plain ``.expand(...)`` with exactly one mapped
        parameter whose iterable is a literal list, and no ``.partial(...)`` fixed args (a for_each
        inner task can't carry them). Anything else -- ``.expand_kwargs``, multiple mapped params, a
        ``.partial(...).expand(...)`` chain, or a non-literal iterable -- leaves ``expand_items_json``
        None so _build_taskflow_task routes the task to the agentic-gap round.
        """
        if not (isinstance(call.func, ast.Attribute) and call.func.attr == "expand"):
            return  # .expand_kwargs(...) or other mapping form -> not Tier 1
        if _has_partial_call(call.func.value):
            return  # .partial(...) fixed args can't be represented on a for_each inner task
        keywords = [kw for kw in call.keywords if kw.arg]
        if len(keywords) != 1 or len(keywords) != len(call.keywords):
            return  # 0 / multiple mapped params, or **expand_kwargs -> not Tier 1
        keyword = keywords[0]
        task.expand_kwarg = keyword.arg
        value = ops.literal_value(keyword.value)
        if isinstance(value, list):
            # Encode each element as its own JSON text, so the for_each `inputs` is a list of JSON
            # strings and the inner notebook's json.loads unambiguously recovers the original value.
            # (A bare list like [1, 2, 3] would make `{{input}}` deliver "1"/"2"/"3" -- indistinguishable
            # from the string elements ["1", "2", "3"]; wrapping each element removes that ambiguity.)
            task.expand_items_json = json.dumps([json.dumps(element) for element in value])

    def _register_taskgroup_call(self, call: ast.Call, var: str | None) -> bool:
        """Records a ``@task_group`` invocation (``pair(...)`` / ``pair.expand(...)``) as a placeholder.

        A ``@task_group`` is a sub-pipeline of tasks, not a single renderable callable, so it can't be
        mechanically lowered here -- it's captured (keyed by *var*, or a synthetic name for a bare
        call) so an edge to/from it resolves, and emitted as a placeholder + gap for the agentic round.
        Returns True when the call resolved to a known group def.
        """
        func = call.func
        mapped = False
        while isinstance(func, ast.Attribute):
            if func.attr == "expand":
                mapped = True
            func = func.value
        if not (isinstance(func, ast.Name) and func.id in self.taskgroup_defs):
            return False
        def_name = func.id
        if var is None:
            self._taskgroup_counter += 1
            var = f"{def_name}__tg{self._taskgroup_counter}"
        self.taskgroup_calls[var] = (var, def_name, mapped)
        self.capture_source_nodes[var] = call
        self._claimed_task_call_ids.add(id(call))
        if self._group_stack:
            self.groups[var] = "__".join(self._group_stack)
        return True

    def _resolve_taskflow_arg(self, arg: ast.expr) -> str | None:
        """Returns the upstream task var an argument refers to, else None (a literal / unknown).

        A bare ``Name`` is an existing task var. A nested ``@task`` call (``transform(extract())``)
        is registered as its own synthetic task instance and its var returned, so the whole
        expression tree becomes a chain of task instances.
        """
        if isinstance(arg, ast.Name):
            resolved = self._resolve_task_names(arg)
            if len(resolved) == 1:
                return resolved[0]
        if isinstance(arg, ast.Call):
            def_name, _mapped, _override = self._taskflow_def_name(arg)
            if def_name is not None:
                self._taskflow_counter += 1
                synthetic = f"{def_name}__tf{self._taskflow_counter}"
                self._register_taskflow_call(arg, synthetic, source_reference=def_name)
                return synthetic
        return None

    def visit_With(self, node: ast.With) -> None:
        pushed_group = False
        opens_dag_scope = False
        for item in node.items:
            call = item.context_expr
            if isinstance(call, ast.Call):
                construct = _construct_name(call.func, self._aliases)
                if construct == "DAG":
                    self._read_dag_kwargs(call)
                    opens_dag_scope = True
                elif construct == "TaskGroup":
                    # `with TaskGroup("etl") as tg:` — namespace the member tasks by group id.
                    kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
                    group_id = (
                        ops.literal_str(kwargs.get("group_id"))
                        or (ops.literal_str(call.args[0]) if call.args else None)
                        or "group"
                    )
                    self._group_stack.append(_sanitize_task_key(group_id))
                    pushed_group = True
                    # Record the `as tg` binding (with the full nested prefix) so a group-level
                    # edge on `tg` resolves to the group's member tasks.
                    if isinstance(item.optional_vars, ast.Name):
                        self.group_vars[item.optional_vars.id] = "__".join(self._group_stack)
                elif _is_task_construct(construct) and item.optional_vars is not None:
                    # `with DbtTaskGroup(...) as g:` — a cosmos group bound to a name.
                    if isinstance(item.optional_vars, ast.Name):
                        var = item.optional_vars.id
                        kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
                        task_id = ops.literal_str(kwargs.get("group_id")) or var
                        self.operators[var] = (task_id, construct, kwargs)
                        self.calls[var] = call
        if opens_dag_scope:
            self._dag_scope_depth += 1
            try:
                for statement in node.body:
                    self._visit_dag_statement(statement)
            finally:
                self._dag_scope_depth -= 1
        elif self._dag_scope_depth:
            for statement in node.body:
                self._visit_dag_statement(statement)
        else:
            self.generic_visit(node)
        if pushed_group:
            self._group_stack.pop()
        self._claimed_statement_ids.add(id(node))

    def _visit_dag_statement(self, statement: ast.stmt) -> None:
        """Visits one DAG-body statement and records any unclaimed structural source."""
        unclaimed_calls_before = len(self.unclaimed_task_calls)
        unresolved_before = len(self.unresolved_constructs)
        self.visit(statement)
        if id(statement) in self._claimed_statement_ids:
            return
        if (
            len(self.unclaimed_task_calls) > unclaimed_calls_before
            or len(self.unresolved_constructs) > unresolved_before
        ):
            return
        if isinstance(statement, (ast.Import, ast.ImportFrom, ast.Pass, ast.Return)):
            self._claimed_statement_ids.add(id(statement))
            return
        if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant):
            self._claimed_statement_ids.add(id(statement))
            return
        self.unclaimed_statements.append(statement)

    def visit_Call(self, node: ast.Call) -> None:
        """Fails closed when a task-producing call in a DAG scope was not captured."""
        in_selected_assigned_dag = False
        is_assigned_task_factory = False
        if self._target_dag_variable is not None:
            direct = _direct_operator_call(node, self._aliases)
            mapped = None if direct is not None else _mapped_operator_call(node, self._aliases)
            operator_call = direct or (mapped[0] if mapped is not None else None)
            if operator_call is not None:
                dag_argument = next((kw.value for kw in operator_call.keywords if kw.arg == "dag"), None)
                in_selected_assigned_dag = (
                    isinstance(dag_argument, ast.Name) and dag_argument.id == self._target_dag_variable
                )
            is_assigned_task_factory = self._helper_targets_assigned_dag(node)
            in_selected_assigned_dag = in_selected_assigned_dag or is_assigned_task_factory
        if (self._dag_scope_depth or in_selected_assigned_dag) and id(node) not in self._claimed_task_call_ids:
            is_operator = _direct_operator_call(node, self._aliases) is not None
            is_mapped_operator = _mapped_operator_call(node, self._aliases) is not None
            is_taskflow = self._taskflow_def_name(node)[0] is not None
            is_taskgroup = any(
                isinstance(candidate, ast.Name) and candidate.id in self.taskgroup_defs
                for candidate in ast.walk(node.func)
            )
            if (
                is_operator
                or is_mapped_operator
                or is_taskflow
                or is_taskgroup
                or is_assigned_task_factory
                or self._helper_factory_return(node)
            ):
                self.unclaimed_task_calls.append(node)
        self.generic_visit(node)

    def _helper_targets_assigned_dag(self, call: ast.Call) -> bool:
        """Returns whether a local helper can construct a task for the selected assigned DAG."""
        if self._target_dag_variable is None or not isinstance(call.func, ast.Name):
            return False
        helper = self._resolve_lexical_function(call.func.id, call)
        if helper is None:
            return False
        parameters = [*helper.args.posonlyargs, *helper.args.args, *helper.args.kwonlyargs]
        bound: dict[str, ast.expr] = {parameter.arg: argument for parameter, argument in zip(parameters, call.args)}
        bound.update({keyword.arg: keyword.value for keyword in call.keywords if keyword.arg})
        for candidate in ast.walk(helper):
            if not isinstance(candidate, ast.Call):
                continue
            direct = _direct_operator_call(candidate, self._aliases)
            mapped = None if direct is not None else _mapped_operator_call(candidate, self._aliases)
            operator_call = direct or (mapped[0] if mapped is not None else None)
            if operator_call is None:
                continue
            dag_argument = next((keyword.value for keyword in operator_call.keywords if keyword.arg == "dag"), None)
            if not isinstance(dag_argument, ast.Name):
                continue
            if dag_argument.id == self._target_dag_variable:
                return True
            bound_argument = bound.get(dag_argument.id)
            if isinstance(bound_argument, ast.Name) and bound_argument.id == self._target_dag_variable:
                return True
        return False

    def _read_dag_kwargs(self, call: ast.Call) -> None:
        kwargs = {kw.arg: _bind_constants(kw.value, self._constants) for kw in call.keywords if kw.arg}
        positional_dag_id = ops.literal_str(call.args[0]) if call.args else None
        self.dag_id = ops.literal_str(kwargs.get("dag_id")) or positional_dag_id
        self._apply_dag_kwargs(kwargs)
        if self.airflow_generation == "1.10" and not {"schedule", "schedule_interval"} & kwargs.keys():
            self.unresolved_constructs.append(("ambiguous_airflow_1_10_default_schedule", call))

    def _apply_dag_kwargs(self, kwargs: dict[str, ast.expr]) -> None:
        self.dag_kwargs.update(kwargs)
        self.captured_dag_settings.update(kwargs)
        self.schedule_node = kwargs.get("schedule_interval") or kwargs.get("schedule")
        self.schedule_interval = ops.literal_str(kwargs.get("schedule_interval")) or ops.literal_str(
            kwargs.get("schedule")
        )
        self.timezone = _extract_timezone(kwargs.get("start_date")) or _extract_timezone(kwargs.get("timezone"))
        self.catchup = ops.literal_value(kwargs.get("catchup")) is True
        self.dag_description = ops.literal_str(kwargs.get("description"))
        tags = ops.literal_value(kwargs.get("tags"))
        if isinstance(tags, (list, tuple)) and all(isinstance(tag, str) for tag in tags):
            self.dag_user_tags = list(tags)
        # default_args is a dict literal of DAG-wide task settings (retries, timeouts, email).
        default_args = kwargs.get("default_args")
        if isinstance(default_args, ast.Dict):
            self.default_args = {
                key.value: val
                for key, val in zip(default_args.keys, default_args.values)
                if isinstance(key, ast.Constant) and isinstance(key.value, str)
            }
            self.captured_dag_settings.update(f"default_args.{name}" for name in self.default_args)
            owner = self.default_args.get("owner")
            if owner is not None:
                self.dag_owner = ops.literal_str(owner)
        # params={...} supplies DAG parameter defaults; each value is a literal or a Param(default=...).
        params = kwargs.get("params")
        if isinstance(params, ast.Dict):
            for key, val in zip(params.keys, params.values):
                if isinstance(key, ast.Constant) and isinstance(key.value, str):
                    if key.value.startswith(templating.FLOWX_INTERNAL_PARAMETER_PREFIX):
                        self.unresolved_constructs.append(("reserved_airflow_parameter_name", key))
                        continue
                    self.dag_params[key.value] = _param_default(val)

    def visit_Expr(self, node: ast.Expr) -> None:
        # Dependency edges come from two forms:
        #   - shift chains: `a >> b >> c`, `a >> [b, c]`, `[a, b] >> c`, `a << b`
        #   - method calls: `a.set_upstream(b)` / `a.set_downstream([b, c])`
        value = node.value
        if isinstance(value, ast.BinOp) and isinstance(value.op, (ast.RShift, ast.LShift)):
            before = len(self.edge_captures)
            self._collect_shift_chain(value)
            if len(self.edge_captures) > before:
                self._claimed_statement_ids.add(id(node))
        elif isinstance(value, ast.Call):
            call_name = _construct_name(value.func, self._aliases)
            if call_name == "chain":
                positions = [self._resolve_task_names(argument) for argument in value.args]
                for left, right in zip(positions, positions[1:]):
                    self._add_edges(left, right, value)
                self._claimed_statement_ids.add(id(node))
                return
            if call_name == "cross_downstream" and len(value.args) >= 2:
                self._add_edges(
                    self._resolve_task_names(value.args[0]),
                    self._resolve_task_names(value.args[1]),
                    value,
                )
                self._claimed_statement_ids.add(id(node))
                return
            if isinstance(value.func, ast.Attribute) and value.func.attr == "append" and value.args:
                owner = value.func.value
                if isinstance(owner, ast.Name):
                    appended = value.args[0]
                    if isinstance(appended, ast.Call):
                        internal = self._register_bare_operator_call(appended)
                        if internal is not None:
                            self._list_bindings.setdefault(owner.id, []).append(internal)
                            self._claimed_statement_ids.add(id(node))
                            return
                    resolved = self._resolve_task_names(appended)
                    if resolved:
                        self._list_bindings.setdefault(owner.id, []).extend(resolved)
                        self._claimed_statement_ids.add(id(node))
                        return
            # A bare TaskFlow call (`extract()` with no assignment) is a task instance keyed by its
            # def name; otherwise it may be a set_upstream/set_downstream dependency call.
            def_name, _mapped, _override = self._taskflow_def_name(value)
            if def_name is not None:
                task_var = def_name
                if task_var in self.taskflow_tasks:
                    self._taskflow_counter += 1
                    task_var = f"{def_name}__tf{self._taskflow_counter}"
                self._register_taskflow_call(value, task_var, source_reference=def_name)
                self._claimed_statement_ids.add(id(node))
            elif self._register_bare_operator_call(value) is not None:
                self._claimed_statement_ids.add(id(node))
                pass  # a bare `SomeOperator(task_id=...)` statement -- registered under a synthetic var
            elif self._register_taskgroup_call(value, None):
                self._claimed_statement_ids.add(id(node))
            else:
                before = len(self.edge_captures)
                self._collect_set_dependency(value)
                if len(self.edge_captures) > before:
                    self._claimed_statement_ids.add(id(node))
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> None:
        """Executes bounded literal/range loops with Python name rebinding semantics."""
        if not isinstance(node.target, ast.Name):
            self.unresolved_constructs.append(("dynamic_loop_target", node))
            self._claimed_statement_ids.add(id(node))
            return
        items = _static_iteration_nodes(node.iter, self._constants)
        if items is None:
            # A tuple/list of task variables is also statically bounded even though the values are
            # capture identities rather than Python literals.
            if isinstance(node.iter, (ast.List, ast.Tuple)):
                items = list(node.iter.elts)
            else:
                self.unresolved_constructs.append(("dynamic_loop_iterable", node))
                self._claimed_statement_ids.add(id(node))
                return
        for item in items:
            resolved_tasks = self._resolve_task_names(item)
            if resolved_tasks:
                self._task_bindings[node.target.id] = resolved_tasks[0] if len(resolved_tasks) == 1 else resolved_tasks
                self._constants.pop(node.target.id, None)
            else:
                value = _safe_static_value(item, self._constants)
                if value is _UNRESOLVED:
                    self.unresolved_constructs.append(("dynamic_loop_value", item))
                    self._claimed_statement_ids.add(id(node))
                    return
                self._constants[node.target.id] = value
                self._task_bindings.pop(node.target.id, None)
            for statement in node.body:
                self._visit_dag_statement(statement) if self._dag_scope_depth else self.visit(statement)
        for statement in node.orelse:
            self._visit_dag_statement(statement) if self._dag_scope_depth else self.visit(statement)
        self._claimed_statement_ids.add(id(node))

    def visit_If(self, node: ast.If) -> None:
        """Follows a statically decidable branch; records ambiguous control flow explicitly."""
        value = _safe_static_value(node.test, self._constants)
        if value is _UNRESOLVED and isinstance(node.test, ast.Name) and node.test.id in self._task_bindings:
            value = True
        if value is _UNRESOLVED:
            self.unresolved_constructs.append(("ambiguous_condition", node))
            self._claimed_statement_ids.add(id(node))
            return
        branch = node.body if bool(value) else node.orelse
        for statement in branch:
            self._visit_dag_statement(statement) if self._dag_scope_depth else self.visit(statement)
        self._claimed_statement_ids.add(id(node))

    def _collect_shift_chain(self, binop: ast.BinOp) -> None:
        self._collect_shift_expression(binop)

    def _collect_shift_expression(self, node: ast.expr) -> list[str]:
        """Collects each shift edge recursively and returns the expression's chain result."""
        tasks, _is_modifier = self._collect_shift_operand(node)
        return tasks

    def _collect_shift_operand(self, node: ast.expr) -> tuple[list[str], bool]:
        """Collects a shift operand while treating Airflow edge metadata as transparent."""
        if not isinstance(node, ast.BinOp) or not isinstance(node.op, (ast.RShift, ast.LShift)):
            is_modifier = (
                isinstance(node, ast.Call) and _construct_name(node.func, self._aliases) in _EDGE_MODIFIER_CONSTRUCTS
            )
            return self._shift_position_names(node), is_modifier
        left, left_is_modifier = self._collect_shift_operand(node.left)
        right, right_is_modifier = self._collect_shift_operand(node.right)
        if right_is_modifier:
            return left, left_is_modifier
        if left_is_modifier:
            return right, right_is_modifier
        upstream, downstream = (left, right) if isinstance(node.op, ast.RShift) else (right, left)
        self._add_edges(upstream, downstream, node)
        return right, False

    def _shift_position_names(self, node: ast.expr) -> list[str]:
        # A shift-chain position resolves to task vars. An inline TaskFlow call (`extract()`) is
        # registered as its own instance so `prep >> finalize()` doesn't drop finalize.
        if isinstance(node, (ast.List, ast.Tuple, ast.Name)):
            return self._resolve_task_names(node)
        if isinstance(node, ast.Call):
            def_name, _mapped, _override = self._taskflow_def_name(node)
            if def_name is not None:
                task_var = def_name
                if task_var in self.taskflow_tasks:
                    self._taskflow_counter += 1
                    task_var = f"{def_name}__tf{self._taskflow_counter}"
                self._register_taskflow_call(node, task_var, source_reference=def_name)
                return [task_var]
            # An inline classic operator (`Op(...) >> Op(...)` with no assignments) is still a task.
            bare_var = self._register_bare_operator_call(node)
            if bare_var is not None:
                return [bare_var]
        return []

    def _resolve_task_names(self, node: ast.expr) -> list[str]:
        """Resolves current Python bindings to stable task capture identities."""
        if isinstance(node, ast.Name):
            binding = self._task_bindings.get(node.id)
            if isinstance(binding, str):
                return [binding]
            if isinstance(binding, list):
                return list(binding)
            if node.id in self._list_bindings:
                return list(self._list_bindings[node.id])
            if node.id in self.group_vars:
                return [node.id]
            if node.id in self.operators or node.id in self.taskflow_tasks or node.id in self.taskgroup_calls:
                return [node.id]
            return []
        if isinstance(node, (ast.List, ast.Tuple)):
            return [task for item in node.elts for task in self._resolve_task_names(item)]
        return []

    def _add_edges(self, upstreams: list[str], downstreams: list[str], node: ast.AST) -> None:
        for upstream_var in upstreams:
            for downstream_var in downstreams:
                self.edges.append((upstream_var, downstream_var))
                self.edge_captures.append(
                    EdgeCapture(upstream_id=upstream_var, downstream_id=downstream_var, span=_span(node))
                )

    def _collect_set_dependency(self, call: ast.Call) -> None:
        # `x.set_upstream(y)` / `x.set_downstream(y)` where y is a Name or a list of Names.
        func = call.func
        if not (isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) and call.args):
            return
        this_names = self._resolve_task_names(func.value)
        others = self._resolve_task_names(call.args[0])
        if func.attr == "set_downstream":
            self._add_edges(this_names, others, call)
        elif func.attr == "set_upstream":
            self._add_edges(others, this_names, call)
