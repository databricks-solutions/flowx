"""Render Oozie EL for Databricks: as dynamic value references, as Python, or not at all (agentic).

Three tiers, matching the design spec (§3.4):

* tier 1 (``render_reference``): the whole value becomes a string of ``{{job.parameters.x}}`` /
  ``{{tasks.a.values.k}}`` / ``{{job.run_id}}`` references, so a task can take it as a plain parameter;
* tier 2 (``render_python``): the value needs computing, so it becomes a Python expression a generated
  bridge notebook evaluates;
* tier 3: there is no faithful translation; callers turn the node into an agentic placeholder.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

from flowx.sources.oozie.loader.el import (
    Binary,
    Call,
    ElSyntaxError,
    Expression,
    Identifier,
    Index,
    Literal,
    Ternary,
    Unary,
    parse_template,
)
from flowx.sources.oozie.loader.properties import STRIPPED_PROPERTIES, PropertyScope, parameter_name


class PropertyCycleError(ValueError):
    """Raised when properties refer to each other in a loop (``a=${b}``, ``b=${a}``)."""


class _Unsupported(Exception):
    """Internal signal: this expression has no translation in the requested tier."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(slots=True, kw_only=True)
class ElEnvironment:
    """What the renderers need to know about the workflow, plus what they discover while rendering."""

    properties: PropertyScope
    workflow_name: str
    action_task_keys: dict[str, str]
    agentic_actions: set[str] = field(default_factory=set)
    job_parameters: dict[str, str | None] = field(default_factory=dict)
    unresolved_properties: set[str] = field(default_factory=set)
    derived_properties: set[str] = field(default_factory=set)
    parameter_names: dict[str, str] = field(default_factory=dict)
    inline_properties: frozenset[str] = frozenset()


@dataclass(slots=True, kw_only=True)
class PythonExpression:
    code: str
    widget_bindings: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class NativeCondition:
    op: str
    left: str
    right: str


@dataclass(slots=True, kw_only=True)
class BridgedCondition:
    code: str
    imports: list[str]
    widget_bindings: dict[str, str]


@dataclass(slots=True, kw_only=True)
class AgenticCondition:
    reason: str


PredicateTranslation = NativeCondition | BridgedCondition | AgenticCondition

_CONDITION_OPS = {
    "eq": "EQUAL_TO",
    "ne": "NOT_EQUAL",
    "gt": "GREATER_THAN",
    "ge": "GREATER_THAN_OR_EQUAL",
    "lt": "LESS_THAN",
    "le": "LESS_THAN_OR_EQUAL",
}
_ORDERING_OPS = frozenset({"gt", "ge", "lt", "le"})
_BUILTIN_PATH_PROPERTIES = {"oozie.wf.application.path": "${workspace.file_path}"}
_AGENTIC_FUNCTIONS = {
    ("hadoop", "counters"): "MapReduce counters (hadoop:counters) have no Databricks equivalent",
    ("hadoop", "conf"): "Hadoop configuration lookups (hadoop:conf) have no Databricks equivalent",
    ("wf", "lastErrorNode"): "wf:lastErrorNode() has no Lakeflow equivalent",
    ("wf", "errorMessage"): "wf:errorMessage() has no Lakeflow equivalent",
    ("wf", "errorCode"): "Oozie error codes (wf:errorCode) don't match Databricks error codes",
    ("wf", "actionExternalId"): "wf:actionExternalId() is a YARN runtime detail",
    ("wf", "actionTrackerUri"): "wf:actionTrackerUri() is a YARN runtime detail",
    ("wf", "callback"): "wf:callback() is an Oozie runtime detail",
    ("wf", "transition"): "wf:transition() is an Oozie runtime detail",
    ("wf", "run"): "wf:run() (rerun count) has no Lakeflow equivalent",
}
_PYTHON_STRING_FUNCTIONS = {
    "concat": "_el_concat",
    "trim": "_el_trim",
    "toUpperCase": "_el_upper",
    "toLowerCase": "_el_lower",
    "replaceAll": "_el_replace_all",
    "appendAll": "_el_append_all",
    "urlEncode": "_el_url_encode",
    "firstNotNull": "_el_first_not_null",
    "timestamp": "_el_timestamp",
}
_PYTHON_FS_FUNCTIONS = {
    "exists": "_fs_exists",
    "isDir": "_fs_is_dir",
    "fileSize": "_fs_file_size",
    "dirSize": "_fs_dir_size",
}
_PYTHON_BINARY = {"+": "+", "-": "-", "*": "*", "/": "/", "%": "%"}

PYTHON_HELPERS = """import datetime as _el_datetime
import re as _el_re
import urllib.parse as _el_urllib_parse


def _el_number(value):
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    return float(text) if any(mark in text for mark in ".eE") else int(text)


def _el_bool(value):
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)


def _el_str(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _el_empty(value):
    return value is None or value == "" or (hasattr(value, "__len__") and len(value) == 0)


def _el_compare(left, operator, right):
    # JSP EL coercion: a number on either side compares numerically, a boolean as booleans, else as strings.
    if any(isinstance(side, (int, float)) and not isinstance(side, bool) for side in (left, right)):
        left, right = _el_number(left), _el_number(right)
    elif any(isinstance(side, bool) for side in (left, right)):
        left, right = _el_bool(left), _el_bool(right)
    else:
        left, right = _el_str(left), _el_str(right)
    return {"eq": left == right, "ne": left != right, "lt": left < right,
            "gt": left > right, "le": left <= right, "ge": left >= right}[operator]


def _el_concat(left, right):
    return _el_str(left) + _el_str(right)


def _el_trim(value):
    return _el_str(value).strip()


def _el_upper(value):
    return _el_str(value).upper()


def _el_lower(value):
    return _el_str(value).lower()


def _el_replace_all(value, pattern, replacement):
    python_replacement = _el_re.sub(r"\\$(\\d+)", r"\\\\g<\\1>", _el_str(replacement))
    return _el_re.sub(_el_str(pattern), python_replacement, _el_str(value))


def _el_append_all(value, suffix, delimiter):
    return _el_str(delimiter).join(item + _el_str(suffix) for item in _el_str(value).split(_el_str(delimiter)))


def _el_url_encode(value):
    return _el_urllib_parse.quote_plus(_el_str(value))


def _el_first_not_null(first, second):
    return first if first not in (None, "") else second


def _el_timestamp():
    return _el_datetime.datetime.now(_el_datetime.timezone.utc).strftime("%Y-%m-%dT%H:%MZ")


def _fs_entries(path):
    try:
        return dbutils.fs.ls(path)  # noqa: F821 - provided by the Databricks runtime
    except Exception:
        return None


def _fs_exists(path):
    return _fs_entries(_el_str(path)) is not None


def _fs_is_dir(path):
    entries = _fs_entries(_el_str(path))
    if entries is None:
        return False
    name = _el_str(path).rstrip("/").rsplit("/", 1)[-1]
    return not (len(entries) == 1 and entries[0].name == name)


def _fs_file_size(path):
    entries = _fs_entries(_el_str(path))
    name = _el_str(path).rstrip("/").rsplit("/", 1)[-1]
    return entries[0].size if entries and len(entries) == 1 and entries[0].name == name else -1


def _fs_dir_size(path):
    if not _fs_is_dir(path):
        return -1
    total = 0
    for entry in _fs_entries(_el_str(path)) or []:
        total += _fs_dir_size(entry.path) if entry.name.endswith("/") else entry.size
    return total
"""


def _string_of(value: str | int | float | bool | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


_PLAIN_KEY = re.compile(r"[A-Za-z0-9_-]+")


def _finite(value: str | int | float | bool | None) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise _Unsupported("non-finite number literal")


def _literal_argument(call: Call, position: int = 0) -> str:
    if len(call.arguments) <= position or not isinstance(call.arguments[position], Literal):
        raise _Unsupported(f"{call.namespace}:{call.function} needs a literal argument")
    return _string_of(call.arguments[position].value)  # type: ignore[union-attr]


def _check_agentic_function(call: Call) -> None:
    reason = _AGENTIC_FUNCTIONS.get((call.namespace or "", call.function))
    if reason:
        raise _Unsupported(reason)
    if call.namespace not in (None, "wf", "fs"):
        raise _Unsupported(f"custom EL function {call.namespace}:{call.function} is site-specific")


class _Renderer:
    """Renders one template; ``stack`` tracks properties being expanded so cycles are caught."""

    def __init__(self, environment: ElEnvironment, stack: tuple[str, ...] = ()) -> None:
        self.environment = environment
        self.stack = stack

    # ----- tier 1 -----------------------------------------------------------------------------------------
    def reference_of_template(self, text: str) -> str:
        parts = parse_template(text).parts
        return "".join(part if isinstance(part, str) else self.reference(part) for part in parts)

    def parameter_for(self, name: str) -> str:
        """Gives each property its own job-parameter name, suffixing ``_2``, ``_3`` when sanitising collides."""
        names = self.environment.parameter_names
        if name not in names:
            base = parameter_name(name)
            candidate, counter = base, 1
            while candidate in names.values():
                counter += 1
                candidate = f"{base}_{counter}"
            names[name] = candidate
        return names[name]

    def property_reference(self, name: str) -> str:
        if name in STRIPPED_PROPERTIES:
            return ""
        if name in _BUILTIN_PATH_PROPERTIES:
            return _BUILTIN_PATH_PROPERTIES[name]
        if name in self.stack:
            raise PropertyCycleError("property cycle: " + " -> ".join((*self.stack[self.stack.index(name) :], name)))
        value = self.environment.properties.lookup(name)
        if name in self.environment.inline_properties and value is not None:
            if "${" in value:
                self.environment.derived_properties.add(name)
            return _Renderer(self.environment, (*self.stack, name)).reference_of_template(value)
        parameter = self.parameter_for(name)
        if value is None:
            self.environment.job_parameters.setdefault(parameter, None)
            self.environment.unresolved_properties.add(name)
            return f"{{{{job.parameters.{parameter}}}}}"
        if "${" in value:
            self.environment.derived_properties.add(name)
            return _Renderer(self.environment, (*self.stack, name)).reference_of_template(value)
        self.environment.job_parameters[parameter] = value
        return f"{{{{job.parameters.{parameter}}}}}"

    def action_value_reference(self, call: Call, key: Expression) -> str:
        action = _literal_argument(call)
        if action in self.environment.agentic_actions:
            raise _Unsupported(f"wf:actionData('{action}') reads output of an agentic action")
        task_key = self.environment.action_task_keys.get(action)
        if task_key is None or not isinstance(key, Literal):
            raise _Unsupported(f"wf:actionData('{action}') can't be resolved")
        if not _PLAIN_KEY.fullmatch(_string_of(key.value)):
            raise _Unsupported(f"wf:actionData key {key.value!r} isn't a plain identifier")
        return f"{{{{tasks.{task_key}.values.{_string_of(key.value)}}}}}"

    def reference(self, expression: Expression) -> str:
        if isinstance(expression, Literal):
            _finite(expression.value)
            return _string_of(expression.value)
        if isinstance(expression, Identifier):
            return self.property_reference(expression.name)
        if isinstance(expression, Index):
            base = expression.target
            while isinstance(base, Index):
                base = base.target
            if isinstance(base, Call):
                _check_agentic_function(base)
        if isinstance(expression, Index) and isinstance(expression.target, Call):
            call = expression.target
            if (call.namespace, call.function) == ("wf", "actionData"):
                return self.action_value_reference(call, expression.key)
        if isinstance(expression, Call):
            _check_agentic_function(expression)
            match (expression.namespace, expression.function):
                case ("wf", "conf"):
                    return self.property_reference(_literal_argument(expression))
                case ("wf", "id"):
                    return "{{job.run_id}}"
                case ("wf", "name"):
                    return self.environment.workflow_name
                case ("wf", "user"):
                    return "${workspace.current_user.userName}"
                case ("wf", "appPath"):
                    return "${workspace.file_path}"
        raise _Unsupported("needs computing at run time")

    # ----- tier 2 -----------------------------------------------------------------------------------------
    def python_of_template(self, text: str, bindings: dict[str, str]) -> str:
        parts = parse_template(text).parts
        meaningful = [part for part in parts if not (isinstance(part, str) and not part.strip())]
        if len(meaningful) == 1 and not isinstance(meaningful[0], str):
            return self.python(meaningful[0], bindings)
        return (
            " + ".join(
                repr(part) if isinstance(part, str) else f"_el_str({self.python(part, bindings)})" for part in parts
            )
            or "''"
        )

    def widget(self, binding_name: str, reference: str, bindings: dict[str, str]) -> str:
        for existing_name, existing_reference in bindings.items():
            if existing_reference == reference:
                return f"dbutils.widgets.get({existing_name!r})"
        base = parameter_name(binding_name)
        unique, counter = base, 1
        while unique in bindings:
            counter += 1
            unique = f"{base}_{counter}"
        bindings[unique] = reference
        return f"dbutils.widgets.get({unique!r})"

    def python_property(self, name: str, bindings: dict[str, str]) -> str:
        try:
            reference = self.property_reference(name)
        except _Unsupported:
            value = self.environment.properties.lookup(name) or ""
            return _Renderer(self.environment, (*self.stack, name)).python_of_template(value, bindings)
        if name in STRIPPED_PROPERTIES:
            return "''"
        return self.widget(f"p_{self.parameter_for(name)}", reference, bindings)

    def python(self, expression: Expression, bindings: dict[str, str]) -> str:
        if isinstance(expression, Literal):
            _finite(expression.value)
            return repr(expression.value)
        if isinstance(expression, Identifier):
            return self.python_property(expression.name, bindings)
        if isinstance(expression, Index):
            reference = self.reference(expression)
            task_key, _, value_key = reference.removeprefix("{{tasks.").removesuffix("}}").partition(".values.")
            return self.widget(f"v_{task_key}_{value_key}", reference, bindings)
        if isinstance(expression, Unary):
            operand = self.python(expression.operand, bindings)
            return {
                "not": f"(not _el_bool({operand}))",
                "-": f"(-_el_number({operand}))",
                "empty": f"_el_empty({operand})",
            }[expression.operator]
        if isinstance(expression, Binary):
            left, right = self.python(expression.left, bindings), self.python(expression.right, bindings)
            if expression.operator in _CONDITION_OPS:
                return f"_el_compare({left}, {expression.operator!r}, {right})"
            if expression.operator in ("and", "or"):
                return f"(_el_bool({left}) {expression.operator} _el_bool({right}))"
            return f"(_el_number({left}) {_PYTHON_BINARY[expression.operator]} _el_number({right}))"
        if isinstance(expression, Ternary):
            condition = self.python(expression.condition, bindings)
            if_true = self.python(expression.if_true, bindings)
            if_false = self.python(expression.if_false, bindings)
            return f"({if_true} if _el_bool({condition}) else {if_false})"
        if isinstance(expression, Call):
            _check_agentic_function(expression)
            arguments = ", ".join(self.python(argument, bindings) for argument in expression.arguments)
            if expression.namespace is None and expression.function in _PYTHON_STRING_FUNCTIONS:
                return f"{_PYTHON_STRING_FUNCTIONS[expression.function]}({arguments})"
            if expression.namespace == "fs" and expression.function in _PYTHON_FS_FUNCTIONS:
                return f"{_PYTHON_FS_FUNCTIONS[expression.function]}({arguments})"
            if expression.namespace == "wf":
                if expression.function == "conf":
                    return self.python_property(_literal_argument(expression), bindings)
                return self.widget(f"wf_{expression.function}", self.reference(expression), bindings)
        raise _Unsupported(f"EL construct {type(expression).__name__} isn't supported")


def render_reference(text: str, environment: ElEnvironment) -> str | None:
    """Tier 1: renders *text* as a string of Databricks references, or ``None`` if it needs computing."""
    try:
        return _Renderer(environment).reference_of_template(text)
    except (_Unsupported, ElSyntaxError):
        return None


def render_python(text: str, environment: ElEnvironment) -> PythonExpression | None:
    """Tier 2: renders *text* as a Python expression for a bridge notebook, or ``None`` (agentic)."""
    bindings: dict[str, str] = {}
    try:
        return PythonExpression(
            code=_Renderer(environment).python_of_template(text, bindings), widget_bindings=bindings
        )
    except (_Unsupported, ElSyntaxError):
        return None


def _native(expression: Expression, renderer: _Renderer) -> NativeCondition | None:
    try:
        if isinstance(expression, Literal) and isinstance(expression.value, bool):
            return NativeCondition(op="EQUAL_TO", left="true", right=_string_of(expression.value))
        if isinstance(expression, Binary) and expression.operator in _CONDITION_OPS:
            numeric_literal = any(
                isinstance(side, Literal) and isinstance(side.value, (int, float)) and not isinstance(side.value, bool)
                for side in (expression.left, expression.right)
            )
            if expression.operator in _ORDERING_OPS and not numeric_literal:
                return None  # EL compares two strings lexically; condition_task only orders numbers.
            boolean_literal = any(
                isinstance(side, Literal) and isinstance(side.value, bool)
                for side in (expression.left, expression.right)
            )
            if expression.operator in ("eq", "ne") and boolean_literal:
                return None  # EL coerces "TRUE" to true case-insensitively; condition_task compares strings.
            if expression.operator in ("eq", "ne") and numeric_literal:
                return (
                    None  # EL compares numerically ("05" eq 5 is true); condition_task compares the strings (measured).
                )
            return NativeCondition(
                op=_CONDITION_OPS[expression.operator],
                left=renderer.reference(expression.left),
                right=renderer.reference(expression.right),
            )
    except (_Unsupported, ElSyntaxError):
        return None
    return None


def translate_predicate(text: str, environment: ElEnvironment) -> PredicateTranslation:
    """Picks the cheapest faithful translation for a decision ``<case>`` predicate."""
    try:
        parts = [part for part in parse_template(text).parts if not (isinstance(part, str) and not part.strip())]
    except ElSyntaxError as error:
        return AgenticCondition(reason=f"couldn't parse EL predicate: {error}")
    renderer = _Renderer(environment)
    if len(parts) == 1 and not isinstance(parts[0], str):
        native = _native(parts[0], renderer)
        if native is not None:
            return native
    bindings: dict[str, str] = {}
    try:
        code = renderer.python_of_template(text, bindings)
    except _Unsupported as unsupported:
        return AgenticCondition(reason=unsupported.reason)
    except ElSyntaxError as error:
        return AgenticCondition(reason=f"couldn't parse EL in a referenced property: {error}")
    return BridgedCondition(
        code=f'"true" if _el_bool({code}) else "false"', imports=[PYTHON_HELPERS], widget_bindings=bindings
    )
