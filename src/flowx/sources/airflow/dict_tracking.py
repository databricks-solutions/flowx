"""Following a module-level dict, such as a DAG's ``default_args``, through the statements that change it.

A DAG file often builds ``default_args`` in steps: a dict literal, then item assignments, ``update()``,
``pop()``, and ``**`` spreads, before passing it to the DAG. The loader and the source audit both need the
dict as it stands when the DAG is built, and both must give up on a dict they cannot follow, so they share
this tracker.
"""

from __future__ import annotations

import ast
from collections.abc import Callable

CHANGING_METHODS = frozenset({"update", "pop", "popitem", "setdefault", "clear"})
_READING_FUNCTIONS = frozenset({"dict", "len", "list", "print", "repr", "sorted", "str", "deepcopy", "dumps"})
# Airflow constructors and decorator factories keep the arguments they are given without changing them.
_CONSTRUCTOR_SUFFIXES = ("Operator", "Sensor", "DAG", "TaskGroup", "Group")
_CONSTRUCTOR_FUNCTIONS = frozenset({"dag", "task_group"})


def _local_name(function: ast.expr) -> str:
    if isinstance(function, ast.Attribute):
        return function.attr
    return function.id if isinstance(function, ast.Name) else ""


class DictTracker:
    """The items of each module-level dict built from literal string keys, kept current through its changes.

    A name stops being tracked as soon as a change cannot be followed: an unknown key, an alias that shares the
    dict, an augmented assignment, or a call that may change it.

    Args:
        stores_arguments: Returns whether a call to the named function only stores its arguments, such as an
            operator class or a TaskFlow task, in addition to the Airflow constructors every caller shares.
        function_name: Returns the name a call's function stands for, resolved through the file's imports, so
            ``from airflow import DAG as Workflow`` makes ``Workflow(...)`` a DAG constructor.

    Attributes:
        items: The tracked dicts by name, each as its items in order.
    """

    def __init__(
        self,
        stores_arguments: Callable[[str], bool] = lambda name: False,
        function_name: Callable[[ast.expr], str] = _local_name,
    ) -> None:
        self.items: dict[str, dict[str, ast.expr]] = {}
        self._stores_arguments = stores_arguments
        self._function_name = function_name

    def literal(self, node: ast.expr) -> dict[str, ast.expr] | None:
        """Reads an expression as a dict with string keys.

        Args:
            node: A tracked name, a dict literal with string keys, or ``dict(key=value)``. A ``**`` spread of a
                tracked name is inlined.

        Returns:
            The dict's items, or None when the expression is anything else.
        """
        if isinstance(node, ast.Name):
            tracked = self.items.get(node.id)
            return dict(tracked) if tracked is not None else None
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "dict" and not node.args:
            if not all(keyword.arg for keyword in node.keywords):
                return None
            return {str(keyword.arg): keyword.value for keyword in node.keywords}
        if not isinstance(node, ast.Dict):
            return None
        items: dict[str, ast.expr] = {}
        for key, value in zip(node.keys, node.values):
            if key is None:
                spread = self.items.get(value.id) if isinstance(value, ast.Name) else None
                if spread is None:
                    return None
                items.update(spread)
            elif isinstance(key, ast.Constant) and isinstance(key.value, str):
                items.pop(key.value, None)
                items[key.value] = value
            else:
                return None
        return items

    def as_dict(self, node: ast.expr) -> ast.Dict | None:
        """Reads an expression like :meth:`literal` and returns the items as one flat dict literal, or None."""
        items = self.literal(node)
        if items is None:
            return None
        return ast.Dict(keys=[ast.Constant(value=key) for key in items], values=list(items.values()))

    def assign(self, targets: list[ast.expr], value: ast.expr) -> set[str]:
        """Follows an assignment, including ``name[key] = value``.

        Args:
            targets: The assignment's targets.
            value: The assigned value.

        Returns:
            The names whose contents changed or stopped being tracked.
        """
        changed = self.follow_calls(value)
        for target in targets:
            if isinstance(target, ast.Name):
                items = self.literal(value) if len(targets) == 1 and not isinstance(value, ast.Name) else None
                if items is None:
                    self.items.pop(target.id, None)
                else:
                    self.items[target.id] = items
                changed.add(target.id)
            elif isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name):
                name, key = target.value.id, target.slice
                if name in self.items and isinstance(key, ast.Constant) and isinstance(key.value, str):
                    self.items[name] = {**self.items[name], key.value: value}
                else:
                    self.items.pop(name, None)
                changed.add(name)
            else:
                for node in ast.walk(target):
                    if isinstance(node, ast.Name):
                        self.items.pop(node.id, None)
                        changed.add(node.id)
        if isinstance(value, ast.Name) and value.id in self.items:
            # An alias shares the dict, so a later change through either name cannot be followed.
            self.items.pop(value.id, None)
            changed.add(value.id)
        return changed

    def augment(self, target: ast.expr) -> set[str]:
        """Follows ``target op= value``, which may change a dict in place (``|=``), so it ends the tracking.

        Returns:
            The name that stopped being tracked, if any.
        """
        if isinstance(target, ast.Name):
            self.items.pop(target.id, None)
            return {target.id}
        return set()

    def delete(self, targets: list[ast.expr]) -> set[str]:
        """Follows ``del name[key]`` and ``del name``.

        Returns:
            The names whose contents changed or stopped being tracked.
        """
        changed: set[str] = set()
        for target in targets:
            if isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name):
                name, key = target.value.id, target.slice
                if name in self.items and isinstance(key, ast.Constant) and isinstance(key.value, str):
                    self.items[name] = {item: value for item, value in self.items[name].items() if item != key.value}
                else:
                    self.items.pop(name, None)
                changed.add(name)
            elif isinstance(target, ast.Name):
                self.items.pop(target.id, None)
                changed.add(target.id)
        return changed

    def follow_calls(self, node: ast.AST) -> set[str]:
        """Applies the changes the calls in an expression make to tracked dicts.

        ``update()`` and ``pop("key")`` are followed. A tracked dict passed to a function that may change it, even
        as ``default_args=``, or given to any other changing method, stops being tracked. Calls that only store
        or read their arguments keep the tracking.

        Returns:
            The names whose contents changed or stopped being tracked.
        """
        changed: set[str] = set()
        for call in ast.walk(node):
            if not isinstance(call, ast.Call):
                continue
            if not self._only_stores_or_reads(call):
                values = [*call.args, *(keyword.value for keyword in call.keywords)]
                for value in values:
                    passed = value.value if isinstance(value, ast.Starred) else value
                    if isinstance(passed, ast.Name) and passed.id in self.items:
                        self.items.pop(passed.id)
                        changed.add(passed.id)
            if (
                isinstance(call.func, ast.Attribute)
                and isinstance(call.func.value, ast.Name)
                and call.func.value.id in self.items
                and call.func.attr in CHANGING_METHODS
            ):
                self._apply_method(call.func.value.id, call)
                changed.add(call.func.value.id)
        return changed

    def _only_stores_or_reads(self, call: ast.Call) -> bool:
        function = self._function_name(call.func).rsplit(".", 1)[-1]
        return (
            function in _READING_FUNCTIONS
            or function in _CONSTRUCTOR_FUNCTIONS
            or function.endswith(_CONSTRUCTOR_SUFFIXES)
            or self._stores_arguments(function)
        )

    def _apply_method(self, name: str, call: ast.Call) -> None:
        """Follows ``update({...})``, ``update(key=value)``, and ``pop("key")``; any other change ends tracking."""
        assert isinstance(call.func, ast.Attribute)
        key = call.args[0] if call.args else None
        if (
            call.func.attr == "pop"
            and isinstance(key, ast.Constant)
            and isinstance(key.value, str)
            and len(call.args) <= 2
        ):
            self.items[name] = {item: value for item, value in self.items[name].items() if item != key.value}
            return
        if call.func.attr == "update" and len(call.args) <= 1 and all(keyword.arg for keyword in call.keywords):
            updated = self.literal(call.args[0]) if call.args else {}
            if updated is not None:
                updated.update({str(keyword.arg): keyword.value for keyword in call.keywords})
                self.items[name] = {**self.items[name], **updated}
                return
        self.items.pop(name, None)
