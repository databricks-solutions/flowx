"""Source code a DAG imports from its own project: custom operators, callables, shared defaults, queries.

flowx converts one DAG file at a time, so a task built from an imported custom operator or callable
becomes a gap. Attaching the imported definition to the gap gives whoever translates it the code
Airflow actually runs. Only files inside the DAG folder, or its parent folders up to the folder discover
was pointed at, are read, since whatever is read is copied into the migration output.
"""

from __future__ import annotations

import ast
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Definitions above this size are recorded by path and size only.
MAXIMUM_ATTACHED_SOURCE_BYTES = 64 * 1024
# Module files above this size are not read.
MAXIMUM_READ_BYTES = 1024 * 1024

# Packages whose code is not part of the user's project.
_EXTERNAL_PACKAGES = frozenset({"airflow", "pendulum", "cosmos", "airflow_dbt"}) | frozenset(sys.stdlib_module_names)


@dataclass(frozen=True, slots=True, kw_only=True)
class ImportedName:
    """One name a module imports: ``from <module> import <attribute> as <local>`` or ``import <module>``."""

    local: str
    module: str
    attribute: str | None
    level: int = 0

    def statement(self) -> str:
        """Returns the import as Python source, as the DAG wrote it."""
        module = "." * self.level + self.module
        if self.attribute is None:
            return f"import {module}" + (f" as {self.local}" if self.local != module.split(".")[0] else "")
        alias = f" as {self.local}" if self.local != self.attribute else ""
        return f"from {module} import {self.attribute}{alias}"


def import_table(module: ast.Module) -> dict[str, ImportedName]:
    """Returns the module-level imports of a DAG file by the local name each binds."""
    table: dict[str, ImportedName] = {}
    for node in module.body:
        if isinstance(node, ast.Import):
            for item in node.names:
                local = item.asname or item.name.split(".")[0]
                table[local] = ImportedName(local=local, module=item.name if item.asname else local, attribute=None)
        elif isinstance(node, ast.ImportFrom):
            for item in node.names:
                if item.name != "*":
                    table[item.asname or item.name] = ImportedName(
                        local=item.asname or item.name, module=node.module or "", attribute=item.name, level=node.level
                    )
    return table


def search_roots(dag_file: Path, source_root: Path | None) -> tuple[Path, ...]:
    """Returns the folders an import may resolve from: the DAG folder, then each parent up to *source_root*.

    Airflow puts the DAGs folder on ``sys.path``, and projects often keep shared code beside or above the
    DAG's own folder.
    """
    folder = dag_file.absolute().parent
    roots = [folder]
    if source_root is not None:
        top = source_root.absolute()
        if folder.is_relative_to(top):
            roots.extend(parent for parent in folder.parents if parent.is_relative_to(top))
    return tuple(roots)


def _is_project_import(imported: ImportedName) -> bool:
    return imported.level > 0 or imported.module.split(".")[0] not in _EXTERNAL_PACKAGES


def _module_files(imported: ImportedName, roots: tuple[Path, ...]) -> list[tuple[Path, Path, bool]]:
    """Returns ``(file, root, is_module_itself)`` candidates for an import, in lookup order.

    For ``from package import name``, *name* may be a submodule, in which case the whole file is the
    definition.
    """
    bases: tuple[Path, ...] = roots
    if imported.level:
        # ``from .x`` is the DAG's own folder; each further dot climbs one folder, never past the roots.
        climbs = imported.level - 1
        bases = (roots[climbs],) if climbs < len(roots) else ()
    parts = [part for part in imported.module.split(".") if part]
    candidates: list[tuple[Path, Path, bool]] = []
    for base in bases:
        module_path = base.joinpath(*parts)
        candidates.append((module_path.with_suffix(".py") if parts else base / "__init__.py", base, False))
        candidates.append((module_path / "__init__.py", base, False))
        if imported.attribute is not None:
            candidates.append((module_path / f"{imported.attribute}.py", base, True))
    return candidates


def _definition_source(text: str, name: str) -> str | None:
    """Returns the source of the top-level class, function, or assignment named *name* in a module."""
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return None
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            start = min([node.lineno, *(decorator.lineno for decorator in node.decorator_list)])
            return "\n".join(text.splitlines()[start - 1 : node.end_lineno])
        targets = (
            node.targets if isinstance(node, ast.Assign) else [node.target] if isinstance(node, ast.AnnAssign) else []
        )
        if any(isinstance(target, ast.Name) and target.id == name for target in targets):
            return ast.get_source_segment(text, node)
    return None


def _reexported_source(package_init: Path, text: str, name: str, root: Path) -> tuple[Path, str] | None:
    """Follows one ``from .module import name`` in a package ``__init__`` to the definition it re-exports."""
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return None
    for node in tree.body:
        if not (isinstance(node, ast.ImportFrom) and node.level == 1 and node.module):
            continue
        original = next((item.name for item in node.names if (item.asname or item.name) == name), None)
        if original is None:
            continue
        module = package_init.parent.joinpath(*node.module.split("."))
        for candidate in (module.with_suffix(".py"), module / "__init__.py"):
            if candidate.is_file() and candidate.resolve().is_relative_to(root.resolve()):
                content = _read_text(candidate)
                source = _definition_source(content, original) if isinstance(content, str) else None
                if source is not None:
                    return candidate, source
    return None


def _read_text(path: Path) -> str | dict[str, Any]:
    """Returns a file's text, or the fields that say why it was not read."""
    try:
        size = path.stat().st_size
        if size > MAXIMUM_READ_BYTES:
            return {"size_bytes": size, "unresolved": "the module file is too large to read"}
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        return {"unresolved": f"could not be read ({error})"}


def resolve_import(imported: ImportedName, roots: tuple[Path, ...]) -> dict[str, Any]:
    """Describes where an imported name is defined, attaching its source when it is found and small."""
    record: dict[str, Any] = {"name": imported.local, "import": imported.statement()}
    for candidate, root, is_module_itself in _module_files(imported, roots):
        if not candidate.is_file():
            continue
        if not candidate.resolve().is_relative_to(root.resolve()):
            return {**record, "unresolved": f"{candidate} is a symbolic link that leads outside {root}"}
        text = _read_text(candidate)
        if not isinstance(text, str):
            return {**record, "resolved_path": str(candidate), **text}
        location: Path = candidate
        source = (
            text if is_module_itself or imported.attribute is None else _definition_source(text, imported.attribute)
        )
        if source is None and imported.attribute is not None and candidate.name == "__init__.py":
            reexported = _reexported_source(candidate, text, imported.attribute, root)
            if reexported is not None:
                location, source = reexported
        if source is None:
            # The name may be a submodule of the package; keep looking in later candidates.
            continue
        record["resolved_path"] = str(location)
        size = len(source.encode("utf-8"))
        if size > MAXIMUM_ATTACHED_SOURCE_BYTES:
            record["size_bytes"] = size
            record["content_note"] = "too large to attach; read it from resolved_path"
        else:
            record["content"] = source
        return record
    searched = ", ".join(str(root) for root in roots)
    return {**record, "unresolved": f"its definition was not found under {searched}"}


def imported_sources(node: ast.AST, imports: dict[str, ImportedName], roots: tuple[Path, ...]) -> list[dict[str, Any]]:
    """Returns the project definitions an expression uses through the DAG file's imports, in order of use."""
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for name in ast.walk(node):
        if not isinstance(name, ast.Name) or name.id in seen:
            continue
        seen.add(name.id)
        imported = imports.get(name.id)
        if imported is not None and _is_project_import(imported):
            records.append(resolve_import(imported, roots))
    return records
