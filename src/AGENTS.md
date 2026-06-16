# Source Code Conventions

 This file documents *code-style* expectations for everything under
`src/flowx/`.  When in doubt, match the existing code in the
neighbouring module. This is a companion to the top-level [AGENTS.md](../AGENTS.md)
 file which documents the plugin architecture. 

## Naming

- **No abbreviated identifiers.**  Use full descriptive names: `notebook`
  not `nb`, `destination` not `dest`, `context` not `ctx`,
  `type_properties` not `tp`, `linked_service` not `ls`, `keyword` not
  `kw`.  Single-letter loop counters (`i`, `j`, `k`) are fine; `e` for an
  exception is fine; `df` for a Spark DataFrame in *generated user-facing
  notebook code* is fine.
- **Module-private names start with `_`.**  Anything imported by another
  module must not have a leading underscore -- promote it to public (or
  move it to a more appropriate module) instead of importing a private
  name.
- **Constants in `UPPER_SNAKE`** (e.g. `JDBC_SOURCE_TYPES`); modules and
  functions in `lower_snake`; classes in `PascalCase`.

## Docstrings

- First line is **third-person present indicative** describing what the
  function does: ``"""Converts X to Y."""`` rather than ``"""Convert
  X..."""`` or ``"""This function converts..."""``.
- **Public** classes and functions use the **Google docstring format**:
  a one-line summary, then ``Attributes:`` (classes), ``Args:``,
  ``Returns:``, ``Raises:``, and ``Notes:`` sections as applicable.
- Private helpers keep a brief one-line summary; add ``Args:`` /
  ``Returns:`` only when the types are genuinely ambiguous.
- Non-obvious design rationale (a constraint, a prior bug, an external
  spec) goes in a ``Notes:`` section or a one-line comment that points to
  the relevant ``AGENTS.md`` design-notes entry -- never as a
  multi-paragraph comment block.

## Comments

- The bar is: **could a new developer trace through this codebase and
  understand the line without the comment?**  If yes, delete the comment.
- Comments explain **why** -- a constraint, a non-obvious invariant, a
  reference to an external spec, a prior bug being avoided.  They never
  restate **what** the code does.
- Prefer a self-documenting variable or function name over a comment.
- One-line comments only; multi-line block comments are a smell.

## Imports

- **Top-level only.**  Inline imports inside function bodies are allowed
  only when avoiding a real circular import.  When you do need one, add a
  one-line comment naming the cycle.
- **No pointless aliases.**  Don't write ``import json as _json`` or
  ``from x import Y as _Y`` unless an alias is genuinely required to
  avoid a name collision.
- **No cross-module private imports.**  If module A needs a name from
  module B, that name lives at the top level of B without a leading
  underscore.  Lazy or circular cases should use a public re-export.

## Mutability

- **Models are immutable by convention.**  All dataclasses use
  ``@dataclass(slots=True, kw_only=True)``.  Frozen dataclasses are
  preferred when the type carries no internal mutation.
- **Collections accumulated across helpers should be returned, not
  mutated.**  Helpers that "fold up" results (e.g.
  :class:`PreparedArtifacts`) take an immutable accumulator and return a
  new one rather than mutating shared lists.
- ``TranslationContext`` is **never modified in place** -- always return
  a new instance via ``context.with_*(...)``.

## Function shape

- Keep functions short.  When a function grows past ~40 lines or 3
  levels of nesting, extract a private helper.
- Use **guard clauses** (early ``return``/``continue``) instead of
  building deep ``if`` pyramids.
- Prefer comprehensions and generator expressions over explicit
  ``for``-append loops when the comprehension fits on one screen line.

## Duplication

- Two near-identical 4-6 line blocks inside a single module: extract a
  module-private helper.
- The same block across modules: extract to a public helper in the most
  semantically appropriate module (often
  ``flowx/utils.py`` or
  ``flowx/preparer/activity_preparers/helpers.py``).

## Tests

- Every translator and preparer module has a corresponding ``tests/unit/``
  test file.  When adding a new helper, add a unit test for it.
- Tests run with ``make test``; integration tests run with
  ``make integration`` (require ADF fixtures).
- Behavioural changes must be verified by checking that bundle YAML
  output for the verdi-test fixtures is byte-identical (or, if it
  differs, that the diff matches the intended change).

## Tooling

- ``make fmt`` runs ``ruff format``, ``ruff check --fix``, and ``mypy``.
- Line length is 120 characters.
- Python 3.12+ syntax is OK (e.g. ``dict[str, Any]`` over
  ``Dict[str, Any]``, ``X | None`` over ``Optional[X]``).
