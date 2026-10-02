# flowx Agentic Validation — Implementation Contract

Branch: `agentic-validation`
Source PRD: [\[PRD\] flowx Agentic Validation](https://docs.google.com/document/d/1ZaWznuw-jq9qtnRIOOEURCxLUEd__dWoXqmB-PTa6ck/edit)
Status: Draft for implementation. Line references are against `main` at branch point (`b8d0766`).

This document is the plan of record. It says exactly what to build, where it plugs into the
existing code, the class and method signatures, the call stacks, and the error cases — so the
implementation is a matter of filling in the bodies, not making architecture decisions.

---

## 1. What we are building

A fourth flowx phase, **`validate`**, that runs after `package`. It:

1. Runs the existing deterministic (Tier-0) checks over the converted IR and the generated bundle.
2. Grades each converted object against the original pipeline's intent, agentically.
3. Stamps every reviewed object with a verdict/score/findings.
4. Writes a `validation_report.json`, and loops (grade → remediate → re-grade) until a correctness
   threshold is met or an iteration budget runs out.
5. Optionally appends per-iteration rows to a Unity Catalog table.

It runs two ways, matching every other phase:
- as a step inside `flowx-migrate` (end to end), and
- standalone via a new `flowx-validate` skill.

### PRD scope honored here

| PRD tier | In this contract |
|----|----|
| **P0** (standalone/step phase, consume convert+package artifacts, static gate, agentic grade, stamp, iterative loop, per-iteration records) | Yes — full design below |
| **P1** (dry-run, interactive remediation gate, suggested fixes, resume from prior state, skip unchanged objects, object diffs, UC table) | Yes — designed in; called out per feature |
| **P2** (custom rules, dashboard, multi-reviewer consensus, customer patterns) | Extension points left open; not implemented |
| **P3** (auto-deploy, actual fine-tuning, runtime data reconciliation) | Out of scope |

---

## 2. The one architecture decision everything follows from

**In flowx, Python never calls an LLM. The skill (the in-session agent) does the reasoning; Python
provides deterministic primitives that prepare, validate, and apply the agent's work.**

Evidence: `src/flowx/agentic.py:1-6` — *"Flowx remains the owner of source parsing, task identity,
graph structure, policy, IR, and packaging. A provider may reason about one captured leaf gap and
return only a constrained payload; this module validates that payload and applies it to an immutable
deterministic baseline."* There are zero LLM calls in `agentic.py`; the `flowx-resolve-airflow-gaps`
skill is what reasons.

**Consequence for the PRD pseudo-code.** The PRD sketches `run_validation_loop(...)` calling
`review_objects(...)` and `apply_remediations(...)` inside Python. We cannot ship that as written —
`review_objects` and remediation are LLM work. Instead we split it exactly like the existing agentic
resolution flow (`resolve-agentic` with `action = prepare | stage | apply`, see
`src/flowx/adapter/__main__.py:96-140` and `src/flowx/agentic.py:172-435`):

```
                         ┌─────────────── the loop lives in the SKILL ───────────────┐
                         │                                                            │
  flowx validate prepare │→ agent grades each object →│ flowx validate stage │→ ...   │
  (Python: static tier,  │  (LLM: reads envelopes,    │ (Python: validate     │       │
   emit review envelopes,│   writes ValidationStamps) │  stamps vs. envelopes)│       │
   hash + skip logic)    │                            │                       │       │
                         │                                                    ▼       │
                         │          flowx validate apply ←────────────────────        │
                         │          (Python: assemble report, approval_rate,          │
                         │           exit-criteria, per-iteration record,             │
                         │           optional remediation apply/patch)                │
                         │                     │                                      │
                         │        threshold met / budget spent? ── no ──→ agent       │
                         │                     │                          remediates  │
                         │                    yes → done                  bundle,     │
                         └─────────────────────────────────────────────  loop again ─┘
```

Python owns: the static gate, envelope construction, content hashing, stamp validation, report
assembly, approval math, exit-criteria, oscillation detection, and the UC-table write. The skill
owns: grading objects, writing stamps, deciding remediations, and editing the bundle.

This keeps the correctness guarantees deterministic and testable, and keeps token spend and model
choice in the session where they belong.

---

## 3. Integration points (where each change lands)

All line numbers are current-state anchors; insert adjacent to the listed sibling.

### 3.1 New Python package: `src/flowx/validate/`

Today `src/flowx/validate/` holds only the two Tier-0 modules (`bundle_invariants.py`,
`dag_equivalence.py`) and re-exports them from `__init__.py`. We add:

| New file | Responsibility |
|---|---|
| `src/flowx/validate/models.py` | Dataclasses + enums + error classes (§5) |
| `src/flowx/validate/static.py` | Run + normalize Tier-0 checks into `ValidationStamp`s (§6.1) |
| `src/flowx/validate/envelopes.py` | Build per-object `ReviewEnvelope`s; content hashing; skip-unchanged logic (§6.2) |
| `src/flowx/validate/agentic.py` | `prepare_validation` / `stage_validation` / `apply_validation` — the three primitives (§6.3). PRD names this file explicitly. |
| `src/flowx/validate/report.py` | Read/write `validation_report.json`; `approval_rate`; exit-criteria; oscillation check (§6.4) |
| `src/flowx/validate/cli.py` | `main(argv)` phase entry point, source-independent (§4, §7.1) |

`__init__.py` gains re-exports of the new public names alongside the existing ones.

### 3.2 CLI: two surfaces, mirroring the two kinds of adapter subcommand

The adapter has **phase runners** (discover/convert/package, forwarded to a source module in
`_run_phase`, `src/flowx/adapter/__main__.py:581-641`) and **explicit subcommands** with an
`action` (e.g. `resolve-agentic`, `src/flowx/adapter/__main__.py:96-140` + subparser at `:422-476`).
We use **both**, because validate has a coarse phase entry and a fine-grained agentic loop:

1. **Phase entry — source-independent, like `package`.** Add `"validate"` to the phase tuple at
   `src/flowx/adapter/__main__.py:69` and to the subparser loop tuple at `:534`. In `_run_phase`
   (`:603`), route `validate` to a fixed module the same way `package` routes to `_PACKAGE_MODULE`:
   ```python
   if phase in ("package", "validate"):
       module_path = _VALIDATE_MODULE if phase == "validate" else _PACKAGE_MODULE
       aliases = {}
   ```
   `_VALIDATE_MODULE = "flowx.validate.cli"`. This runs the whole loop non-interactively with
   defaults (used by `flowx-migrate`). It is source-independent: it reads the report and bundle from
   `<output_dir>`, and reads the verbatim ADF source from `<output_dir>/metadata/<pipeline>.arm.json`
   for the DAG-equivalence tier (no original `--source-path` needed).

2. **Agentic subcommand — `validate` with `action`, mirroring `resolve-agentic`.** Add
   `_run_validate(args)` next to `_run_resolve_agentic` (`:96`) and a subparser next to the
   `resolve_agentic` one (`:422`). Actions: `prepare | stage | apply`. This is what the
   `flowx-validate` skill drives turn by turn. Argument shape (mirror `resolve-agentic`):
   ```
   validate <prepare|stage|apply>
     --output-dir PATH        (required)  shared migration dir
     --report PATH            (prepare)   translation report; default <output_dir>/.work/translation_report.stamped.json → .json
     --threshold FLOAT        (apply)     default 0.90
     --max-iterations INT     (apply)     default 3
     --stamp PATH             (stage)     agent-authored ValidationStamp JSON; repeatable (append)
     --ruleset PATH           (prepare)   optional custom ruleset (P2); default built-in
     --dry-run                (apply)     P1: assemble+report but do not apply remediations
     --iteration INT          (prepare/apply) current loop index (1-based)
   ```
   Register `_run_validate` in the same dispatch block that routes the other explicit subcommands
   (the `args.command == ...` chain in `main`, alongside where `_run_resolve_agentic` is wired).

> Naming note: the phase name is `validate`; the fine-grained subcommand is also `validate` with an
> `action` positional (exactly as `resolve-agentic` takes `prepare|stage|apply`). `_run_phase`
> intercepts `validate` **only** when it is the first raw token with no `action` in a phase context;
> keep the subcommand explicit (`validate prepare …`) so the argparse path handles it. If the shared
> token proves ambiguous in `main`'s pre-parse at `:69`, name the subcommand `validate-agentic` to
> match `resolve-agentic` precisely. **Recommended: use `validate-agentic` for the subcommand** to
> avoid the collision entirely; keep `validate` for the phase.

### 3.3 MCP server: one new command

Add `_cmd_validate(p)` next to `_cmd_resolve_agentic` in `src/flowx/mcp/server.py`, and register
`"validate": _cmd_validate` in the `_COMMANDS` dict (`:495-509`, insert after `"migrate"`). It
resolves the source dir if present, calls `runner.run_adapter(["validate", "--output-dir", …])` for
the coarse phase, or `["validate-agentic", action, …]` for the loop, and wraps the result with
`_phase_result(...)` (`:69`). Update the tool docstring command list (`:532+`). Genie Code note: the
`prepare`/`stage`/`apply` payloads must be JSON-serializable text (the tool already returns JSON via
`structured_output=False`).

### 3.4 Inputs mechanism

- `src/flowx/adapter/constants.py:26-28`: add `PHASE_VALIDATE: Final[str] = "validate"`. Add input
  ids: `INPUT_VALIDATION_THRESHOLD`, `INPUT_VALIDATION_MAX_ITERATIONS`, `INPUT_VALIDATION_RESULTS_TABLE`
  (reuse `INPUT_RESULTS_WAREHOUSE`), `INPUT_VALIDATION_DRY_RUN`, `INPUT_VALIDATION_INTERACTIVE`.
- `src/flowx/adapter/session.py:420`: add `PHASE_VALIDATE` to `_SUPPORTED_PHASES`.
- `src/flowx/adapter/session.py:423-431`: extend `_options_for` with a `PHASE_VALIDATE` branch
  returning a new `_VALIDATE_OPTIONS` tuple (threshold, max-iterations, dry-run, interactive-gate,
  results-table, warehouse). Validate is source-independent, so it does **not** require `--source`
  (return before the `source not in _SOURCE_PATH_OPTION` check, like `PHASE_PACKAGE`).

### 3.5 Skill + plugin registration + docs

- New skill dir `skills/flowx-validate/` with `SKILL.md` (+ `references/ruleset.md`,
  `references/loop.md`). Frontmatter matches house style (§8).
- `.claude-plugin/plugin.json`: append `"./skills/flowx-validate"` to `skills`. Bump `version`
  (0.1.0 → 0.2.0). Same for `.claude-plugin/marketplace.json`.
- `skills/flowx-migrate/SKILL.md`: add a "Phase 4: Validate" step and a checkpoint after package;
  add `validate` to the phase list and the MCP command list.
- `skills/flowx-migrate/references/workflow.md`: add a "Phase 4: Validate" section after Phase 3.
- `AGENTS.md`: add `validate` to the phase table and module descriptions.
- `docs/content/docs/`: add a validation section to `architecture.mdx` and `guide.mdx`; update
  `meta.json` if a new page is added.

---

## 4. Phase position and artifacts

```
convert  → <output_dir>/.work/translation_report[.stamped].json
package  → <output_dir>/{databricks.yml, resources/*.yml, src/…}  (+ prunes .work/)
validate → <output_dir>/validation/validation_report.json         (final)
           <output_dir>/validation/iteration-<n>.json              (per iteration)
           <output_dir>/validation/.work/envelopes/<object_id>.json (transient, prepare)
           <output_dir>/validation/.work/stamps/<object_id>.json    (transient, stage)
```

Problem: `package` prunes `.work/` and the translation report with it (migrate skill Step 7). Validate
needs the report. **Fix:** `package` must copy the final stamped report to a durable location before
pruning — write `<output_dir>/metadata/translation_report.json` in `dab_writer` (next to the existing
`check_bundle_dir` postflight at `dab_writer.py:580`). Validate reads from there, falling back to
`.work/` when present. This is a small, required change to `package`, called out as task T2 in §12.

---

## 5. Data model — class and enum signatures

`src/flowx/validate/models.py`. All dataclasses use the house pattern `@dataclass(slots=True,
kw_only=True)` (AGENTS.md). Enums are plain `str, Enum` for JSON-friendliness.

```python
from __future__ import annotations
from dataclasses import dataclass, field
from enum import StrEnum

# --- vocabularies -----------------------------------------------------------

class Severity(StrEnum):
    BLOCKER = "blocker"
    MAJOR = "major"
    MINOR = "minor"
    ADVISORY = "advisory"

class Verdict(StrEnum):
    APPROVED = "approved"
    APPROVED_WITH_FINDINGS = "approved_with_findings"
    REJECTED = "rejected"

class ObjectKind(StrEnum):
    PIPELINE = "pipeline"
    JOB = "job"
    TASK = "task"
    NOTEBOOK = "notebook"

class Termination(StrEnum):
    THRESHOLD_MET = "threshold_met"
    BUDGET_EXHAUSTED = "budget_exhausted"
    OSCILLATING = "oscillating"          # PRD open-question: hash repeat / score plateau
    STATIC_REJECTED = "static_rejected"  # Tier-0 violations short-circuited the loop

# --- findings & stamps (shapes taken directly from the PRD) -----------------

@dataclass(slots=True, kw_only=True)
class ValidationFinding:
    code: str                      # e.g. "IDIOM-COPY-NOT-AUTOLOADER"
    severity: Severity
    message: str
    location: str = ""             # file / job / task
    suggested_fix: str = ""        # P1

@dataclass(slots=True, kw_only=True)
class ValidationStamp:
    object_id: str                 # "<pipeline>/<task_key>" (§5.1)
    object_kind: ObjectKind
    verdict: Verdict
    score: int                     # 0-100
    findings: list[ValidationFinding] = field(default_factory=list)
    reviewed_by: str = ""          # model id, e.g. "claude-opus-5" (maps to agentic model.name)
    ruleset_version: str = ""
    reviewed_at: str = ""          # ISO-8601 (UTC, "Z")
    iteration: int = 0
    content_hash: str = ""         # sha256 of the reviewed artifact (§5.2)

# --- the review envelope (Python → skill), analogous to GapEnvelope ---------

@dataclass(slots=True, kw_only=True)
class ReviewEnvelope:
    object_id: str
    object_kind: ObjectKind
    iteration: int
    ruleset_version: str
    content_hash: str              # binds a stamp to exactly this artifact
    ir_excerpt: dict               # the task/pipeline IR dict under review
    bundle_excerpt: dict           # the resource YAML fragment for this object
    source_excerpt: dict           # the ADF/source fragment (intent ground truth)
    static_findings: list[ValidationFinding] = field(default_factory=list)
    prior_stamp: ValidationStamp | None = None   # P1: enables skip-unchanged

# --- the report (matches the PRD JSON) --------------------------------------

@dataclass(slots=True, kw_only=True)
class ValidationReport:
    run_id: str
    run_by: str
    ruleset_version: str
    reviewed_by: str
    iteration: int
    stamps: list[ValidationStamp] = field(default_factory=list)
    approval_rate: float = 0.0
    terminated: Termination | None = None
    content_hash: str = ""         # hash of the whole bundle this iteration (oscillation)
    static: "StaticSummary | None" = None

@dataclass(slots=True, kw_only=True)
class StaticSummary:
    bundle_ok: bool
    dag_equivalent: bool
    violations: int
    warnings: int

# --- static-tier result (dataclass lives here, not with the actor in static.py) ---

@dataclass(slots=True, kw_only=True)
class StaticResult:
    bundle: "BundleInvariantResult"          # from flowx.validate.bundle_invariants
    dag: dict[str, "DagEquivalenceResult"]   # keyed by pipeline name; empty when no source
    @property
    def has_violations(self) -> bool: ...    # any bundle.violations or dag[*].violations
    def as_summary(self) -> StaticSummary: ...
```

All validation dataclasses (including `StaticResult`) live in `models.py`; `static.py` holds only the
functions that build and consume them. This keeps data definitions separate from the actors that use
them. `BundleInvariantResult` / `DagEquivalenceResult` are imported from the existing Tier-0 modules.

### 5.1 Object identity

`object_id = f"{pipeline.name}/{task.task_key}"` for tasks; `pipeline.name` for pipeline-level
stamps. Task keys are unique within a pipeline (`ir.py:64`, `Activity.task_key`). This matches the
PRD example `"pl_ingest_customer/copy_customers"`.

### 5.2 Content hashing (reuse the existing convention)

Reuse the projection-hash approach from `agentic.py:1347-1353` (`_graph_hash` +
`_sha256_bytes(_json_bytes(...))`, sorted keys, compact separators). Add
`content_hash(obj: dict) -> str` in `envelopes.py` computing `"sha256:" + sha256(canonical json)`.
Two uses:
- **Per object** — the stamp's `content_hash`; a stamp is valid only for the artifact it was cut
  against (stage rejects a stamp whose hash ≠ envelope hash).
- **Per iteration** — the report's `content_hash` over the whole bundle, for oscillation detection.

---

## 6. Method signatures and call stacks

### 6.1 Static tier — `src/flowx/validate/static.py`

Wraps the two existing checks and normalizes their findings into `ValidationFinding` /
`ValidationStamp`. `check_bundle_dir` is already run by `package` as preflight
(`dab_writer.py:498/528/580`); validate re-runs it as the gate so validate is independently runnable.
`check_dag_equivalence` is currently only exercised by tests — validate is its first production caller.

```python
def run_static_tier(*, output_dir: Path) -> StaticResult: ...

@dataclass(slots=True, kw_only=True)
class StaticResult:
    bundle: BundleInvariantResult          # from flowx.validate.bundle_invariants
    dag: dict[str, DagEquivalenceResult]   # keyed by pipeline name; empty if no source
    @property
    def has_violations(self) -> bool: ...  # any bundle.violations or dag[*].violations
    def as_summary(self) -> StaticSummary: ...

def findings_from_static(result: StaticResult) -> list[ValidationFinding]:
    """Map BundleFinding/DagFinding → ValidationFinding.
    severity: 'violation'→BLOCKER, 'warning'→MINOR, 'tolerated'→dropped.
    code: pass through. location: BundleFinding.location / DagFinding.nodes joined."""

def reject_from_static(findings: list[ValidationFinding], *, iteration: int,
                       ruleset_version: str) -> list[ValidationStamp]:
    """One REJECTED stamp per object carrying a BLOCKER static finding, score=0."""
```

Severity mapping (source: `bundle_invariants.py:41-44`, `dag_equivalence.py:58-61` — both use
`severity` strings `violation`/`warning`, plus `tolerated` for DAG):

| source severity | ValidationFinding.severity | counts toward rejection? |
|---|---|---|
| `violation` | `BLOCKER` | yes (hard) |
| `warning` | `MINOR` | no |
| `tolerated` (DAG only) | dropped | no |

To build the DAG map, `run_static_tier` reads each `<output_dir>/metadata/<pipeline>.arm.json`
(verbatim ADF, written by discover) → `AdfPipeline`, and the matching pipeline IR from the durable
translation report → `Pipeline`, then calls `check_dag_equivalence(adf, ir)`
(`dag_equivalence.py:139`). When no source metadata exists (non-ADF, or older runs), the DAG tier is
skipped and `StaticSummary.dag_equivalent` is reported as `True` with a warning note.

### 6.2 Envelopes — `src/flowx/validate/envelopes.py`

```python
def build_envelopes(*, output_dir: Path, iteration: int, ruleset_version: str,
                    prior: ValidationReport | None,
                    static: StaticResult) -> list[ReviewEnvelope]:
    """One envelope per reviewable object (task-level, aggregated to pipeline — PRD unit-of-review
    Option 2). Attaches the object's IR excerpt, resource-YAML fragment, source fragment, and any
    static findings. P1 skip-unchanged: when prior has an 'approved' stamp whose content_hash equals
    this object's hash, mark the envelope skipped=True (carried as prior_stamp, no re-grade)."""

def content_hash(obj: dict) -> str: ...        # "sha256:…", canonical json (§5.2)
def bundle_hash(output_dir: Path) -> str: ...  # whole-bundle hash for oscillation

def object_diff(prev: ReviewEnvelope, curr: ReviewEnvelope) -> dict:  # P1: emit object diffs
```

Unit of review = **per task, aggregated to pipeline** (PRD open-question, Option 2 — enables
skip-unchanged and precise remediation targeting). A pipeline's verdict is the worst of its tasks'
verdicts.

### 6.3 The three primitives — `src/flowx/validate/agentic.py`

These mirror `prepare_airflow_resolutions` / `stage_airflow_resolutions` /
`apply_airflow_resolutions` (`agentic.py:172/271/341`) in signature style, return shape (a
`dict[str, Any]` status payload), and hash-binding discipline.

```python
CONTRACT_VERSION = "1"   # validation contract, independent of the agentic-resolution one

def prepare_validation(*, output_dir: Path, report_path: Path | None = None,
                       iteration: int = 1, ruleset_version: str = "1.0.0",
                       ruleset_path: Path | None = None) -> dict[str, Any]:
    """Run the static tier. If it has violations, write REJECTED stamps and return status
    'static_rejected' (short-circuit, no envelopes). Otherwise build ReviewEnvelopes, write them to
    <output_dir>/validation/.work/envelopes/, and return:
      {"status": "prepared", "contract_version": CONTRACT_VERSION, "iteration": n,
       "ruleset_version": …, "envelope_count": k, "skipped_count": s,
       "static": {...}, "envelopes_dir": "…"}"""

def stage_validation(*, output_dir: Path, stamp_paths: list[Path],
                     replace: bool = False) -> dict[str, Any]:
    """Validate each agent-authored ValidationStamp JSON against its envelope: schema, enum values,
    score in 0..100, content_hash == envelope hash, verdict/severity consistency (a BLOCKER finding
    forbids an 'approved' verdict). Persist accepted stamps to …/.work/stamps/. Return a manifest:
      {"status": "staged", "staged": [{object_id, verdict, score, content_hash}...],
       "rejected": [{path, reason}...]}"""

def apply_validation(*, output_dir: Path, iteration: int, threshold: float = 0.90,
                     max_iterations: int = 3, dry_run: bool = False) -> dict[str, Any]:
    """Assemble ValidationReport from staged (+ static reject) stamps for this iteration. Compute
    approval_rate, decide Termination, write iteration-<n>.json and (when terminal) validation_report.json.
    Return:
      {"status": "applied", "terminated": <Termination|None>, "approval_rate": r,
       "iteration": n, "next_iteration": n+1 | None, "report_path": "…",
       "actionable_findings": [ …for the agent to remediate… ]}
    dry_run: assemble + report only; never touch the bundle. Remediation itself is the SKILL's job —
    Python only surfaces actionable_findings; it does not edit the bundle."""
```

### 6.4 Report + criteria — `src/flowx/validate/report.py`

```python
def approval_rate(stamps: list[ValidationStamp]) -> float:
    """Weighted (PRD open-question Option 3): any BLOCKER ⇒ that object cannot count as approved;
    MAJOR findings weighted down. Default weighting documented in references/ruleset.md."""

def decide_termination(*, report: ValidationReport, threshold: float, iteration: int,
                       max_iterations: int, prior_hashes: list[str]) -> Termination | None:
    """threshold met → THRESHOLD_MET; bundle_hash repeats a prior iteration → OSCILLATING
    (PRD Option 1); iteration == max_iterations → BUDGET_EXHAUSTED; else None (continue)."""

def write_report(output_dir: Path, report: ValidationReport) -> Path: ...
def read_report(output_dir: Path, iteration: int | None = None) -> ValidationReport | None: ...
def actionable_findings(report: ValidationReport) -> list[ValidationFinding]:
    """BLOCKER + MAJOR findings on non-approved objects, with suggested_fix, for the agent."""
```

### 6.5 Phase entry — `src/flowx/validate/cli.py`

```python
def main(argv: list[str] | None = None) -> int:
    """Non-interactive full run for flowx-migrate. Parses --output-dir/--threshold/--max-iterations/
    --results-table/--warehouse-id/--dry-run. Runs prepare→(static-only grading)→apply once; the
    static tier alone produces a report when no agent is present (PRD threshold Option 1: bundle
    validate). Returns 0 on threshold_met, 1 on budget_exhausted/oscillating/static_rejected, 2 on
    bad args. Prints a human summary and, when --results-table is set, appends rows via §6.6."""
```

The full agentic loop is driven by the **skill** through `prepare`/`stage`/`apply`; the phase entry
gives `flowx-migrate` a deterministic static-plus-report pass without an interactive agent turn.

### 6.6 UC results table — `src/flowx/reporting/validation_results.py`

Mirror `src/flowx/reporting/results.py` exactly (same warehouse resolution, create/alter/insert,
`CURRENT_TIMESTAMP()`/`CURRENT_USER()` stamping). Columns are the PRD's `VALIDATION_RESULTS_COLUMNS`:

```python
VALIDATION_RESULTS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "STRING"), ("run_date", "TIMESTAMP"), ("run_by", "STRING"),
    ("iteration", "INT"), ("object_id", "STRING"), ("object_kind", "STRING"),
    ("verdict", "STRING"), ("score", "INT"), ("finding_count", "INT"),
    ("blocker_count", "INT"), ("major_count", "INT"), ("reviewer", "STRING"),
    ("ruleset_version", "STRING"), ("content_hash", "STRING"),
)

def write_validation_results(validation_dir: Path, table_fqn: str,
                             warehouse_id: str | None = None,
                             client: Any | None = None) -> tuple[str, int]:
    """One row per (iteration, object). Appends across iterations so the score trajectory is
    queryable. Reuses resolve_warehouse_id / _execute from reporting.results (import them)."""
```

### 6.7 Call stacks

**A. `flowx-migrate` (or `flowx validate`) — non-interactive phase:**
```
adapter.__main__.main(["validate","--output-dir",D])
  → _run_phase("validate", …)                         # __main__.py:581
    → importlib.import_module("flowx.validate.cli").main(mapped)
      → prepare_validation(output_dir=D, iteration=1)  # validate/agentic.py
          → run_static_tier(output_dir=D)              # validate/static_tier.py
              → check_bundle_dir(bundle_dir)           # bundle_invariants.py:230
              → check_dag_equivalence(adf, ir)         # dag_equivalence.py:139  (per pipeline)
          → (violations?) reject_from_static(...) | build_envelopes(...)
      → apply_validation(output_dir=D, iteration=1, threshold, max_iterations)
          → approval_rate / decide_termination / write_report      # validate/report.py
      → (opt) write_validation_results(D/"validation", table)      # reporting/validation_results.py
```

**B. `flowx-validate` skill — agentic loop (per iteration):**
```
SKILL turn:
  flowx(command="validate", parameters={action:"prepare", output_dir:D, iteration:n})
    → runner.run_adapter(["validate-agentic","prepare","--output-dir",D,"--iteration",n])
      → _run_validate(args) → prepare_validation(...)  # returns envelopes or static_rejected
  AGENT reads envelopes, grades each object, writes ValidationStamp JSON files   ← LLM work
  flowx(command="validate", parameters={action:"stage", stamp:[…]})
    → _run_validate → stage_validation(...)            # validates stamps vs envelopes
  flowx(command="validate", parameters={action:"apply", iteration:n, threshold, max_iterations})
    → _run_validate → apply_validation(...)            # report + terminated? + actionable_findings
  if not terminated:
    AGENT applies remediations to the bundle (edits resources/*.yml, notebooks)  ← LLM work
    loop with iteration = n+1
```

---

## 7. Remediation strategy (PRD open questions — recommended defaults)

| PRD open question | Decision for v1 | Why |
|---|---|---|
| Mutate bundle in place vs. emit patch | **Mutate in place**, persist every `iteration-<n>.json` | Simplest loop; the DAB stays the single source of truth (PRD Option 1). `--dry-run` (P1) covers the audit case. |
| Fix IR vs. fix bundle | **Fix the bundle** | Enables agentic tuning of generated output (PRD Option 1). Re-running `package` from IR would erase agentic edits. |
| Unit of review | **Per task, aggregated to pipeline** | Fine-grained scores + skip-unchanged (PRD Option 2). |
| Approval threshold | **Weighted** (BLOCKER excludes; MAJOR down-weighted) | Most faithful to "correctness"; falls back to static (`bundle validate`) as the hard gate (PRD Options 1+3). |
| Anti-oscillation | **Bundle-hash repeat** stops early; also stop on score plateau | Cheap and deterministic (PRD Option 1). |
| Ground truth | **Source pipeline intent** (Option 1); reference patterns are advisory findings, not blockers | Keeps faithfulness the bar; idiomatic gaps are `MINOR`/`ADVISORY`. |
| Scope of review | **Orchestration + code** | Findings like `IDIOM-COPY-NOT-AUTOLOADER` need the code. |

The remediation *actions* are chosen and applied by the skill; Python only emits
`actionable_findings` (BLOCKER+MAJOR with `suggested_fix`). Remediation never runs in Python.

---

## 8. Skill — `skills/flowx-validate/SKILL.md`

Frontmatter (house style, cf. `flowx-package/SKILL.md:1-13`):
```yaml
---
name: flowx-validate
description: >
  Validate converted pipelines against the original source intent: run static (Tier-0) checks, grade
  each converted object, stamp verdicts/scores/findings, and iterate grade→remediate until a
  correctness threshold or an iteration budget is reached. Phase 4 of the flowx migration workflow.
triggers:
  - "validate pipelines"
  - "validate conversion"
  - "grade the migration"
  - "check converted pipelines"
  - "validation report"
---
```
Body sections (match the other skills): Context; dual-path run block (MCP `flowx(command="validate",
…)` vs venv `$PY -m flowx.adapter validate-agentic …`); the loop (prepare → grade → stage → apply →
check → remediate); how to grade (the ruleset in `references/ruleset.md`); reviewer = the model id;
interactive remediation gate (P1); examples; output artifacts. `references/loop.md` holds the exact
per-iteration protocol; `references/ruleset.md` holds finding codes, severities, and scoring weights.

---

## 9. Error cases and error classes

Follow the existing convention: one contract error class, surfaced by the CLI as a nonzero exit with
a printed message (cf. `AgenticContractError` at `agentic.py:86` and `_run_resolve_agentic`'s
`except (AgenticContractError, OSError, json.JSONDecodeError)` → exit 1 at `__main__.py:136-138`).

```python
# src/flowx/validate/models.py
class ValidationContractError(ValueError):
    """A validation input violated the contract (bad stamp, hash mismatch, unknown object_id,
    malformed envelope, score out of range, verdict/severity conflict)."""
```

| # | Case | Raised where | Handling / exit |
|---|---|---|---|
| E1 | Translation report missing (not in `metadata/` or `.work/`) | `prepare_validation` | print "run convert/package first"; exit 1 |
| E2 | Bundle dir has no `databricks.yml`/`resources/` | `run_static_tier` | print guidance; exit 1 |
| E3 | Tier-0 **violations** present | not an error — normal `static_rejected` termination | report written; phase exit 1 (validation failed, not a crash) |
| E4 | Stamp references unknown `object_id` | `stage_validation` | `ValidationContractError` → exit 1 |
| E5 | Stamp `content_hash` ≠ envelope hash (stale/oscillated) | `stage_validation` | `ValidationContractError` ("stamp cut against a different artifact; re-run prepare") → exit 1 |
| E6 | `score` out of 0..100, or unknown `verdict`/`severity` enum | `stage_validation` | `ValidationContractError` → exit 1 |
| E7 | `verdict == approved` but a BLOCKER finding present | `stage_validation` | `ValidationContractError` (verdict/severity conflict) → exit 1 |
| E8 | `apply` before any `stage` this iteration | `apply_validation` | `ValidationContractError` ("no staged stamps") → exit 1 |
| E9 | Malformed stamp/ruleset JSON | prepare/stage | `json.JSONDecodeError` caught → exit 1 |
| E10 | UC write fails / no warehouse | `write_validation_results` | catch broadly, print actionable message, non-fatal to the phase (degrade like `record-results`, `__main__.py:154-161`) |
| E11 | No ADF source metadata for DAG tier | `run_static_tier` | not an error — skip DAG tier, note in `StaticSummary` |
| E12 | Unknown `action` for the subcommand | argparse `choices` | exit 2 |

`_run_validate` wraps the three primitives in `try/except (ValidationContractError, OSError,
json.JSONDecodeError)` → print to stderr, return 1 — identical to `_run_resolve_agentic`.

---

## 10. Testing plan

Mirror existing test files (`tests/unit/test_bundle_invariants.py`, `test_dag_equivalence.py`,
`test_reporting_results.py`, `test_mcp_migrate.py`).

| New test file | Covers |
|---|---|
| `tests/unit/test_validation_static_tier.py` | `findings_from_static` severity mapping; `reject_from_static`; DAG-map build from ARM metadata; E11 skip |
| `tests/unit/test_validation_envelopes.py` | `content_hash` stability (sorted-keys canonical); `build_envelopes` object set; P1 skip-unchanged; `object_diff` |
| `tests/unit/test_validation_agentic.py` | prepare (static_rejected short-circuit vs. envelopes); stage E4–E9; apply approval_rate + termination |
| `tests/unit/test_validation_report.py` | weighted `approval_rate`; `decide_termination` (threshold/oscillation/budget); round-trip write/read |
| `tests/unit/test_validation_results.py` | `VALIDATION_RESULTS_COLUMNS` SQL build; per-(iteration,object) rows; injected fake client |
| `tests/unit/test_mcp_validate.py` | `flowx(command="validate", …)` dispatch for all three actions |
| `tests/unit/test_adapter_validate.py` | phase routing at `__main__.py:69/534`; `validate-agentic` subparser + `_run_validate` exit codes |

`make fmt` (ruff + mypy) and `make test` must pass. All new dataclasses use `slots=True, kw_only=True`
and full type hints (mypy clean).

---

## 11. Out of scope (do not build)

Custom rulesets beyond a pluggable `--ruleset` hook (P2), the dashboard (P2), multi-reviewer
consensus (P2), customer-supplied patterns (P2), auto-deploy on approval (P3), actual fine-tuning
(P3), runtime ADF↔Databricks data reconciliation (P3). Leave the `--ruleset` argument and
`ReviewEnvelope.ruleset_version` as the extension seam for P2.

---

## 12. Implementation sequence (task list)

1. **T1** — `validate/models.py`: enums, dataclasses, `ValidationContractError`. Tests: none yet.
2. **T2** — `package` change: persist final report to `metadata/translation_report.json` before
   pruning `.work/` (`dab_writer.py` near `:580`). Test: extend `test_bundler`/`test_package_invariants`.
3. **T3** — `validate/static_tier.py` + `test_validation_static_tier.py`.
4. **T4** — `validate/envelopes.py` (+ `content_hash`, skip-unchanged) + tests.
5. **T5** — `validate/report.py` (approval math, termination, IO) + tests.
6. **T6** — `validate/agentic.py` (prepare/stage/apply) + tests.
7. **T7** — `validate/cli.py` phase entry; wire `_VALIDATE_MODULE` + phase tuples in
   `adapter/__main__.py` (`:69`, `:534`, `:603`) + `test_adapter_validate.py`.
8. **T8** — `validate-agentic` subcommand: `_run_validate` + subparser (mirror `resolve-agentic`) +
   dispatch wiring.
9. **T9** — `reporting/validation_results.py` + `test_validation_results.py`.
10. **T10** — MCP `_cmd_validate` + `_COMMANDS` entry + docstring + `test_mcp_validate.py`.
11. **T11** — inputs: `constants.py`, `session.py` `_options_for`/`_SUPPORTED_PHASES`/`_VALIDATE_OPTIONS`.
12. **T12** — `skills/flowx-validate/` (SKILL.md + references); register in both `.claude-plugin/*.json`;
    bump version to 0.2.0.
13. **T13** — docs: `flowx-migrate/SKILL.md` Phase 4, `references/workflow.md`, `AGENTS.md`,
    `docs/content/docs/*`.
14. **T14** — `make fmt && make test` green.

Each task is independently testable; T1–T6 are pure Python with no wiring, so they can land first and
be reviewed before the CLI/MCP/skill surfaces (T7–T13) attach to them.
