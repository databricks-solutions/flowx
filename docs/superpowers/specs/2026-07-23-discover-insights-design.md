# Design — Agentic `insights` in `inventory.json` (discover phase)

**Issue:** databricks-field-eng/flowx #11 — *[FEATURE]: Agentic insights in inventory.json (discover phase)*
**Depends on:** #9 (deterministic lineage), implemented in PR #18 on branch `feat/discover-lineage`.
**Branch:** `feat/discover-insights`, cut from `feat/discover-lineage`. PR bases on `feat/discover-lineage`; retarget to `main` after #18 merges.
**Status:** Design approved; ready for implementation planning.

---

## 1. Summary

Add an agent-authored **`insights`** key to `metadata/inventory.json` during the **discover** phase. It gives
the later **convert** phase two things it cannot derive deterministically:

1. **What each pipeline is trying to achieve** and the Databricks pattern that maps to it.
2. **How pipelines relate across the whole factory**, by *annotating* #9's deterministic `lineage` edges.

This issue only **produces** the block. A separate follow-up wires `convert` to consume it (mirrors the
#9 produce → consume split).

**Design principle — every edge is accountable.** The inventory + `lineage` are the source of truth for
deterministic *facts*. The agent adds *judgment*, referencing pipelines and edges by their existing identifiers.
Most insight is therefore cheap to validate: named pipelines must exist; annotated (`control`/`data`) edges must
resolve to a real lineage edge — *annotate, don't rediscover*. The one exception is genuine coupling the
deterministic layer structurally cannot see (data flow inside notebook code, external triggers): the agent may
assert an `inferred` edge, but it is held to the same accountability by a different key — it must cite non-empty
`evidence` and a `confidence` level, and is clearly distinguished from proven lineage. Scope is deliberately
**factory / pipeline / relationship** level — **no per-activity fields** (convert already handles that level
well).

---

## 2. The two steps: "author" → "enrich"

Step 5 of the discover skill is a small loop with a clear division of labour. There is **no LLM inside the
tool** — the tool only validates and merges.

| Step | Who | What |
|---|---|---|
| discover (pass 1) | deterministic | Writes pure `inventory.json` (`pipelines`, `summary`, `lineage`). No `insights`. |
| **author** | the agent | Reads inventory + lineage + profile; writes the `insights` JSON (intent, patterns, relationships). The LLM-judgment part. |
| **enrich** | the tool | `flowx(command="enrich")`: validates the authored JSON against the inventory, then merges. Pure code. |
| enrich (pass 2) | deterministic | Appends **only** the `insights` key; re-serializes the deterministic portion **byte-identical**. Idempotent. |

**Author happens first; enrich happens second.** The tool always validates whatever it is handed. The decision
of *whether* Step 5 runs lives in the **skill**, never in the tool — and per the decisions below, it **always
runs**.

### Call shape

```python
flowx(command="enrich", parameters={
    "output_dir": "./flowx_output",         # locates metadata/inventory.json
    "insights": { ... },                    # inline dict (hosted / Genie path) — OR —
    "insights_path": "/path/insights.json", # a readable path (local / CLI path)
})
# Returns {ok, process, ...} + a merge summary.
# On ANY validation failure → {ok: false, violations: [...]} and DOES NOT WRITE the file.
```

---

## 3. Resolved decisions

These supersede the issue's open questions and loose wording.

- **`edge_identity` grammar (resolves the issue's open question).**
  - `edge_type: "control"` → `edge_identity` = the `ControlEdge.activity_name` (the ExecutePipeline activity name).
  - `edge_type: "data"` → `edge_identity` = the `DataEdge.match_key` — **not** "shared table/path". #9's data
    edges carry a two-tier join (`match_kind` = `identity` | `expression`); `match_key` is the canonical join
    value and `identity` is `null` for expression edges, so `match_key` is the only stable key. **Validation
    matches on `match_key`.**
  - `edge_type: "inferred"` → an agent-asserted coupling the deterministic layer never found, so there is **no**
    lineage edge to resolve against. `edge_identity` is an agent-authored descriptor of the coupling (e.g. the
    shared table/asset). Because it cannot be checked against a fact, the edge **must** instead carry a non-empty
    `evidence` string and a `confidence` ∈ {`high`, `medium`, `low`}; validation enforces those and skips lineage
    resolution. `evidence` / `confidence` are inferred-only — supplying them on a `control`/`data` edge is a
    violation.
- **Why the `inferred` tier (generic data-flow capture).** The annotate-only edges (`control`/`data`) can only
  describe couplings the deterministic layer surfaced. But a notebook-centric factory expresses its real data
  flow *inside* notebook code (one notebook writes a table another reads), which ADF never names, so #9 finds
  **zero** data edges there. The `inferred` tier gives the agent a structured, accountable place to record that
  coupling instead of burying it in prose. It is deliberately **pattern-agnostic**: it does not encode *why* the
  deterministic layer missed the edge (notebook I/O, external trigger, message queue, an ADF pattern we have not
  seen), so it generalises. The evidence+confidence requirement preserves the invariant's spirit — every edge is
  accountable to something (a proven fact for annotations, stated evidence for inferences) and an inference can
  never masquerade as proven lineage.
- **No new inventory fields; ARM is the deep-dive source.** The agent must characterize data flow, which for
  notebook-centric factories means reading what activities actually do. Rather than lift selected `typeProperties`
  (e.g. `notebookPath`) into `inventory.json` — a treadmill that would repeat for every known and unknown activity
  type and defeat the "generic" goal — Step 5 sends the agent to the verbatim `metadata/*.arm.json`, which already
  contains everything for every pattern. The inventory stays the lean deterministic skeleton.
- **ARM files are addressed by glob-and-match, never a constructed name.** `write_pipeline_arm` emits one
  `<sanitized-name>.arm.json` per pipeline via `_sanitize_filename` (a lossy slug: `[^0-9A-Za-z._-]+ → _`). A
  filename therefore cannot be reliably reconstructed from a pipeline name (spaces/parens/unicode are mangled;
  distinct names can collide to one stem). Step 5 instructs the agent to **glob `metadata/*.arm.json` and match on
  the top-level `"name"` field inside each file**, not to build `<pipeline>.arm.json`. There is no single fixed
  filename. Each file is a **flat single-pipeline object** (`{"name", "properties": {"activities": [...]}}`) — not
  a multi-resource ARM envelope, so there is no `resources[]` array and no top-level `type`; activities live under
  `properties.activities` (recurse nested `ForEach`/`If`/`Switch`). Step 5's wording states this shape explicitly
  (a clean-room run misparsed an assumed `resources[]` envelope, so the shape is called out to prevent it).
- **Step 5 authoring guidance is criteria-based, not count-based (validated by a clean-room run).** A no-context
  subagent run on a 327-pipeline factory surfaced two instruction gaps, fixed per prompt-engineering best practice
  (Anthropic prompting docs: criteria over fixed numbers, explain the *why*, diverse non-skewed examples; few-shot
  surface-feature bias, Zhao et al. 2021):
  - *Sparse selection* is expressed as **ANY-of inclusion tests + coverage-by-role + a decision-relevance bar +
    "omit is the default"**, with an explicit "guide, not a quota" escape hatch — so it scales from tiny to huge
    factories without a hardcoded count (the run guessed with no sense of scale).
  - *Inferred vs. annotation* is defined **by one axis — did the deterministic phase already record this as a
    lineage edge — explicitly NOT by mechanism**, with an ordered decision rule, an "if in doubt → inferred"
    tie-breaker, and diverse sub-case examples (data-in-code, `dependsOn` ordering, shared control asset) plus a
    near-miss. The prior single-flavour examples had biased the tier toward the data-in-code sub-case.
- **Two whole-factory recommendation patterns in Step 5 (isolated, revertible).** A second-opinion review of a
  real enriched run found the agent tends to *transliterate* (re-implement an ADF tier as a called job) when the
  better migration is to *replace* it with a native capability, and does not actively surface *clone families*.
  Step 5 now teaches two generic patterns — **"replace, don't transliterate"** (a tier whose sole purpose is a
  capability Databricks offers natively → eliminate it: observability→system tables, control tables→task values,
  config engines→Python-on-Jobs-API; guarded so it never recommends deleting pipelines that do real work) and
  **"collapse clone families"** (cluster by activity signature, emit one insight per family recommending a single
  parameterized job with the count). Both reuse the existing insight fields (no schema change) and are kept as a
  single self-contained commit so they can be reverted wholesale if the added opinionation proves low-value.
- **No `schema_version` field.** A draft one was removed on #9's branch; it had no consumer. The presence of the
  top-level **`insights` key IS the "enriched" marker**.
- **Skip gate: none — always enrich.** The value of `insights` (recovering intent + cross-pipeline
  relationships that deterministic analysis structurally cannot recover) holds at every factory size; if
  anything it grows with scale, because a per-pipeline converter is most blind to the whole-system picture on a
  large factory. Step 5 therefore always authors + enriches. See §9 for the future-review note on partitioning
  at scale.
- **Validator is hand-rolled, violation-collecting.** `validate_insights(raw, inventory) -> list[str]` walks the
  dict, checks required/unknown fields explicitly, and resolves FKs against sets built from the inventory. No new
  dependency; collects **all** violations (not fail-fast) so the agent fixes everything in one pass; matches the
  plain-dict style of `merge_agentic_results`.
- **Inline payload reaches the CLI via a temp file.** `_cmd_enrich` materializes an inline `insights` dict to a
  temp JSON file and passes `--insights-path` (mirrors `materialize_adf_definitions`), cleaning up after. The CLI
  therefore needs only one input mode; `--insights` (raw JSON string) is a thin convenience for direct CLI users.
- **Byte-identical write.** Read `inventory.json` text → `json.loads` → set the single `insights` key →
  `json.dumps(obj, indent=2)` write-back, using discover's exact dump options. Existing keys keep order and
  formatting; re-running is idempotent. Validation runs **before** any write.

---

## 4. Data models (`models/adf_ast.py`)

Four new `@dataclass(slots=True, kw_only=True)` types, placed right after `Lineage`. FKs are required (no
default); everything else is optional so authoring stays sparse.

```python
@dataclass(slots=True, kw_only=True)
class LineageEdgeRef:
    """A typed reference from a PipelineRelationship to one cross-pipeline edge.
    control/data annotate a deterministic #9 edge; inferred is an agent-asserted coupling."""
    edge_type: Literal["control", "data", "inferred"]
    edge_identity: str          # control → ControlEdge.activity_name; data → DataEdge.match_key;
                                # inferred → agent-authored descriptor of the coupling
    evidence: str | None = None                          # required for inferred; must be absent otherwise
    confidence: Literal["high", "medium", "low"] | None = None  # required for inferred; absent otherwise

@dataclass(slots=True, kw_only=True)
class PipelineInsight:
    """Per-pipeline judgment; references a pipeline by name (FK)."""
    pipeline: str                                    # FK → pipelines[].name (validated)
    pattern_name: str | None = None
    intent: str | None = None
    databricks_pattern: str | None = None
    recommended_databricks_features: list[str] = field(default_factory=list)
    conversion_notes: list[str] = field(default_factory=list)

@dataclass(slots=True, kw_only=True)
class PipelineRelationship:
    """Cross-pipeline judgment; annotates one #9 lineage edge."""
    from_pipeline: str                               # FK (validated)
    to_pipeline: str                                 # FK (validated)
    lineage_edge: LineageEdgeRef                      # must resolve to a real #9 edge (validated)
    relationship_summary: str | None = None
    databricks_pattern: str | None = None
    risk_if_ignored: str | None = None

@dataclass(slots=True, kw_only=True)
class Insights:
    overview: str | None = None
    pipeline_insights: list[PipelineInsight] = field(default_factory=list)
    pipeline_relationships: list[PipelineRelationship] = field(default_factory=list)
```

These dataclasses are the **typed round-trip / serialization** side. Validation of the raw agent JSON is done by
the pure validator (§5) *before* any dataclass is constructed, so unknown/missing fields produce collected
violation strings rather than raw `TypeError`s.

### Why `LineageEdgeRef` (and not just `from`/`to` + prose)

`LineageEdgeRef` carries **no facts of its own** — it is a typed foreign key into #9's lineage. It exists for
three reasons:

1. **Binds judgment to a validatable fact.** A relationship's prose is unanchored on its own; the ref forces it
   to point at one real edge (echoing `activity_name` / `match_key`), so `enrich` can resolve it and reject a
   relationship that references an edge #9 never found. This is the mechanism that enforces "annotate, don't
   rediscover."
2. **Disambiguates multiple facets of one pair.** The same `(from, to)` can be connected by both a control
   invocation *and* a data hand-off, each needing a different Databricks pattern and carrying a different risk.
   Relationships key on `(from, to, edge_type, edge_identity)`; the ref makes the pair-plus-facet addressable.
3. **Stable across re-runs.** The ref points at #9's canonical keys, not prose or array indices, so a
   regenerated lineage still resolves (or fails cleanly if the edge genuinely disappeared).

---

## 5. Parser module (`parser/pipeline_insights.py`)

New file, sibling to #9's `parser/lineage.py`, shaped like `merge_agentic_results`.

```python
def load_insights(*, insights: dict | None = None, insights_path: Path | None = None) -> dict:
    """Return the RAW insights dict from an inline dict OR a JSON file.
    Exactly one source must be provided (not both, not neither). Not yet validated."""

def validate_insights(raw: dict, inventory: dict) -> list[str]:
    """Pure validator. Returns a list of human-readable violation strings (empty == valid).
    Collects ALL violations, never fail-fast. Checks:
      - top-level shape: only {overview, pipeline_insights, pipeline_relationships}
      - each PipelineInsight: 'pipeline' present & ∈ inventory pipeline names;
        no unknown fields; field types (lists are lists, strings are strings)
      - each PipelineRelationship: from_pipeline / to_pipeline present & ∈ names;
        lineage_edge present, well-formed (edge_type ∈ {control, data, inferred}, edge_identity str),
        no unknown fields, and per tier:
          control  → edge_identity RESOLVES to some ControlEdge.activity_name; no evidence/confidence
          data     → edge_identity RESOLVES to some DataEdge.match_key;      no evidence/confidence
          inferred → NOT resolved against lineage; requires non-empty evidence str
                     and confidence ∈ {high, medium, low}
    """

def merge_into_inventory(inventory: dict, raw: dict) -> dict:
    """Pure. Return a NEW dict identical to `inventory` with exactly one added key,
    'insights', set to `raw`. Does not mutate the input. No I/O."""

def enrich_inventory(output_dir: Path, *, insights=None, insights_path=None) -> dict:
    """Orchestrator (the only function with I/O):
      1. read <output_dir>/metadata/inventory.json  (error if missing)
      2. raw = load_insights(...)
      3. violations = validate_insights(raw, inventory)
      4. if violations: return {ok: False, violations, ...}   # NO WRITE
      5. merged = merge_into_inventory(inventory, raw)
      6. write back with json.dumps(merged, indent=2)          # byte-identical prior keys
      7. return {ok: True, violations: [], pipeline_insights: N, relationships: M}
    """
```

Invariant-locking details:

- **Validation before I/O** — step 4 returns before any write, satisfying "on failure, do not write the file."
- **FK sets built once** from `inventory["pipelines"][*]["name"]`, `lineage.control_edges[*].activity_name`, and
  `lineage.data_edges[*].match_key` — plain set membership, no guessing.
- **Byte-identical** falls out of re-dumping the parsed dict with `indent=2` (discover's exact options) and only
  *adding* a key — existing keys keep order and formatting. Idempotent because re-running overwrites `insights`
  with an equal value.
- **No `default=str`** (unlike `merge_agentic_results`) — insights are plain JSON scalars/lists; matching
  discover's `json.dumps(..., indent=2)` call exactly is what guarantees the byte-for-byte round-trip.

---

## 6. Adapter subcommand & MCP command

### 6a. Adapter CLI subcommand `enrich` (`adapter/__main__.py`, modeled on `record-results`)

Standalone subcommand — deliberately **not** a `discover` flag — so it re-runs without re-parsing ADF.

```python
enrich = subparsers.add_parser(
    "enrich",
    help="Merge agent-authored insights into metadata/inventory.json (validate + append).",
)
enrich.add_argument("--output-dir", type=Path, required=True,
    help="Migration output directory (reads/writes metadata/inventory.json).")
enrich.add_argument("--insights-path", type=Path, default=None,
    help="Path to a JSON file holding the insights object.")
enrich.add_argument("--insights", type=str, default=None,
    help="Insights object as an inline JSON string (convenience for direct CLI use).")

# in main():
if args.command == "enrich":
    return _run_enrich(args)
```

```python
def _run_enrich(args) -> int:
    """Validate + merge insights into inventory.json. Returns 0 on success, 1 on any failure
    (missing inventory, unreadable/absent/both payload sources, validation violations)."""
    from flowx.parser.pipeline_insights import enrich_inventory
    metadata_dir = args.output_dir / "metadata"
    if not (metadata_dir / "inventory.json").exists():
        print(f"No inventory.json under {metadata_dir}; run discover first.", file=sys.stderr)
        return 1
    # resolve exactly one payload source (parse --insights JSON string if given)
    result = enrich_inventory(args.output_dir, insights=<parsed --insights>, insights_path=args.insights_path)
    if not result["ok"]:
        for v in result["violations"]:
            print(f"  - {v}", file=sys.stderr)
        print(f"Insights validation failed ({len(result['violations'])} violation(s)); "
              f"inventory not modified.", file=sys.stderr)
        return 1
    print(f"Enriched inventory: {result['pipeline_insights']} pipeline insight(s), "
          f"{result['relationships']} relationship(s).")
    return 0
```

### 6b. MCP command `_cmd_enrich` (`mcp/server.py`, added to `_COMMANDS` as `"enrich"`)

Mirrors `_cmd_discover`'s inline-payload handling: materialize the inline dict to a temp file, pass
`--insights-path`, clean up.

```python
def _cmd_enrich(p: dict[str, Any]) -> dict[str, Any]:
    output_dir = p.get("output_dir", "./flowx_output")
    insights = p.get("insights")            # inline dict (hosted / Genie path)
    insights_path = p.get("insights_path")  # readable path (local path)
    if insights is None and not insights_path:
        return {"ok": False, "error": "Provide 'insights' (inline dict) or 'insights_path'."}
    tmp = None
    try:
        if insights is not None:
            tmp = runner.materialize_json(insights)   # temp file, mirrors materialize_adf_definitions
            insights_path = tmp
        args = ["enrich", "--output-dir", output_dir, "--insights-path", insights_path]
        result = runner.run_adapter(args)
        out = Path(output_dir)
        return _phase_result(result, out, inventory=runner.summarize_inventory(out))
    finally:
        if tmp:
            runner.cleanup_materialized(tmp)
```

Additions:
- `runner.materialize_json(obj) -> str` — writes `json.dumps(obj)` to a temp file, parallel to
  `materialize_adf_definitions`.
- Register `"enrich": _cmd_enrich` in `_COMMANDS`; update the `flowx` tool docstring / `_COMMANDS` param docs to
  list `enrich`.
- Confirm the runner captures subprocess stderr into `result` so validation violation lines surface to the
  agent; if not, `_cmd_enrich` parses and echoes them explicitly.

---

## 7. Skill Step 5 (`skills/flowx-discover/SKILL.md`)

Insert the **author → enrich** loop as the new **Step 5**; renumber the existing reporting steps (5–8) down.

**New Step 5 — Author and merge agentic insights** (always runs):

1. **Read** the just-written `inventory.json` (`pipelines`, `lineage`, `summary`) plus `profile_report.csv`.
2. **Author** an `insights` object:
   - `overview` — the whole factory as one system + the single biggest migration steer.
   - `pipeline_insights[]` — **sparse**; per-pipeline `intent` + `databricks_pattern` (+ optional
     features/notes). Omit pipelines with nothing worth saying.
   - `pipeline_relationships[]` — annotate #9 lineage edges: each carries a `lineage_edge`
     (`edge_type` + `edge_identity` echoed verbatim from a real edge — `activity_name` for control, `match_key`
     for data), plus `relationship_summary`, `databricks_pattern`, `risk_if_ignored`.
   - Authoring rules in the prose: reference only pipeline names that exist; every `lineage_edge` must echo a
     real edge; don't invent relationships #9 didn't find (annotate, don't rediscover).
3. **Enrich** — call `flowx(command="enrich", parameters={"output_dir": ..., "insights": {...}})` (inline dict on
   the hosted/Genie path; `insights_path` on the local CLI path). Local CLI fallback:
   `"$PY" -m flowx.adapter enrich --output-dir <dir> --insights-path <file>`.
4. **On `ok:false`** — the tool did **not** write; read `violations`, fix the offending FK/edge/field, and
   re-call. On `ok:true`, the `insights` key is merged into `inventory.json`.

**Reworded summary step** (the old "Present the summary"): after the counts table, surface the enriched
judgment — the factory `overview`, and per-pipeline `pattern_name` / `intent` — so the user sees *what the
factory does* and the recommended Databricks patterns, not just coverage numbers.

---

## 8. Testing & optional reporting

**Test file:** `tests/unit/test_pipeline_insights.py`, following `test_merge_agentic.py` precedent (helper
writers, `tmp_path`, assert **structure/schema — never prose**). **No live LLM** — all fixtures are stubbed JSON.

**Fixtures** under `tests/resources/json/`:
- A small **good** `insights` object referencing a known inventory.
- Edge-binding tests reuse the existing `pipeline_execute_pipeline_nested.json`, whose known
  `ControlEdge.activity_name` values are `"Run Ingestion Pipeline"`, `"Run Transform Pipeline"`, and
  `"Run Cleanup Pipeline"`. The test builds a real inventory dict from it (via `load_adf_definitions` +
  `build_lineage` + `_inventory_to_dict`, mirroring `test_lineage.py`) so FK/edge sets are genuine, not
  hand-faked.

**Test cases (1:1 with the issue's invariants):**

| Test | Asserts |
|---|---|
| `test_validator_accepts_good_insights` | `validate_insights` returns `[]`; `enrich_inventory` returns `ok:True` with correct counts |
| `test_rejects_pipeline_not_in_inventory` | FK `pipeline`/`from_pipeline`/`to_pipeline` not in names → non-empty violations |
| `test_rejects_unresolvable_lineage_edge` | `lineage_edge` with no matching edge → violation |
| `test_rejects_missing_required_field` | missing `pipeline` / `from_pipeline` / `lineage_edge` → violation |
| `test_rejects_unknown_field` | extra key in any insight/relationship → violation |
| `test_control_edge_binding_matches_and_rejects` | on the nested fixture: `edge_identity="Run Ingestion Pipeline"` validates; a bogus name rejects |
| `test_data_edge_binds_on_match_key` | data edge resolves on `match_key`; a non-matching key rejects |
| `test_inferred_edge_with_evidence_and_confidence_validates` | `inferred` edge with real endpoints + non-empty `evidence` + valid `confidence` → `[]` |
| `test_inferred_edge_requires_evidence` | `inferred` edge missing `evidence` → violation |
| `test_inferred_edge_requires_valid_confidence` | `inferred` edge with bad/missing `confidence` → violation |
| `test_inferred_edge_does_not_resolve_against_lineage` | `inferred` `edge_identity` is *not* checked against lineage sets (arbitrary descriptor validates) |
| `test_annotation_edge_rejects_evidence_confidence` | `control`/`data` edge carrying `evidence`/`confidence` → violation (inferred-only fields) |
| `test_two_pass_deterministic_keys_byte_identical` | pre-enrich vs post-enrich: all keys except `insights` byte-identical |
| `test_enrich_is_idempotent` | running `enrich_inventory` twice → identical file bytes |
| `test_validation_failure_does_not_write` | on violations: `ok:False`, `violations` populated, **file unchanged on disk** |

**Optional reporting** (`reporting/coverage.py`): a `has_insights` (bool) or `pattern_name` column per pipeline,
**gated on the `insights` key existing** so non-enriched inventories are unaffected. Added only if it stays
trivial and doesn't perturb existing coverage tests; the issue marks it optional, so it will not hold up the
core.

**End-to-end verification** (before declaring done): a real `discover` then `enrich` over
`tests/resources/json/`, confirming the inventory gains a valid `insights` block while the deterministic keys are
byte-identical. Plus full unit suite + `make fmt` (ruff + mypy) clean.

---

## 9. Scope

**In scope:**
- The `insights` schema + 4 dataclasses in `models/adf_ast.py`.
- `parser/pipeline_insights.py` (`load_insights` → `validate_insights` → `merge_into_inventory` →
  `enrich_inventory`).
- The `enrich` adapter subcommand + `_cmd_enrich` MCP command (+ `runner.materialize_json`).
- The two-pass byte-identical write.
- Step 5 in `flowx-discover` (always runs) + reworded summary step.
- Optional reporting column.

**Out of scope:**
- Consuming insights in `convert` (separate follow-up).
- The #9 extraction itself; a new skill/phase; an in-code LLM client.
- Per-activity insights; domains grouping; per-edge narrative; #10 deploy-ordering.
- Any `schema_version` field.

**Future considerations (review later — not #11):**
- Insights are currently authored in a single pass over the whole factory. For very large factories, revisit
  **partitioning the authoring across subagents keyed on lineage clusters** — the connected components of the
  combined control/data-edge graph — so each subagent reasons about one coherent subsystem rather than the whole
  corpus at once. The open question that motivates this is "are all these pipelines even related?"; the lineage
  graph already holds the answer. For #11 we always enrich in one pass.

---

## 10. Customer confidentiality

All **code, tests, fixtures, comments, commit messages, and the PR body** use generic placeholders only — they
must **never** contain real customer names or customer-derived vocabulary.

- **Placeholders to use everywhere:** "Factory A"/"Factory B" (factories), `entityID` (loop key),
  `config-params/entity-versions` (watermark path), "dummy dataset" (any dataset name).
- **Denylist — this design doc only, as a validation reference:** the terms below are recorded here **solely so
  we can grep the diff, commits, and PR against them to confirm none leaked**. They must not appear in any
  shipped artifact (code/tests/fixtures/comments/commits/PR): "a customer factory", "a large factory", `engagementID`,
  `engagementDBVersions`, `etl-parameters`. A pre-flight check before opening the PR greps the branch for each of
  these and must return zero hits outside this spec file.
