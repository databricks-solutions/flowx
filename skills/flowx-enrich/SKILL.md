---
name: flowx-enrich
description: >
  Enrich the discover inventory with an agent-authored layer of judgment — a factory-wide
  architecture recommendation, per-pipeline intent + recommended Databricks patterns, and
  cross-pipeline relationships. The library validates it, records it in agentic_insights.json and
  rebuilds inventory.json from discovery plus that file. The default next step after flowx-discover
  and the input the routing step consumes.
triggers:
  - "enrich inventory"
  - "enrich pipelines"
  - "author insights"
  - "agentic insights"
  - "recommend databricks patterns"
  - "annotate inventory"
  - "enrich discover"
---

# Enrich the Inventory with Agentic Insights

The deterministic discover pass records what each source workflow **is**; it cannot record what to
**do** about it. That judgment — a factory-wide architectural recommendation, each pipeline's intent
and recommended Databricks patterns, and how pipelines couple — is authored by **you, the agent**,
and recorded by the library in its own file, `metadata/agentic_insights.json`, bound to the saved
`metadata/source_graphs.json` it was checked against. The library then rebuilds
`metadata/inventory.json` from `source_graphs.json` plus that file, so it carries the same block under
a single additive `insights` key; you never pass it anything for that step.

This is the standard step **between discover and route** in the flowx workflow. `flowx-discover`
chains into this skill by default; the routing step (`flowx-route`) reads the `insights` block to
present the agentic conversion option per connected component.

## No LLM inside flowx: you author, the library validates and records

**There is no LLM inside flowx.** You author the insights JSON; the library (`enrich`) only
*validates and records* it, using the same author, validate, record, fingerprint-bound contract the
agentic gap-resolution and routing paths use. That keeps the deterministic inventory trustworthy and
every insight accountable: foreign keys must point at real pipelines, and every cross-pipeline edge
is either an annotation of a proven lineage edge or an explicitly-flagged inference with cited
evidence.

`enrich` is **additive**: it writes `agentic_insights.json` and adds only the matching `insights`
block to the inventory, leaving every existing inventory key byte-identical. It changes no conversion, IR, or routing decision on its own — the `insights` are
descriptive data that `flowx-route` later consumes.

## When to skip enrich (deterministic-only)

Enrichment is on by default, but it is skippable. A standalone, deterministic-only discover — no
agent, no LLM — is fully valid: `metadata/inventory.json` from discover is complete and self-standing
without an `insights` block, and `flowx-route` still recommends and records a plan from the
deterministic structure alone (the agentic option simply shows no recommended patterns). Skip enrich
when the caller asked for a headless/deterministic pass, or when no human reader needs a migration
narrative.

## How to author (three steps)

1. **Read the deterministic inventory.** Load `<output_dir>/metadata/inventory.json` (on the hosted
   MCP server, through `enrich` with `action="prepare"`; see below). Note every
   pipeline `name` (these are the only valid foreign keys), and each pipeline's `lineage` block — in
   particular `lineage.control_edges`, each `{source_workflow, target_workflow, via_task_key}`. A
   deterministic **control** relationship you annotate must match one of these exactly. When the
   inventory records a top-level `source_graphs_sha256` (ADF discover does), copy it into
   `authored_against` by default, so enrich can refuse your insights if discover runs again before
   you submit them; when it records none, leave `authored_against` out.
2. **Read the source artifacts** you need to form judgment — the per-pipeline `raw` payloads in the
   inventory, the ADF `metadata/<pipeline>.arm.json` provenance, or the DAG source — enough to state
   each pipeline's *intent* and the Databricks patterns that fit. Ground every recommended pattern in
   a **real, publicly-documented** Databricks capability; never invent a product name.

   **Verify GA/Preview status before recommending a connector or Lakeflow Connect pattern, then
   record it as a structured field.** Do **not** hardcode GA/Preview status or dates — release state
   changes. Before recommending a connector or a Lakeflow Connect ingestion pattern (or naming it in
   `recommended_patterns` / `system_recommendation`), you **must** verify against the **current public
   Databricks docs** both that (a) the feature's GA-vs-Preview release state is acceptable and (b) it
   is available in the target workspace. Verification happens **before** the recommendation, never
   after: never emit a "recommend now, verify later" conditional.

   Record the resolved state on the pattern with the **structured `release_state` field** — *not* prose
   buried in `fit` / `conversion_notes` — and cite the doc that grounds it in `release_state_source`.
   The validator enforces this. The state is a factual **disclosure**, not a warning; `flowx-route`
   surfaces it as a neutral label (no alarm), tiered as:

   - `ga` — Generally Available. **Silent** (not surfaced).
   - `unknown` — the release state could not be verified. **Silent**, treated exactly like `ga` (there
     is no separate label — the two are indistinguishable in the surfaced output).
   - `public_preview` — surfaced as the informational label **"Public Preview (production-ready)"**:
     Public Preview is generally production-ready and supported per Databricks. Still confirm the
     feature is available in the target workspace. A cited `release_state_source` is required.
   - `private_preview` — surfaced as the plain factual label **"Private Preview"**. A cited
     `release_state_source` is required.
   - `beta` — surfaced as the plain factual label **"Beta"**. A cited `release_state_source` is
     required.

   `release_state` is **required** on any pattern you mark `simplification_pattern: true` (a distinctive
   capability must declare its verified release state). There is **no recommendation-eligibility gate** on
   top of that: even a verified **Private Preview** feature may be suggested. **Disclose the release state
   and let the customer decide** — record the verified state on the structured `release_state` /
   `release_state_source` fields, note that Private Preview typically requires workspace
   enrollment/entitlement, and leave the decision to proceed to the customer. The tool does **not**
   unilaterally refuse to suggest it (there is no `doNotSuggest` gate). This is disclosure, not a gate —
   and it does not relax the rule above: you still **verify the release state against the current public
   Databricks docs before recommending** (never "recommend now, verify later").
3. **Author the insights JSON, then call `enrich`.** The library validates it against the inventory
   and the saved source graphs and, only when clean, writes `agentic_insights.json` and rebuilds the
   inventory. On any violation both files are left untouched and you get the full list of
   problems to fix in one pass.

See **`insights.md`** in this skill directory for the exact insights shape, every field, and the
validation rules the library enforces.

## Run enrich — MCP tool or venv CLI

Run the **`setup`** skill first if you haven't. Both paths run the same validate-and-record contract.

- **MCP tool (Databricks Genie Code, or a local stdio registration):** call the single **`flowx`**
  tool with `command="enrich"`. On the hosted server you cannot read `output_dir`, so first read the
  inventory with `action="prepare"`. It returns `metadata/` (`inventory.json`, `source_graphs.json`,
  the `.arm.json` provenance) inline under `bundle.files`, the same way `package` returns a bundle;
  pass `output_volume_path` or `output_workspace_path` to have large metadata uploaded instead. Then
  record the insights (the default `action="apply"`) with either inline insights or a file:

  ```
  flowx(command="enrich", parameters={"output_dir": "<dir>", "action": "prepare"})   # read metadata/
  flowx(command="enrich", parameters={"output_dir": "<dir>", "insights": { ... }})   # inline object
  flowx(command="enrich", parameters={"output_dir": "<dir>", "insights_path": "<file>"})
  ```

  Provide **exactly one** of `insights` (inline object) or `insights_path`. `ok` reflects validation;
  `result.violations` lists any problems. Run **no** `python3`/`$PY` commands on this path.

- **venv CLI (local, no MCP server):** ensure the venv exists (`setup` / `bootstrap.sh`), then:

  ```bash
  export PYTHONPATH="<plugin_dir>/src"
  PY="$(cat <plugin_dir>/.migration-venv)"
  "$PY" -m flowx.adapter enrich --output-dir <dir> --insights-path authored_insights.json
  ```

  Both `--output-dir` and `--insights-path` are required; `--out <file>` optionally writes the result
  JSON to a file instead of stdout. **Exit code 0** means the insights were recorded; **exit code 1** prints
  the violations JSON and leaves the inventory untouched.

## Idempotency & safety

`enrich` is idempotent: it records the validated insights in
`metadata/agentic_insights.json` (stamping `schema_version`, `inventory_sha256`, `source_graphs_sha256`
and `agentic_insights_sha256`), then rebuilds `inventory.json` itself from the saved `source_graphs.json`
(or, for an inventory that records no graphs hash, from its existing deterministic part) plus that
document rather than patching it in place. The `insights` block is replaced wholesale (never stacks),
and every deterministic inventory key stays byte-identical. Re-running with the same insights rewrites the same bytes; re-running
with different insights replaces the block. A validation failure writes nothing.

Only one `enrich` writes an output directory at a time: it holds `metadata/.enrich.lock` from reading
the inventory until both files are replaced, and a second call made meanwhile fails with a violation and
writes nothing. The two files are replaced one right after the other. If replacing `inventory.json`
fails, the previous `agentic_insights.json` is put back. A process killed or interrupted (Ctrl-C)
between those two steps, or a failure while putting the previous file back, can leave
`agentic_insights.json` one write ahead of `inventory.json`. That run's lock then stays behind, and the
next `enrich` refuses until you delete the lock; running `enrich` again then rewrites both.

## Next step

After the inventory is enriched, continue with **`flowx-route`** to recommend and record a
per-connected-component conversion route (deterministic vs. agentic), then convert and package.

## Reference

- `insights.md` — the insights shape, every field, and the validator's rules.
