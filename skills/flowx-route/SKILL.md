---
name: flowx-route
description: >
  Route each connected component of the discovered inventory to a deterministic (1:1 engine) or
  agentic (LLM-assisted re-architecture) conversion, record the fingerprint-bound conversion plan,
  and fill the routed-agentic groups with fill-agentic combine. Runs after enrich, before/with
  convert.
triggers:
  - "route pipelines"
  - "route conversion"
  - "conversion plan"
  - "deterministic or agentic"
  - "fill agentic"
  - "combine pipelines"
  - "agentic conversion"
  - "recommend conversion route"
---

# Route the Conversion (deterministic vs. agentic) and Fill the Gaps

After discover (and, by default, `flowx-enrich`), routing decides **per connected component** whether
each part of the factory converts **deterministically** (the typed engine's 1:1 translation) or
**agentically** (an LLM-authored re-architecture, e.g. collapsing five extractors onto one Lakeflow
Connect pipeline). It records the decision as a fingerprint-bound `metadata/conversion_plan.json`,
edits the translation report so routed-agentic groups become placeholder gaps, and then you author
the fill.

**ADF only (current scope).** The discover → enrich → route → edit → fill agentic-conversion flow is
supported for **ADF today**, and `route` enforces it: for an Airflow inventory it recommends and
records only `deterministic` decisions and rejects an `agentic` one (the plan is not recorded and the
report is left untouched). `convert --merge-agentic --source airflow` is disabled too. Airflow
DAGs are often one connected component, so Airflow agentic conversion follows the Airflow track's own
per-gap and patch contract rather than this whole-component flow. For **Airflow agentic gaps today,
use the `flowx-resolve-airflow-gaps` skill** (the strict per-gap resolver), not this routing flow.

**There is no LLM inside flowx.** The library computes the recommendation deterministically and only
*validates and records* the decision and the authored fill — the same author → validate → merge
contract `enrich` uses. This is additive and non-breaking: with **no recorded plan**, `convert` and
`package` behave exactly as before.

**Who decides what.** The library **recommends** a route per component; the **customer decides**
deterministic-vs-agentic for each component; the **agent** presents the options and the
recommendation, serializes only the customer's *approved* decision into the plan, and authors the
agentic fill. The agent never picks the route on the customer's behalf — it records the customer's
choice and does the mechanical work of the approved fill.

## Where routing sits

```
discover → enrich (default) → convert (deterministic baseline) → route (decide + edit) → fill-agentic → package
```

Convert builds the deterministic baseline report **before** routing (route can trigger it in-process
via `--source` / `--source-path`); routing then edits that report. If you run `convert` again after
routing, run `route` straight after it: a fresh report carries no routing record, so route takes it
as the new baseline and re-applies the plan and the stored combines (merges of convert's own gaps
made before that convert must be merged again).

Pipelines are grouped into weak/undirected **connected components** over the inventory's control
lineage (`lineage.control_edges`), so mutually-referencing pipelines are decided together and a
caller/callee reference is never split across incompatible routes. Only resolved control edges join
a component; an unresolved call is reported as a finding. ADF emits these control edges (from
`ExecutePipeline`) in discovery. Agentic routing is ADF-only today whatever lineage an inventory
carries (see the scope note above).

Run the **`setup`** skill first if you haven't. Everything below has an MCP-tool path (Genie Code, or
a local stdio registration — call the single **`flowx`** tool, run no `python3`/`$PY`) and a venv-CLI
path (local; `PY="$(cat <plugin_dir>/.migration-venv)"` and `export PYTHONPATH="<plugin_dir>/src"`).

## Step 1 — Recommend (the dry run you read first)

Call `route` with **no decision** to get the recommendation: every component with its `members`, both
first-class conversion `options` (a *deterministic* option carrying the engine-capability assessment
+ any uncovered gaps, and an *agentic* option carrying the `recommended_patterns` from `enrich`, with
any `simplification_pattern` flagged), the `findings` (unresolved/dangling control edges kept, never
severed), and a ready-to-record `default_plan` proposing `decision == recommended` for every
component.

- **MCP tool:** `flowx(command="route", parameters={"output_dir": "<dir>"})`
- **venv CLI:**

  ```bash
  export PYTHONPATH="<plugin_dir>/src"
  PY="$(cat <plugin_dir>/.migration-venv)"
  "$PY" -m flowx.adapter route --output-dir <dir>
  ```

  On a non-TTY with no `--plan-path`, this emits the recommendation and exits 0 (it edits nothing).
  `route` reads `metadata/inventory.json`; run discover first. `--out <file>` writes the JSON to a
  file instead of stdout.

`recommended` is the library's starting suggestion — `deterministic` when the whole component is
engine-capable, else `agentic`. Present each component's members, its recommendation, and the agentic
option's patterns (flag any `has_simplification` re-architecture prominently).

## Step 2 — Decide and record the plan

Take the per-component decision and record it. The plan is **freely editable** — change a component's
decision from agentic to deterministic, or vice versa, without restriction. `route` validates the plan,
writes the fingerprint-bound `metadata/conversion_plan.json`, and then rebuilds `.work/translation_report.json`
+ `gaps.json` from the immutable deterministic baseline, the plan, and any stored combines:

- **deterministic** components: the baseline pipelines stay (deterministic outcome).
- **agentic components with a stored combine** whose members match: the combine's pipelines replace the
  members; `gaps.json` still lists the members' routed tasks (agentic-applied outcome).
- **agentic components without a matching combine**: placeholders and gaps as before (agentic-not-viable).
- **switching back to deterministic**: restores that component's baseline exactly (deterministic outcome).
  When the whole plan is deterministic, route writes the baseline report and gaps back byte for byte,
  with no record — the non-breaking guarantee.

When it edits the report, route also stamps a **routing record** onto it (the top-level
`_routing_record` key): the plan's hash, the hashes of the immutable deterministic baseline report and
gaps it started from, and one entry per `component_id` with its `members`, `decision`, `outcome`,
`combine_sha256`, `fingerprint`, and `replacements` (tracking changes to a component's fingerprint
across re-routes). Every re-route rebuilds from the baseline and current plan; identical input gives
identical output (byte-for-byte, deterministic and idempotent).

There are three ways to supply the decision:

- **Interactive prompt (TTY).** Run `route` with no `--plan-path` on a real terminal and it asks, per
  component, `route [d]eterministic / [a]gentic (default=<recommended>)`. An empty answer accepts the
  recommendation. This is the from-the-seat path.
- **Authored plan file** — `--plan-path <file>` (or `--plan-path -` to read the plan JSON from stdin).
- **MCP tool** — pass the plan inline as `plan` (or `plan_path`):

  ```
  flowx(command="route", parameters={"output_dir": "<dir>", "plan": { ...authored plan... }})
  ```

### The plan shape

The plan carries **only** the decision (and an optional rationale) per component — and the decision is
the **customer's**, not yours. Your job is to present each component's members, its `recommended`
route, and both `options`, then serialize the customer's pick; you do not choose the route yourself.
The library recomputes `members`, `recommended`, and both `options` on record, so recorded facts
cannot drift from the inventory or be faked. Start from the recommendation's `default_plan` and flip
the components the customer chose to override:

```json
{
  "components": [
    {"component_id": "component-1", "members": ["IngestSalesforce", "IngestWorkday"], "decision": "agentic",
     "rationale": "Collapse both extractors onto one Lakeflow Connect pipeline"},
    {"component_id": "component-2", "members": ["BuildMart"], "decision": "deterministic"}
  ]
}
```

Rules the validator enforces (all violations returned at once; nothing written on failure):

- `decision` must be `"deterministic"` or `"agentic"`; `rationale` (optional) must be a non-empty
  string when present.
- Every `member` must be a real inventory pipeline, and a component's `members` must **exactly match
  one computed connected component** — a decision can never split a component or span two.
- The plan is a **bijection**: every component is decided exactly once (no partial plan, no
  duplicate/conflicting decisions).
- `component_id` is **required** on every component — a non-empty string that must match the computed
  component for those `members`. Start from the recommendation's `default_plan`, which already carries
  the correct `component_id` for each component.

### Triggering convert if the report is missing

`route` edits `.work/translation_report.json`, which the convert phase produces. If it is missing,
pass `--source <adf|airflow>` **and** `--source-path <path>` (MCP: `source` plus the source-specific
path param — `adf_source_path` or `airflow_source_path`; a generic `source_path` is **ignored**, and
ADF also accepts `adf_definitions` / `adf_volume_path` / `adf_workspace_path`) and `route` triggers
the convert phase in-process first. Otherwise run `flowx-convert` before routing.

### venv CLI

```bash
"$PY" -m flowx.adapter route --output-dir <dir> --plan-path plan.json \
  [--source adf --source-path <path>]     # only needed to trigger convert when the report is missing
```

Exit 1 (nothing written) on a missing report it cannot produce or a plan that fails validation.
Re-routing under different decisions is always allowed; it rebuilds from the baseline.

## Step 3 — Fill the routed-agentic groups

Every routed-agentic pipeline's tasks are now `PlaceholderActivity` nodes with one pipeline-tagged
`AgenticGap` each. A routed-agentic group is filled **only** by `fill-agentic combine`, which replaces
the whole group with pipelines you author (no LLM in the library, which validates and records).

### 3a — Keep a pipeline 1:1

To convert a routed-agentic pipeline agentically but keep it as one pipeline, combine its component
with one authored pipeline of the **same name** (see 3b). The per-pipeline
`convert --merge-agentic` is for convert's **own** agentic gaps only: it refuses, writing nothing, any
result that names a routed-agentic pipeline, or whose activity would first land in one, with "this
pipeline is routed agentic; fill it with fill-agentic combine (to change the approach, change the plan,
re-run convert and route, then combine again)".

### 3b — Cross-pipeline COMBINE (N pipelines → M)

When the decision is a re-architecture that changes pipeline count — e.g. five extractor pipelines
collapse onto **one** Lakeflow Connect pipeline — use `fill-agentic combine`. You author the
replacement pipeline(s) as IR dicts and the whole routed group is swapped for them.

```bash
"$PY" -m flowx.adapter fill-agentic combine \
  --output-dir <output_dir> \
  --members "IngestSalesforce,IngestWorkday" \
  --pipelines-path authored_pipelines.json \
  [--out <result.json>]
```

MCP: `flowx(command="fill_agentic", parameters={"output_dir": ..., "members": [...], "pipelines": [...]})`
(pass `pipelines` inline as a list, or `pipelines_path`).

`combine` is the only action. Its guarantees:

- `--members` (comma-separated; MCP accepts a list) must **exactly match** a routed-**agentic**
  component in the recorded `metadata/conversion_plan.json`, whose `inventory_sha256`,
  `source_graphs_sha256` and `source_insights_sha256` must still match the current inventory. A partial group, a superset, a typo, or a deterministic component is refused
  — you can't swap pipelines the plan didn't route agentic. The report must carry a routing record
  that matches that plan (route has applied it).
- **Same authored pipelines** (the canonical hash covers every field of every authored pipeline):
  `already_combined: true` with the message "already applied, unchanged"; nothing is written. This
  is a no-op even after a re-route, allowing idempotent tooling. If the report no longer reflects the
  stored combine, or the stored entry was edited by hand, combine stores the pipelines again,
  rebuilds, validates and writes the result.
- **Different authored pipelines**: replaces the stored combine for that component only, rebuilds,
  and writes the result (if structural validation passes). The `route_audit.json` records the replacement,
  showing the old `fingerprint` in `replacements` and the new `fingerprint`.
- **Unique names**: an authored pipeline name may reuse a member of this component, but not another
  authored name or any other pipeline in the report; a clash is refused and nothing is written.
- `--pipelines-path` is a JSON **list** of pipeline IR dicts (the authored replacements), typically
  carrying `AgenticComponentActivity` nodes (see below). Empty `pipelines` list is refused.
- Each authored pipeline **must** carry the source tag `"tags": {"source": "adf"}` (routing/agentic
  conversion is ADF-only). Combine asserts this up front and **fails closed** (nothing written, with
  a clear message) on a missing or non-`adf` tag, so a mis-tagged pipeline is caught here rather than
  surviving to the package preflight.
- The merged report is **always** validated with the structural bundle invariants (a real
  `prepare → write_bundle` pass) **before it is written** — no bypass — so a duplicate key, dangling
  dependency, cycle, or dangling pipeline/run_job reference can never land on disk. Validation happens
  first; if any violation is found, `ok` is `false`, `violations` lists them, and nothing is written
  (all-or-nothing).

### Authoring an `AgenticComponentActivity` (the escape hatch)

When the target can't be expressed by the typed engine (e.g. a managed Lakeflow Connect ingestion
pipeline), emit an `AgenticComponentActivity` task inside the authored pipeline. The authored
**pipeline** carries the required `tags.source == "adf"`; the task carries the raw bundle components
the package phase writes verbatim:

```json
{
  "name": "IngestAll",
  "tags": {"source": "adf"},
  "tasks": [
    {
      "name": "IngestAll",
      "task_key": "ingest_all",
      "type": "AgenticComponentActivity",
      "files": [
        {"path": "src/ingest/lakeflow_connect.py", "content": "# authored pipeline source ..."}
      ],
      "resources": [
        {"resource_key": "ingest_all_pipeline", "definition": { "channel": "current", "...": "raw pipeline resource" }}
      ],
      "task": {"pipeline_task": {"pipeline_id": "${resources.pipelines.ingest_all_pipeline.id}"}},
      "raw_definition": { "...": "original source definitions, retained for auditing" }
    }
  ]
}
```

- `tags` — the authored pipeline **must** set `tags.source` to `"adf"` (routing/agentic is ADF-only);
  combine fails closed otherwise (see the combine guarantees above).
- `files` — files written below the bundle `src/`; each is `path` + either UTF-8 `content` or
  base64 `binary_content`.
- `resources` — bundle resources in the `resource_key` + raw `definition` shape the bundle writer
  expects.
- `task` — the raw Databricks task fragment (`pipeline_task` or `notebook_task`) wiring to an
  authored resource or file.
- `raw_definition` — the original source definition, retained for provenance.

**Default the Lakeflow pipeline / Connect resource `channel` to `current` (stable/GA).** Do **not**
emit `channel: preview` by default. Let the enriched insight's **structured `release_state`** drive
the channel — the routing recommendation exposes each agentic option's states as a neutral
`release_disclosures` list (factual labels, no alarm). Surface the state as disclosure, tiered as:

- `ga` — `channel: current`; **silent** (not surfaced).
- `unknown` — **silent**, treated exactly like `ga` (no separate label — we can't distinguish them).
- `public_preview` — the informational label **"Public Preview (production-ready)"**: generally
  production-ready and supported per Databricks; still confirm workspace availability. Emit
  `channel: preview` only with the cited `release_state_source`.
- `private_preview` — the plain factual label **"Private Preview"**. Emit `channel: preview` only
  with the cited `release_state_source`.
- `beta` — the plain factual label **"Beta"**. Emit `channel: preview` only with the cited
  `release_state_source`.

Separately from *how the state is surfaced*, keep the recommendation-eligibility rule: treat a
**Private Preview** connector as `doNotSuggest` unless the workspace has confirmed
enrollment/entitlement. And **verify GA-vs-Preview status before recommending any connector or
Lakeflow Connect pattern**: do not hardcode GA/Preview status or dates (release state changes), check
the feature's current release state **and** target-workspace availability against the current public
Databricks docs, and cite the source. (Same structured grounding rule the `flowx-enrich` skill
applies when authoring recommended patterns.)

## Step 4 — Continue to package

Once the routed-agentic groups are filled and the report validates, continue with `flowx-convert`'s
just-in-time configuration (`inspect`/`modify`) as usual, then `flowx-package`. The recorded
`metadata/conversion_plan.json` is kept alongside `inventory.json` as the decision of record. It is the
library's typed `ConversionPlan` (schema 2): one decision per component, bound to the inventory
fingerprint, the saved `source_graphs.json` and the saved `source_insights.json` it was decided on,
with a reserved, empty `assignments` list per component for per-node routing later.

Package verifies the routing plan is still current against the inventory, checks the saved baseline
still has the hashes the routing record names ("re-run convert, then route" otherwise) and that no
stored combine was edited ("re-run fill-agentic combine"), then replays `rebuild` from the baseline,
the plan, and the stored combines. The live `.work/translation_report.json` must match the rebuild
exactly, record included; otherwise it refuses with "re-run route". Modify's configured
`.work/translation_report.stamped.json` must carry the rebuilt routing record; otherwise it refuses
with "the configured report is out of date; re-run modify". Route, combine and merge never rewrite
the configured report, so run `modify` again after any of them.

`metadata/route_audit.json` records per component the `decision`, `outcome`, `fingerprint`,
`combine_sha256`, and `replacements` (changes to the fingerprint across re-routes), plus the hashes
of the plan, the baseline report and gaps, the inventory, and the packaged report. It survives the
prune of `.work/` and documents what was packaged and how.

## Reference

- `flowx-enrich` skill — the `insights` layer routing's agentic option consumes.
- `flowx-convert` — the deterministic engine, `merge_agentic` for convert's own gaps, and just-in-time config.
- `flowx-package` — turns the (filled) report into the deployable DAB bundle.
