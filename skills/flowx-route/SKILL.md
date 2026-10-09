---
name: flowx-route
description: >
  Route each connected component of the discovered inventory to a deterministic (1:1 engine) or
  agentic (LLM-assisted re-architecture) conversion: route writes its recommendation and the
  suggested groupings into metadata/conversion_plan.json and a standard review page, the user decides,
  route applies the plan, and fill-agentic fills each routed-agentic unit. Runs after enrich and
  convert, before package.
triggers:
  - "route pipelines"
  - "route conversion"
  - "conversion plan"
  - "deterministic or agentic"
  - "fill agentic"
  - "group pipelines"
  - "agentic conversion"
  - "recommend conversion route"
---

# Route the Conversion (deterministic vs. agentic) and Fill the Gaps

After discover (and, by default, `flowx-enrich`), routing decides **per connected component** whether
each part of the factory converts **deterministically** (the typed engine's 1:1 translation) or
**agentically** (an LLM-authored re-architecture, e.g. collapsing five extractors onto one Lakeflow
Connect pipeline). Route writes its recommendation straight into `metadata/conversion_plan.json`, the
one readable record of the decisions, and draws it as `metadata/routing_review.html`. The user decides,
you edit the plan, route applies it to the translation report, and you author the fill.

**ADF only (current scope).** The discover → enrich → route → fill agentic-conversion flow is
supported for **ADF today**, and `route` enforces it: for an Airflow inventory it recommends and
records only `deterministic` decisions, suggests no groupings, and rejects an `agentic` decision (the
plan is not recorded and the report is left untouched). `convert --merge-agentic --source airflow` is
disabled too. Airflow DAGs are often one connected component, so Airflow agentic conversion follows the
Airflow track's own per-gap and patch contract rather than this whole-component flow. For **Airflow
agentic gaps today, use the `flowx-resolve-airflow-gaps` skill** (the strict per-gap resolver), not
this routing flow.

**There is no LLM inside flowx.** The library computes the recommendation deterministically and only
*validates and records* the decision and the authored fill — the same author → validate → merge
contract `enrich` uses. This is additive and non-breaking: with **no recorded plan**, `convert` and
`package` behave exactly as before.

**Who decides what.** The library **recommends** a route per component and **suggests** groupings;
the **customer decides** deterministic-vs-agentic for each component and which groupings to accept;
the **agent** presents the review page, asks the customer what they want the conversion to achieve,
records only the customer's decisions in the plan, and authors the agentic fill. The agent never picks
the route on the customer's behalf. Route leaves every decision **pending** until it is made, and
applies nothing while one is pending.

## The files

| Stage | File | Written by |
|---|---|---|
| describe | `metadata/source_graphs.json`, `metadata/agentic_insights.json` (optional), `metadata/inventory.json` | discover, enrich |
| decide | `metadata/conversion_plan.json` — components, decisions, suggested groupings, conversation | route writes it; you edit it |
| decide | `metadata/routing_review.html` — the standard review page (generated, never edited) | route, fill-agentic |
| convert | `.work/translation_report.json`, `.work/gaps.json` — the deterministic report with the agentic decision's edits | convert, then route |
| convert | `metadata/agentic_conversion.json` — the agent's conversion output (per unit, plus fills of convert's own gaps) | fill-agentic, merge_agentic |
| convert | `.work/route_baseline/` — convert's report as route first found it (internal, never rewritten) | route |
| after | the bundle, `metadata/route_audit.json` | package |

## Where routing sits

```
discover → enrich (default) → convert (deterministic baseline) → route (recommend → decide → apply) → fill-agentic → package
```

Convert builds the deterministic baseline report **before** routing (route can trigger it in-process
via `--source` / `--source-path`); routing then edits that report. If you run `convert` again after
routing, run `route` straight after it: a fresh report carries no routing record, so route takes it
as the new baseline and re-applies the plan and the agent's stored outputs (fills of convert's own
gaps merged against the earlier report are dropped and must be merged again; route says how many).

Pipelines are grouped into weak/undirected **connected components** over the inventory's control
lineage (`lineage.control_edges`), so mutually-referencing pipelines are decided together and a
caller/callee reference is never split across incompatible routes. Only resolved control edges join
a component; an unresolved call is reported as a finding. ADF emits these control edges (from
`ExecutePipeline`) in discovery.

Run the **`setup`** skill first if you haven't. Everything below has an MCP-tool path (Genie Code, or
a local stdio registration — call the single **`flowx`** tool, run no `python3`/`$PY`) and a venv-CLI
path (local; `PY="$(cat <plugin_dir>/.migration-venv)"` and `export PYTHONPATH="<plugin_dir>/src"`).

## Step 1 — Route writes the recommendation into the plan

Run `route` with no plan. It writes `metadata/conversion_plan.json` and `metadata/routing_review.html`
and touches nothing else:

- **MCP tool:** `flowx(command="route", parameters={"output_dir": "<dir>"})` — the plan and the page
  come back under `bundle` (or are uploaded to `output_volume_path` / `output_workspace_path`), because
  a hosted agent cannot read the server's `output_dir`.
- **venv CLI:** `"$PY" -m flowx.adapter route --output-dir <dir>` (`--out <file>` writes the result
  JSON to a file).

The plan lists every component with its `members`, the library's `recommended` route, both
first-class conversion `options` (a *deterministic* option carrying the engine-capability assessment
and any uncovered activities, and an *agentic* option carrying the `recommended_patterns` from enrich,
with any `simplification_pattern` flagged and release states disclosed), and `decision: null`
(pending). It also lists the `suggested_groupings`, an empty `conversation`, and the `findings`
(unresolved control edges kept, never severed). A decision already in the file for a component with
the same members is kept, so re-running route after a re-enrich or re-discover keeps your decisions
and leaves only new or regrouped components pending.

### Present it with the review page

`metadata/routing_review.html` is the standard way to show the customer what their source does and how
it will be converted. Every run has the same eight sections in the same order: summary, what the
source does (the insights), components, suggested groupings, how each unit will be converted, the
routing conversation, findings, and how to steer. Open it with the customer, or walk through the same
sections in that order.

Ask open questions grounded in what discover and enrich found — what they want the conversion to
achieve, the databases behind the extractors, whether they are after a specific pattern such as
Lakeflow Connect — and record each question and answer in the plan's `conversation`
(`[{"question": "...", "answer": "..."}]`). Package copies it into `route_audit.json`.

### Suggested groupings

A **grouping** is two or more whole components the insights suggest converting together, agentically,
as **one unit**: one agent output replaces all of their pipelines (for example five independent
extractors → one Lakeflow Connect pipeline). It never splits a component. Route suggests one from two
kinds of link in the insights:

- an `inferred` pipeline relationship (with its evidence and confidence) between pipelines of
  different components;
- one simplification pattern (same pattern name, `simplification_pattern: true`) recommended for
  pipelines in more than one component — the independent extractors one managed pattern could replace.

Components joined by these links, directly or through each other, form one suggestion
(`grouping-<n>`), so suggestions never overlap. Each lists its `components`, `members` and `basis`
(the relationships or the shared pattern behind it), and `accepted: false`. Only a suggested grouping
can be accepted; to group other components, record an inferred relationship through enrich and route
again.

## Step 2 — Decide, then route again to apply

Edit only these fields in `metadata/conversion_plan.json` (everything else is library-owned and
recomputed on every route, so edits to it are ignored; an unknown key is refused):

- each component's `decision` — `"deterministic"` or `"agentic"` (the customer's choice) — and an
  optional `rationale`;
- to accept a grouping, its `accepted: true`, and decide each of its components `"agentic"`;
- `conversation`.

Then run `route` again (no plan). Once **no decision is pending** it validates the plan, records it,
and applies it. Other ways to supply the decisions:

- **A plan** — `--plan-path <file>` (or `--plan-path -` for stdin), or MCP `plan` inline / `plan_path`:
  the decisions alone (`{"components": [{component_id, members, decision}], "suggested_groupings":
  [{grouping_id, accepted}], "conversation": [...]}`), or an edited copy of the recorded plan. This is
  the hosted path: edit the plan route returned and send it back as `plan`.
- **Interactive prompt (TTY)** — `route` on a real terminal offers each suggested grouping whose
  components are pending, then asks `route [d]eterministic / [a]gentic (default=<recommended>)` for
  each pending component.

Rules the validator enforces (all violations returned at once; nothing written on failure):

- `decision` is `"deterministic"`, `"agentic"`, or `null` while pending; `rationale` (optional) is a
  non-empty string.
- Every component is listed exactly once, and its `members` must exactly match one computed
  connected component; `component_id` must match too.
- An accepted grouping has no component decided `"deterministic"`; agentic decisions and groupings are
  ADF-only.
- `conversation` entries are `{question, answer}` with non-empty strings.

### What applying does

Route rebuilds `.work/translation_report.json` + `gaps.json` from convert's baseline, the stored fills
of convert's own gaps, the plan, and the agent's outputs in `metadata/agentic_conversion.json`. Each
**routing unit** (a component, or an accepted grouping) becomes:

- **deterministic**: the baseline pipelines stay (outcome `deterministic`).
- **agentic with a stored output** whose members match: the output's pipelines replace the members,
  and their routed gaps are dropped (outcome `agentic-applied`).
- **agentic without one**: every task becomes a `PlaceholderActivity` with one pipeline-tagged gap,
  the deterministic translation kept as its `raw_definition` (outcome `agentic-not-viable` — package
  refuses until it is filled).

When nothing is routed agentic (and no gap fill is stored), route writes convert's report and gaps back
byte for byte, with no record — the non-breaking guarantee. Otherwise it stamps a **routing record**
onto the report (`_routing_record`): the plan's hash, the baseline report and gaps hashes, and one
entry per unit with its `members`, `decision`, `outcome`, `output_sha256` (set only while its stored
output is applied) and `fingerprint` (a grouping also lists its `components`). The plan is freely
editable: change it and route again as often as needed; the same plan gives the same bytes, and a
changed unit is re-derived.

## Step 3 — Fill each routed-agentic unit with fill-agentic

A routed-agentic unit is filled **only** by `fill-agentic`, which replaces the whole unit with
pipelines you author (no LLM in the library, which validates and records).

```bash
"$PY" -m flowx.adapter fill-agentic \
  --output-dir <output_dir> \
  --members "IngestSalesforce,IngestWorkday" \
  --pipelines-path authored_pipelines.json \
  [--out <result.json>]
```

MCP: `flowx(command="fill_agentic", parameters={"output_dir": ..., "members": [...], "pipelines": [...]})`
(pass `pipelines` inline as a list, or `pipelines_path`).

To keep a pipeline 1:1, fill its component with one authored pipeline of the **same name**. The
per-pipeline `convert --merge-agentic` is for convert's **own** agentic gaps only: it refuses, writing
nothing, any result that names a routed-agentic pipeline, or whose activity would first land in one,
with "this pipeline is routed agentic; fill its component with fill-agentic (run fill-agentic again
with new pipelines to change the approach)". After routing, a merge into the live report is also
stored as a gap fill in `metadata/agentic_conversion.json`, so every rebuild re-applies it on top of
the unchanged baseline.

Its guarantees:

- `--members` (comma-separated; MCP accepts a list) must **exactly match** a routed-**agentic** unit —
  a component, or an accepted grouping's full member list — in the recorded plan, whose
  `inventory_sha256`, `source_graphs_sha256` and `agentic_insights_sha256` must still match the
  current inventory, with nothing pending. A partial group, a superset, a typo, or a deterministic
  component is refused. The report must carry a routing record that matches that plan.
- **Same authored pipelines** (the canonical hash covers every field of every authored pipeline):
  `already_applied: true` with the message "already applied, unchanged"; nothing is written. If the
  report no longer reflects the stored output, or the stored entry was edited by hand, it stores the
  pipelines again, rebuilds, validates and writes the result.
- **Different authored pipelines** replace the stored output for that unit only and add
  `{"from", "to"}` to the entry's `replaced` history (the first fill is not a replacement). The history
  lives in `agentic_conversion.json`, so it survives switching the unit to deterministic and back.
- **Unique names**: an authored pipeline name may reuse a member of this unit, but not another name in
  the same list, a member of another unit, a pipeline another unit's output authored, or a name that
  shares a bundle folder with another pipeline once normalised (`Sales Load` and `Sales-Load` would
  overwrite each other).
- **Fully converted**: an authored pipeline that still holds a `PlaceholderActivity` (top level or
  inside a ForEach / If / Switch) is refused.
- Each authored pipeline **must** carry `"tags": {"source": "adf"}`; a missing or non-`adf` tag fails
  closed. An empty `pipelines` list is refused.
- The merged report is **always** validated with the structural bundle invariants (a real
  `prepare → write_bundle` pass) **before it is written** — no bypass — so a duplicate key, dangling
  dependency, cycle, or dangling pipeline/run_job reference can never land on disk (all-or-nothing).

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
  fill-agentic fails closed otherwise.
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

Once every agentic unit is filled, continue with `flowx-convert`'s just-in-time configuration
(`inspect`/`modify`) as usual, then `flowx-package`. The recorded `metadata/conversion_plan.json` is the
library's typed `ConversionPlan` (schema 3), bound to the inventory fingerprint, the saved
`source_graphs.json` and the saved `agentic_insights.json` it was decided on, with a reserved, empty
`assignments` list per component for per-node routing later.

Package refuses, writing nothing, when:

- `agentic_insights.json` and `inventory.json` disagree (or the file fails its own hash), plan or no
  plan — enrich may have stopped between its two writes; run enrich again with the same insights
  (locally, delete `metadata/.enrich.lock` first if it is still there; on the hosted server, run
  discover again, then enrich prepare and apply);
- the report carries a routing record but `conversion_plan.json` is missing;
- the plan is stale against the inventory, is not a complete decision, or has a decision pending;
- a routed-agentic unit has **no agent output yet** — an agentic decision is packaged agentically,
  never as the deterministic stand-in; fill it, or decide it deterministic and route again;
- the saved baseline no longer has the hashes the record names ("re-run convert, then route"), or a
  stored output or gap fill was edited ("re-run fill-agentic", or merge that gap again);
- the live `.work/translation_report.json` does not match a fresh rebuild ("re-run route"), or any
  other packaged report — modify's configured `.work/translation_report.stamped.json`, or a
  `modify --out` copy — does not carry the live report's routing record ("the configured report is
  out of date; re-run modify"). Route, fill-agentic and merge never rewrite the configured report, so
  run `modify` again after any of them.

`metadata/route_audit.json` records per unit the `decision`, `outcome`, `fingerprint`, `output_sha256`
and `replaced` history (a grouping also lists `grouped_components`), the gap fills applied, the routing
`conversation`, and the hashes of the plan (the same canonical hash the routing record holds), the
baseline report and gaps, the inventory, and the packaged report. It survives the prune of `.work/`
and documents what was packaged and how.

## Reference

- `flowx-enrich` skill — the `insights` layer routing's agentic option and groupings consume.
- `flowx-convert` — the deterministic engine, `merge_agentic` for convert's own gaps, and just-in-time config.
- `flowx-package` — turns the (filled) report into the deployable DAB bundle.
