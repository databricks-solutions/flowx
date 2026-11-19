---
name: flowx-route
description: >
  Route each connected component of the discovered inventory to a deterministic (1:1 engine) or
  agentic (LLM-assisted re-architecture) conversion: route writes its recommendation and the
  suggested groupings into metadata/conversion_plan.json and a standard review page, the user decides,
  route applies the plan, and fill-agentic fills each routed-agentic unit. Runs after convert (and
  the optional enrich), before package.
  Use when the user asks to route the conversion, choose deterministic or agentic per component,
  review the conversion plan, group pipelines, or fill agentic units.
---

# Route the Conversion (deterministic vs. agentic) and Fill the Gaps

After discover (and the optional `flowx-enrich`, which runs unless the user asked for a
deterministic-only pass), routing decides **per connected component** whether each part of the
factory converts **deterministically** (the typed engine's 1:1 translation) or **agentically** (an
LLM-authored re-architecture, e.g. collapsing five extractors onto one Lakeflow Connect pipeline).
Route writes its recommendation straight into `metadata/conversion_plan.json`, the one readable
record of the decisions, and draws it as `metadata/routing_review.html`. The user decides, you edit
the plan, route applies it to the translation report, and you author the fill.

**ADF only (current scope).** Enrich runs for every source, but the route → fill agentic-conversion
flow is supported for **ADF only**, and `route` enforces it: for an Airflow inventory it recommends and
records only `deterministic` decisions, suggests no groupings, and rejects an `agentic` decision (the
plan is not recorded and the report is left untouched). `convert --merge-agentic --source airflow` is
disabled too. Airflow DAGs are often one connected component, so Airflow agentic conversion follows the
Airflow track's own per-gap and patch contract rather than this whole-component flow. For **Airflow
agentic gaps, use the `flowx-resolve-airflow-gaps` skill** (the strict per-gap resolver), not
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
discover → enrich (optional) → convert (deterministic baseline) → route (recommend → decide → apply) → fill-agentic → package
```

Run convert **before** route: it builds the deterministic baseline report that routing edits, and
once every decision is made route refuses without it ("run the convert phase first"; after a package,
see "After package" below). If you run
`convert` again after routing, run `route` straight after it: a fresh report carries no routing
record, so route takes it as the new baseline and re-applies the plan and the agent's stored outputs
(fills of convert's own gaps merged against a different report are kept but not applied, so merge
them again for the new report; route says how many it skipped). Route and fill-agentic also refuse, writing nothing, an `inventory.json` not
shaped the way discover writes it (not a JSON object, a pipeline with no name or no `activities`, a
motif with no string `motif_id` or no `member_task_keys`, or its `pipelines`, a pipeline's
`activities` or `motifs`, or a motif's `member_task_keys` not a list of the right kind), since
reading one as empty would make a pipeline look fully deterministic; run discover again.

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
  a hosted agent cannot read the server's `output_dir`. When either is missing or unreadable, or too
  large to return inline, `ok` is false, nothing is uploaded, and the error says which; call route
  again (with one of the upload paths when a file was too large).
- **venv CLI:** `"$PY" -m flowx.adapter route --output-dir <dir>` (`--out <file>` writes the result
  JSON to a file).

The plan lists every component with its `members`, the library's `recommended` route, both
first-class conversion `options` (a *deterministic* option carrying the engine-capability assessment
and any uncovered activities, and an *agentic* option carrying the `recommended_patterns` from enrich,
with any `simplification_pattern` flagged and release states disclosed), and `decision: null`
(pending). The library recommends `agentic` for a component only when it has an activity the engine
cannot translate 1:1 **and** enrich recommended at least one pattern for one of its pipelines (any
entry in the agentic option's `recommended_patterns`); otherwise it recommends `deterministic`, and
each untranslatable activity stays a per-activity gap (for ADF, the one `convert --merge-agentic` fills). So without
enrich, or when enrich found no pattern, every component is recommended `deterministic`. The agentic
route is a redesign onto a modernisation or simplification pattern, separate from that per-activity
gap fill, and it is still the customer's decision. The plan also lists the `suggested_groupings`, an
empty `conversation`, and the `findings`
(unresolved control edges kept, never severed). A decision already in the file for a component with
the same members is kept, so re-running route, or running it after a re-enrich, keeps your decisions,
except the components of an accepted grouping route no longer suggests, which go back to pending and
are listed under `dropped_groupings` (see Suggested groupings below). A re-discover does not:
discover moves the earlier `metadata/`, `.work/` and bundle into `metadata/archive/<UTC time>/`, with
an `archive.json` record of what moved, so `conversion_plan.json` and the agent's outputs in
`agentic_conversion.json` are kept for audit but no longer used; route and fill again afterwards.

### Present it with the review page

`metadata/routing_review.html` is the standard way to show the customer what their source does and how
it will be converted. Every run has the same eight sections in the same order: at a glance, what the
source does (the insights), the recommended conversion, suggested groupings, what happens next, what
you told us (the routing conversation), notes (the findings), and how to change a decision. Open it
with the customer, or walk through the same sections in that order.

Ask what they want the conversion to achieve, and qualify the target pattern for each ingestion
component (next section). Record each question and answer in the plan's `conversation`
(`[{"question": "...", "answer": "..."}]`). Package copies it into `route_audit.json`.

### Qualify the target pattern before you suggest one

A component that ingests from a database can land in one of four patterns. Before you suggest one, read [`references/ingestion-patterns.md`](references/ingestion-patterns.md): the four patterns, the checks C1 to C13 and the limits each rests on (read live), and how to show a pattern you rule out.

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
(`grouping-<hash of its members>`), so suggestions never overlap; the same members keep the same id,
so an accepted grouping stays accepted, and keeps its agent output, when enrich runs again and other
suggestions change. Each lists its `components`, `members` and `basis`
(the relationships or the shared pattern behind it), and `accepted: false`. Only a suggested grouping
can be accepted; to group other components, record an inferred relationship through enrich and route
again.

When enrich changes an accepted grouping's own members, route no longer suggests it. Route then lists
it under `dropped_groupings` in its result (its `components`, `members`, and `replaced_by`, the
current suggestions that share its components), says so in its message and on the review page, and
sets its components back to pending, so nothing is applied until you decide them again.

## Step 2 — Decide, then route again to apply

Edit only these fields in `metadata/conversion_plan.json` (everything else is library-owned and
recomputed on every route, so edits to it are ignored; an unknown key is refused):

- each component's `decision` — `"deterministic"` or `"agentic"` (the customer's choice) — and an
  optional `rationale`;
- to accept a grouping, its `accepted: true`, and decide each of its components `"agentic"`;
- `conversation`.

Then run `route` again (no plan). It checks the edited file with the same rules as a passed plan, so a
misspelt key or a value such as `accepted: "true"` is refused and the file is left as you wrote it.
Once **no decision is pending** it validates the plan, records it, and applies it. You can also pass a
plan — `--plan-path <file>`, or MCP `plan` inline / `plan_path`: the decisions alone
(`{"components": [{component_id, members, decision}], "suggested_groupings": [{grouping_id,
accepted}], "conversation": [...]}`), or an edited copy of the recorded plan. This is the hosted path:
edit the plan route returned and send it back as `plan`.

Rules the validator enforces (all violations returned at once; nothing written on failure):

- `decision` is `"deterministic"`, `"agentic"`, or `null` while pending; `rationale` (optional) is a
  non-empty string.
- Every component is listed exactly once, and its `members` must exactly match one computed
  connected component; `component_id` must match too.
- An accepted grouping has no component decided `"deterministic"`; agentic decisions and groupings are
  ADF-only (an inventory that records no `source` is refused too; re-run discover).
- A grouping entry names a current suggestion by its `grouping_id`, with the same `components` and
  `members` when it lists them. Ids derive from members, so after enrich changes the suggestions an
  edited copy of an older plan can name a grouping that no longer exists: route refuses it and says to
  run route without a plan to see the current ones.
- `conversation` entries are `{question, answer}` with non-empty strings.

### What applying does

Route rebuilds `.work/translation_report.json` + `gaps.json` from convert's baseline, the stored fills
of convert's own gaps, the plan, and the agent's outputs in `metadata/agentic_conversion.json`. Each
**routing unit** (a component, or an accepted grouping) becomes:

- **deterministic**: the baseline pipelines stay (outcome `deterministic`).
- **agentic with a stored output** whose members match: the output's pipelines replace the members,
  and their routed gaps are dropped (outcome `agentic-applied`). So are convert's untagged gaps for the
  activities the unit owns: its tasks, the activities a motif collapsed, and activities only the
  inventory lists for its pipelines. Such a gap is kept only when a pipeline that stays deterministic
  also holds that name.
- **agentic without one**: every task becomes a `PlaceholderActivity` with one pipeline-tagged gap,
  the deterministic translation kept as its `raw_definition` (outcome `agentic-not-viable` — package
  refuses until it is filled).

A stored fill of convert's own gap also takes that gap, and the gaps of anything nested in the filled
task, out of `gaps.json`, so the list shows only what is left. Convert's gaps carry no pipeline name, so
a gap is kept when another pipeline outside the routed units still has that name open.

When nothing is routed agentic (and no gap fill is stored), route writes convert's report and gaps back
byte for byte, with no record — the non-breaking guarantee. Otherwise it stamps a **routing record**
onto the report (`_routing_record`): the plan's hash, the baseline report and gaps hashes, the hash
of the gap fills applied, and one entry per unit with its `members`, `decision`, `outcome`, `output_sha256` (set only while its stored
output is applied) and `fingerprint` (a grouping also lists its `components`). The plan is freely
editable: change it and route again as often as needed; the same plan gives the same bytes, and a
changed unit is re-derived.

## Step 3 — Fill each routed-agentic unit with fill-agentic

A routed-agentic unit is filled **only** by `fill-agentic`, which replaces the whole unit with
pipelines you author (no LLM in the library, which validates and records).

```bash
"$PY" -m flowx.adapter fill-agentic \
  --output-dir <output_dir> \
  --member IngestSalesforce --member IngestWorkday \
  --pipelines-path authored_pipelines.json \
  [--out <result.json>]
```

MCP: `flowx(command="fill_agentic", parameters={"output_dir": ..., "members": [...], "pipelines": [...]})`
(pass `pipelines` inline as a list, or `pipelines_path`).

Author the pattern the customer chose (see "Qualify the target pattern"), from the facts recorded in
the plan's `conversation`, and don't pick one they didn't choose. An M1 fill authors one ingestion
pipeline per shard, each within L tables and with no table name twice; an M2 fill authors the job, a
seed for its control table, and its notebooks.

To keep a pipeline 1:1, fill its component with one authored pipeline of the **same name**. The
per-pipeline `convert --merge-agentic` is for convert's **own** agentic gaps only: it refuses, writing
nothing, any result that names a routed-agentic pipeline, or whose activity would first land in one,
with "this pipeline is routed agentic; fill its component with fill-agentic (run fill-agentic again
with new pipelines to change the approach)". After routing, a merge into the live report is also
stored as a gap fill in `metadata/agentic_conversion.json`, so every rebuild re-applies it on top of
the unchanged baseline.

Its guarantees:

- The members (one `--member` per name, taken exactly as given, so a name may hold a comma; or
  `--members` comma-separated; MCP accepts a list) must **exactly match** a routed-**agentic** unit —
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
  lives in `agentic_conversion.json`, so it survives switching the unit to deterministic and back. An
  entry edited by hand no longer checks out, so filling it again starts a fresh history.
- **Unique names**: an authored pipeline name may reuse a member of this unit, but not another name in
  the same list, a member of another unit, a pipeline another unit's applied output authored, or a name
  that shares a bundle folder with another pipeline once normalised (`Sales Load` and `Sales-Load`
  would overwrite each other). A stored output that is no longer applied (its unit routed
  deterministic since, or joined into a grouping) holds no name.
- **Buildable**: each authored pipeline needs a non-empty string `name` and a non-empty `tasks` list
  of task objects, and must not be marked `migration_status: "excluded"` (package skips those).
- **Fully converted**: an authored pipeline that still holds a `PlaceholderActivity` or an
  `UnsupportedActivity`, or a task whose `type` is not an IR task type (for example a misspelt
  `NotebokActivity`), at the top level or inside a ForEach / If / Switch, is refused: package would
  build each of those as a stub.
- Each authored pipeline **must** carry `"tags": {"source": "adf"}`; a missing or non-`adf` tag fails
  closed. An empty `pipelines` list is refused.
- The merged report is **always** validated with the structural bundle invariants (a real
  `prepare → write_bundle` pass) **before it is written** — no bypass — so a duplicate key, dangling
  dependency, cycle, or dangling pipeline/run_job reference can never land on disk (all-or-nothing).
- **One at a time per output folder**: route, fill-agentic and a merge into the live report each hold
  `metadata/.route.lock` while they work, and refuse, writing nothing, while another holds it (route
  and fill-agentic return the reason under `violations`; a merge prints it and exits 1). Run fills one
  after another, never in parallel. A lock is left behind when a run is stopped mid-write (the hosted
  server stops a command after `FLOWX_MCP_TIMEOUT` seconds). The next command takes it over once the
  process that held it has exited; a lock written on another machine, or any lock on Windows, once
  it is older than `FLOWX_MCP_TIMEOUT` (1800 seconds by default). Then run route, and the refused fill or merge again.
  Locally you can also delete the lock first.

### Authoring an `AgenticComponentActivity` (the escape hatch)

When the target can't be expressed by the typed engine (for example a managed Lakeflow Connect ingestion pipeline), the authored pipeline carries an `AgenticComponentActivity` task. Read [`references/agentic-component.md`](references/agentic-component.md) before you author one: its fields, the pipeline `channel` default, and how to show a release state.

## Step 4 — Continue to package

Once every agentic unit is filled, continue with `flowx-convert`'s just-in-time configuration
(`inspect`/`modify`) as usual, then `flowx-package`. The recorded `metadata/conversion_plan.json` is the
library's typed `ConversionPlan` (schema 3), bound to the inventory fingerprint, the saved
`source_graphs.json` and the saved `agentic_insights.json` it was decided on. Phase 1 decides whole
components, so a plan holds no per-node assignments.

Package refuses, writing nothing, when:

- `agentic_insights.json` and `inventory.json` disagree (or the file fails its own hash, or its
  `inventory_sha256` no longer matches the inventory), plan or no plan — enrich may have stopped
  between its two writes, or a file was edited; run enrich again with the same insights (locally,
  delete `metadata/.enrich.lock` first if it is still there; on the hosted server, run discover again,
  then enrich prepare and apply — running discover again moves `conversion_plan.json` and
  `agentic_conversion.json` into `metadata/archive/`, so route and fill again afterwards);
- the report carries a routing record but `conversion_plan.json` is missing;
- the plan is stale against the inventory, is not a complete decision, or has a decision pending; or
  it holds a key route does not write, or a recommendation, option, grouping or finding that no longer
  matches what route records from the inventory (edit only the decisions, `accepted` and the
  conversation, then route again);
- a routed-agentic unit has **no agent output yet** — an agentic decision is packaged agentically,
  never as the deterministic stand-in; fill it, or decide it deterministic and route again;
- the saved baseline no longer has the hashes the record names ("re-run convert, then route"; route,
  fill-agentic and merge refuse it too), or a stored output or gap fill was edited, moved to another
  unit or pointed at another activity ("re-run fill-agentic", or merge that gap again). An edited
  entry for a unit the plan no longer has, or no longer routes agentic, is never applied, so package
  only warns about it. Every stored
  entry carries a hash over all its fields, so any edit to it shows, and the store must hold exactly
  `components` and `gap_fills`, so an added key is never dropped on the next save and a deleted one
  never silently drops what it held;
- a plan is recorded and the report carries no routing record while the plan routes a unit agentic
  or `agentic_conversion.json` stores a gap fill that applies to it, as after a re-convert ("re-run
  route", which applies the fill). With no plan recorded the
  folder is not routed, so package ignores the stored outputs and fills;
- the live `.work/translation_report.json` does not match a fresh rebuild ("re-run route"), or any
  other packaged report — modify's configured `.work/translation_report.stamped.json`, or a
  `modify --out` copy — does not carry the live report's routing record ("the configured report is
  out of date; re-run modify"). Route, fill-agentic and merge never rewrite the configured report, so
  run `modify` again after any of them.

`metadata/route_audit.json` records per unit the `decision`, `outcome`, `fingerprint`, `output_sha256`
and `replaced` history (a grouping also lists `grouped_components`), the gap fills applied, the routing
`conversation`, and the hashes of the plan (the same canonical hash the routing record holds), the
baseline report and gaps, the inventory, the saved source graphs and agentic insights the plan was
bound to, and the packaged report. It survives the prune of `.work/`
and documents what was packaged and how.

## After package: change the conversion

Package prunes `.work/`, so route and fill-agentic refuse straight after a package. To change a decision, an answer or an agent output after a package, follow [`references/after-package.md`](references/after-package.md).

## Reference

- `references/ingestion-patterns.md`: the four ingestion patterns and the checks C1 to C13.
- `references/agentic-component.md`: authoring an `AgenticComponentActivity` and its release state.
- `references/after-package.md`: changing the conversion after package has run.
- `flowx-enrich` skill — the `insights` layer routing's agentic option and groupings consume.
- `flowx-convert` — the deterministic engine, `merge_agentic` for convert's own gaps, and just-in-time config.
- `flowx-package` — turns the (filled) report into the deployable DAB bundle.
