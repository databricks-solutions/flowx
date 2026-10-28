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

**ADF only (current scope).** Enrich runs for every source, but the route → fill agentic-conversion
flow is supported for **ADF today**, and `route` enforces it: for an Airflow inventory it recommends and
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

Run convert **before** route: it builds the deterministic baseline report that routing edits, and
once every decision is made route refuses without it ("run the convert phase first"; after a package,
see "After package" below). If you run
`convert` again after routing, run `route` straight after it: a fresh report carries no routing
record, so route takes it as the new baseline and re-applies the plan and the agent's stored outputs
(fills of convert's own gaps merged against the earlier report are dropped and must be merged again;
route says how many). Route and fill-agentic also refuse, writing nothing, an `inventory.json` not
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
(pending). It also lists the `suggested_groupings`, an empty `conversation`, and the `findings`
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

A component that ingests from a database can land in one of four patterns. Tell which components do
from the `source:` line enrich wrote in a pipeline's `conversion_notes`, or else from the signals in
`flowx-discover`'s `sources/adf.md`; the connector decides which limits apply. The library doesn't run
these checks; you do, from the facts and the limits, and you record them as free text in the plan's
`conversation`.

| Pattern | What it is | Facts it needs | Decision |
|---|---|---|---|
| **D1** deterministic 1:1 | The engine's port (For each + JDBC notebooks), same behaviour | None | `deterministic` |
| **M1** Lakeflow Connect managed | Gateway or integrated CDC ingestion pipelines generated from the worklist, sharded by database or schema | Scale, repeat, fan-in, change, bar, policy, shaping | `agentic` |
| **M2** metadata-driven worklist | A job over the worklist that skips unchanged sources, reads the change feed over JDBC, MERGEs (fan-in allowed) and reseeds when retention lapses | Scale, change, fan-in, bar | `agentic` |
| **A1** agentic re-architecture | Bespoke, for example several workspaces or metastores, an accelerator or partner tool, or mixed patterns | Whatever conflicts remain | `agentic` |

The facts are the ones `flowx-enrich` asks about. **Follow-up questions:** carry each question and
answer from the insights' `conversion_notes` into `conversation`, then ask at most three more, only on
facts still open for the patterns still in play. These are usually platform-side: whether a feature is
enabled on the workspace, which compute is allowed, how many tables the metastore already holds. Never
re-ask a fact already answered, a "don't know" included. End each answer with the fact on one line,
`fact: value (source: code | data | user | assumed)`.

**Limits** are read live, as `flowx-enrich` does; flowx keeps no copy of them, because they change.
Before you suggest a pattern, open the current Databricks docs for every limit its checks below rest
on, and record each in `conversation` as `limit: <name> = <value> (source: <page>, read <date>)`.
Reuse a limit enrich recorded in `conversion_notes` only after confirming it on its page. For SQL
Server they are on the connector's limits and integrated CDC pages, the resource limits page, the Unity
Catalog metastore page, the For each and pipeline task pages and the serverless Private Link page; for
another connector, read its limits page the same way. The docs don't state three claims the checks
use, marked *Assumption:* below: say so beside the pattern, and confirm the claim with the account
team before ruling a pattern out on it. When the docs can't be reached, show the pattern as
*not qualified: limits not checked*; never use a remembered number.

Let **N** be the total tables and **L** the recommended tables per pipeline for the chosen mode
(gateway or integrated CDC). When a pipeline can't hold two tables with the same name, even from
different schemas or databases, and names repeat across databases, the pipeline count is
**P** = Σ over databases of ⌈tables in the database / L⌉; otherwise P = ⌈N / L⌉. In gateway mode, let
**G** be the gateways those P pipelines need: P when each ingestion pipeline needs its own gateway.
When the pipelines run from a pipeline task inside For each, let **F** be how many iterations that task
runs at once. Read all three rules live like any limit; if the same-name page now lets a pipeline hold
more than one table of a name, size P from what it allows and show the sum in `conversation`.

| Check | Passes when | Limits | When it fails |
|---|---|---|---|
| C1 fan-in | Fan-in isn't required, or only a few sources feed each logical table (a UNION view over a handful is fine) | *Assumption:* each destination table is written by one pipeline only | M1 out |
| C2 pipelines | P ≤ saved pipelines per workspace; in gateway mode count P + G (the pipelines and their gateways). Warn when P > concurrent updates; in gateway mode, when G running gateways plus the ingestion updates exceed it | Saved pipelines per workspace; concurrent pipeline updates per workspace; *Assumption:* a running gateway holds one concurrent update | M1 strained or out |
| C3 tables | N plus the tables the metastore already holds fits per metastore, and tables per schema and schemas per catalog fit | Tables per metastore; tables per schema; schemas per catalog | M1 out, or a quota and a multi-catalog plan |
| C4 history | Current state is enough, or every table has a change feed that carries every change, on a source tier that supports it | Source docs | M1 is limited to SCD1 without CDC and M2 gives state per run only, so SCD2 required without CDC tips to A1 |
| C5 mode | Gateway: G always-on classic gateways are acceptable. Integrated CDC: the workspace has it enabled | One gateway per ingestion pipeline; the compute a gateway runs on; integrated CDC enabled on the workspace | Pick the other mode and re-check; if neither mode passes, M1 out, so D1 or A1 |
| C6 run time | ⌈P / min(slots, chosen concurrency, F)⌉ × the minimum update run fits the allowed run time (leave F out when no For each pipeline task runs them), where slots is concurrent updates, or concurrent updates − G in gateway mode. If slots is zero or less (G at or above concurrent updates in gateway mode), M1 is out in gateway mode; re-check with integrated CDC | Concurrent pipeline updates per workspace; *Assumption:* a running gateway holds one concurrent update; the minimum run of each integrated CDC extraction; concurrent iterations of a pipeline task inside For each | M1 out |
| C7 cost floor | P × updates per day × the minimum update run × the cluster rate, plus G always-on classic gateway clusters in gateway mode, fits the budget or today's spend. Compare M2, which pays mostly for sources that changed | The minimum run of each integrated CDC extraction; the compute a gateway runs on | M1 not preferred |
| C8 retention | Change-feed retention is longer than the longest gap between successful runs, with margin | Source docs | Disclose on whichever pattern is suggested: a reseed risk, and history is lost on a full refresh |
| C9 network | Classic compute has a network path to the source; serverless has NCC private endpoints within the region's limit | NCC private endpoints per region | Choose the compute |
| C10 connections | The source servers fit the connections per metastore | Connections per metastore | M1 needs a quota, or M2 |
| C11 shaping | No expression, join or row filter has to happen at landing | None | M1 isn't 1:1; M2 or D1 |
| C12 release | The chosen mode's release state, checked live as `flowx-enrich` requires, is disclosed and the customer accepts it | *Assumption:* integrated CDC's release state, which its page doesn't label; state what the page says | M1 in the other mode only, re-checked; if that mode fails too, D1 or A1 |
| C13 jobs (M2, D1) | Tasks and parent tasks running at once, and saved jobs, fit; For each inputs fit through task values, so batch sources per task | Tasks running at once; Run job and For each parent tasks running at once; saved jobs per workspace; For each inputs passed through task values | Batch, or A1 |

**Suggestion order:**

1. **M1** when C1 to C7 and C9 to C12 pass. It's the managed option. A C8 failure doesn't rule it out; it's a
   reseed and lost-history risk you disclose on whichever pattern is suggested.
2. **M2** when M1 fails on C1, C2, C3, C6, C7, C10 or C11, and C13 passes.
3. **D1** when the customer wants like-for-like, or no database ingestion is in scope.
4. **A1** when no pattern passes, or the requirements conflict.

Beside each pattern you rule out, show the failing check and its source: the limit's page and the
date you read it, or the fact and where it came from. A pattern whose facts are still unknown
shows as *not qualified: needs <fact>*; present the remaining candidates with their assumptions marked
rather than asking again. The customer still decides, and the fill (Step 3) builds the pattern they
chose. On a source where route records deterministic decisions only, D1 is the decision and the facts
inform the per-gap fills.

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
  --members "IngestSalesforce,IngestWorkday" \
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
  lives in `agentic_conversion.json`, so it survives switching the unit to deterministic and back. An
  entry edited by hand no longer checks out, so filling it again starts a fresh history.
- **Unique names**: an authored pipeline name may reuse a member of this unit, but not another name in
  the same list, a member of another unit, a pipeline another unit's applied output authored, or a name
  that shares a bundle folder with another pipeline once normalised (`Sales Load` and `Sales-Load`
  would overwrite each other). A stored output that is no longer applied (its unit routed
  deterministic since, or joined into a grouping) holds no name.
- **Buildable**: each authored pipeline needs a non-empty string `name` and a non-empty `tasks` list
  of task objects, and must not be marked `migration_status: "excluded"` (package skips those).
- **Fully converted**: an authored pipeline that still holds a `PlaceholderActivity` (top level or
  inside a ForEach / If / Switch) is refused.
- Each authored pipeline **must** carry `"tags": {"source": "adf"}`; a missing or non-`adf` tag fails
  closed. An empty `pipelines` list is refused.
- The merged report is **always** validated with the structural bundle invariants (a real
  `prepare → write_bundle` pass) **before it is written** — no bypass — so a duplicate key, dangling
  dependency, cycle, or dangling pipeline/run_job reference can never land on disk (all-or-nothing).
- **One at a time per output folder**: route, fill-agentic and a merge into the live report each hold
  `metadata/.route.lock` while they work, and refuse, writing nothing, while another holds it (route
  and fill-agentic return the reason under `violations`; a merge prints it and exits 1). Run fills one
  after another, never in parallel. A lock is left behind only when a run is stopped mid-write, or when
  writing the lock itself fails and the command can't confirm the lock is its own (it then writes
  nothing); then delete it, run route, and run the refused fill or merge again (on the hosted server,
  run discover again, which moves `metadata/`, `.work/` and the lock into `metadata/archive/`, then
  enrich if you used it, convert, route, and every fill and merge again).

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
        {"path": "ingest/lakeflow_connect.py", "content": "# authored pipeline source ..."}
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

- `tags`: the authored pipeline **must** set `tags.source` to `"adf"` (routing/agentic is ADF-only);
  fill-agentic fails closed otherwise.
- `files`: files written below the bundle `src/`, so `path` is relative to it (a resource refers to
  one as `../src/<path>`); each is `path` + either UTF-8 `content` or base64 `binary_content`.
- `resources`: bundle resources in the `resource_key` + raw `definition` shape the bundle writer
  expects.
- `environments` (optional): job environments, each `environment_key` + `spec`, for a serverless task
  that names one.
- `task`: the raw Databricks task fragment (`pipeline_task` or `notebook_task`) wiring to an
  authored resource or file. Libraries and compute go here, never on the activity.
- `raw_definition`: the original source definition, retained for provenance.

The full contract, including every field package refuses, is the `AgenticComponentActivity` section of
`flowx-convert`'s `sources/adf.md`. fill-agentic runs each authored pipeline through package's prepare
and bundle-writing path, so a component package would refuse is refused at fill, with nothing written.

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

As in `flowx-enrich`, the state is disclosure, not a gate: there is no `doNotSuggest` rule, so a
verified **Private Preview** connector may still be recommended. Show its label, say that Private
Preview typically needs workspace enrollment, and let the customer decide. And **verify GA-vs-Preview
status before recommending any connector or Lakeflow Connect pattern**: do not hardcode GA/Preview status or dates (release state changes), check
the feature's current release state **and** target-workspace availability against the current public
Databricks docs, and cite the source. (Same structured grounding rule the `flowx-enrich` skill
applies when authoring recommended patterns.)

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
  unit or pointed at another activity ("re-run fill-agentic", or merge that gap again). Every stored
  entry carries a hash over all its fields, so any edit to it shows, and the store must hold exactly
  `components` and `gap_fills`, so an added key is never dropped on the next save and a deleted one
  never silently drops what it held;
- a plan is recorded and the report carries no routing record while the plan routes a unit agentic
  or `agentic_conversion.json` stores a gap fill, as after a re-convert ("re-run route", which applies
  the fill, or drops it when it was merged against an earlier convert). With no plan recorded the
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

Package prunes `.work/` once it has built the bundle, so route and fill-agentic refuse straight
after a package and say that package pruned `.work/`. Everything they rebuild from is kept in
`metadata/`: the decisions, accepted groupings and conversation in `conversion_plan.json`, every
stored fill and its `replaced` history in `agentic_conversion.json`, and modify's answers in
`configuration.json`. To change a decision, an answer or an agent output after a package, run:

1. **convert**, with the same source and options. It writes `.work/` again.
2. **route**, after editing the plan if a decision or an answer is what changes. Route takes the fresh
   report as its new baseline and re-applies the plan and the stored outputs. Fills of convert's own
   gaps come back when convert wrote the same report as before; otherwise route says how many it
   dropped, and you merge them again.
3. **fill-agentic**, only for a unit whose output changes. Revise the fill when a changed answer
   changes the pattern or its shape; nothing flags that for you. New pipelines replace the stored ones
   and add an entry to the unit's `replaced` history.
4. **modify**, if it was used, with the same answers (`metadata/configuration.json` lists them).
5. **package**.

Package overwrites the bundle files it generates, so hand edits to them are lost, and moves any
`resources/*.yml` it does not write again (one added by hand included) into
`metadata/archive/<UTC time>/`. To keep a change to an agentic unit's resource, put it in the authored
pipeline's resource `definition` and run fill-agentic. With `--keep-intermediates` (MCP
`keep_intermediates`) package leaves `.work/` in place, so you can start at route or fill-agentic. A
re-package after the pipeline count or names change moves the earlier bundle folders it recorded in
`metadata/packaged_bundles.json` into `metadata/archive/<UTC time>/` before it writes the new ones, so
a pipeline an agentic unit replaced under a new name no longer sits beside the new one. A re-discover
moves `metadata/` and `.work/`, the plan and the fills with them, into `metadata/archive/<UTC time>/`.
A deploy uploads `metadata/archive/` with the bundle unless it is git-ignored; nothing in it is a job
or pipeline the bundle deploys.

## Reference

- `flowx-enrich` skill — the `insights` layer routing's agentic option and groupings consume.
- `flowx-convert` — the deterministic engine, `merge_agentic` for convert's own gaps, and just-in-time config.
- `flowx-package` — turns the (filled) report into the deployable DAB bundle.
