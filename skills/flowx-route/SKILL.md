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

# Route the conversion and fill the agentic units

Phase 2.a of the flowx workflow, ADF only. Route decides, per connected component, whether each part of
the factory converts deterministically (the typed engine's 1:1 translation) or agentically (an
LLM-authored re-architecture, for example several near-identical extractors consolidated onto one managed
ingestion or metadata-driven pipeline).
It reads the inventory, the insights enrich wrote and convert's report; it writes its recommendation into
`metadata/conversion_plan.json`, the one readable record of the decisions, and draws it as
`metadata/routing_review.html`. The user decides, you edit the plan, route applies it to the translation
report, and you author each agentic unit's pipelines with `fill-agentic`.

```
discover → enrich (optional) → convert (deterministic baseline) → route (recommend → decide → apply) → fill-agentic → package
```

**There is no LLM inside flowx.** The library computes the recommendation deterministically and only
validates and records the decision and the authored fill: the same author, validate, merge contract
`enrich` uses. With no recorded plan, `convert` and `package` behave exactly as they would without route.

**Who decides what.** The library recommends a route per component and suggests groupings. The customer
decides deterministic or agentic for each component and which groupings to accept. You present the
review page, ask the customer what they want the conversion to achieve, record only the customer's
decisions in the plan, and author the agentic fill. Never pick the route on the customer's behalf. Route
leaves every decision pending until it is made, and applies nothing while one is pending.

## Before you start

- Convert has run: it builds the deterministic baseline report route edits. Once every decision is made,
  route refuses without it ("run the convert phase first"). Straight after a package, see Step 5.
- The source is ADF. Enrich runs for every source, but for an Airflow inventory route records only
  `deterministic` decisions, suggests no groupings, and refuses an `agentic` decision (the plan is not
  recorded and the report is left untouched); `convert --merge-agentic --source airflow` is disabled
  too. Airflow DAGs are often one connected component, so for Airflow agentic gaps use the
  `flowx-resolve-airflow-gaps` skill (the strict per-gap resolver), not this flow.
- If you run convert again after routing, run route straight after it: a fresh report carries no routing
  record, so route takes it as the new baseline and re-applies the plan and the agent's stored outputs.
  Fills of convert's own gaps merged against a different report are kept but not applied, so merge them
  again for the new report; route says how many it skipped.
- Route and fill-agentic refuse, writing nothing, an `inventory.json` not shaped the way discover writes
  it (not a JSON object, a pipeline with no name or no `activities`, a motif with no string `motif_id` or
  no `member_task_keys`, or its `pipelines`, a pipeline's `activities` or `motifs`, or a motif's
  `member_task_keys` not a list of the right kind), because reading one as empty would make a pipeline
  look fully deterministic. Run discover again.

## How to run

- MCP: `flowx(command="route", parameters={"output_dir": "<dir>"})`, adding `plan` inline or
  `plan_path` to apply a plan; `flowx(command="fill_agentic", parameters={"output_dir": ..., "members": [...], "pipelines": [...]})`
  (or `pipelines_path`).
- CLI: `"$PY" -m flowx.adapter route --output-dir <dir> [--plan-path <file>] [--out <file>]` and
  `"$PY" -m flowx.adapter fill-agentic --output-dir <dir> --member <A> --member <B> --pipelines-path <file> [--out <file>]`.
  `--out` writes the result JSON to a file.

Common MCP and CLI rules: [`../shared/running-flowx.md`](../shared/running-flowx.md).

## Steps

### Step 1: Route writes the recommendation into the plan

Run `route` with no plan. With none recorded yet, it writes `metadata/conversion_plan.json` and
`metadata/routing_review.html` and touches nothing else. Run without `--plan-path` again after editing the
recorded plan: while a decision is pending it only records the plan, and once every decision is made it
applies it, rebuilding `.work/translation_report.json` and `gaps.json`. Over MCP the plan and the page come back under `bundle` (or are uploaded to
`output_volume_path` or `output_workspace_path`), because a hosted agent cannot read the server's
`output_dir`. When either is missing or unreadable, or too large to return inline, `ok` is false, nothing
is uploaded, and the error says which; call route again (with an upload path when a file was too large).

Pipelines are grouped into weak, undirected connected components over the inventory's control lineage
(`lineage.control_edges`, which ADF emits from `ExecutePipeline` in discovery), so pipelines that
reference each other are decided together and a caller and its callee are never split across
incompatible routes. Only resolved control edges join a component; an unresolved call is reported as a
finding.

The plan lists every component with its `members`, the library's `recommended` route, both conversion
`options` and `decision: null` (pending). The deterministic option carries the engine-capability
assessment and any uncovered activities; the agentic option carries the `recommended_patterns` from
enrich, with any `simplification_pattern` flagged and release states disclosed. The plan also lists the
`suggested_groupings`, an empty `conversation`, and the `findings` (unresolved control edges, kept, never
severed).

The library recommends `agentic` for a component only when it has an activity the engine cannot
translate 1:1 and enrich recommended at least one pattern for one of its pipelines. Otherwise it
recommends `deterministic`, and each untranslatable activity stays a per-activity gap (for ADF, the one
`convert --merge-agentic` fills). So without enrich, or when enrich found no pattern, every component is
recommended `deterministic`. The agentic route is a redesign onto a modernisation or simplification
pattern, separate from that per-activity gap fill, and it is still the customer's decision.

A decision already in the file for a component with the same members is kept, so running route again, or
after a re-enrich, keeps your decisions, except the components of an accepted grouping route no longer
suggests (see Suggested groupings below). A re-discover does not keep them: discover moves the earlier `metadata/`, `.work/`
and bundle into `metadata/archive/<UTC time>/` with an `archive.json`, so `conversion_plan.json` and the
agent's outputs in `agentic_conversion.json` are kept for audit but no longer used; route and fill again.

#### Present it with the review page

`metadata/routing_review.html` is the standard way to show the customer what their source does and how it
will be converted. Every run has the same eight sections in the same order: at a glance, what the source
does (the insights), the recommended conversion, suggested groupings, what happens next, what you told
us (the routing conversation), notes (the findings), and how to change a decision. Open it with the
customer, or walk through the same sections in that order.

#### Qualify the target pattern before you suggest one

Ask what they want the conversion to achieve. For a component that doesn't ingest from a database (files,
APIs, transformations), suggest only patterns enrich recommended, checked against their limits the same
way (read live). A component that ingests from a database can land in one of four patterns: before you
suggest one, read
[`references/ingestion-patterns.md`](references/ingestion-patterns.md) for the patterns, the checks C1 to
C13 and the limits each rests on (read live), and how to show a pattern you rule out. Record each
question and answer in the plan's `conversation` (`[{"question": "...", "answer": "..."}]`); package
copies it into `route_audit.json`.

#### Suggested groupings

A grouping is two or more whole components the insights suggest converting
together, agentically, as one unit: one agent output replaces all of their pipelines (for example several
independent extractors become one ingestion pipeline). It never splits a component. Route
suggests one from two kinds of link in the insights:

- an `inferred` pipeline relationship (with its evidence and confidence) between pipelines of different
  components;
- one simplification pattern (same pattern name, `simplification_pattern: true`) recommended for
  pipelines in more than one component: the independent extractors one managed pattern could replace.

Components joined by these links, directly or through each other, form one suggestion
(`grouping-<hash of its members>`), so suggestions never overlap. The same members keep the same id, so an
accepted grouping stays accepted, and keeps its agent output, when enrich runs again and other
suggestions change. Each suggestion lists its `components`, `members` and `basis` (the relationships or
the shared pattern behind it), and `accepted: false`. Only a suggested grouping can be accepted; to group
other components, record an inferred relationship through enrich and route again.

When enrich changes an accepted grouping's own members, route no longer suggests it. It lists it under
`dropped_groupings` in its result (its `components`, `members`, and `replaced_by`, the current suggestions
that share its components), says so in its message and on the review page, and sets its components back
to pending, so nothing is applied until you decide them again.

### Step 2: Record the decisions, then route again to apply

Edit only each component's `decision` and optional `rationale`, a grouping's `accepted`, and
`conversation`; then run route again. Once no decision is pending it validates the plan, records it, and
applies it: each unit stays deterministic, takes its stored agent output, or becomes placeholders until it
is filled. The editable fields, passing a plan instead (the hosted path), every validator rule, and what
applying writes, including the routing record, are in
[`references/plan-rules.md`](references/plan-rules.md).

### Step 3: Fill each routed-agentic unit

A routed-agentic unit is filled only by `fill-agentic`, which replaces the whole unit with pipelines you
author; the library validates and records them. Author the pattern the customer chose (Step 1), from the
facts recorded in the plan's `conversation`, and don't pick one they didn't choose. For the database
ingestion patterns, an M1 fill authors one ingestion pipeline per shard, each within L tables and with no table name twice; an M2 fill authors the
job, a seed for its control table, and its notebooks. To keep a pipeline 1:1, fill its component with one
authored pipeline of the same name.

When the target can't be expressed by the typed engine (for example a managed Lakeflow Connect ingestion
pipeline), the authored pipeline carries an `AgenticComponentActivity` task: read
[`references/agentic-component.md`](references/agentic-component.md) first for its fields, the pipeline
`channel` default, and how to show a release state. Before you run fill-agentic, read
[`references/fill-agentic-rules.md`](references/fill-agentic-rules.md): which unit it accepts, what
happens when you fill again, the naming rules, what an authored pipeline must hold, and the lock that
makes fills run one at a time.

`convert --merge-agentic` is for convert's own agentic gaps only: it refuses, writing nothing, any result
that names a routed-agentic pipeline, or whose activity would first land in one, with "this pipeline is
routed agentic; fill its component with fill-agentic (run fill-agentic again with new pipelines to change
the approach)". After routing, a merge into the live report is also stored as a gap fill in
`metadata/agentic_conversion.json`, so every rebuild re-applies it on top of the unchanged baseline.

### Step 4: Continue to package

Once every agentic unit is filled, continue with the `flowx-convert` skill's just-in-time configuration
(`inspect` and `modify`), then the `flowx-package` skill. The recorded plan is the library's typed
`ConversionPlan` (schema 3), bound to the inventory fingerprint, the saved `source_graphs.json` and the
saved `agentic_insights.json` it was decided on. Route decides whole components, so a plan holds no
per-node assignments.

Package refuses, writing nothing, when:

- `agentic_insights.json` and `inventory.json` disagree (or the file fails its own hash, or its
  `inventory_sha256` no longer matches the inventory), plan or no plan. Enrich may have stopped between
  its two writes, or a file was edited: run enrich again with the same insights (locally, delete
  `metadata/.enrich.lock` first if it is still there; on the hosted server, run discover again, then
  enrich prepare and apply; running discover again moves `conversion_plan.json` and
  `agentic_conversion.json` into `metadata/archive/`, so route and fill again afterwards);
- the report carries a routing record but `conversion_plan.json` is missing;
- the plan is stale against the inventory, is not a complete decision, or has a decision pending; or it
  holds a key route does not write, or a recommendation, option, grouping or finding that no longer
  matches what route records from the inventory (edit only the decisions, `accepted` and the
  conversation, then route again);
- a routed-agentic unit has no agent output: an agentic decision is packaged agentically, never as the
  deterministic stand-in, so fill it, or decide it deterministic and route again;
- the saved baseline no longer has the hashes the record names ("re-run convert, then route"; route,
  fill-agentic and merge refuse it too), or a stored output or gap fill was edited, moved to another unit
  or pointed at another activity ("re-run fill-agentic", or merge that gap again). Every stored entry
  carries a hash over all its fields, so any edit to it shows, and the store must hold exactly
  `components` and `gap_fills`, so an added key is never dropped on the next save and a deleted one never
  silently drops what it held. An edited entry for a unit the plan no longer has, or no longer routes
  agentic, is never applied, so package only warns about it;
- a plan is recorded and the report carries no routing record while the plan routes a unit agentic or
  `agentic_conversion.json` stores a gap fill that applies to it, as after a re-convert ("re-run route",
  which applies the fill). With no plan recorded the folder is not routed, so package ignores the stored
  outputs and fills;
- the live `.work/translation_report.json` does not match a fresh rebuild ("re-run route"), or any other
  packaged report (modify's configured `.work/translation_report.stamped.json`, or a `modify --out` copy)
  does not carry the live report's routing record ("the configured report is out of date; re-run
  modify"). Route, fill-agentic and merge never rewrite the configured report, so run `modify` again
  after any of them.

### Step 5 (optional): After package, change the conversion

Package prunes `.work/`, so route and fill-agentic refuse straight after a package and say so. To change a
decision, an answer or an agent output after a package, follow
[`references/after-package.md`](references/after-package.md).

## Output

| File | Written by |
|---|---|
| `metadata/conversion_plan.json`: components, decisions, suggested groupings, conversation | route; you edit it |
| `metadata/routing_review.html`: the standard review page (generated, never edited) | route, fill-agentic |
| `.work/translation_report.json`, `.work/gaps.json`: the deterministic report with the agentic decisions' edits | convert, then route |
| `metadata/agentic_conversion.json`: the agent's output per unit, plus fills of convert's own gaps | fill-agentic, merge_agentic |
| `.work/route_baseline/`: convert's report as route first found it (internal, never rewritten) | route |
| `metadata/route_audit.json` | package |

Route reads `metadata/source_graphs.json`, `metadata/agentic_insights.json` (optional) and
`metadata/inventory.json` from discover and enrich. `metadata/route_audit.json` records per unit the
`decision`, `outcome`, `fingerprint`, `output_sha256` and `replaced` history (a grouping also lists
`grouped_components`), the gap fills applied, the routing `conversation`, and the hashes of the plan (the
same canonical hash the routing record holds), the baseline report and gaps, the inventory, the saved
source graphs and agentic insights the plan was bound to, and the packaged report. It survives the prune
of `.work/` and documents what was packaged and how. The folder layout is in
[`../shared/output-folder.md`](../shared/output-folder.md).

## Reference

- [`references/ingestion-patterns.md`](references/ingestion-patterns.md): the four ingestion patterns and the checks C1 to C13.
- [`references/plan-rules.md`](references/plan-rules.md): the editable fields, passing a plan, the validator's rules, and what applying writes.
- [`references/fill-agentic-rules.md`](references/fill-agentic-rules.md): everything fill-agentic checks, and the one-at-a-time lock.
- [`references/agentic-component.md`](references/agentic-component.md): authoring an `AgenticComponentActivity` and its release state.
- [`references/after-package.md`](references/after-package.md): changing the conversion after package has run.
- [`../shared/running-flowx.md`](../shared/running-flowx.md): the common MCP and CLI rules.
- [`../shared/output-folder.md`](../shared/output-folder.md): what each phase writes in the output folder.
- The `flowx-enrich` skill writes the insights the agentic option and the groupings use; the `flowx-convert` skill owns the deterministic engine, `merge_agentic` and the just-in-time configuration; the `flowx-package` skill builds the bundle.
