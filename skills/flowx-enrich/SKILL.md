---
name: flowx-enrich
description: >
  Enrich the discover inventory with an agent-authored layer of judgment — a factory-wide
  architecture recommendation, per-pipeline intent + recommended Databricks patterns, and
  cross-pipeline relationships. The library validates it, records it in agentic_insights.json and
  rebuilds inventory.json from discovery plus that file. Optional phase 1.a, after discover and
  before convert; use it unless the user asked for a deterministic-only pass. The routing step reads
  it.
  Use when the user asks to enrich or annotate the inventory, author agentic insights, or recommend
  Databricks patterns.
---

# Enrich the inventory with agentic insights

Optional phase 1.a of the flowx workflow, after discover and before convert. Discover records what each
source workflow is; you, the agent, author what to do about it: a factory-wide recommendation, each
pipeline's intent and recommended Databricks patterns, and how pipelines couple. Enrich validates that
and records it in `metadata/agentic_insights.json`, bound to the saved `metadata/source_graphs.json` it
was checked against, then rebuilds `metadata/inventory.json` from `source_graphs.json` plus that file, so
the inventory carries the same block under one additive `insights` key (you pass nothing for that
rebuild). Airflow discover saves no `source_graphs.json`, so for Airflow the file is bound to
`inventory.json` alone (through its `inventory_sha256`) and the inventory is rebuilt from that.
`flowx-route` reads the `insights` block to present the agentic option per connected component.

## Before you start

- Discover must have run for this `<output_dir>`.
- `flowx-migrate` runs enrich unless the user asks for a deterministic-only pass. Skip it when the caller
  asked for a headless or deterministic pass, or when no human reader needs a migration narrative. A
  deterministic-only discover, with no agent and no LLM, is fully valid: its `inventory.json` is complete
  without an `insights` block, and `flowx-route` still recommends and records a plan from the
  deterministic structure (its agentic option then shows no recommended patterns).
- The one-shot MCP `migrate` command runs discover, convert and package without enrich, so it records
  no insights.

## How to run

- **MCP:** you can't read `output_dir` on the hosted server, so first read the inventory and the activity
  details with `action="prepare"`. It returns `metadata/inventory.json` as `bundle.files["inventory.json"]`
  and, when discover wrote it (ADF does, Airflow does not), `metadata/source_graphs.json` as
  `bundle.files["source_graphs.json"]`; pass `output_volume_path` or `output_workspace_path` to have them
  uploaded there instead. A file too large to return inline makes `prepare` fail with `ok: false`, naming
  that file and those two options. Then record the insights (the default `action="apply"`) with exactly
  one of `insights` (inline) or `insights_path`. `ok` reflects validation; `result.violations` lists any
  problems.

  ```
  flowx(command="enrich", parameters={"output_dir": "<dir>", "action": "prepare"})
  flowx(command="enrich", parameters={"output_dir": "<dir>", "insights": { ... }})
  flowx(command="enrich", parameters={"output_dir": "<dir>", "insights_path": "<file>"})
  ```

- **CLI:** `--output-dir` and `--insights-path` are required; `--out <file>` writes the result JSON to a
  file instead of stdout. Exit code 0 means the insights were recorded; exit code 1 prints the violations
  JSON and leaves the inventory untouched.

  ```bash
  "$PY" -m flowx.adapter enrich --output-dir <dir> --insights-path authored_insights.json
  ```

Both paths run the same validate-and-record step. The rest of running a phase is in
[`../shared/running-flowx.md`](../shared/running-flowx.md).

## Steps

### Step 1: Read the inventory

Load `<output_dir>/metadata/inventory.json` (on the hosted server, from `prepare`). Note every pipeline
`name`: these are the valid foreign keys. Note each pipeline's `lineage` block, in particular
`lineage.control_edges`, each `{source_workflow, target_workflow, via_task_key}`.

- A control relationship you annotate must match one of these edges exactly, and only an edge marked
  `"resolved": true` can be annotated. Such an edge can be annotated even when its `target_workflow` is
  not a listed pipeline (discover ran with `--pipeline`, or the called pipeline has no activities).
- Every other relationship endpoint, and every `pipeline_insights[].pipeline`, must be a listed pipeline.
- When the inventory records a top-level `source_graphs_sha256` (ADF discover does), copy it into
  `authored_against` by default, so enrich can refuse your insights if discover runs again before you
  submit them. When it records none, leave `authored_against` out.

### Step 2: Read the source and check each pattern you would recommend

The inventory carries no `raw` payloads. Read each activity's `raw` payload in
`metadata/source_graphs.json` (on the hosted server, `bundle.files["source_graphs.json"]` from `prepare`),
the ADF `metadata/<pipeline>.arm.json`, or the DAG source: enough to state each pipeline's intent and the
Databricks patterns that fit. Ground every recommended pattern in a real, publicly documented Databricks
capability; never invent a product name.

**Check the release state of every capability you recommend, then record it.** Release states change,
so never take one from memory or from this skill. Before you name a connector or other capability in
`recommended_patterns` or `system_recommendation`, check on the current public Databricks docs that its
release state is acceptable and that it is available in the target workspace;
[Databricks preview releases](https://docs.databricks.com/aws/en/release-notes/release-types) explains
the states. Read the edition for the target workspace's cloud (the page's cloud selector, or
learn.microsoft.com for Azure Databricks). Check first, then recommend. Record the state in the pattern's `release_state` (`ga`,
`public_preview`, `private_preview`, `beta`, or `unknown` when you could not verify it) and cite the page
in `release_state_source`. The validator requires a state on every `simplification_pattern: true` pattern
and a source for the preview and beta states. A preview or beta capability may still be recommended:
record its state and let the customer decide. `flowx-route` shows the state as a plain label, nothing
for `ga`, and "Release state not verified" for `unknown`.

**Check every pattern you recommend against the current Databricks platform limits.** Before you name a
pattern in `recommended_patterns` or `system_recommendation`, read the limits it rests on live from the
current public Databricks docs and check the estate's facts against them.

- **Where to read them:** the capability's own requirements and limitations pages, the workspace and
  resource limits pages, and the pages of what it runs on, found in the current official Databricks
  documentation. `flowx-route`'s
  [`references/target-patterns.md`](../flowx-route/references/target-patterns.md) uses the same method.
- Don't recommend a pattern a limit rules out.
- Never use a remembered number.
- Release state isn't a limit; it follows the rule above (an unlabelled page is
  `release_state: unknown`).

Record the outcome in the `conversion_notes` of every pipeline the recommendation covers (for
`system_recommendation`, each pipeline it applies to):

- each limit you relied on: `limit: <name> = <value> (source: <page>, read <date>)`;
- a pattern whose limit check needs an estate fact still unknown: `not qualified: needs <fact>`;
- a pattern whose limits page couldn't be reached: `not qualified: limits not checked`.

### Step 3: Summarise what you found and ask the open questions

Some facts that decide the target pattern aren't in the export: the size and shape of the workload, how
its data changes, the service bar it must meet, and what policy allows. Asking now, before convert,
brings them up before anything is converted. This works the same for every source; on a source where
route records deterministic decisions only (Airflow), the answers still shape the port and the per-gap
fills.

1. Read the raw source behind each loop, copy and query, and any worklist you can reach. A worklist is a
   runtime list that drives a loop: a control table, a CSV, a Lookup result, an array parameter or a
   mapped-task input. If the user can share the worklist, or you can query it, count it rather than
   asking. "Where the facts sit" below says where to look.
2. Summarise it back briefly (a few lines; more for a large estate): what the workflows do; their shape
   in plain words (for example "a list read at run time drives a loop that runs one copy per item;
   daily"); what that tells you, naming the activity or task it comes from; which candidate patterns are
   still in play (every current option the official Databricks documentation offers for that source or
   component, plus flowx's deterministic port and convert's options, as `flowx-route`'s target-patterns
   reference describes) and the facts each needs; and what the export can't tell.
3. Pick the questions. For each fact a candidate pattern needs, decide whether the code shows it (cite
   the code), whether it is data you can read (read it), or whether it is unknown. Ask about an unknown
   only when a plausible answer would rule a candidate in or out, and ask first about the one whose
   answer splits the candidates most. Ask only those, together, in one turn.

| Fact | What it means | What in the code raises it |
|---|---|---|
| Scale | How many sources and objects a component covers, how much data, now and in a year or two; whether object names recur across sources | A loop over a worklist; one worklist inside another; one object list applied to every item |
| Data shape | Keys, a column that only ever grows, hard deletes, schema changes | Key columns, watermark or range filters, schema settings |
| Fan-in | One target per source object, or one per logical object across all sources | Targets templated by source; a merge downstream |
| Change | How much changes per run and how the change is captured and kept | A version or watermark read before copying; skip-if-unchanged; change functions in query text |
| History | Current state only, or every change | Rarely visible in code |
| Bar | Schedule or latency, allowed run time, today's run time and cost | Triggers, schedules, concurrency and batch settings |
| Policy | What may be enabled on the source and the workspace; which compute and network are allowed | Connections and runtimes |
| Shaping | Whether transforms inside the extraction must stay at landing | Column lists, casts, joins or filters in extraction queries |
| Consumers | Who reads the targets and what they rely on | Downstream activities, shared tables |

Ask within these categories: size and shape (scale, data shape, fan-in); change and history; service
bar; constraints (policy, shaping); consumers. They are a floor, not a cap: when an option's current
documentation names a requirement or limit that turns on another fact, ask about that fact too.

Don't ask about anything the code shows or data you can read, or anything whose answer wouldn't change the
candidates. Ask about the estate and the business, in the user's words, and name the code that raised each
question. The form is *"`<activity>` <does what the code shows>. <What the export can't tell>?"*, for
example *"`<activity>` reads a list at run time and runs one copy per item. How many items does that
list hold in production?"* Don't ask in platform terms, such as *"do you need more objects than the
product allows?"*

Record each answer in the `conversion_notes` of the pipeline that raised it, as the question and its
answer, then the fact on one line: `fact: value (source: code | data | user | assumed)`. A "don't know" is
recorded as `fact: unknown` and isn't asked again. A count the user only estimates is `assumed`, not
`user`. `flowx-route` carries these into the plan's `conversation` and asks only about facts still open.

#### Where the facts sit

In ADF, read each activity's `raw` JSON in `metadata/source_graphs.json` (and
`metadata/<pipeline>.arm.json`):

- a Lookup feeding `ForEach.items`, and an `ExecutePipeline` called once per item (scale);
- dataset parameters and sink paths templated by item (fan-in);
- query text, such as a version or watermark read before the copy, or change functions (change, data shape, shaping);
- `ForEach.batchCount` and trigger schedules (bar);
- a Copy or Lookup `typeProperties.source.type` that names a database. flowx's own list is
  `JDBC_SOURCE_TYPES` in `src/flowx/models/source_types.py`; a type outside it may still be a database,
  so read the dataset's linked service too.

In Airflow, read the DAG source:

- dynamic task mapping (`expand`) and task groups, and the list a mapped task is given (scale);
- operator SQL, such as a watermark read before the load (change, shaping);
- the DAG's `schedule` and `max_active_tasks` (bar).

When a source signal is present, also record the source system and the source type in that pipeline's
`conversion_notes`, as `source: <system> via <source type>`.

### Step 4: Author the insights and run enrich

Insights are advice for the people planning the migration, not a full report. Spend your effort where a
wrong or missing insight would change the migration:

- Start with the factory-wide decision (`system_recommendation`) when one choice reshapes many
  pipelines. Leave it out when there is none.
- Write a pipeline insight only when a migrator would act on it: a target pattern that differs from a
  like-for-like port, a risk, or a conversion note. A pipeline that ports as it is needs no entry.
- Record the couplings a migrator could miss and that would break if dropped. Give an `inferred` one only
  with evidence you can cite, and set its `confidence` honestly.
- Keep `risk_if_ignored` for what is critical: data loss, wrong results, a broken dependency or a missed
  schedule. Leave it out rather than state a generic risk.
- Leave a field out rather than guess. Name only publicly documented Databricks capabilities.
- Keep each text to a sentence or two. The 20,000-character cap is a ceiling, not a target. For a large
  factory, cover the factory-wide decision and the pipelines it touches first; enrich records whatever
  valid insights you give it, so a partial set is fine.

The exact shape, every field and the validator's rules are in [`references/insights.md`](references/insights.md). Then run
enrich (see How to run). Only when the insights are clean does it write `agentic_insights.json` and
rebuild the inventory; on any violation both files are left untouched and you get the full list of
problems to fix in one pass.

## Output

`metadata/agentic_insights.json` (enrich stamps `schema_version`, `inventory_sha256`,
`source_graphs_sha256` and `agentic_insights_sha256`) and a rebuilt `metadata/inventory.json`. The rest of
the folder is in [`../shared/output-folder.md`](../shared/output-folder.md). Next comes convert, then
`flowx-route`.

### Idempotency and safety

Enrich rebuilds `inventory.json` from the saved `source_graphs.json` (or, for an inventory that records no
graphs hash, from its existing deterministic part) plus the insights, rather than patching it in place.
The `insights` block is replaced whole, never stacked, and every deterministic inventory key stays
byte-identical. The same insights rewrite the same bytes; different insights replace the block. A
validation failure writes nothing.

Don't run enrich alongside discover or another enrich on the same output folder. If it fails, run it
again. If it reports that `metadata/.enrich.lock` exists and no other enrich is running, an earlier run
stopped mid-write. Clear the lock, then enrich again:

- **Locally**, delete `metadata/.enrich.lock` and run enrich again; it rewrites both files.
- **On the hosted MCP server** you can't delete files in `output_dir`, so run discover again (it moves the
  earlier `metadata/`, the lock with it, into `metadata/archive/<UTC time>/`), then enrich with
  `action="prepare"` and apply the insights again.

## Reference

- [`references/insights.md`](references/insights.md): the insights shape, every field, and the validator's rules.
- [`../shared/running-flowx.md`](../shared/running-flowx.md): the MCP and CLI paths every phase shares.
- [`../shared/output-folder.md`](../shared/output-folder.md): what each phase writes in `<output_dir>`.
