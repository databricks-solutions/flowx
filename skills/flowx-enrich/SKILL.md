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

# Enrich the Inventory with Agentic Insights

The deterministic discover pass records what each source workflow **is**; it cannot record what to
**do** about it. That judgment — a factory-wide architectural recommendation, each pipeline's intent
and recommended Databricks patterns, and how pipelines couple — is authored by **you, the agent**,
and recorded by the library in its own file, `metadata/agentic_insights.json`, bound to the saved
`metadata/source_graphs.json` it was checked against. The library then rebuilds
`metadata/inventory.json` from `source_graphs.json` plus that file, so it carries the same block under
a single additive `insights` key; you never pass it anything for that step. Airflow discover saves no
`source_graphs.json` yet, so for Airflow the file is bound to `inventory.json` alone (through its
`inventory_sha256`), and the inventory is rebuilt from that.

This is an optional phase 1.a of the flowx migration workflow. `flowx-migrate` runs it after discover
unless the user asks for a deterministic-only pass; the routing step (`flowx-route`) reads the
`insights` block to present the agentic conversion option per connected component. The one-shot MCP `migrate` command
runs discover, convert and package without it, so it records no insights.

## How much to write

Insights are advice for the people planning the migration, not a full report. Spend your effort where a
wrong or missing insight would change the migration:

- **Start with the factory-wide decision** (`system_recommendation`) when one choice, such as a managed
  connector for a family of extractors, reshapes many pipelines. Leave it out when there is none.
- **Write a pipeline insight only when a migrator would act on it**: a target pattern that differs from
  a like-for-like port, a risk, or a conversion note. A pipeline that ports as it is needs no entry.
- **Record the couplings a migrator could miss** and that would break if dropped. Give an `inferred`
  one only with evidence you can cite, and set its `confidence` honestly.
- **Keep `risk_if_ignored` for what is critical**: data loss, wrong results, a broken dependency or a
  missed schedule. Leave it out rather than state a generic risk.
- **Leave a field out rather than guess.** Name only publicly documented Databricks capabilities.
- **Keep each text to a sentence or two.** The 20,000-character cap is a ceiling, not a target.

For a large factory, cover the factory-wide decision and the pipelines it touches first. Enrich records
whatever valid insights you give it, so a partial set is fine.

## When to skip enrich (deterministic-only)

Enrichment is optional: `flowx-migrate` runs it unless the user asks for a deterministic-only pass.
A standalone, deterministic-only discover — no agent, no LLM — is fully valid:
`metadata/inventory.json` from discover is complete and self-standing without an `insights` block,
and `flowx-route` still recommends and records a plan from the deterministic structure alone (the
agentic option simply shows no recommended patterns). Skip enrich when the caller asked for a
headless/deterministic pass, or when no human reader needs a migration narrative.

## How to author (four steps)

1. **Read the deterministic inventory.** Load `<output_dir>/metadata/inventory.json` (on the hosted
   MCP server, through `enrich` with `action="prepare"`, which also returns `source_graphs.json`; see
   below). Note every
   pipeline `name` (these are the valid foreign keys), and each pipeline's `lineage` block — in
   particular `lineage.control_edges`, each `{source_workflow, target_workflow, via_task_key}`. A
   deterministic **control** relationship you annotate must match one of these exactly, and only an
   edge marked `"resolved": true` can be annotated. Such an edge can be annotated even when its
   `target_workflow` is not a listed pipeline (discover ran with `--pipeline`, or the called pipeline
   has no activities); every other relationship endpoint, and every `pipeline_insights[].pipeline`,
   must be a listed pipeline. When the
   inventory records a top-level `source_graphs_sha256` (ADF discover does), copy it into
   `authored_against` by default, so enrich can refuse your insights if discover runs again before
   you submit them; when it records none, leave `authored_against` out.
2. **Read the source artifacts** you need to form judgment: each activity's `raw` payload in
   `metadata/source_graphs.json` (on the hosted server, `bundle.files["source_graphs.json"]` from
   `prepare`), the ADF `metadata/<pipeline>.arm.json` provenance, or the DAG source.
   The inventory carries no `raw` payloads. Read enough to state
   each pipeline's *intent* and the Databricks patterns that fit. Ground every recommended pattern in
   a **real, publicly-documented** Databricks capability; never invent a product name.

   **Check the release state of every capability you recommend, then record it.** Release states
   change, so never take one from memory or from this skill. Before you name a connector or other
   capability in `recommended_patterns` or `system_recommendation`, check on the current public
   Databricks docs that its release state is acceptable and that it is available in the target
   workspace; [Databricks preview releases](https://docs.databricks.com/aws/en/release-notes/release-types)
   explains the states. Check first, then recommend; never recommend now and verify later.

   Record the state in the pattern's `release_state` (`ga`, `public_preview`, `private_preview`,
   `beta`, or `unknown` when you could not verify it) and cite the page in `release_state_source`. The
   validator requires a state on every `simplification_pattern: true` pattern and a source for the
   preview and beta states. A preview or beta capability may still be recommended: record its state and
   let the customer decide. `flowx-route` shows the state as a plain label, and nothing for `ga` and
   `unknown`.

   **Check every pattern you recommend against the current Databricks platform limits.** Before you name
   a pattern in `recommended_patterns` or `system_recommendation`, read the limits it rests on live from
   the current public Databricks docs and check the estate's facts against them.

   - **Where to read them:** for Lakeflow Connect, the connector's limits page and the resource limits
     page; `flowx-route` lists the limits its database-ingestion patterns rest on; for any other
     pattern, that capability's own pages.
   - Don't recommend a pattern a limit rules out.
   - Never use a remembered number.
   - Release state isn't a limit; it follows the rule above (an unlabelled page is
     `release_state: unknown`).

   Record the outcome in the `conversion_notes` of every pipeline the recommendation covers (for
   `system_recommendation`, each pipeline it applies to):

   - each limit you relied on: `limit: <name> = <value> (source: <page>, read <date>)`;
   - a pattern whose limit check needs an estate fact still unknown: `not qualified: needs <fact>`;
   - a pattern whose limits page couldn't be reached: `not qualified: limits not checked`.

3. **Tell the user what you found, then ask the open questions that decide the target pattern.**
   Some facts that decide it aren't in the export: the size and shape of the workload, how its data
   changes, the service bar it must meet, and what policy allows. Asking now, before convert, brings
   them up before anything is converted. See "Summarise and ask" below.
4. **Author the insights JSON, then call `enrich`.** The library validates it against the inventory
   and the saved source graphs and, only when clean, writes `agentic_insights.json` and rebuilds the
   inventory. On any violation both files are left untouched and you get the full list of
   problems to fix in one pass.

See **`insights.md`** in this skill directory for the exact insights shape, every field, and the
validation rules the library enforces.

## Summarise and ask (step 3)

This works the same for every source. On a source where route records deterministic decisions only
(Airflow), the answers still shape the port and the per-gap fills.

1. **Read** the raw source behind each loop, copy and query, and any worklist you can reach. A
   worklist is a runtime list that drives a loop: a control table, a CSV, a Lookup result, an array
   parameter or a mapped-task input. If the user can share the worklist, or you can query it, count it
   rather than asking. "Where the facts sit" below says where to look.
2. **Summarise it back** in about five lines:
   - what the workflows do;
   - their shape in plain words, for example "a list read at run time drives a loop that calls one
     child per item; the child copies each table from the last saved version; hourly";
   - what that tells you, naming the activity or task it comes from;
   - which candidate patterns are still in play (`flowx-route` lists them, and the facts each needs);
   - what the export can't tell.
3. **Pick the questions.** For each fact a candidate pattern needs, decide whether the code shows it
   (cite the code), whether it is data you can read (read it), or whether it is unknown. Ask about an
   unknown only when a plausible answer would rule a candidate in or out, and ask first about the one
   whose answer splits the candidates most. Ask only those, together, in one turn.

The facts, named for what the workload does:

| Fact | What it means | What in the code raises it |
|---|---|---|
| Scale | How many sources and objects a loop covers, now and in a year or two | A loop over a worklist; one worklist inside another |
| Repeat | Whether the same object names recur across sources | A per-item child that applies one object list to every item |
| Fan-in | One target per source object, or one per logical object across all sources | Targets templated by source; a merge downstream |
| Change | The share of sources that change per run; the change feed's kind and retention | A version or watermark read before copying; skip-if-unchanged; change functions in query text |
| History | Current state only, or every change | Rarely visible in code |
| Bar | Schedule, allowed run time, today's run time and cost | Triggers, schedules, concurrency and batch settings |
| Policy | What may be enabled on the source and the workspace; which compute and network are allowed | Connections and runtimes |
| Shaping | Whether transforms inside the extraction must stay at landing | Column lists, casts, joins or filters in extraction queries |

Keep each question within these guardrails:

- **Size and shape:** how much the workload covers and how it repeats or fans in (scale, repeat,
  fan-in).
- **Change and history:** how much changes per run, how the change is captured, and how much history
  must be kept (change, history).
- **Service bar:** when it must run, how long it may take and what it may cost (bar).
- **Constraints:** what may be enabled or used on the source and the workspace, and which transforms
  must stay at landing (policy, shaping).

Don't ask about anything the code shows or data you can read, or anything whose answer wouldn't change
the candidates. Ask about the estate and the business, in the user's words, and name the code that
raised each question. For example: *"`Orchestrator` loops over a list of databases it reads at run time. How many
does that list hold in production, and does every database get the same table list?"* Don't ask in
platform terms, such as *"do you need more than 250 tables per pipeline?"*

Record each answer in the `conversion_notes` of the pipeline that raised it, as the question and its
answer, then the fact on one line: `fact: value (source: code | data | user | assumed)`. A "don't
know" is recorded as `fact: unknown` and isn't asked again. A count the user only estimates is
`assumed`, not `user`. `flowx-route` carries these into the plan's `conversation` and asks only about
facts still open.

### Where the facts sit

The inventory carries no `raw` payloads. In ADF, read each activity's `raw` JSON in
`metadata/source_graphs.json` (and `metadata/<pipeline>.arm.json`):

- a Lookup feeding `ForEach.items`, and an `ExecutePipeline` called once per item (scale, repeat);
- dataset parameters and sink paths templated by item (fan-in);
- query text, such as a version or watermark read before the copy, or change functions (change,
  shaping);
- `ForEach.batchCount` and trigger schedules (bar);
- a Copy or Lookup `typeProperties.source.type` of `SqlServerSource`, `AzureSqlSource` or
  `SqlMISource`, which marks a SQL Server source.

In Airflow, read the DAG source:

- dynamic task mapping (`expand`) and task groups, and the list a mapped task is given (scale,
  repeat);
- operator SQL, such as a watermark read before the load (change, shaping);
- the DAG's `schedule` and `max_active_tasks` (bar).

When a database-source signal is present, also record the source system and
connector in that pipeline's `conversion_notes`, for example `source: SQL Server via AzureSqlSource`
(or `SqlServerSource`, `SqlMISource`).

## Run enrich — MCP tool or venv CLI

Run the **`setup`** skill first if you haven't. Both paths run the same validate-and-record contract.

- **MCP tool (Databricks Genie Code, or a local stdio registration):** call the single **`flowx`**
  tool with `command="enrich"`. On the hosted server you cannot read `output_dir`, so first read the
  inventory and the activity details with `action="prepare"`. It returns `metadata/inventory.json` inline
  as `bundle.files["inventory.json"]` and, when discover wrote it (ADF does, Airflow does not),
  `metadata/source_graphs.json` as `bundle.files["source_graphs.json"]`, the same way `package` returns a
  bundle; pass `output_volume_path` or `output_workspace_path` to have them uploaded there instead. A file
  too large to return inline makes `prepare` fail with `ok: false`, naming that file and those two
  options. Then record the insights (the default `action="apply"`) with either inline insights or a file:

  ```
  flowx(command="enrich", parameters={"output_dir": "<dir>", "action": "prepare"})   # read inventory.json + source_graphs.json
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

Don't run `enrich` alongside `discover` or another `enrich` on the same output folder. If it fails, run
it again.

If `enrich` reports that `metadata/.enrich.lock` exists and no other `enrich` is running, an earlier
run stopped mid-write. Clear the lock, then enrich again:

- **Locally**, delete `metadata/.enrich.lock` and run `enrich` again; it rewrites both files.
- **On the hosted MCP server** you cannot delete files in `output_dir`, so run `discover` again (it
  moves the earlier `metadata/`, the lock with it, into `metadata/archive/<UTC time>/`), then `enrich`
  with `action="prepare"` and apply the insights again.

## Next step

After the inventory is enriched, convert it (`flowx-convert` builds the deterministic baseline), then
continue with **`flowx-route`** to recommend and record a per-connected-component conversion route
(deterministic vs. agentic), then package.

## Reference

- `insights.md` — the insights shape, every field, and the validator's rules.
