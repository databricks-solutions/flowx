---
name: flowx-discover
description: >
  Parse a source orchestrator's pipeline definitions (Azure Data Factory, Apache Airflow) into a
  typed inventory that classifies every task as deterministic, agentic, or unsupported, then author
  agentic insights (intent, ranked target patterns, cross-pipeline relationships) over it. Phase 1
  of the flowx migration workflow; routes to a source-specific guide.
triggers:
  - "discover pipelines"
  - "discover ADF"
  - "discover airflow"
  - "load pipelines"
  - "parse pipelines"
  - "import pipelines"
  - "inventory source"
---

# Discover Source Pipeline Definitions

Parse a source orchestrator's definitions into a typed inventory. This is phase 1 of the flowx
migration workflow; it produces `metadata/inventory.json` (consumed by `flowx-convert`) plus a
per-pipeline complexity report at `metadata/profile_report.csv`.

flowx supports more than one **source**, and discovery is source-specific — ADF ships ARM JSON,
Airflow ships Python DAG modules, and the two share no parser. This skill routes to the right
source guide; the shared mechanics (output layout, inventory shape, how to run a phase) live here.

## Step 1 — Identify the source (required)

Ask the user which orchestrator they are migrating **from**, or infer it from the input:

- **Azure Data Factory / Fabric Data Factory** → source `adf` → read `sources/adf.md`
- **Apache Airflow** → source `airflow` → read `sources/airflow.md`

There is no default source. Every phase invocation passes `--source <name>` explicitly.

## Step 2 — Follow the source guide

Read the matching `sources/<source>.md` in this skill directory and follow it. Each guide covers
the source's input layout, the exact discover command, and how to read its inventory.

## How to run a phase — MCP tool or venv CLI

Both paths are the same across sources; only `--source` and the source path differ. Run the
**`setup`** skill first if you haven't.

- **MCP tool (Databricks Genie Code, or a local stdio registration):** call the single **`flowx`**
  tool with `command="discover"` and `parameters` including `"source": "<source>"`. Run **no**
  `python3`/`$PY` commands on this path.

- **venv CLI (local, no MCP server):** ensure the venv exists (`setup` / `bootstrap.sh`), then:

  ```bash
  export PYTHONPATH="<plugin_dir>/src"
  PY="$(cat <plugin_dir>/.migration-venv)"
  "$PY" -m flowx.adapter discover --source <source> --source-path <path> --output-dir <dir> [--pipeline <name>]
  ```

  `--source-path` is the generic flag (each source also accepts its own alias, e.g.
  `--adf-source-path`); both normalise to the phase's `--source-dir`. `--source` is required.

## Output artifacts (shared across sources)

All under the shared `<output_dir>/metadata/` folder:

| File | Description |
|---|---|
| `metadata/inventory.json` | Classified activity inventory (later enriched with agentic `insights`) for the convert phase |
| `metadata/profile_report.csv` | Per-pipeline complexity report (counts + T-shirt size) |
| `metadata/<pipeline>.arm.json` | (ADF) Verbatim original source for each pipeline (provenance) |

The inventory classifies every task into one of three strategies:

- **Deterministic** — a built-in translator exists; converted without an LLM.
- **Agentic** — requires LLM-assisted translation from the source definition.
- **Unsupported** — no known translation path; needs manual intervention.

After classification, discovery also **authors agentic insights** over the inventory and merges
them under an `insights` key — see *Author and merge agentic insights* below. This runs for every
source; the source-specific inputs come from each `sources/<source>.md`.

## Author and merge agentic insights (all sources)

The deterministic inventory records *what* each pipeline contains; it cannot record *what the
factory is trying to do* or *how the pipelines relate as a system*. Author that judgment now and
merge it into `inventory.json` under an `insights` key. This always runs.

**This step is source-neutral — it runs for every source.** The insight *schema*, the *analysis
method*, and the *pattern framework* below are shared; the source-specific inputs (how to deep-dive
the source, and its construct→Databricks pattern vocabulary) come from the "Insights — deep-dive &
pattern vocabulary" section of your `sources/<source>.md`.

1. **Read** the just-written `inventory.json` (`pipelines`, `lineage`, `summary`)
   and `profile_report.csv`. **Then, before authoring, deep-dive the source.** The
   inventory is a deterministic skeleton (types, strategy, control edges); the *why*
   and *how* — queries, branch conditions, notebook paths, parameters — live only in
   the verbatim source artifacts. **Which artifact to read, and how, is
   source-specific: follow the "Insights — deep-dive & pattern vocabulary" section of
   the `sources/<source>.md` guide you used in Step 2.** Read the source for any
   pipeline you write an insight or relationship about.
2. **Author** an `insights` object:
   - `overview` — the whole factory as one system, plus the single biggest
     migration steer.
   - `system_recommendation` *(optional; preferred on any multi-pipeline factory)* —
     the **one top-level architectural decision** a migrator must make **before** any
     per-pipeline work, because it *cascades* across pipelines. A per-pipeline card
     alone can't show it: e.g. "adopt a managed connector for the whole extraction
     family" turns the child extractors into connector pipelines, deletes the
     watermark store, **and** empties the fan-out orchestrator all at once. Author
     this **first**, then keep each `pipeline_insights[].recommended_patterns`
     consistent with the branch it recommends. Fields:
     - `headline` — one line naming the decision (e.g. "Managed ingestion collapses
       the extraction factory").
     - `recommended_patterns` — **1–4 whole-system branches**, ranked best-first and
       shaped exactly like a pipeline's (each with `pattern`, `fit`,
       `simplification_pattern`). `[0]` is the recommended branch;
       later entries are the ranked fallbacks. Example: `[0]` = "Adopt **Lakeflow
       Connect** for the whole SQL Server extraction family" (`simplification_pattern:
       true`); `[1]` = "For-each orchestrator + 2 collapsed parameterized jobs"
       (`simplification_pattern: false`).
     - `cascade` — what choosing `[0]` **collapses or eliminates across the system**
       (e.g. "5 child extractors → managed connector pipelines"; "version-watermark
       CSV → gone"; "fan-out orchestrator → near-empty"). This is the payoff the
       reader cannot see from any one pipeline. Omit (or `[]`) when the decision does
       not cascade.
     - `decision_driver` *(optional)* — the gating question that picks the branch
       (e.g. "Is the Lakeflow Connect SQL Server connector GA/approved for this
       source?").
     Use it whenever a **system-wide** capability (a managed connector for a whole
     source, one observability tier, one control layer) would reshape many pipelines
     at once; skip it for a single isolated pipeline.
   - `pipeline_insights[]` — a **sparse, selective** list (per entry an `intent`
     and `recommended_patterns`; optionally `pattern_name`, `databricks_pattern`,
     `conversion_notes`, `risk_if_ignored`).
     Omitting a pipeline is the default and needs no justification — a short,
     high-signal list the reader can trust beats a note on every pipeline.
     - **`risk_if_ignored`** (optional) — a one-line consequence a migrator faces
       if they port this pipeline naively (e.g. "Switch-nested calls are invisible
       in `lineage.control_edges`, so this reads as a leaf"). Use it only when the
       insight carries a genuine migration hazard; otherwise omit.
     - **`recommended_patterns` — the grounded, ranked recommendation.** A list of
       **1–4** Databricks target patterns for this pipeline, ordered **best-first**.
       Author it from a **holistic read of the whole pipeline** — its activities,
       dependencies, datasets, linked-service source types, parameters, and intent —
       **not** from a single pattern label. Each entry is an object:
       - `pattern` — the **named, publicly-documented** Databricks capability (e.g.
         `Lakeflow Connect SQL Server connector`, `Auto Loader`,
         `Lakeflow Declarative Pipelines AUTO CDC`). Name **only** capabilities that
         actually exist; **docs.databricks.com is the reference**. Never invent a name.
       - `fit` — one line: why it fits *this* pipeline / what bespoke logic it replaces.
       - `simplification_pattern` — `true` **only** when the pattern uses a *distinctive*
         Databricks capability that collapses or eliminates a whole legacy pattern:
         a managed connector (**Lakeflow Connect**), declarative CDC (**`AUTO CDC`**),
         **Auto Loader**, or **system tables** replacing a home-grown logging tier.
         Set it `false` for a like-for-like port **and** for plain native building
         blocks that merely re-home the same work — a bare parameterized **Lakeflow
         Job**, a for-each/run-job orchestrator, a plain Delta control table,
         `MERGE INTO`. "Runs on Databricks" is **not** a simplification: almost
         everything you migrate is native, so reserve this flag for the capability
         that makes the old pattern *disappear*. Rank the `true` patterns **first**.

       **Rank simplification-first:** prefer managed ingestion over a hand-rolled
       extract, declarative CDC over custom watermark logic, and collapsing clones
       over N ports — but flag `simplification_pattern: true` only on the entries that
       truly use a distinctive capability, not on the plain-orchestration fallback.
       Keep it to 1–4 (don't pad); **omit the field** when you have no grounded
       recommendation. Note GA/Preview status in `fit` when it affects the decision —
       **verify** a connector's status in the docs/release notes (e.g. the Lakeflow
       Connect SQL Server connector) rather than assuming GA.

       **Recognized-pattern vocabulary — a reference menu, NOT an allowlist.** Each
       source guide carries a **source-construct → Databricks** mapping table (with
       **current** product names) in its "Insights — deep-dive & pattern vocabulary"
       section — use the one in the `sources/<source>.md` you followed. The Databricks
       (target) side is source-neutral; use it to stay grounded and consistent, but reach
       past it whenever the holistic view calls for a better or newer fit.

       **Emit current names, not legacy ones:** Lakeflow Jobs (was Databricks
       Workflows), Lakeflow Declarative Pipelines (was Delta Live Tables/DLT), `AUTO CDC`
       (was `APPLY CHANGES INTO`), Declarative Automation Bundles (was Databricks Asset
       Bundles), AI/BI dashboards (was Lakeview), `system.lakeflow` (was
       `system.workflow`).
     - **`databricks_pattern`** (optional) — a one-line **headline** naming the primary
       target architecture. `recommended_patterns[0]` is the structured form of it, so
       omit `databricks_pattern` unless a short prose headline genuinely adds signal.
     - **Include a pipeline only if it meets ANY of these tests:** it anchors a
       reusable *framework* or *pattern* many others depend on (an orchestrator,
       a shared engine/wrapper, a logging/control-table hub); its classification
       or role is *surprising* given its name; or it carries a *risk or caveat* a
       migrator must know before porting it.
     - **Cover distinct roles, not a fixed count.** One representative note per
       notable role/archetype is usually enough — if forty pipelines are near-
       identical wrappers around one engine, note the engine and one representative
       wrapper, not all forty. Scale is set by how many *distinct* roles exist, not
       by pipeline count: on a large factory you will typically flag only a small
       minority. This is a guide, not a quota — include fewer if fewer qualify.
     - Rule of thumb: include a note only if it would **change a reader's decision
       or surprise a domain expert**. When in doubt, omit.
     - **Two whole-factory recommendation patterns** (apply when the evidence is
       there; use `pattern_name` to tag them, record the target as a
       `recommended_patterns` entry with `simplification_pattern: true`, and
       quantify the payoff in `intent` / `conversion_notes`. When either reshapes
       the *whole* system, also surface it as the `system_recommendation`):
       - **Replace, don't transliterate.** When a *whole tier or sub-factory*
         exists only to provide a capability Databricks offers **natively**,
         recommend eliminating it, not re-implementing it as a called job. Common
         generic mappings: a logging/observability tier → system tables
         (`system.lakeflow.*`) + native Lakeflow job notifications + an AI/BI
         dashboard; run-state / control tables → Lakeflow job & task run state and
         `dbutils.jobs.taskValues`; a config-driven Switch/template "engine" with
         no native equivalent → a Python orchestrator driving the Jobs API. **Guard
         against over-firing:** only recommend REPLACE when the tier's *sole*
         purpose is the native capability (e.g. it only logs / only records run
         state). If a pipeline does real domain work alongside the boilerplate,
         migrate it normally — do not tell the reader to delete real logic.
       - **Collapse clone families.** Cluster pipelines by their activity
         *signature* (ordered activity/task types) and shared child-edge set across the
         whole inventory. Where a family of near-identical pipelines exists, emit
         **one** insight (anchored on a representative pipeline that exists in the
         inventory) that names the family and its count, recommends collapsing the
         N clones into a **single parameterized job invoked N times**, and
         quantifies the win (e.g. "14 near-identical ingest pipelines, identical
         activity signature → 1 parameterized job"). List the members in
         `conversion_notes`. This supersedes writing N near-duplicate per-pipeline notes.
   - `pipeline_relationships[]` — characterize **how data and control flow
     between the pipelines**, whatever the mechanism. Each relationship carries
     `from_pipeline` and `to_pipeline` (both must be pipeline names that exist in
     the inventory) plus a `lineage_edge`, `relationship_summary`,
     `databricks_pattern`, and `risk_if_ignored`.

     **A `lineage_edge` is one of two tiers. The tier is decided by ONE thing:
     whether the deterministic phase already recorded this coupling as an edge in
     `lineage` — NOT by the coupling's mechanism** (control call, dataset, table
     written in notebook code, ordering dependency, external trigger, …). Do not
     classify by mechanism.

     - **Annotation** (`edge_type` = `control` or `data`) — the coupling is
       *already* an edge in `lineage`; you are adding interpretation to it.
       `edge_identity` is copied **verbatim** from that edge: the `activity_name`
       for a `control_edges` entry, the `match_key` for a `data_edges` entry. Do
       not add `evidence` / `confidence`.
     - **Inferred** (`edge_type` = `inferred`) — a *real* coupling the
       deterministic phase did **not** record as an edge, by any mechanism. Set
       `edge_identity` to a short descriptor of what couples the two pipelines
       (e.g. the shared table/asset, or the nature of the dependency), and **you
       must** supply `evidence` (the concrete source observation behind it) and
       `confidence` (`high` / `medium` / `low`). Report only couplings you can
       actually evidence; do not invent them.

     **Decide in order:**
     1. Is this coupling already an edge in `lineage` (a `control_edges` /
        `data_edges` entry)? → **annotation** (`control` / `data`).
     2. Otherwise, is it a real coupling not present in `lineage`? → **inferred**.
     3. If in doubt — the coupling is real but you cannot point to the `lineage`
        edge that names it — classify it **inferred** (never annotate an edge that
        is not there).

     **Inferred covers several sub-cases — do not restrict it to any one:**
     - *Data-in-code:* one pipeline's notebook writes a table another's notebook
       reads (no declared dataset, so `data_edges` never saw it).
     - *Ordering dependency:* a producer→consumer hand-off expressed only as
       sibling ordering inside a parent orchestrator, which the deterministic
       phase did not emit as a cross-pipeline edge.
     - *Shared control/config asset, external trigger, message queue,* or any
       other real coupling flowx could not represent.
     - *Near-miss (this is an annotation, not inferred):* pipeline A invokes B and
       that call is already a `control_edges` entry — even though B then does its
       real work in a notebook, the coupling itself was recorded, so annotate it.
   - **Authoring rules:** reference only pipeline names that exist in the
     inventory; an annotation edge (`control`/`data`) must echo a real lineage
     edge (annotate, don't rediscover); an `inferred` edge must carry non-empty
     `evidence` and a `confidence` level and must not be dressed up as proven
     lineage.
3. **Enrich** — merge the object in:

   - **MCP tool path** (inline dict; the only path in Genie Code):

     ```
     flowx(command="enrich", parameters={
       "output_dir": "<output dir>",
       "insights": { ...authored object... }})
     ```

   - **venv CLI fallback** (write the object to a JSON file first):

     ```bash
     "$PY" -m flowx.adapter enrich --output-dir <dir> --insights-path <file>
     ```

4. **On `ok:false`** the tool did **not** write the file: read `violations`, fix
   the offending pipeline name / lineage edge / field, and call `enrich` again.
   On `ok:true` the `insights` key is now merged into `inventory.json`. Present the
   authored judgment back to the user (the factory `overview`, and each
   `pipeline_insights` entry's `pattern_name` / `intent` and top ranked
   `recommended_patterns`) as part of the source guide's summary step.

## Future considerations

Insights are currently authored in a single pass over the whole factory. For very
large factories, revisit partitioning the authoring across subagents keyed on
lineage clusters (the connected components of the combined control/data-edge
graph), so each subagent reasons about one coherent subsystem. Out of scope for
now — always enrich in one pass.

## Reference

- `sources/adf.md` — Azure Data Factory discovery (ARM JSON, UC-volume download, complexity report) + ADF insight deep-dive & pattern vocabulary
- `sources/airflow.md` — Apache Airflow discovery (DAG `.py` parsing, operator classification) + Airflow insight deep-dive & pattern vocabulary
