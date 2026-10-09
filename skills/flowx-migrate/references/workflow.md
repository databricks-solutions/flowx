# flowx Migration Workflow

End-to-end architecture for migrating Azure Data Factory (ADF) pipelines to Databricks Lakeflow Jobs via Declarative Automation Bundles (DABs).

## Overview

flowx follows a three-phase pipeline architecture. Each phase is independently runnable and produces artifacts consumed by the next phase. The design principle is **deterministic-first, agentic fallback**: well-known ADF patterns are translated by fast, reliable Python code, while complex or ambiguous patterns are handled by LLM-assisted skills.

```
ADF JSON Exports
      |
      v
  Phase 1: PROFILE
  (parse + classify)
      |
      v
  inventory.json
      |
      v
  Phase 2: TRANSLATE
  (deterministic + agentic)
      |
      v
  translation_report.json + IR + notebooks
      |
      v
  Phase 3: PREPARE
  (bundle generation)
      |
      v
  Databricks DABs Project
  (databricks.yml + resources/ + src/ + setup/)
      |
      v
  databricks bundle deploy
```

## Phase 1: Discover

**Skill:** `flowx:flowx-discover`

**Input:** Directory of ADF JSON export files (from ARM template export, Azure DevOps, or manual export)

**Process:**
1. Discover and parse all JSON files (pipelines, datasets, linked services, triggers)
2. Build a typed AST for each pipeline, resolving activity references
3. Classify every activity into a translation strategy:
   - **Deterministic** — a built-in translator handles this type
   - **Agentic** — requires LLM-assisted translation
   - **Unsupported** — no known translation path
4. Generate the inventory with summary statistics

**Output:**
- `inventory.json` — classified activity inventory
- `ast/*.json` — typed AST per pipeline
- `parse_errors.json` — any parsing failures

**Key decisions:**
- Classification is based on the activity `type` field, not the activity content. This makes classification fast and deterministic.
- Datasets and linked services are parsed for context but not independently translated — they inform the activity translators.
- Triggers are included in the inventory and translated in phase 2.

## Between Discover and Convert: Enrich (default)

**Skill:** `flowx:flowx-enrich`

After discover writes the deterministic inventory, the standard flow enriches it before convert.
Enrich is additive and contains **no LLM** — the agent authors, the library validates and merges:

- **Enrich (default):** the agent authors an `insights` layer (factory-wide recommendation,
  per-pipeline intent + recommended Databricks patterns, cross-pipeline relationships) and `enrich`
  records it in `metadata/agentic_insights.json` (bound to the saved `source_graphs.json`) and
  rebuilds `metadata/inventory.json` with it under a single additive `insights` key, leaving every
  deterministic key byte-identical. Skippable for a deterministic-only, headless pass.

## Phase 2: Convert

**Skill:** `flowx:flowx-convert`

**Input:** `inventory.json` from phase 1 + original ADF JSON files

**Process:**
1. Run deterministic translators for all activities classified as `deterministic`
2. For each `agentic` activity, perform LLM-assisted translation from the activity's ARM JSON:
   - ExecuteDataFlow activities → a purpose-built PySpark notebook
   - control flow and external-call activities → a notebook implementing the activity's semantics
   - complex ADF expressions → Python/SQL equivalents
   - trigger schedules → Databricks schedule/trigger configuration
3. Merge deterministic and agentic results into a unified translation report
4. Generate Databricks IR (intermediate representation) for each activity

**Output:**
- `translation_report.json` — unified report with IR for all activities
- `ir/*.json` — Databricks IR per activity
- `notebooks/*.py` — generated helper notebooks
- `agentic_results/*.json` — raw agentic skill outputs

**Key decisions:**
- Deterministic translators run first because they are fast and reliable. Agentic skills are only invoked for gaps.
- The IR is an intermediate format that decouples translation from DABs generation. This allows the package phase to target different output formats in the future.
- Each source's deterministic translators are standalone Python modules under `src/flowx/sources/<source>/` (e.g. ADF's live in `src/flowx/sources/adf/translators/`). Adding support for a new activity type means adding a new module there.
- Agentic results are saved separately before merging, so they can be inspected, retried, or manually overridden.

## Between Convert and Package: Route

**Skill:** `flowx:flowx-route`

Route runs after convert, on the deterministic report convert wrote (once every decision is made it
refuses without one: "run the convert phase first"). Like enrich, it contains **no LLM**. Route
groups pipelines into connected components over control lineage and writes its per-component
recommendation, plus the groupings the insights suggest (components linked by an inferred
relationship or a shared simplification pattern), straight into `metadata/conversion_plan.json`
with every decision pending, and draws it as the standard review page
`metadata/routing_review.html`. The customer decides `deterministic` or `agentic` per
component and which groupings to accept (an accepted grouping is converted as one agentic unit);
the agent records those decisions, and the routing conversation, in the plan and routes again. Once
nothing is pending, route rebuilds `.work/translation_report.json` and `gaps.json` from convert's
unchanged baseline, the stored fills of convert's own gaps, the plan, and the agent's outputs:
deterministic units stay as-is; agentic units with a stored output get it applied; agentic units
without one become placeholders. A fully-deterministic plan (or no plan) leaves the report
byte-identical to the baseline — the non-breaking guarantee. Route stamps a routing record onto the
report (per unit: members, decision, outcome, output hash, and fingerprint) and every re-route
rebuilds deterministically and idempotently. A fresh convert (a report without a routing record)
becomes the new baseline on the next route.

Routed-agentic units are then filled only by `fill-agentic`, which applies authored pipelines (N→M,
e.g. a Lakeflow Connect collapse, or one same-named pipeline to stay 1:1), typically using
`AgenticComponentActivity` nodes, and stores them in `metadata/agentic_conversion.json`. Same authored
pipelines as already applied returns "already applied, unchanged" and writes nothing (idempotent).
Different pipelines replace the stored output, recorded in its `replaced` history, and rebuild.
`convert --merge-agentic` fills convert's own gaps and refuses routed-agentic pipelines; after routing,
an in-place merge into `.work/translation_report.json` is also stored as a gap fill, so the next
rebuild keeps it and the baseline stays as convert wrote it. Package refuses until every agentic unit
is filled. Route, fill-agentic and merge never rewrite modify's configured report; package asks to
re-run `modify` when it is out of date.

## Phase 3: Package

**Skill:** `flowx:flowx-package`

**Input:** `translation_report.json` from phase 2

**Process:**
1. Read the unified translation report
2. For each pipeline, generate a Databricks Lakeflow Job definition (YAML)
3. Map ADF activity dependencies to DABs task dependencies
4. Generate the `databricks.yml` root configuration with target environments
5. Copy/generate notebooks to `src/notebooks/`
6. Generate setup scripts for infrastructure (volumes, secrets, connections)
7. Generate skeleton test files

**Output:**
- Complete DABs project ready for `databricks bundle validate` and `databricks bundle deploy`

**Key decisions:**
- One Databricks Job per ADF pipeline. Activities within a pipeline become tasks within the job.
- ADF dependency chains (`dependsOn`) are mapped to DABs task dependencies.
- Environment-specific values (catalog, schema, warehouse ID) are parameterized as bundle variables.
- Three default targets: dev, staging, prod. Each can have different variable values.
- Setup scripts are generated but not run automatically — the user must review and run them.

## Design Principles

### Deterministic-first, agentic fallback

The majority of ADF activities (Copy, DatabricksNotebook, ForEach, IfCondition, etc.) have well-defined Databricks equivalents. Building deterministic translators for these ensures:
- **Speed** — no LLM calls needed for 70-80% of activities
- **Reliability** — same input always produces same output
- **Testability** — translators are unit-testable Python functions
- **Cost** — no token consumption for the bulk of translation

Agentic skills handle the remaining 20-30% that require interpretation or generation. This is where LLM reasoning adds value — complex data flows, exotic activity types, expression conversion.

### Phased execution with checkpoints

Each phase produces a persistent artifact (`inventory.json`, `translation_report.json`, DABs project). This allows:
- **Resumability** — if a phase fails, restart from that phase
- **Inspection** — review intermediate artifacts before proceeding
- **Modularity** — run phases independently or skip phases
- **Debugging** — trace issues back to specific phases

### IR as decoupling layer

The Databricks IR (intermediate representation) sits between ADF semantics and DABs output. This decouples:
- **Parsing** — understanding ADF JSON structure
- **Semantic mapping** — translating ADF concepts to Databricks concepts
- **Serialization** — writing DABs YAML and notebooks

This means the package phase could target different output formats (Terraform, raw API calls, etc.) without changing the translation logic.

## ADF Concepts to Databricks Mapping

| ADF Concept | Databricks Equivalent |
|---|---|
| Pipeline | Lakeflow Job |
| Activity | Job Task |
| Activity dependency (`dependsOn`) | Task dependency (`depends_on`) |
| Pipeline parameter | Job parameter |
| Pipeline variable | Task value |
| Linked service | Unity Catalog connection or secret scope |
| Dataset | Unity Catalog table/volume path |
| Data flow | DLT pipeline or PySpark notebook |
| Trigger (schedule) | Job schedule (`quartz_cron_expression`) |
| Trigger (tumbling window) | Job schedule (periodic) |
| Trigger (event) | File arrival trigger |
| Integration runtime | Job cluster or serverless compute |

## Error Handling

- **Parse errors** — logged to `parse_errors.json`, skipped in inventory
- **Translation failures** — marked as `failed` in translation report, get placeholder tasks in DABs
- **Agentic failures** — saved with error details, can be retried with additional context
- **Unsupported activities** — warned at discover, get placeholder tasks with TODO comments in DABs
