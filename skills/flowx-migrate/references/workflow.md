# flowx Migration Workflow

End-to-end architecture for migrating Azure Data Factory (ADF) pipelines to Databricks Lakeflow Jobs via Declarative Automation Bundles (DABs).

## Contents

- [Overview](#overview)
- [Phase 1: Discover](#phase-1-discover)
- [Phase 1.a: Enrich (optional)](#phase-1a-enrich-optional)
- [Phase 2: Convert](#phase-2-convert)
- [Phase 2.a: Route](#phase-2a-route)
- [Phase 3: Package](#phase-3-package)
- [Design Principles](#design-principles)
- [ADF Concepts to Databricks Mapping](#adf-concepts-to-databricks-mapping)
- [Error Handling](#error-handling)

## Overview

flowx follows three main phases (discover, convert, package), with optional enrich and route steps between them. Each phase is independently runnable and produces artifacts consumed by the next phase. The design principle is **deterministic-first, agentic fallback**: well-known ADF patterns are translated by fast, reliable Python code, while complex or ambiguous patterns are handled by LLM-assisted skills.

```
ADF JSON Exports
      |
      v
  Phase 1: DISCOVER
  (parse + classify)
      |
      v
  inventory.json
      |
      v
  Phase 2: CONVERT
  (deterministic + agentic)
      |
      v
  translation_report.json + IR + notebooks
      |
      v
  Phase 3: PACKAGE
  (bundle generation)
      |
      v
  Databricks DABs Project
  (databricks.yml + resources/ + src/ + SETUP.md)
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
- `metadata/inventory.json` — classified activity inventory
- `metadata/profile_report.csv` — the complexity report
- `metadata/source_graphs.json` and `metadata/<pipeline>.arm.json` — the saved source graphs and each pipeline's verbatim ARM JSON (ADF only)

**Key decisions:**
- Classification is based on the activity `type` field, not the activity content. This makes classification fast and deterministic.
- Datasets and linked services are parsed for context but not independently translated — they inform the activity translators.
- Triggers are included in the inventory and translated in phase 2.

## Phase 1.a: Enrich (optional)

**Skill:** `flowx:flowx-enrich`

**Input:** `inventory.json` from phase 1, and for ADF the saved `source_graphs.json`

**Process:**
1. The agent reads the inventory and the source artifacts
2. The agent tells the user what it found and asks the open questions the export can't answer
3. The agent authors an `insights` layer: a factory-wide recommendation, each pipeline's intent and recommended Databricks patterns, and cross-pipeline relationships
4. `enrich` validates the insights against the inventory and the saved source graphs and, only when they are valid, records them and rebuilds the inventory

**Output:**
- `agentic_insights.json`: the validated insights, bound to the saved `source_graphs.json` (for Airflow, to `inventory.json`)
- `inventory.json`: rebuilt with a single additive `insights` key

**Key decisions:**
- Enrich contains no LLM: the agent authors, the library validates and records.
- Every deterministic inventory key stays byte-identical.
- `flowx-migrate` runs enrich unless the user asks for a deterministic-only pass; the one-shot MCP `migrate` command never runs it.

## Phase 2: Convert

**Skill:** `flowx:flowx-convert`

**Input:** the same source export discover read (convert reads the source again, not `inventory.json`)

**Process:**
1. Run the deterministic translators for every activity and trigger they handle
2. Leave each activity they can't translate as an agentic gap with a placeholder task; convert runs no LLM
3. Write the deterministic baseline report with Databricks IR (intermediate representation) for each pipeline

**Output (under `.work/`):**
- `translation_report.json` — the deterministic baseline, with IR for every task and the gaps marked
- `<pipeline>.json` — IR per pipeline
- `gaps.json` — the agentic gaps, when there are any

The agent fills the gaps afterwards: route and fill-agentic for routed-agentic units, `convert --merge-agentic` for convert's own gaps (ADF only), and the `flowx-resolve-airflow-gaps` skill for Airflow.

**Key decisions:**
- Deterministic translators run first because they are fast and reliable. Agentic skills are only invoked for gaps.
- The IR is an intermediate format that decouples translation from DABs generation. This allows the package phase to target different output formats in the future.
- Each source's deterministic translators are standalone Python modules under `src/flowx/sources/<source>/` (e.g. ADF's live in `src/flowx/sources/adf/translators/`). Adding support for a new activity type means adding a new module there.
- Agentic fills are stored in `metadata/agentic_conversion.json`, apart from convert's baseline, so they can be inspected, revised, or applied again after a fresh convert.

## Phase 2.a: Route

**Skill:** `flowx:flowx-route`

**Input:** `translation_report.json` from phase 2, the inventory and source graphs from phase 1, the insights from phase 1.a when present, and any stored fills in `agentic_conversion.json`

**Process:**
1. Group pipelines into connected components over control lineage
2. Recommend `deterministic` or `agentic` per component, plus the groupings the insights suggest, and draw them on a review page
3. Record the customer's decision per component, the groupings they accept (each converted as one agentic unit), and the routing conversation
4. Once nothing is pending, rebuild the translation report from convert's unchanged baseline, the plan and the stored fills; an agentic unit with no fill yet becomes a placeholder
5. Fill each routed-agentic unit with `fill-agentic`, which stores the fill in `agentic_conversion.json` and rebuilds the report

**Output:**
- `conversion_plan.json`: the recommendation, the decisions and the routing conversation
- `routing_review.html`: the review page drawn from the plan
- `translation_report.json` and `gaps.json`: rebuilt; they carry a routing record when any unit is agentic or a gap fill applies

**Key decisions:**
- Route contains no LLM. Applying a fully decided plan needs the report convert wrote; without one it refuses ("run the convert phase first").
- A fully deterministic plan with no gap fills, or no plan, leaves the report byte-identical to convert's, with no routing record: the non-breaking guarantee.
- Every re-route rebuilds deterministically from the baseline; a fresh convert becomes the new baseline.
- Only `fill-agentic` fills routed-agentic units; applying the same output again changes nothing, and a different output replaces it.
- `convert --merge-agentic` fills only convert's own gaps; an in-place merge after routing is stored in `agentic_conversion.json` and kept across re-routes.
- Route, fill-agentic and merge never rewrite `modify`'s configured report; package asks to re-run `modify` when it is out of date.
- Package refuses until every agentic unit is filled.

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
