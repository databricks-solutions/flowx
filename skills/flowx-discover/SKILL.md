---
name: flowx-discover
description: >
  Parse a source orchestrator's pipeline definitions (Azure Data Factory, Apache Airflow) into a
  typed inventory that classifies every task as deterministic, agentic, or unsupported. Phase 1 of
  the flowx migration workflow; routes to a source-specific guide.
  Use when the user asks to discover, load, parse or import ADF or Airflow pipelines, or to
  inventory a source.
---

# Discover the source pipelines

Phase 1 of the flowx workflow. Discover parses a source orchestrator's definitions into a typed inventory
that classifies every task as deterministic (a built-in translator exists, no LLM needed), agentic (needs
LLM-assisted translation from the source definition) or unsupported (no known translation path; needs
manual work). It writes `metadata/inventory.json`, which `flowx-convert` reads, and a per-pipeline
complexity report, `metadata/profile_report.csv`. Each source has its own parser and guide: ADF ships ARM
JSON, Airflow ships Python DAG modules.

## Before you start

- The user needs a copy of the definitions. For ADF without an export, run the `flowx-extract-metadata`
  skill first; it returns the exact path to use.

## How to run

The common MCP and CLI rules are in [`../shared/running-flowx.md`](../shared/running-flowx.md).

- **MCP:** `flowx(command="discover", parameters={"source": "<source>", ...})`; each source guide lists
  its source keys.
- **CLI:** `"$PY" -m flowx.adapter discover --source <source> --source-path <path> --output-dir <dir> [--pipeline <name>]`.
  `--source-path` is the generic flag; each source also accepts its own alias (for example
  `--adf-source-path`), and both become the phase's `--source-dir`.

## Steps

### Step 1: Identify the source

Ask which orchestrator the user is migrating from, or infer it from the input. There is no default
source.

- Azure Data Factory or Fabric Data Factory: source `adf`, read [`sources/adf.md`](sources/adf.md).
- Apache Airflow: source `airflow`, read [`sources/airflow.md`](sources/airflow.md).

### Step 2: Follow the source guide

Each guide covers the source's input layout, the exact discover command and how to read its inventory.

## Output

In `<output_dir>/metadata/` (layout in [`../shared/output-folder.md`](../shared/output-folder.md)):

| File | What it holds |
|---|---|
| `inventory.json` | The classified inventory for convert |
| `source_graphs.json` | ADF only: the saved source graphs (activities, lineage and detected motifs), versioned and content-hashed; the inventory is built from it and records its hash as `source_graphs_sha256` |
| `profile_report.csv` | Per-pipeline complexity (counts and a T-shirt size) |
| `<pipeline>.arm.json` | ADF only: the verbatim source of each pipeline |
| `archive/<UTC time>/` | Only on a re-run: the earlier run's `metadata/`, `.work/` and bundle, with an `archive.json` of what moved, from where and when |

Discover starts each run afresh: over a folder that already holds a run, it moves that run into a new
archive folder instead of deleting it, and says so. Package keeps `metadata/`, so the archive stays.

## Reference

- [`sources/adf.md`](sources/adf.md): ADF discovery (ARM JSON, volume download, complexity report).
- [`sources/airflow.md`](sources/airflow.md): Airflow discovery (DAG `.py` parsing, operator
  classification).
- The `flowx-convert` skill's `sources/airflow-coverage.md`: what the Airflow path converts and what it
  does not.
- The `flowx-enrich` skill: adds agent-authored insights to the inventory (`flowx-migrate` runs it after
  discover unless the user asks for a deterministic-only pass).
- [`../shared/running-flowx.md`](../shared/running-flowx.md) and
  [`../shared/output-folder.md`](../shared/output-folder.md): running a phase and the output folder.
