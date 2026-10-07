---
name: flowx-discover
description: >
  Parse a source orchestrator's pipeline definitions (Azure Data Factory, Apache Airflow) into a
  typed inventory that classifies every task as deterministic, agentic, or unsupported. Phase 1 of
  the flowx migration workflow; routes to a source-specific guide.
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
| `metadata/inventory.json` | Classified activity inventory for the convert phase |
| `metadata/profile_report.csv` | Per-pipeline complexity report (counts + T-shirt size) |
| `metadata/<pipeline>.arm.json` | (ADF) Verbatim original source for each pipeline (provenance) |

The inventory classifies every task into one of three strategies:

- **Deterministic** — a built-in translator exists; converted without an LLM.
- **Agentic** — requires LLM-assisted translation from the source definition.
- **Unsupported** — no known translation path; needs manual intervention.

## Reference

- `sources/adf.md` — Azure Data Factory discovery (ARM JSON, UC-volume download, complexity report)
- `sources/airflow.md` — Apache Airflow discovery (DAG `.py` parsing, operator classification)
