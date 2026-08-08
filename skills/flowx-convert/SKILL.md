---
name: flowx-convert
description: >
  Translate a source's parsed inventory into Databricks IR (intermediate representation): run
  deterministic translators for known types, then agentic (LLM-assisted) translation for the gaps.
  Phase 2 of the flowx migration workflow; routes to a source-specific guide.
triggers:
  - "convert pipelines"
  - "translate pipelines"
  - "convert ADF"
  - "convert airflow"
  - "run translation"
---

# Convert Source to Databricks IR

Convert a source's discovered inventory into Databricks intermediate representation (IR). This is
phase 2 of the flowx migration workflow; it produces a transient translation report under
`<output_dir>/.work/` that the `flowx-package` skill turns into a Databricks Asset Bundle.

Translation is **source-specific** (ADF activity translators vs. Airflow operator mapping), so this
skill routes to the right source guide. The shared mechanics — how to run the phase, the report
contract, and the `inspect`/`modify` machinery — live here. The legacy `merge_agentic` command is
ADF-only; Airflow uses `resolve-agentic prepare|stage|apply` instead.

## Step 1 — Identify the source (required)

Use the same source the discover phase used (it is recorded as `"source"` in
`<output_dir>/metadata/inventory.json`):

- **Azure Data Factory / Fabric Data Factory** → source `adf` → read `sources/adf.md`
- **Apache Airflow** → source `airflow` → read `sources/airflow.md`

There is no default source. Every phase invocation passes `--source <name>` explicitly.

## Step 2 — Follow the source guide

Read the matching `sources/<source>.md` and follow it. ADF has a rich deterministic-first +
agentic-gap flow with just-in-time configuration. Airflow converts deterministically first and may
then use the separately reviewed, fingerprint-bound `flowx-resolve-airflow-gaps` workflow.

## How to run this phase — MCP tool or venv CLI

Run the **`setup`** skill first if you haven't.

- **MCP tool (Genie Code, or local stdio):** call the single **`flowx`** tool with
  `command="convert"` and `parameters` including `"source": "<source>"` and `"output_dir": "<dir>"`.

- **venv CLI (local):**

  ```bash
  export PYTHONPATH="<plugin_dir>/src"
  PY="$(cat <plugin_dir>/.migration-venv)"
  "$PY" -m flowx.adapter convert --source <source> --source-path <path> --output-dir <dir> [--pipeline <name>]
  ```

  `--source` is required. The convert phase writes `<output_dir>/.work/translation_report.json`.

## The translation report contract (shared)

Every source's convert phase writes `<output_dir>/.work/translation_report.json` in the same shape:
a single pipeline IR dict (keys `name`, `tasks`, optional `schedule`/`parameters`), or a
`{"pipelines": [...]}` wrapper for many. The `flowx-package` phase consumes this regardless of
source. IR serialization is source-neutral (`flowx.ir_serde`), so the report format is identical
across ADF and Airflow.

## Shared adapter commands

`inspect` and `modify` operate on the report rather than raw source definitions. The ADF guide uses
them heavily; Airflow currently needs only the base conversion:

- `inspect <report>` — emit the full just-in-time option schema (each option annotated with a
  `show_when` condition). Walk it locally; ask an option only when its `show_when` is satisfied.
- `modify <report> --output-dir <dir> --answer OPTION_ID=VALUE ...` — validate and apply collected
  answers, writing `.work/translation_report.stamped.json` + `metadata/configuration.json`.
- `merge_agentic --report <report> --agentic-results <dir>` — **ADF only**. Fold agent-produced
  per-activity translations into an ADF report. Airflow's legacy name-based merge is disabled.

## Output artifacts (shared, transient under `<output_dir>/.work/`)

| File | Description |
|---|---|
| `.work/translation_report.json` | Full translation report with IR for all tasks |
| `.work/<pipeline>.json` | Per-pipeline Databricks IR |
| `.work/gaps.json` | Unmapped source constructs; agentic inputs for ADF and review-only gaps for Airflow |
| `.work/translation_report.stamped.json` | Configuration-stamped report (written by `modify`) |

## Reference

- `sources/adf.md` — ADF translation: deterministic engine, agentic gaps, just-in-time config,
  notify motifs, metadata-driven consolidation. See also `references/activity-mapping.md`.
- `sources/airflow.md` — Airflow deterministic-first translation plus reviewed leaf-gap resolution.
