---
name: flowx-convert
description: >
  Translate a source's parsed inventory into Databricks IR (intermediate representation): run
  deterministic translators for known types, then agentic (LLM-assisted) translation for the gaps.
  Phase 2 of the flowx migration workflow; routes to a source-specific guide.
  Use when the user asks to convert or translate ADF or Airflow pipelines, or to run the
  translation.
---

# Convert the inventory to Databricks IR

Phase 2 of the flowx workflow. Convert reads the source again (the same path discover read; it does not read
`<output_dir>/metadata/inventory.json`) and writes a translation report under `<output_dir>/.work/`:
deterministic translators handle the known types, and the gaps are left for agentic translation. The
`flowx-package` skill turns the report into a bundle. Translation is source-specific, so each source has
its own guide.

## Before you start

- Discover must have run for this `<output_dir>`. Use the same source it used: it is recorded as
  `"source"` in `metadata/inventory.json`. There is no default source.

## How to run

The common MCP and CLI rules are in [`../shared/running-flowx.md`](../shared/running-flowx.md).

- **MCP:** `flowx(command="convert", parameters={"source": "<source>", "output_dir": "<dir>", ...})`
  with the same source key discover read (`adf_definitions`, `adf_volume_path`, `adf_workspace_path` or
  `airflow_source_path`): convert reads the source again.
- **CLI:** `"$PY" -m flowx.adapter convert --source <source> --source-path <path> --output-dir <dir> [--pipeline <name>]`

## Steps

### Step 1: Pick the source guide

- Azure Data Factory or Fabric Data Factory: source `adf`, read [`references/adf.md`](references/adf.md).
- Apache Airflow: source `airflow`, read [`references/airflow.md`](references/airflow.md).

### Step 2: Follow the source guide

ADF converts deterministically first, then fills its agentic gaps and asks the just-in-time
configuration. Airflow converts deterministically first and may then use the separately reviewed,
fingerprint-bound `flowx-resolve-airflow-gaps` skill.

### Step 3: Use the shared report commands the guide calls for

`inspect` and `modify` work on the report, not on the source. The ADF guide uses them; Airflow needs only
the base conversion. Their flags and the options the adapter raises are in
[`../shared/translation-options.md`](../shared/translation-options.md).

- `inspect <report>`: the full just-in-time option schema, each option with a `show_when` condition. Walk
  it locally and ask an option only when its `show_when` is satisfied.
- `modify <report> --output-dir <dir> --answer OPTION_ID=VALUE ...`: validates and applies the answers,
  writing `.work/translation_report.stamped.json` and `metadata/configuration.json`.
- `merge_agentic --report <report> --agentic-results <dir>`: ADF only. Fills convert's own agentic gaps
  with the agent's per-activity translations. It refuses, and writes nothing, if a result lands in a
  pipeline routed agentic (fill those with `fill-agentic`). After routing, a re-route keeps an in-place
  merge but not a merge written to an `--output` copy. Airflow's name-based merge is disabled; Airflow
  uses `resolve-agentic prepare|stage|apply` instead.

## Output

Under `<output_dir>/.work/` (transient; layout in [`../shared/output-folder.md`](../shared/output-folder.md)):
`translation_report.json` (the report with the IR for every task), `<pipeline>.json` (per-pipeline IR),
`gaps.json` (unmapped constructs: agentic inputs for ADF, review-only gaps for Airflow), and
`translation_report.stamped.json` when `modify` runs.

Every source writes the report in the same shape: one pipeline IR dict (keys `name`, `tasks`, optional
`schedule` and `parameters`), or a `{"pipelines": [...]}` wrapper for many. IR serialization is
source-neutral (`flowx.ir_serde`), so `flowx-package` reads it the same way for every source.

## Reference

- [`references/adf.md`](references/adf.md): ADF translation: the deterministic engine, agentic gaps,
  just-in-time configuration, notify motifs and metadata-driven consolidation.
- [`references/adf-coverage.md`](references/adf-coverage.md): what flowx does with each ADF feature area, and
  what you and the user must do.
- [`references/activity-mapping.md`](references/activity-mapping.md): each ADF activity type and its
  strategy.
- [`references/expression-functions.md`](references/expression-functions.md): how each ADF expression
  function translates.
- [`references/airflow.md`](references/airflow.md): Airflow deterministic-first translation plus reviewed
  leaf-gap resolution.
- [`../shared/airflow-coverage.md`](../shared/airflow-coverage.md): what the Airflow path converts and what it
  does not.
- [`../shared/translation-options.md`](../shared/translation-options.md): the `inspect` and `modify` commands
  and the options they raise.
- [`../shared/running-flowx.md`](../shared/running-flowx.md) and
  [`../shared/output-folder.md`](../shared/output-folder.md): running a phase and the output folder.
