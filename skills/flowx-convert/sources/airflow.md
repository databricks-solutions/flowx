# Convert — Apache Airflow

Source guide for `--source airflow`. Translate parsed Airflow DAGs into Databricks IR. See the
parent `SKILL.md` for how to run the phase and the report contract.

Airflow translation is **deterministic** today: the same static parse the discover phase uses
produces the Pipeline IR directly. There is no separate agentic-gap round for Airflow yet —
operators without a mapping are emitted as placeholder tasks the user fills in manually (or via a
future agentic pass), not as pending gaps in the report.

## Step 1 — Run the translation

```bash
"$PY" -m flowx.adapter convert --source airflow \
  --airflow-source-path <path_to_dag_or_dir> \
  --output-dir <output_dir> \
  [--pipeline <dag_id>]
```

Use the same source path and `--pipeline` scoping the discover phase used. This writes
`<output_dir>/.work/translation_report.json` in the shared report shape (one pipeline dict, or a
`{"pipelines": [...]}` wrapper for a folder of DAGs).

## Step 2 — Review the report

Read `<output_dir>/.work/translation_report.json`. Each task is a `NotebookActivity` (from a
PythonOperator callable or BashOperator command, carrying `generated_source`) or a
`PlaceholderActivity` (an unmapped operator). Dependencies come from `>>` / `<<`; the DAG's cron
`schedule_interval` is carried as the pipeline `schedule`.

## Step 3 — Handle placeholders (optional)

For any `PlaceholderActivity`, decide whether to hand-write the notebook body now or leave the
placeholder for the package phase to emit (it ships a notebook with a clear TODO). The shared
`inspect`/`modify`/`merge_agentic` commands from the parent `SKILL.md` are available if you want to
apply agent-translated results, but the ADF just-in-time option chain (notify motifs,
metadata-driven consolidation) does not apply to Airflow.

## Step 4 — Proceed to package

Run `flowx-package` with the same `<output_dir>`. Package is source-independent — it consumes the
translation report and emits the DABs bundle (databricks.yml, resources/, src/ notebooks, SETUP.md)
identically for every source.
