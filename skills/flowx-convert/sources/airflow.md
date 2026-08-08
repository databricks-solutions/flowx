# Convert — Apache Airflow

Source guide for `--source airflow`. Translate parsed Airflow DAGs into Databricks IR. See the
parent `SKILL.md` for how to run the phase and the report contract.

Airflow translation is currently **deterministic-only**. The static parse maps ~35 operator/sensor
families directly to IR (Tier 1-3). Operators with no deterministic mapping become
`PlaceholderActivity` tasks and are recorded in `gaps.json` with their raw source for review. The
placeholder remains a deliberate runtime failure until it is resolved manually; a supported Airflow
agentic replacement workflow is not available yet.

**Before converting, check [`sources/airflow-coverage.md`](airflow-coverage.md)** — the verified
support matrix (classic operators, TaskFlow, sensors, TaskGroups, dbt factory) and the constructs
still **not** handled (including dynamic TaskGroup mapping). Constructs flowx can't lower
deterministically — callables reading task context (`**context` / `ti`) or XCom, and runtime-branching
decorators — are routed to a placeholder + `gaps.json` rather than emitted as broken code.

dbt workloads default to static explosion; pass `--dbt-mode pydabs` to emit a deploy-time PyDABs hook
instead (see the dbt factory section of the coverage doc).

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

## Step 3 — Review deterministic gaps

If convert wrote `<output_dir>/.work/gaps.json`, each entry describes an unmapped construct whose
generated Job task points to a notebook that raises `NotImplementedError`. Review every gap before
deployment. The shared `merge_agentic` command is not a supported Airflow workflow yet; keep the
placeholder, exclude the DAG, or implement and validate the replacement explicitly.

## Step 4 — Proceed to package

Run `flowx-package` with the same `<output_dir>`. Package is source-independent — it consumes the
translation report and emits the DABs bundle (databricks.yml, resources/, src/ notebooks, SETUP.md)
identically for every source.
