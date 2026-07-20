# Convert — Apache Airflow

Source guide for `--source airflow`. Translate parsed Airflow DAGs into Databricks IR. See the
parent `SKILL.md` for how to run the phase and the report contract.

Airflow translation is **deterministic-first with an agentic-gap round**, like the ADF source. The
static parse maps ~35 operator/sensor families directly to IR (Tier 1-3). Operators with no
deterministic mapping become `PlaceholderActivity` tasks *and* are recorded in `gaps.json`, each
carrying the operator's raw source so an agent can reason out the translation and replace the
placeholder — the same `gaps.json` + `merge_agentic` flow the ADF source uses.

**Before converting, check [`sources/airflow-coverage.md`](airflow-coverage.md)** — the verified
support matrix (classic operators, TaskFlow, sensors, TaskGroups, dbt factory) and the constructs
still **not** handled (including dynamic TaskGroup mapping). Constructs flowx can't
lower deterministically — callables reading task context (`**context` / `ti`) or XCom, and
runtime-branching decorators — are routed to a placeholder + `gaps.json` for the agentic round rather
than emitted as broken code.

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

## Step 3 — Handle agentic gaps

If convert wrote `<output_dir>/.work/gaps.json`, each entry is an unmapped operator awaiting
LLM-assisted translation. For each gap, read its `raw_definition` (the operator's source, embedded
in the placeholder notebook too) and translate it into a real Databricks task — most portably a
notebook you write to the workspace. Reason from the operator's arguments: e.g. a
`KubernetesPodOperator` running a Python image becomes a notebook (or `%pip install` + the image's
entrypoint logic); an `HttpSensor` becomes a polling notebook using `requests`; a `LivyOperator`
submits Spark directly.

Write one result JSON per gap into `<output_dir>/agentic_results/` and merge them with the shared
`merge_agentic` command (see the parent `SKILL.md`):

```bash
"$PY" -m flowx.adapter convert --source airflow --merge-agentic \
  --report <output_dir>/.work/translation_report.json \
  --agentic-results <output_dir>/agentic_results
```

The ADF just-in-time option chain (notify motifs, metadata-driven consolidation) does not apply to
Airflow; only the agentic-gap round does.

## Step 4 — Proceed to package

Run `flowx-package` with the same `<output_dir>`. Package is source-independent — it consumes the
translation report and emits the DABs bundle (databricks.yml, resources/, src/ notebooks, SETUP.md)
identically for every source.
