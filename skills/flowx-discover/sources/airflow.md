# Discover — Apache Airflow

Source guide for `--source airflow`. Parse Airflow DAG `.py` modules into a classified inventory.
See the parent `SKILL.md` for the shared output layout, inventory shape, and how to run a phase.

## How it works

flowx reads DAG modules **statically** with Python's `ast` — no Airflow install, and the DAGs are
never executed. It extracts operators, `>>` / `<<` task dependencies, the DAG's
`schedule_interval`, and inline PythonOperator callables / BashOperator commands. Each task is
classified:

- **Deterministic** — a mapped operator (PythonOperator, BashOperator) that becomes a generated
  notebook task.
- **Agentic** — an operator with no deterministic mapping yet; emitted as a placeholder for
  LLM-assisted translation.

## Step 1 — Determine the Airflow source path

Ask the user for either a single DAG `.py` file or a directory of DAGs (scanned recursively; files
with no `DAG(` / `@dag` construct are skipped). Local paths only — the parser reads source text.

## Step 2 — Run the parser

```bash
"$PY" -m flowx.adapter discover --source airflow \
  --airflow-source-path <path_to_dag_or_dir> \
  --output-dir <output_dir> \
  [--pipeline <dag_id>]
```

`--airflow-source-path` is the Airflow alias of `--source-path`; both normalise to `--source-dir`.
Pass `--pipeline <dag_id>` to scope to a single DAG.

## Step 3 — Read and validate the inventory

Read `<output_dir>/metadata/inventory.json` (`"source": "airflow"`). Each pipeline entry lists its
tasks with a `strategy`. `metadata/profile_report.csv` carries one row per DAG (`pipeline`,
`activities`, `complexity_size`).

## Step 4 — Present the summary

```
Airflow Discover Summary
========================
DAGs parsed:        3
Total tasks:        8
  Deterministic:    7
  Agentic:          1
Coverage:           87.5%
```

## Step 5 — Detail agentic tasks

For `agentic` tasks, name the operator that has no deterministic mapping yet (e.g. a custom or
provider operator) and note it will be emitted as a placeholder notebook for the convert phase to
fill via LLM-assisted translation.

## Coverage notes

Current deterministic coverage: `PythonOperator` (callable body → generated PySpark notebook) and
`BashOperator` (command → `%sh` notebook). Dependencies (`>>` / `<<`) and cron
`schedule_interval` → Quartz are handled. Other operators become placeholders. Confirm the output
location and proceed to `flowx-convert` with the same `<output_dir>` and `--source airflow`.
