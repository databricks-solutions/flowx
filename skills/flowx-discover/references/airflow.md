# Discover — Apache Airflow

Source guide for `--source airflow`. Parse Airflow DAG `.py` modules into a classified inventory.
See the parent `SKILL.md` for the shared output layout, inventory shape, and how to run a phase.

## How it works

flowx reads DAG modules **statically** with Python's `ast` — no Airflow install, and the DAGs are
never executed. It extracts operators, `>>` / `<<` task dependencies, the DAG's
`schedule_interval`, and inline PythonOperator callables / BashOperator commands. Each task is
classified:

- **Deterministic**: a mapped operator or sensor that becomes a notebook, SQL, Spark, run-job or dbt
  task; [`../../shared/airflow-coverage.md`](../../shared/airflow-coverage.md) lists each one.
- **Agentic**: an operator with no deterministic mapping; emitted as a placeholder for
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
DAGs parsed:          <n>
  Ready to deploy:    <n>  (verified, no gaps or placeholders)
Total tasks:          <n>
  Converted:          <n>  (deterministic; runs as translated)
  Placeholders:       <n>  (agentic; must be translated before deploying)
  Failed:             <n>
  Excluded:           <n>
Converted:            <pct>% of tasks
Classified:           <pct>% of tasks (converted or placeholder; not readiness)
Reconciliation:       <status>

Job settings to review:
  - nightly_load: The job's schedule is emitted paused because the migration has open gaps or placeholder tasks. Unpause it once every task is converted and checked.
```

Lead with **Converted %** and **Ready to deploy**. **Classified** counts placeholders too, so a full
Classified figure does not mean anything converted. Repeat every line under "Job settings to review" to the user: they
name a schedule emitted paused, a timezone assumed to be UTC, a job name that fell back to the file
name, or a job that allows one run at a time where Airflow allowed more. Each DAG's `ready_to_deploy` flag in `inventory.json` says the same per DAG.

## Step 5 — Detail agentic tasks

For `agentic` tasks, name the operator that has no deterministic mapping (e.g. a custom or
provider operator) and note it will be emitted as a placeholder notebook for the convert phase to
fill via LLM-assisted translation.

## Coverage notes

Confirm the output location and proceed to `flowx-convert` with the same `<output_dir>` and
`--source airflow`.

For the full verified support matrix — classic operators, TaskFlow (`@dag`/`@task`), sensors,
TaskGroups (incl. group-level dependencies), and dbt factory (static + PyDABs) — plus the constructs
that are **not** handled (dynamic TaskGroup mapping, shared multi-DAG bundle), see
[`../../shared/airflow-coverage.md`](../../shared/airflow-coverage.md).
Callables reading Airflow task context (`**context` / `ti`) or XCom, and runtime-branching
decorators, are routed to placeholders for manual/agentic translation rather than converted.
