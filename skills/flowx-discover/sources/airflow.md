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
DAGs parsed:          3
  Ready to deploy:    2  (verified, no gaps or placeholders)
Total tasks:          8
  Converted:          7  (deterministic; runs as translated)
  Placeholders:       1  (agentic; must be translated before deploying)
  Failed:             0
  Excluded:           0
Converted:            87.5% of tasks
Classified:           100.0% of tasks (converted or placeholder; not readiness)
Reconciliation:       verified_with_gaps

Job settings to review:
  - nightly_load: The job's schedule is emitted paused because the migration has open gaps or placeholder tasks. Unpause it once every task is converted and checked.
```

Lead with **Converted %** and **Ready to deploy**. **Classified** counts placeholders too, so 100% there
does not mean anything converted. Repeat every line under "Job settings to review" to the user: they
name a schedule emitted paused, a timezone assumed to be UTC, or a job name that fell back to the file
name. Each DAG's `ready_to_deploy` flag in `inventory.json` says the same per DAG.

## Step 5 — Detail agentic tasks

For `agentic` tasks, name the operator that has no deterministic mapping yet (e.g. a custom or
provider operator) and note it will be emitted as a placeholder notebook for the convert phase to
fill via LLM-assisted translation.

## Coverage notes

Current deterministic coverage: `PythonOperator` (callable body → generated PySpark notebook) and
`BashOperator` (command → `%sh` notebook). Dependencies (`>>` / `<<`) and cron
`schedule_interval` → Quartz are handled. Other operators become placeholders. Confirm the output
location and proceed to `flowx-convert` with the same `<output_dir>` and `--source airflow`.

For the full verified support matrix — classic operators, TaskFlow (`@dag`/`@task`), sensors,
TaskGroups (incl. group-level dependencies), and dbt factory (static + PyDABs) — plus the constructs
that are **not** handled (dynamic TaskGroup mapping, shared multi-DAG bundle), see
[`../../flowx-convert/sources/airflow-coverage.md`](../../flowx-convert/sources/airflow-coverage.md).
Callables reading Airflow task context (`**context` / `ti`) or XCom, and runtime-branching
decorators, are routed to placeholders for manual/agentic translation rather than converted.
