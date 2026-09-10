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

For the full verified support matrix — classic operators, TaskFlow (`@dag`/`@task`), sensors,
TaskGroups (incl. group-level dependencies), and dbt factory (static + PyDABs) — plus the constructs
that are **not** handled (dynamic TaskGroup mapping, shared multi-DAG bundle), see
[`../../flowx-convert/sources/airflow-coverage.md`](../../flowx-convert/sources/airflow-coverage.md).
Callables reading Airflow task context (`**context` / `ti`) or XCom, and runtime-branching
decorators, are routed to placeholders for manual/agentic translation rather than converted.

## Insights — deep-dive & pattern vocabulary

Reference for the shared agentic-insights step (parent `SKILL.md` Step 5, "Author and merge agentic
insights"). Do this deep-dive before authoring insights for any DAG.

**Deep-dive the DAG source.** The inventory is a deterministic skeleton (task types, strategy,
dependencies); the *why* and *how* live in the **DAG source** — the `.py` files under the
`--source-path` you discovered from. Read the DAG module for any pipeline you write about: task
callables (`PythonOperator` bodies), operator arguments, templated params, hooks / connections, and
`set_upstream` / `>>` dependencies. Recurse into `TaskGroup`s and dynamically mapped (`.expand`)
tasks. The parser already extracts operators, `>>` / `<<` edges, `schedule_interval`, and inline
callables (see "How it works" above), so read the source for the intent the static parse can't
capture — what a callable actually *does*, what a hook connects to, and why the tasks are ordered as
they are.

**Airflow operators → Databricks** — a reference menu, NOT an allowlist; the target side uses current
product names, so reach past it whenever a better or newer fit exists. Flag `simplification_pattern:
true` only on entries that use a distinctive capability, never on the plain-orchestration fallback.

| DAG uses… | Simplifying target — `simplification_pattern: true` (rank first) | Fallback — `false` |
|---|---|---|
| DB extract via `MsSqlOperator` / `JdbcOperator` / custom hook | **Lakeflow Connect** managed connector (change-tracking/CDC → Delta) | JDBC read + `MERGE INTO` |
| Incremental load w/ XCom or Variable watermark | **Lakeflow Declarative Pipelines `AUTO CDC`** | Delta `MERGE INTO` + `dbutils.jobs.taskValues` |
| File sensor + load (`*FileSensor` → transform) | **Auto Loader** (`cloudFiles`, file-notification mode) | — |
| `SparkSubmitOperator` / `DatabricksSubmitRunOperator` | native **Lakeflow Job** task (notebook / JAR / Python) | — |
| `PythonOperator` glue / bespoke script | notebook or Python task in a **Lakeflow Job** | — |
| `TriggerDagRunOperator` / `ExternalTaskSensor` fan-out | **Lakeflow Jobs** run-job task + job parameters | — |
| Dynamic task mapping (`.expand`) over a list | **Lakeflow Jobs** for-each task | — |
| `BashOperator` shelling out to a script | native task (notebook / Python) driven by job parameters | — |
| Custom logging / observability via XComs or a side table | **system tables (`system.lakeflow.*`) + native job notifications + AI/BI dashboard** | — |

**Emit current names, not legacy ones:** Lakeflow Jobs (was Databricks Workflows), Lakeflow
Declarative Pipelines (was Delta Live Tables/DLT), `AUTO CDC` (was `APPLY CHANGES INTO`), Declarative
Automation Bundles (was Databricks Asset Bundles), AI/BI dashboards (was Lakeview), `system.lakeflow`
(was `system.workflow`).

**Then author the insights (shared method).** With this deep-dive and pattern vocabulary in hand,
author and merge the `insights` object by following the source-neutral "Author and merge agentic
insights" step in the parent `SKILL.md`. The insight schema and the authoring method are shared
across sources; only the deep-dive and the construct mappings above are Airflow-specific.
