# Airflow 3 Recognition and Migration Guide

Reference for converting DAGs authored against **Apache Airflow 3.x**. Airflow 3 keeps the same operator/sensor *semantics* as Airflow 2 — the DABs mappings in `references/operator-mapping.md` are unchanged — but the **import paths and scheduling APIs moved**. The risk in a naïve conversion is not a wrong mapping; it is a DAG whose tasks are **silently missed** because the parser only recognized Airflow 2 import paths. Recognize the Airflow 3 authoring surface, map the clean equivalents, and flag the rest.

This skill's approach for Airflow 3 is **recognize → safe-map → flag**:
- **Recognize** the `airflow.sdk` and `apache-airflow-providers-standard` import paths so no task is dropped.
- **Safe-map** the constructs with clean Lakeflow equivalents (operators via the existing tiers; `Asset`-based scheduling per the resolution rule).
- **Flag** constructs with no clean equivalent (`@asset` pipelines, `AssetWatcher`, asset aliases, DAG versioning, deadline alerts) in `MIGRATION_NOTES.md` — do not invent a mapping.

---

## How to tell a DAG is Airflow 3

Any of these signals Airflow 3 authoring; parse accordingly:

- Imports from `airflow.sdk` (e.g. `from airflow.sdk import dag, task, task_group, Asset`).
- Imports from `airflow.providers.standard.*` for common operators/sensors.
- `Asset(...)` (the Airflow 3 name for `Dataset`) in `schedule=`.
- `schedule=` used with a **list** of assets, a **boolean** asset expression (`|`, `&`), or an `AssetOrTimeSchedule`.

`schedule_interval=` is **removed** in Airflow 3 (use `schedule=`), and `SubDagOperator` is **removed** (see below). `@dag` / `@task` / `@task_group` behave the same as in Airflow 2 once their import path is recognized.

---

## Airflow 3 scheduling defaults and semantics

Reading the DAG's schedule/backfill intent depends on these Airflow 3 defaults and behaviors:

- **`schedule` defaults to `None`** — a DAG with no `schedule=` runs on manual trigger only. Emit no DABs `schedule`/`trigger` for it (manual/`run_job_task`-driven).
- **`catchup` defaults to `False`** — an unset `catchup` means the DAG does **not** backfill missed intervals. Only treat backfill as intended when `catchup=True` is explicit; note the backfill expectation (and that DABs jobs have no catchup) in `MIGRATION_NOTES.md`.
- **A raw-cron `schedule` uses `CronTriggerTimetable`** — the run's `logical_date` is the fire time (run-after), not the start of a data interval. When a cron/timetable DAG is date-sensitive (its tasks read `logical_date`/`{{ ds }}` to pick the processing window), confirm the intended window and record it before mapping `{{ ds }}` → `{{job.parameters.run_date}}`; flag any timetable that can't be mapped deterministically.

---

## Airflow 3 execution-model additions: native async and resumable

Two execution-model constructs are new in Airflow 3 and affect what you parse. Neither has a DABs "mode" switch; migrate the underlying operation. (**Deferrable operators are NOT Airflow-3-specific** — they date from Airflow 2.2 — so their migration rule lives with the operator mappings in `references/operator-mapping.md`, not here.)

### Native async TaskFlow (`@task` on `async def`) — Airflow 3.2.0

Airflow **3.2.0** added native async TaskFlow tasks: `@task` decorating an `async def`, using `await`, `asyncio.gather`, and async hooks (`HttpAsyncHook`, `SFTPHookAsync`). This is **distinct from deferrable** — async tasks do many concurrent I/O ops within **one** worker slot on a shared event loop; deferrable frees the slot during a wait. Migration:

- Map to a `notebook_task` / wheel task; keep the concurrent I/O **inside one task** by default.
- The coroutine is **not runnable as-is** — rewrite Airflow async hooks and Connections to native async clients (e.g. `aiohttp`, `asyncssh`) with auth from `dbutils.secrets`; the notebook drives the event loop itself.
- Optionally split independent `asyncio.gather()` items into a `for_each_task` — flag the changed retry and UI granularity. There is no DABs "async" setting.

Reference: https://airflow.apache.org/docs/task-sdk/stable/deferred-vs-async-operators.html

### Resumable external jobs (`ResumableJobMixin`) — Airflow 3.3.0

Airflow **3.3.0** added `ResumableJobMixin`: an operator persists the external job id before polling and, on retry, **reattaches** to the running external job instead of resubmitting (implementers provide `submit_job`, `get_job_status`, `is_job_active`, `is_job_succeeded`, `poll_until_complete`, `get_job_result`). Migration:

- If the operation becomes a **native Databricks task**, drop the resumption mechanics.
- If the **external job is retained**, preserve the external job id / idempotency / reattachment or **flag** for review — never silently turn a resumable submission into a notebook that resubmits the external job on every retry.

Reference: https://airflow.apache.org/docs/task-sdk/stable/resumable-job-mixin.html

---

## Task SDK import equivalence (`airflow.sdk`)

Airflow 3 exposes the stable authoring interface under `airflow.sdk`. Map these to the same handling as their Airflow 2 equivalents:

| Airflow 3 (`airflow.sdk`) | Airflow 2 equivalent | Handling |
|---|---|---|
| `from airflow.sdk import dag` | `from airflow.decorators import dag` | Same — DAG metadata source. |
| `from airflow.sdk import task` | `from airflow.decorators import task` | Same — TaskFlow `@task` (see `operator-mapping.md`). |
| `from airflow.sdk import task_group` | `from airflow.decorators import task_group` | Same — TaskGroup / mapped task group. |
| `from airflow.sdk import Asset` | `from airflow.datasets import Dataset` | `Asset` == renamed `Dataset` — asset scheduling below. |
| `from airflow.sdk import DAG` / `BaseOperator` | `from airflow import DAG` / `airflow.models.BaseOperator` | Same. |
| `from airflow.sdk import Variable` / `Connection` | `airflow.models.Variable` / `Connection` | Same — Variables → job params/bundle vars; Connections → secrets/UC connections. |
| `from airflow.sdk import chain` / `cross_downstream` | `airflow.models.baseoperator.chain` / `cross_downstream` | Same — dependency-graph helpers. |
| `from airflow.sdk import Param` (or `airflow.sdk.definitions.param.Param`) | `airflow.models.param.Param` | Same — DAG/task `params` → job parameters. |

---

## Standard-provider import paths (`apache-airflow-providers-standard`)

In Airflow 3, common operators and sensors moved out of `airflow-core` into the `apache-airflow-providers-standard` provider. The **classes and their DABs mappings are unchanged** — only the import path differs. Recognize both the new and legacy paths.

| Class | Airflow 3 import path | DABs mapping (unchanged) |
|---|---|---|
| `PythonOperator` | `airflow.providers.standard.operators.python` | `notebook_task` (Tier 1) |
| `BranchPythonOperator` | `airflow.providers.standard.operators.python` | `condition_task` (Tier 2) |
| `ShortCircuitOperator` | `airflow.providers.standard.operators.python` | `condition_task` (Tier 2) |
| `PythonVirtualenvOperator` | `airflow.providers.standard.operators.python` | `notebook_task` + env note (Tier 2) |
| `ExternalPythonOperator` | `airflow.providers.standard.operators.python` | `notebook_task` + env note (Tier 2) |
| `BashOperator` | `airflow.providers.standard.operators.bash` | `notebook_task` / `spark_python_task` (Tier 1) |
| `TriggerDagRunOperator` | `airflow.providers.standard.operators.trigger_dagrun` | `run_job_task` (Tier 1) |
| `LatestOnlyOperator` | `airflow.providers.standard.operators.latest_only` | Flag — no direct equivalent |
| `ExternalTaskSensor` | `airflow.providers.standard.sensors.external_task` | `trigger.table_update` / `depends_on` (Tier 3) |
| `FileSensor` | `airflow.providers.standard.sensors.filesystem` | `trigger.file_arrival` (Tier 3) |
| `TimeSensor` | `airflow.providers.standard.sensors.time` | absorbed into `schedule` (Tier 3) |
| `TimeDeltaSensor` | `airflow.providers.standard.sensors.time_delta` | absorbed into `schedule` (Tier 3) |
| `DayOfWeekSensor` | `airflow.providers.standard.sensors.weekday` | map to `schedule` day-of-week (Tier 3) |
| `EmptyOperator` | `airflow.providers.standard.operators.empty` | Remove + rewire `depends_on` (Tier 2) |

> There is no `DateTimeSensor` in the standard provider; use `TimeSensor` / `TimeDeltaSensor` /
> `DayOfWeekSensor`.

**Legacy paths:** In Airflow 3.0–3.1 the old `airflow.operators.*` / `airflow.sensors.*` import paths still work with deprecation warnings and are slated for removal in a later release. Recognize **both** the legacy and standard-provider paths so a DAG on either side converts identically.

---

## Assets vs Datasets, and asset scheduling

"Datasets" (Airflow 2) are renamed **Assets** (Airflow 3): `airflow.sdk.Asset` replaces `airflow.datasets.Dataset`. Asset-based **scheduling** maps to Lakeflow `trigger.table_update`; the boolean/list/time-combined forms and the **Asset → UC-table resolution rule** are documented in `references/schedule-trigger-mapping.md` (§ Timetable, Dataset, and Asset Scheduling). Summary:

- `schedule=[asset]` → `trigger.table_update` on the resolved table (single).
- `schedule=[a, b]` (list = ALL) → `condition: ALL_UPDATED`; `a | b` → `ANY_UPDATED`; `a & b` → `ALL_UPDATED`.
- `AssetOrTimeSchedule(...)` — and its Airflow 2.4–2.10 spelling `DatasetOrTimeSchedule(...)` — carries time **and** asset conditions → **flag and generate a manual job with neither arm**; a single Lakeflow job takes a schedule **or** a trigger, not both as a clean 1:1. Require the user to select the time arm, asset arm, or split jobs before adding automation.
- An `Asset` URI is an arbitrary string, so map to a table **only** via explicit `extra={"databricks_table": "catalog.schema.table"}`, a user-supplied mapping, or the skill-local `x-databricks-table:` scheme — otherwise **flag**. Never infer a table from an arbitrary URI.

### `@asset` and related — flag, do not auto-map

These Airflow 3 asset features have no clean Lakeflow equivalent; **flag** them in `MIGRATION_NOTES.md` rather than inventing a mapping:

- The **`@asset` decorator** (defining asset-producing workflows) — distinct from using `Asset` objects in `schedule=`.
- **`AssetWatcher`** and event-driven asset watchers.
- **Asset aliases**.
- **DAG versioning / DAG bundles** (a deployment concept, not a task-graph one).
- **Deadline alerts** (the Airflow 3 successor to SLAs).

---

## Removed in Airflow 3

| Removed | Replacement / handling |
|---|---|
| `schedule_interval=` | Use `schedule=`; the parser reads both. |
| `SubDagOperator` | Use dynamic task mapping / `TaskGroup`. The SubDag flatten in `operator-mapping.md` applies to Airflow 2 DAGs only. |
| `execution_date` context var | Use `logical_date` / `run_id`; the Jinja `{{ ds }}`/`{{ execution_date }}` mappings in `schedule-trigger-mapping.md` still apply for templated strings. |
| `fail_stop` DAG arg | Renamed `fail_fast` (stop the DAG run on first task failure). Record the fail-fast intent in `MIGRATION_NOTES.md`; a Lakeflow job has no single equivalent switch. |

---

## Recognize → safe-map → flag checklist

1. **Recognize imports.** Accept `airflow.sdk.*` and `airflow.providers.standard.{operators,sensors}.*` in addition to the Airflow 2 `airflow.operators.*` / `airflow.sensors.*` paths. A task whose import path is unrecognized must be surfaced, never dropped.
2. **Map operators/sensors** through the existing Tier tables in `operator-mapping.md` — the mapping is import-path-independent.
3. **Map asset scheduling** per the resolution rule (above / `schedule-trigger-mapping.md`).
4. **Flag** `@asset`, `AssetWatcher`, asset aliases, DAG versioning, deadline alerts, `AssetOrTimeSchedule`, and any asset whose URI does not resolve to a UC table — in `MIGRATION_NOTES.md`, with the reason.
