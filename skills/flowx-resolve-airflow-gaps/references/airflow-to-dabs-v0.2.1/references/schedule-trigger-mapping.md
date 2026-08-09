# Airflow Schedule and Trigger Mapping Reference

Maps Airflow scheduling mechanisms (cron expressions, presets, sensors) to Databricks Asset Bundles schedule and trigger configurations.

---

## Cron Expression Conversion

Airflow uses **5-field Unix cron** (minute, hour, day-of-month, month, day-of-week).
DABs uses **6-field Quartz cron** (second, minute, hour, day-of-month, month, day-of-week).

### Key Differences

| Feature | Airflow (Unix cron) | DABs (Quartz cron) |
|---|---|---|
| Fields | 5: `MIN HOUR DOM MON DOW` | 6-7: `SEC MIN HOUR DOM MON DOW [YEAR]` |
| Seconds | Not supported | First field, usually `0` |
| Day-of-week | `0-7` (Sun=0 or 7) or `SUN-SAT` | `1-7` (Sun=1) or `SUN-SAT` |
| Mutual exclusion | Both DOM and DOW can be `*` | Use `?` for one when the other is set |
| Timezone | `start_date` timezone or `schedule_interval` | `timezone_id` field (IANA format) |

### Conversion Rule

Prepend `0` for seconds. Replace `*` in day-of-week with `?` when day-of-month is specified (and vice versa).

```
Airflow:  MIN HOUR DOM MON DOW
DABs:     0   MIN  HOUR DOM MON DOW
```

If both DOM and DOW are `*` in Airflow, set DOW to `?` in Quartz:
`* * * * *` -> `0 * * * * ?`

If DOW is numeric, shift by +1 for Quartz and normalize Sunday:
- Airflow `0` or `7` (Sunday) -> Quartz `1`
- Airflow `1-6` (Mon-Sat) -> Quartz `2-7`

Prefer named days (`MON`..`SUN`) to avoid off-by-one conversion bugs.

---

## Airflow Preset to Quartz Cron

| Airflow Preset | Airflow Cron | Quartz Cron | Description |
|---|---|---|---|
| `@once` | N/A | *(no schedule, manual trigger)* | Run once. Remove schedule, trigger manually. |
| `@continuous` | N/A | `continuous.pause_status: UNPAUSED` | Continuous execution mode. |
| `@hourly` | `0 * * * *` | `0 0 * * * ?` | Top of every hour |
| `@daily` / `@midnight` | `0 0 * * *` | `0 0 0 * * ?` | Midnight daily |
| `@weekly` | `0 0 * * 0` | `0 0 0 ? * 1` | Midnight Sunday |
| `@monthly` | `0 0 1 * *` | `0 0 0 1 * ?` | Midnight first day of month |
| `@yearly` / `@annually` | `0 0 1 1 *` | `0 0 0 1 1 ?` | Midnight Jan 1 |
| `None` | N/A | *(no schedule)* | Manual trigger only |

---

## Common Cron Conversions

| Description | Airflow | DABs Quartz |
|---|---|---|
| Every 15 minutes | `*/15 * * * *` | `0 */15 * * * ?` |
| Every 6 hours | `0 */6 * * *` | `0 0 */6 * * ?` |
| 8 AM daily | `0 8 * * *` | `0 0 8 * * ?` |
| 8 AM weekdays | `0 8 * * 1-5` | `0 0 8 ? * 2-6` |
| 6 PM last day of month | `0 18 28-31 * *` | `0 0 18 L * ?` |
| Every Monday 9 AM | `0 9 * * 1` | `0 0 9 ? * 2` |

> **Day-of-week note:** Airflow `1` = Monday, Quartz `2` = Monday. Also map Airflow `7` (Sunday) to Quartz `1`. Using named days avoids numeric ambiguity.

---

## DABs Schedule YAML

```yaml
schedule:
  quartz_cron_expression: "0 0 8 * * ?"
  timezone_id: "America/New_York"
  pause_status: UNPAUSED                   # PAUSED or UNPAUSED
```

### Timezone Mapping

Airflow timezone comes from `default_timezone` in `airflow.cfg` or the DAG's `start_date` timezone. Map to IANA timezone ID for DABs.

| Common Airflow Value | DABs `timezone_id` |
|---|---|
| `UTC` | `UTC` |
| `US/Eastern` | `America/New_York` |
| `US/Pacific` | `America/Los_Angeles` |
| `US/Central` | `America/Chicago` |
| `Europe/London` | `Europe/London` |
| `Asia/Tokyo` | `Asia/Tokyo` |

---

## Sensor to Trigger Mapping

Airflow sensors that block execution until a condition is met map to DABs job-level triggers or are absorbed into task dependencies.

### File-Based Sensors -> `trigger.file_arrival`

| Airflow Sensor | Trigger Config |
|---|---|
| `S3KeySensor` | `trigger.file_arrival` with `url: s3://bucket/prefix/` |
| `GCSObjectExistenceSensor` | `trigger.file_arrival` with `url: gs://bucket/prefix/` |
| `FileSensor` | `trigger.file_arrival` with `url:` pointing to UC volume |

**Airflow:**

```python
wait_for_data = S3KeySensor(
    task_id="wait_for_data",
    bucket_name="landing-zone",
    bucket_key="data/{{ ds }}/*.parquet",
    poke_interval=60,
    timeout=3600,
)
```

**DABs:**

```yaml
trigger:
  file_arrival:
    url: s3://landing-zone/data/
    min_time_between_triggers_seconds: 60
    wait_after_last_change_seconds: 60
```

**Key differences:**
- Airflow sensors are task-level (block one task). DABs triggers are job-level (start the whole job).
- Move the sensor to the job trigger. Downstream tasks that depended on the sensor now just run as the first task(s) in the job.

---

### Table-Based Sensors -> `trigger.table_update`

| Airflow Sensor | Trigger Config |
|---|---|
| `ExternalTaskSensor` (if upstream writes to a table) | `trigger.table_update` monitoring the output table |
| `SqlSensor` (if checking table freshness/existence) | `trigger.table_update` with `condition: ANY_UPDATED` |

**Airflow:**

```python
wait_for_upstream = ExternalTaskSensor(
    task_id="wait_for_upstream",
    external_dag_id="upstream_etl",
    external_task_id="write_silver_table",
    timeout=3600,
)
```

**DABs:**

```yaml
trigger:
  table_update:
    condition: ANY_UPDATED
    table_names:
      - "main.silver.transactions"
    min_time_between_triggers_seconds: 300
    wait_after_last_change_seconds: 60
```

> **Continuous Lakeflow Connect pipelines** (streaming connectors like Kafka/RabbitMQ, or any connector
> documented continuous-only) are **not** driven by a `pipeline_task` hop. Run the pipeline standalone
> and have the downstream job depend on a job-level `trigger.table_update` on the pipeline's destination
> table — the same mechanism above. A **triggered** ingestion pipeline uses a `pipeline_task` instead.
> See `references/lakeflow-connect.md`.

---

### Dependency-Based Sensors -> `depends_on` or `run_job_task`

| Airflow Sensor | DABs Equivalent |
|---|---|
| `ExternalTaskSensor` (same job/bundle) | `depends_on` with `task_key` |
| `ExternalTaskSensor` (cross-job) | Upstream job triggers downstream via `run_job_task` |

---

### Time-Based Sensors -> Schedule Adjustment

| Airflow Sensor | DABs Equivalent |
|---|---|
| `TimeSensor` / `TimeDeltaSensor` at DAG start | Adjust `schedule.quartz_cron_expression` to the target time |
| `TimeSensor` / `TimeDeltaSensor` mid-pipeline | Flag in MIGRATION_NOTES.md -- no direct equivalent |
| `DayOfWeekSensor` | Adjust cron to run only on specified days |

---

## Airflow Timetable, Dataset, and Asset Scheduling

Airflow DAGs can use non-cron schedule APIs that are not 1:1 with a Quartz cron. In **Airflow 3**,
"Datasets" are renamed **Assets** (`airflow.sdk.Asset`); the scheduling mappings below apply to
both `Dataset(...)` (Airflow 2) and `Asset(...)` (Airflow 3). See `references/airflow3-migration.md`.

| Airflow Scheduling Pattern | DABs Mapping |
|---|---|
| `schedule=[Dataset(...)]` / `schedule=[Asset(...)]` (single) | `trigger.table_update` on the upstream Unity Catalog table — **only** when the asset resolves to a UC table (see resolution rule below); otherwise flag. |
| `schedule=[asset_a, asset_b]` (list — Airflow: ALL updated) | `trigger.table_update` on both tables with `condition: ALL_UPDATED`. |
| `schedule=(asset_a \| asset_b)` (OR) | `trigger.table_update` with `condition: ANY_UPDATED`. |
| `schedule=(asset_a & asset_b)` (AND) | `trigger.table_update` with `condition: ALL_UPDATED`. |
| `AssetOrTimeSchedule(timetable=..., assets=...)` (time **and** asset) | **Flag** — a single Lakeflow job takes either a `schedule` **or** a trigger, not both as a clean 1:1. Choose the dominant intent (or split), and record the tradeoff in `MIGRATION_NOTES.md`. |
| Custom `Timetable` subclass | Flag for manual review and map to `schedule` or `trigger` based on business intent. |
| `@continuous` | Use job-level `continuous` (not periodic trigger). |

**Asset → UC-table resolution rule.** An Airflow `Asset` URI is an arbitrary string (it may be an
S3 path, a file path, a custom scheme, or a bare name) — there is **no** official Airflow/Databricks
convention that encodes a Unity Catalog table in it. So the default is to **flag**, and an asset
maps to `trigger.table_update` only when the target table is stated unambiguously by one of:

1. **Explicit metadata (recommended):** `Asset("orders-raw", extra={"databricks_table": "<catalog>.<schema>.<table>"})`.
2. **A user-supplied URI→table mapping** given in the conversion prompt.
3. **A skill-local scheme with exact parsing:** the URI is `x-databricks-table:<catalog>.<schema>.<table>`.
   This is a convention of *this skill only* — document it as such in `MIGRATION_NOTES.md`; it is not
   an Airflow or Databricks standard.

Any other asset URI (`s3://…`, `file://…`, a bare string, or an ambiguous scheme) → **flag for
manual review** in `MIGRATION_NOTES.md`. Never guess a table from the URI.

If a dataset/asset or timetable schedule cannot be deterministically mapped, add a required action
item in `MIGRATION_NOTES.md`.

---

## Airflow `default_args` Mapping

Common `default_args` fields and their DABs equivalents:

| Airflow `default_args` | DABs Equivalent |
|---|---|
| `owner` | *(no direct mapping -- do not auto-map identity; document intended run identity in MIGRATION_NOTES.md)* |
| `retries` | `max_retries` on task |
| `retry_delay` | `min_retry_interval_millis` on task |
| `email` | `email_notifications.on_failure` |
| `email_on_failure` | `email_notifications.on_failure` |
| `email_on_retry` | *(no direct equivalent, note in migration notes)* |
| `depends_on_past` | *(no direct equivalent, note in migration notes)* |
| `start_date` | *(not needed -- DABs jobs start when deployed)* |
| `end_date` | *(no direct equivalent -- pause the schedule manually)* |
| `execution_timeout` | `timeout_seconds` on task |
| `sla` | *(no direct equivalent -- use monitoring/alerts)* |
| `catchup` | `catchup=True` → use native [Databricks backfill](https://docs.databricks.com/aws/en/jobs/backfill-jobs) to replay history (requires `{{ ds }}` mapped to a job parameter — see the execution-date section); `catchup=False` (the Airflow 3 default) → no backfill. Note the expectation in MIGRATION_NOTES.md. |

---

## `trigger_rule` → `run_if` Mapping

Lakeflow `run_if` takes exactly six values: `ALL_SUCCESS`, `ALL_DONE`, `NONE_FAILED`,
`AT_LEAST_ONE_SUCCESS`, `ALL_FAILED`, `AT_LEAST_ONE_FAILED`. Airflow has more trigger rules than
that, so some map exactly, some are approximations that must be recorded, and the rest have no
faithful mapping and must be flagged.

**Exact:**

| Airflow `trigger_rule` | `run_if` |
|---|---|
| `all_success` (default) | `ALL_SUCCESS` (omit — it is the default) |
| `all_done` | `ALL_DONE` |
| `all_failed` | `ALL_FAILED` |
| `one_success` | `AT_LEAST_ONE_SUCCESS` |
| `one_failed` | `AT_LEAST_ONE_FAILED` |

**Approximate — map, but record the behavioral delta in `MIGRATION_NOTES.md`:**

| Airflow `trigger_rule` | `run_if` | Delta to record |
|---|---|---|
| `none_failed` | `NONE_FAILED` | Confirm skip semantics for the specific fan-in. |
| `none_failed_min_one_success` | `NONE_FAILED` | Drops the "at least one succeeded" clause: the task **also runs when every upstream skipped**. `AT_LEAST_ONE_SUCCESS` is the wrong substitute — it allows the task to run while another upstream has failed. |
| `none_failed_or_skipped` (deprecated alias) | `NONE_FAILED` | Same as `none_failed`. |

**Flag — no faithful mapping; use a `condition_task` or surface for manual review:**

`always` / `dummy` (Airflow runs regardless of upstream state, *including upstreams that never ran*;
`ALL_DONE` still waits for upstreams to reach a terminal state), `none_skipped`, `all_skipped`,
`one_done`, and the setup/teardown-specific rules. `all_skipped` mapped to the default inverts to its
opposite condition — it would run only when upstreams *succeeded*.

> An unrecognized or dynamically-computed `trigger_rule` must be flagged, never defaulted. A default
> of `ALL_SUCCESS` is indistinguishable from a correctly-mapped `all_success`, which hides the loss.

---

## Execution date (`{{ ds }}` / `execution_date`) semantics and backfill

Airflow's `{{ ds }}`/`execution_date` has **no single Databricks equivalent** — its correct mapping
depends on what the DAG *means* by it, and the wrong choice silently processes the wrong data (most
dangerously under backfill). Decide the semantics per DAG before mapping, and **ask the user when it
is ambiguous** — do not default silently.

**Step 1 — classify the intent of each `{{ ds }}` use:**

- **Wall-clock / "today's data"** — the task just wants the date the run happens on, with no
  historical-replay meaning. Rare in scheduled ETL. → default the parameter to
  `{{job.start_time.iso_date}}` (actual execution start).
- **Logical interval / partition key** — `{{ ds }}` identifies *which* data window is being processed
  (a `WHERE date = '{{ ds }}'` filter, a partition path `.../{{ ds }}/...`, an incremental cursor).
  This is the common case and the one that must survive backfill. → default the parameter to
  **`{{job.trigger.time.iso_date}}`** (the scheduled trigger time), not `start_time` — `start_time`
  drifts with queue delay and retries.

  > Airflow 2 vs Databricks convention: an Airflow 2 scheduled run's `logical_date` is the **start of
  > the data interval** (typically one period *behind* the run's fire time), while Databricks
  > `{{job.trigger.time}}` and Airflow 3's `CronTriggerTimetable` `logical_date` are the **fire time**.
  > If the DAG's `{{ ds }}` relied on the Airflow 2 "process the previous interval" convention,
  > confirm the intended window with the user and offset in code if needed; flag when unsure.

> `{{job.trigger.time}}` is defined for **cron/scheduled** runs. If the DAG's schedule became an
> **event trigger** (`file_arrival`, `table_update`, `continuous`) — e.g. a cron+sensor collapsed to
> file arrival — there is no scheduled logical date: `{{job.trigger.time}}` is not a reliable source.
> Derive the partition from the event itself (e.g. parse the date from the arriving file path /
> `{{job.trigger.file_arrival.location}}`), fall back to `{{job.start_time.iso_date}}` as an
> approximation, or use backfill for exact historical windows — and flag the change.

**Step 2 — always make it a real job parameter (backfill resilience).** Whichever default you choose,
`{{ ds }}` must map to a named **job parameter** (e.g. `run_date`), never a hardcoded date or an
inline `{{job.start_time...}}` buried in a task. Native [Databricks backfill](https://docs.databricks.com/aws/en/jobs/backfill-jobs)
replays a job over a historical range by **overriding an existing date/time job parameter** per
replayed window with `{{backfill.iso_date}}` (the start of that window's range). What makes a job
backfillable is that such a parameter **exists** to be overridden — a job that hardcodes the date or
computes it inline from `{{job.start_time...}}` gives backfill nothing to override, so history cannot
be replayed for the right window. (During a backfill the override wins regardless of the parameter's
default; the default only governs **normal** runs — which is why a logical/partition date should
default to `{{job.trigger.time.iso_date}}`, not `{{job.start_time.iso_date}}`, whose drift on delayed
or retried runs would process the wrong date.) So: expose `run_date`, default it to the Step-1 choice,
and record in `MIGRATION_NOTES.md` that a backfill should override `run_date` with `{{backfill.iso_date}}`. (Backfills always run the whole job; **pipeline tasks are not parameterized**
and run as-is, so a pipeline-only workload can't carry a backfill date — flag it.)

## Jinja Template Variable Conversion

Airflow Jinja variables used in operators/SQL need conversion to DABs dynamic value references.

> Dynamic value references belong in PARAMETER values (`base_parameters`, `sql_task.parameters`, task `parameters` lists) — never inline in SQL files. SQL files use `:name` parameter markers whose values are supplied via `sql_task.parameters` (see `references/dab-schema-reference.md`).

| Airflow Jinja | DABs Equivalent | Notes |
|---|---|---|
| `{{ ds }}` | `{{job.parameters.run_date}}` | Define `run_date` as a job parameter (so backfill can override it). Default `{{job.trigger.time.iso_date}}` for a logical/partition date on a scheduled job (correct on normal runs); `{{job.start_time.iso_date}}` only for wall-clock "today" semantics or an event-triggered job. See the semantics + backfill section above. |
| `{{ ds_nodash }}` | *(compute in notebook)* | No direct equivalent. Derive from `run_date` in code. |
| `{{ execution_date }}` | `{{job.parameters.run_date}}` | Same as `ds`; classify wall-clock vs logical per the section above. |
| `{{ prev_ds }}` | *(compute in notebook)* | No direct equivalent. Calculate in code. |
| `{{ next_ds }}` | *(compute in notebook)* | No direct equivalent. Calculate in code. |
| `{{ params.x }}` | `{{job.parameters.x}}` | Define as job parameter |
| `{{ var.value.x }}` | `${var.x}` | Define as bundle variable |
| `{{ task_instance.xcom_pull(...) }}` | `{{tasks.<key>.values.<name>}}` | Use `dbutils.jobs.taskValues.set/get` |
| `{{ run_id }}` | `{{job.run_id}}` | Direct mapping |
| `{{ dag.dag_id }}` | `${bundle.name}` or hardcode | Bundle name is typically the DAG equivalent |
| `{{ macros.ds_add(ds, -1) }}` | *(compute in notebook)* | No macro support. Calculate in Python/SQL. |
