# Lakeflow Connect (managed ingestion) as a migration target

Reference for converting Airflow **ingestion** tasks to Databricks **Lakeflow Connect** managed
ingestion pipelines, emitted as DABs `resources.pipelines` entries. Lakeflow Connect is the right
target for **recurring ingestion/replication from an external source into Delta** — not a generic
fallback for any operator a Lakeflow Jobs task type doesn't cover. Use the Source-aware classification
step in `references/operator-mapping.md` first; this file covers what to do once a task is a Connect
candidate.

## When Lakeflow Connect (vs a regular Jobs task)

A task is a Connect candidate only when **all** hold — otherwise map it to a Jobs task (notebook/SDK,
federation, Auto Loader) or flag it:

- The operation is **recurring ingestion / replication** (not a one-shot backfill, not a transform).
- A **connector exists** for the source. The list below is **illustrative, not exhaustive** — the SaaS
  connector set grows, and foreign-catalog ingestion covers **all Lakehouse Federation sources**.
  Classify from the **current Databricks docs / connector metadata**, not this list alone.
- **Connect can create and own the destination streaming table.** Ingestion into a table that already
  exists is not supported — an existing production target needs a new landing table + a downstream
  merge/cutover step, or a different strategy.
- Source **objects / columns / cursor / primary keys / deletion handling** are representable.
- No **intermediate file that is itself an external contract** (e.g. "land a CSV another team consumes").
- The required **UC connection + networking** are known.
- The connector's **release state is acceptable**; for a **Private Preview** connector, the workspace
  has **confirmed enrollment/entitlement** (not merely user acceptance).

Not ingestion → regular Jobs task. Files from cloud storage → **Auto Loader** (not Connect).
Unsupported source → notebook/SDK using the driver, and flag.

## Ingestion styles

1. **CDC / log-based** — database connectors reading the change log: **MySQL, PostgreSQL, SQL Server**.
2. **Query-based direct** — cursor/incremental over a direct connection (not log CDC): **Oracle,
   Teradata, SQL Server, MySQL, MariaDB, PostgreSQL**. (SQL Server / MySQL / PostgreSQL support both
   CDC and query-based; pick per source capability and customer preference.)
3. **Query-based from a UC foreign catalog** — ingest from a **Lakehouse Federation** source through a
   foreign catalog, no dedicated connector: **Snowflake, Redshift, Synapse, BigQuery**. This is how
   **recurring Snowflake→Delta** is done — there is no dedicated Snowflake managed connector.

## Connectors (verify current release state — status changes)

Name connectors by capability; **do not hardcode GA/Preview status or dates** — always tell the user to
verify the connector's current release state in the Databricks docs before relying on it.

- **SaaS**: Salesforce, Workday, ServiceNow, Google Analytics, HubSpot.
- **Files**: Google Drive, SharePoint.
- **Streaming**: Kafka (Lakeflow Connect managed Kafka connector), RabbitMQ — **continuous-only** (see
  Orchestration).
- **Databases**: per the ingestion styles above.

## DABs generation contract

Pick the architecture by source and emit the matching resources (schema/examples in
`references/dab-schema-reference.md`):

- **Combined ingestion** (SaaS, files, query-based DB, CDC via `connection_name`): ONE
  `resources/<source>_ingestion.pipeline.yml` with `ingestion_definition` (+ `connector_type` when the
  source supports both query-based and CDC). No gateway. The bundle schema's `ingestion_definition`
  description historically warned it "cannot be used with the `libraries`/`schema`/`target`/`catalog`
  settings," but current query-based-ingestion DAB examples do pair it with `catalog`/`target` — follow
  the field combination in the current Databricks docs / `databricks bundle schema` for your CLI version
  rather than treating it as a blanket ban.
- **Gateway CDC** (log-based DB via a gateway — **Private Preview / `doNotSuggest`**): a **separate**
  gateway pipeline (`gateway_definition`) + an ingestion pipeline joined by `ingestion_gateway_id`.
  Only with connector-specific verification + confirmed Private-Preview enrollment; never the default.
- **Foreign-catalog ingestion** (Snowflake/BigQuery/Redshift/Synapse): `ingest_from_uc_foreign_catalog:
  true` + `source_catalog/schema/table`. The foreign catalog is a `resources.catalogs` entry
  (`connection_name` **plus** source-specific `options`, e.g. `options.database`) and requires
  `bundle.engine: direct`. **Reference an existing foreign catalog by default; create only when the
  bundle should own it.**
- **UC connection**: a documented **manual prerequisite**, not a bundle resource — reference by name.

## Orchestration (mode-driven, per connector)

- **Triggered** ingestion pipeline → a `pipeline_task` at the original dependency position in the Jobs
  graph.
- **Continuous** pipeline (streaming connectors; any connector documented continuous-only) → run it
  standalone and have the downstream job depend on a **job-level `trigger.table_update`** on the
  destination table. `trigger.table_update` is job-level, not a task dependency — if continuous
  ingestion sits mid-DAG, the graph splits into (upstream job) → (continuous pipeline) → (downstream
  job); flag that upstream gating semantics change.
- **Confirm the connector's run mode; do not assume it** from the architecture. If unknown, flag.

## MIGRATION_NOTES.md checklist

Record for every Connect conversion:

- UC **connection name** + the out-of-band creation prerequisite, and the **auth method**.
- **Foreign-catalog** configuration (catalog name, `options`) when federated; whether the bundle
  creates or references it.
- **Source and destination** objects; **cursor / primary keys**; **deletion / SCD** behavior.
- **Networking** prerequisites (private link / firewall / gateway).
- The connector's **release state** (and Private-Preview enrollment if applicable).
- Whether the conversion is **exact** or a **rearchitecture** (e.g. new landing table + merge because
  the original target already exists, or a continuous-mode job-graph split).
