# Coverage results and the dashboard

An optional last step of package and migrate: record this run's migration coverage in a Unity Catalog
table, and publish a dashboard over it. Only with workspace auth (Genie Code, or a configured Databricks
CLI profile), and only when the user gave `results_table` (and `install_dashboard = yes` for the
dashboard) in the `inputs package` answers, which also offer `results_warehouse_id`.

## Contents

- [Record the results](#record-the-results)
- [Install the dashboard](#install-the-dashboard)

## Record the results

```bash
"$PY" -m flowx.adapter record-results --output-dir <output_dir> \
  --results-table <catalog.schema.table> [--warehouse-id <sql_warehouse_id>]
```

Over MCP: `flowx(command="record_results", parameters={"output_dir": "<dir>", "results_table": "catalog.schema.table", "warehouse_id": "<optional>"})`.

It reads `metadata/inventory.json` and `metadata/profile_report.csv`, creates the table if needed, and
inserts one row per pipeline: activity, dataset and linked service counts, collapsible pattern count,
complexity size, and the deterministic, agentic and unsupported breakdown. Every row carries the run's
`run_id` (a UUID), `run_date` (`CURRENT_TIMESTAMP()`) and `run_by` (`CURRENT_USER()`). Without
`--warehouse-id` it picks a warehouse, preferring a running serverless one. It prints the `run_id` and the
row count.

## Install the dashboard

```bash
"$PY" -m flowx.adapter install-dashboard --results-table <catalog.schema.table> \
  [--warehouse-id <sql_warehouse_id>] [--dashboard-name "<name>"] [--parent-path "/Workspace/Users/<you>"]
```

Over MCP: `flowx(command="install_dashboard", parameters={"results_table": "catalog.schema.table", "warehouse_id": "<optional>"})`.

It builds an AI/BI dashboard from a template (counters for pipelines, coverage and the activity totals, a
complexity bar chart, coverage over runs, and a per-pipeline table), publishes it and prints the URL.
Both commands stop with a clear message when workspace auth or a warehouse is missing.
