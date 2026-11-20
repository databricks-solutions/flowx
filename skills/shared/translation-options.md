# Translation options: the inspect and modify commands

Read this from Step 3 of the `flowx-convert` skill or Step 9 of the `flowx-migrate` skill: the `inspect` and `modify`
commands, their flags and the options the adapter raises.

Before bundle generation, run the adapter inspect CLI on the translation
report to surface any pipeline-modifier options the IR raises:

```bash
"$PY" -m flowx.adapter inspect <output_dir>/.work/translation_report.json
```

`inspect` returns the full option tree at once. Ask each option only when its `show_when` clauses
(`{option_id, in:[values]}`) are all satisfied by the answers collected so far (empty = always);
present its rationale, choices, and affected task keys. Then apply all collected answers in one
`modify` call as repeatable `--answer OPTION_ID=VALUE` flags:

```bash
"$PY" -m flowx.adapter modify \
  <output_dir>/.work/translation_report.json \
  --output-dir <output_dir> \
  --answer copy_activity_paradigm=sdp \
  --answer non_databricks_task_compute=serverless \
  [--lookup-csv "<csv-or-path>"]
```

`modify` writes the stamped report to `<output_dir>/.work/translation_report.stamped.json`
and the kept answers record to `<output_dir>/metadata/configuration.json`. The package phase
reads the stamped report from `.work/` automatically. When inspect emits no options for any
pipeline, skip `modify`: package falls back to the un-stamped report.

The options the adapter raises. `inspect` returns only those that apply to the report, and an option with
`show_when` follows an earlier answer:

| `option_id` | Allowed values | Default |
|---|---|---|
| `copy_activity_paradigm` | `notebook`, `sdp` | `notebook` |
| `non_databricks_task_compute` | `serverless`, `classic` | `serverless` |
| `use_lakeflow_connectors` | `existing`, `lakeflow_connect` | `existing` |
| `metadata_driven_consolidate` | `keep`, `consolidate` | `keep` |
| `metadata_driven_access` and `metadata_driven_size`, after `metadata_driven_consolidate=consolidate` | `yes`, `no`; `small`, `medium`, `large` | `no`; `large` |
| `metadata_driven_lookup_tool`, after `metadata_driven_access=yes` too | `have`, `none` | `none` |
| `consolidate_motif:<motif_id>` | `keep`, `consolidate` | `keep` |
| `notify_destination` | `keep`, `email`, `slack`, `teams`, `pagerduty`, `webhook` | `keep` |
| after a `notify_destination` other than `keep`: `notify_events` | `on_failure`, `on_success`, `both` | `both` |
| after a `notify_destination` other than `keep`: `notify_destination_name` and the destination's fields (`notify_email_recipients`, `notify_slack_url`, `notify_slack_channel_id`, `notify_slack_oauth_token`, `notify_teams_url`, `notify_pagerduty_integration_key`, `notify_webhook_url`, `notify_webhook_username`, `notify_webhook_password`) | free text | none |

There is no connector type option: with `use_lakeflow_connectors=lakeflow_connect`, modify picks the
connector each Copy can use (CDC for a table read, query-based for a query with a cursor column).

`inspect` raises the `metadata_driven_*` options only for the loops the code can consolidate, lists them
in `affected_task_keys`, and says in each option's rationale which sources apply
(`src/flowx/adapter/operations.py`).
`metadata_driven_consolidate=consolidate` takes effect when `metadata_driven_access=yes` and
`metadata_driven_size` is `small` or `medium`; it then emits one Lakeflow Connect pipeline. When the
answer is given for a loop that is not consolidated, it is not applied, and the task description says why. Modify refuses lookup rows that name the same source or destination table twice.

DatabricksNotebook and DatabricksSparkPython tasks always inherit the cluster binding derived from
their source linked service.

For each multi-activity motif the detector matches (rest_api_pagination,
incremental_load_watermark, metadata_driven_bulk_copy, ...) the adapter emits one
`consolidate_motif:<motif_id>` option. The user must explicitly opt in to `consolidate`
for each detected pattern.
