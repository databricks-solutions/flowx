# Translation options: the inspect and modify commands

Read this from Step 9 of the `flowx-migrate` skill.

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
pipeline, skip `modify` — package falls back to the un-stamped report.

The options the adapter raises:

| `option_id` | Allowed values | Default |
|---|---|---|
| `copy_activity_paradigm` | `notebook`, `sdp` | `notebook` |
| `non_databricks_task_compute` | `serverless`, `classic` | `serverless` |
| `use_lakeflow_connectors` | `existing`, `lakeflow_connect` | `existing` |
| `consolidate_motif:<motif_id>` | `keep`, `consolidate` | `keep` |

DatabricksNotebook and DatabricksSparkPython tasks always inherit the cluster binding derived from
their source linked service.

For each multi-activity motif the detector matches (rest_api_pagination,
incremental_load_watermark, metadata_driven_bulk_copy, ...) the adapter emits one
`consolidate_motif:<motif_id>` option. The user must explicitly opt in to `consolidate` 
for each detected pattern.
