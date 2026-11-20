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

`inspect` returns every option that applies to the report, each with its `option_id`, `prompt`,
`rationale`, `choices`, `free_text`, `default`, `affected_task_keys` and `show_when`
(`src/flowx/adapter/operations.py`); walk that schema rather than a list kept here. An option with
`show_when` follows an earlier answer.

There is no connector type option: with `use_lakeflow_connectors=lakeflow_connect`, modify picks the
connector each Copy can use (CDC for a table read, query-based for a query with a cursor column).

`inspect` raises the `metadata_driven_*` options only for the loops the code can consolidate, lists them
in `affected_task_keys`, and says in each option's rationale which sources apply
(`src/flowx/adapter/operations.py`).
`metadata_driven_consolidate=consolidate` takes effect only for such a loop, when
`metadata_driven_access=yes` and `metadata_driven_size` is `small` or `medium`; it then emits one
Lakeflow Connect pipeline. Any other loop keeps its for-each port, and when the loop's source is the
reason, the task description says why. Modify refuses lookup rows that name the same source or destination table twice.

DatabricksNotebook and DatabricksSparkPython tasks always inherit the cluster binding derived from
their source linked service.

For each multi-activity motif the detector matches, the adapter emits one
`consolidate_motif:<motif_id>` option. The user must explicitly opt in to `consolidate`
for each detected pattern.
