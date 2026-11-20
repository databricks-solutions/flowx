# Running a flowx phase

Every flowx skill runs its phase one of two ways. Each skill's "How to run" section names its own command
and parameters; this file holds what is the same for all of them. Run the `flowx-setup` skill first if it
hasn't run.

## Contents

- [MCP path](#mcp-path)
- [CLI path](#cli-path)
- [The output folder](#the-output-folder)

## MCP path

The only path in Databricks Genie Code, and an option locally with a stdio registration.

- Call the single `flowx` tool, one `command` per step, with a `parameters` object. Run no `python3`,
  `$PY` or `bash` commands on this path; the CLI snippets in a skill are the local fallback only.
- `source` (`"adf"` or `"airflow"`) is required for discover and convert and for `inputs discover` and
  `inputs convert`. Call `inputs` for a phase just before that phase, not for every phase at the start.
- Each call returns a structured result (summaries, file trees); use it rather than reading files.
- The hosted server's `output_dir` is temporary and not reachable from the workspace. To keep a bundle,
  pass `output_volume_path` (`/Volumes/...`) or `output_workspace_path` (`/Workspace/...`); the server
  uploads it there and returns `bundle_uploaded`. Grant the `mcp-flowx` app's service principal read on
  the source and write on the target.
- The MCP tools don't deploy the generated bundle; that stays a user step, with the `databricks` CLI
  (web terminal, local CLI or CI/CD) or from the workspace as https://docs.databricks.com/aws/en/dev-tools/bundles/workspace describes (read the edition for the target workspace's cloud (the page's cloud selector, or learn.microsoft.com for Azure Databricks)).

## CLI path

Local, with the virtual environment `flowx-setup` created:

```bash
export PYTHONPATH="<plugin_dir>/src"
PY="$(cat <plugin_dir>/.migration-venv)"
"$PY" -m flowx.adapter <command> --source <source> ...
```

Read the interpreter path from the marker file `<plugin_dir>/.migration-venv`; never hardcode it. Use
`$PY` anywhere a command shows `python3`. If Python or pip is missing, `bootstrap.sh` prints what to
install; relay it and stop until the user has Python 3.12 or later and pip.

## The output folder

All phases share one `<output_dir>` (default `./flowx_output`). What each phase writes there is in
[`output-folder.md`](output-folder.md).
