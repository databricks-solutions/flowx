# Workspace files the bundle needs

Some reports point at files that live in the Databricks workspace: notebooks under `/Shared/`, Spark
Python files, Spark JAR libraries, or DBFS paths. Package downloads them into the bundle so it deploys on
its own. This file covers the check before package and the login it may need. The `flowx-package` and
`flowx-migrate` skills run it just before package.

## Contents

- [Check which files the bundle needs](#check-which-files-the-bundle-needs)
- [Log in when it asks](#log-in-when-it-asks)
- [What package does with them](#what-package-does-with-them)

## Check which files the bundle needs

```bash
"$PY" -m flowx.adapter workspace-paths <output_dir>/.work/translation_report.stamped.json \
  --source <source> --source-dir <source_dir>
```

Over MCP: `flowx(command="workspace_paths", parameters={"source": "<source>", "report_path": "...", "source_dir": "<optional>"})`.

Pass the report package will read: the stamped report above, or `.work/translation_report.json` when
modify didn't run. A path that doesn't exist returns no workspace files rather than an error.

`--source` is `adf` or `airflow`. It returns `paths` (the files the bundle will download; you may show
them to the user), `suggested_hosts` (the Databricks workspace hosts found in the source: ADF linked
services or Airflow DAGs) and `needs_auth`. When
`needs_auth` is false, there is nothing to do.

## Log in when it asks

When `needs_auth` is true:

1. Confirm the workspace host with the user. Default to the first `suggested_hosts` entry; with none,
   ask with no default.
2. Run `databricks auth login --host <host>` and wait for it. It writes a profile to
   `~/.databrickscfg`; if the user chose a profile name, add `--profile <name>` to the login.
3. Pass `--profile <name>` to package (the default profile name is `DEFAULT`).

On a Databricks runtime (serverless or a cluster) auth is set up from the notebook context: flowx sees
`DATABRICKS_RUNTIME_VERSION` and writes `~/.databrickscfg` from `dbruntime.databricks_repl_context`, so
skip the login and pass `--profile DEFAULT` or no profile.

## What package does with them

Downloads are on by default. Downloaded notebooks go under `src/notebooks/`, are bound to the default
`job_cluster` (they may rely on classic compute), and the resource YAML points at
`../src/notebooks/<file>.py`. `--no-download-workspace-files` keeps the original workspace paths.

If no CLI auth is found (`~/.databrickscfg` empty and no `DATABRICKS_CONFIG_PROFILE`, or
`DATABRICKS_HOST` with `DATABRICKS_TOKEN`), package lists the paths it was about to download and asks
whether to continue with placeholders. `n` stops with exit code 2 so the user can log in and run again;
`y` continues with placeholder notebooks at the original paths. A non-interactive session continues with
placeholders.
