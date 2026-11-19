---
name: flowx-profile
description: >
  Estimate what an Azure Data Factory estate costs today: a live scan of the factories' recent
  pipeline and activity runs, priced at Azure list rates and reconciled against Azure-billed
  actuals from Cost Management. Writes a cost report under metadata/tco/. Independent of the
  discover/convert/package phases.
  Use when the user asks what their ADF costs, for an ADF TCO or cost report, or to profile a data
  factory.
---

# Estimate the current ADF spend

A separate phase, ADF only. Profile scans Azure Data Factory live and estimates what it costs: every
pipeline run and activity run in the last N days (default 30) is priced at the Azure Retail Prices list
rates for the factory's region, then compared with what Azure billed (Cost Management). The result is the
source-side cost for a migration evaluation, written to `metadata/tco/`. It talks to Azure directly, does
not read `metadata/inventory.json`, and does not need `discover` to have run. `--source airflow` is
rejected with "profiling is not supported for the airflow source".

## Before you start

- **The Azure SDK is an optional extra.** If there is no venv, run the `flowx-setup` skill and answer yes
  to its live Azure features question (it runs `bootstrap.sh --source-deps azure`). For an existing venv:
  `"$PY" -m pip install 'azure-identity>=1.17'` (from a source checkout, `pip install -e '.[azure]'` is the
  same extra `flowx extract` uses). Without it the phase exits with code 2 and the same install hint.
- The roles the Azure identity needs are in [`sources/adf.md`](sources/adf.md).

## How to run

The common MCP and CLI rules are in [`../shared/running-flowx.md`](../shared/running-flowx.md).

- **CLI:** `"$PY" -m flowx.adapter profile --source adf --output-dir <output_dir> [--days 30] [--subscription-id <id>] [--resource-group <rg> [--factory-name <name>]]`
- **MCP:** `flowx(command="profile", parameters={"source": "adf", "output_dir": "...", "days": 30, ...})`
  with the same scope keys (`subscription_id`, `resource_group`, `factory_name`). The app needs the Azure
  extra (deploy it with `source_deps=azure` or `SOURCE_DEPS=azure`; see `flowx-setup`) and the service
  principal env vars from Step 1. On success the result carries the report as `report_md` and
  `report_csv`.

## Steps

### Step 1: Give it Azure credentials

The profiler uses `DefaultAzureCredential`, so the code is the same everywhere; only the setup differs.

| Where you run | How to authenticate |
|---|---|
| Claude Code or a local laptop | `az login` (a browser opens), or `az login --use-device-code`. A service principal through env vars also works. |
| Genie Code or a Databricks serverless notebook | A service principal from the customer's Azure tenant, kept in a Databricks secret scope and exported as env vars (recipe below). There is no browser or `az` CLI there, and the workspace's own identity belongs to the wrong tenant. |
| Headless or CI | The `AZURE_*` service principal env vars (in a notebook, read them from a secret scope, as `extract` does). |

The service principal env vars are `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` and `AZURE_TENANT_ID`.

**Secret scope recipe (Genie Code / serverless).** One-time, from a machine with the Databricks CLI:

```bash
databricks secrets create-scope flowx-profiler --profile <workspace-profile>
databricks secrets put-secret flowx-profiler azure-client-id     --string-value "<appId>"    --profile <workspace-profile>
databricks secrets put-secret flowx-profiler azure-client-secret --string-value "<password>" --profile <workspace-profile>
databricks secrets put-secret flowx-profiler azure-tenant-id     --string-value "<tenant>"   --profile <workspace-profile>
```

Then, in the notebook before running the phase:

```python
import os
os.environ["AZURE_CLIENT_ID"] = dbutils.secrets.get("flowx-profiler", "azure-client-id")
os.environ["AZURE_CLIENT_SECRET"] = dbutils.secrets.get("flowx-profiler", "azure-client-secret")
os.environ["AZURE_TENANT_ID"] = dbutils.secrets.get("flowx-profiler", "azure-tenant-id")
```

Serverless egress to `management.azure.com` and `prices.azure.com` must be allowed. If the
workspace blocks it, run the profile locally instead. An authentication failure exits with code 2
and names both setup paths above.

### Step 2: Choose the scope and run

- `--subscription-id` limits the scan to one subscription; `--resource-group` to one resource group.
- `--factory-name` narrows it to one factory and needs `--resource-group`; alone it exits with code 2.
- `--days` sets the window (default 30). ADF keeps pipeline runs for 45 days only: a longer window adds
  Cost Management data but no run history, which over-estimates per-run costs.

Over MCP, keep scans scoped: set `resource_group` and/or `factory_name` and keep `days` small. The scan
makes about one REST call per pipeline run, so a subscription-wide scan can take hours, longer than an MCP
call can wait; run those from a notebook with `python -m flowx.adapter profile ...` instead. A call that
runs past `FLOWX_MCP_TIMEOUT` (default 1800 s) returns `ok: false` with no report and a message in
`process.stderr` saying how to narrow the scan.

### Step 3: Read the report with the user

Explain the three points under "How to read it" below before quoting any number.

## Output

Written to `<output_dir>/metadata/tco/` and overwritten on every run, so there is always
exactly one current report:

| File | Contents |
|---|---|
| `metadata/tco/cost_comparison.csv` | One table with a level column: factory estimates and actuals, and pipeline estimates |
| `metadata/tco/cost_comparison.md` | The same, plus profiling context (window, region, pricing source), a per-subscription summary, any Cost Management access issues, and a Warnings section listing every scan warning (missing roles, price fallback, incomplete billing) when there are any |

How to read it:

- **Estimates** are list/retail rates before discounts, reserved capacity, or enterprise agreements.
- **Actual compute** counts only billed rows whose meter subcategory is `Azure Data Factory v2`
  (ADF's compute/orchestration meter) or `Azure Data Factory v2 Data Flow - <tier>` (Mapping Data
  Flow vCores). Infrastructure meters (Managed Airflow, Private Link and others) and wrangling data flows
  are left out so the comparison is like-for-like.
- **Pipeline rows** are estimates only. Azure bills per factory unless per-pipeline billing is
  enabled in Factory Settings.

- ADF Datasets are intentionally not scanned: they are definitions only and have no runtime meter, so
  they add no cost.
- Nothing outside `metadata/tco/` is touched. `discover`'s `metadata/profile_report.csv` is a
  different report (pipeline complexity) and is left alone.

## Reference

- [`sources/adf.md`](sources/adf.md): the Azure roles needed, what each one unlocks, and the known
  limits of the estimate.
- [`../shared/running-flowx.md`](../shared/running-flowx.md): running a phase.
