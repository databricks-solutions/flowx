---
name: flowx-profile
description: >
  Estimate what an Azure Data Factory estate costs today: a live scan of the factories' recent
  pipeline and activity runs, priced at Azure list rates and reconciled against Azure-billed
  actuals from Cost Management. Writes a cost report under metadata/tco/. Independent of the
  discover/convert/package phases.
triggers:
  - "profile ADF cost"
  - "ADF TCO"
  - "estimate ADF spend"
  - "what does our ADF cost"
  - "ADF cost report"
  - "profile data factory"
---

# Profile Current ADF Spend

Scan Azure Data Factory **live** and estimate its current spend: every pipeline run and activity
run in the last N days (default 90) is priced with the Azure Retail Prices list rates for the
factory's region, then compared with what Azure actually billed (Cost Management). The result is
the source-side "what does ADF cost today" number for a migration evaluation.

This phase stands alone: it talks to Azure directly, does **not** read `metadata/inventory.json`,
and does not require `discover` to have run first. Only the `adf` source has a profiler —
`--source airflow` is rejected with "profiling is not supported for the airflow source".

## Step 1 — Install the optional `profile` dependencies

The Azure SDK stack is an optional extra so the other phases never need it. Run the **`setup`**
skill first if there is no venv yet, then add the extra to it:

```bash
PY="$(cat <plugin_dir>/.migration-venv)"
"$PY" -m pip install 'azure-identity>=1.17'
```

(From a source checkout, `pip install -e '.[profile]'` is equivalent.) Without it, the phase exits
with code 2 and the same install hint.

## Step 2 — Give it Azure credentials

The profiler uses `DefaultAzureCredential`, so the code path is identical everywhere — only the
setup differs. Pick the row that matches where you are running:

| Where you run | How to authenticate |
|---|---|
| Claude Code / local laptop | `az login` (a browser opens), or `az login --use-device-code`. A service principal via env vars also works. |
| Genie Code / Databricks serverless notebook | A **service principal** from the customer's Azure tenant, stored in a Databricks **secret scope** and exported as env vars (recipe below). There is no browser or `az` CLI there, and the workspace's own identity belongs to the wrong tenant. |
| Headless / CI | Service principal env vars, or the `--tenant-id` / `--client-id` / `--client-secret` flags. |

Service principal env vars: `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`.

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

The roles the identity needs are in `sources/adf.md`.

## Step 3 — Run the phase

```bash
export PYTHONPATH="<plugin_dir>/src"
"$PY" -m flowx.adapter profile --source adf --output-dir <output_dir> \
  [--days 90] [--subscription-id <id>] [--resource-group <rg> [--factory-name <name>]]
```

Scope flags:

- `--subscription-id` limits the scan to one subscription.
- `--resource-group` limits it to one resource group.
- `--factory-name` narrows to one factory **only together with `--resource-group`**. On its own it
  is ignored and the whole subscription is scanned (behavior carried over from the original
  profiler).

The phase is not exposed as an MCP `flowx` command yet. In Genie Code, run it from a serverless
notebook as above.

## Output

Written to `<output_dir>/metadata/tco/` and **overwritten** on every run, so there is always
exactly one current report:

| File | Contents |
|---|---|
| `metadata/tco/cost_comparison.csv` | Factory-level estimate vs actual, then pipeline-level estimates |
| `metadata/tco/cost_comparison.md` | The same, plus profiling context (window, region, pricing source), a per-subscription summary, and any Cost Management access issues |

How to read it:

- **Estimates** are list/retail rates before discounts, reserved capacity, or enterprise agreements.
- **Actual compute** counts only billed rows whose meter subcategory is `Azure Data Factory v2`
  (ADF's compute/orchestration meter). Infrastructure meters (Managed Airflow, Private Link, …)
  are left out so the comparison is like-for-like.
- **Pipeline rows** are estimates only. Azure bills per factory unless per-pipeline billing is
  enabled in Factory Settings.

Nothing outside `metadata/tco/` is touched. `discover`'s `metadata/profile_report.csv` is a
different report (pipeline complexity) and is left alone.

## Reference

- `sources/adf.md` — required Azure roles, what each one unlocks, and known estimate limitations
