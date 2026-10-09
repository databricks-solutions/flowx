# Profile — Azure Data Factory

Source guide for `flowx profile --source adf`. See the parent `SKILL.md` for install, credentials,
the command, and the output layout.

## Required Azure roles

Grant these to whichever identity runs the scan (your `az login` user or the service principal),
on the subscription or the resource group being profiled:

| Role | Unlocks | If missing |
|---|---|---|
| **Reader** | Listing subscriptions, factories, and integration runtimes | No factories found, so the report is empty |
| **Data Factory Contributor** | Querying pipeline runs and activity runs (needed for the estimate) | A warning per factory ("need Data Factory Contributor"); that factory has no estimate |
| **Cost Management Reader** (or Billing Reader) | Azure-billed actuals | A warning, and the report's "Cost Management Access Issues" section lists the subscription; estimates still appear |

A missing role never stops the run. The phase degrades and says which role to add.

Example for a service principal scoped to one resource group:

```bash
az ad sp create-for-rbac --name flowx-profiler-sp --role Reader \
  --scopes /subscriptions/<SUB>/resourceGroups/<RG>
az role assignment create --assignee <appId> --role "Data Factory Contributor" \
  --scope /subscriptions/<SUB>/resourceGroups/<RG>
az role assignment create --assignee <appId> --role "Cost Management Reader" \
  --scope /subscriptions/<SUB>
```

## How the estimate is computed

For each activity run in the window:

- **Orchestration**: the integration runtime's per-1000-runs rate, per activity run, plus one trigger
  orchestration per pipeline run at the Azure IR rate.
- **Execution**: the run's duration, billed in whole minutes with a 1-minute minimum, times:
  - Copy on Azure IR: DIUs used × the data-movement rate per DIU-hour. On a self-hosted IR: the
    hourly data-movement rate.
  - Pipeline activities (Lookup, ForEach, Wait, …) and unknown types: the pipeline-activity rate.
  - External activities (Databricks, stored procedure, Web, …): the external-activity rate.
  - Mapping Data Flow: 8 vCores × $0.274 per vCore-hour.

Rates come from the public Azure Retail Prices API for the first factory's region. If that call
fails, the built-in list rates are used, and the report's "Pricing Source" line says which applied.

## Known limitations (carried over from the original profiler, tracked as follow-ups)

- Self-hosted IR VM cost is not included: the estimate covers ADF's own meters only, not the VMs
  a self-hosted IR runs on.
- **Managed virtual network IRs: the keep-warm time isn't included (biggest known gap).** A managed-VNet
  IR keeps its compute warm for a time-to-live (TTL, e.g. 60 minutes) after each activity, and Azure bills
  that time. The estimate counts only the activity's own duration. On one live factory, Azure billed
  44.6 hours for 45 short notebook runs while the estimate counted 7.25 hours.
- A Copy run that doesn't report its DIUs is assumed to have used 4.
- Data Flow cost assumes a fixed 8 vCores, regardless of the cluster size actually used.
- Billed rows are attributed to a factory by substring match on the resource ID, so `etl` also
  claims `etl-prod`'s rows when both exist.
- Actuals count only the `Azure Data Factory v2` meter subcategory (and empty ones). A compute
  meter Azure introduces under a new name would be left out until it's added.
