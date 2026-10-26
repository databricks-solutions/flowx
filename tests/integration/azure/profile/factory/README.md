# flowx profiler test factories

Two small ADF factories that exercise every cost bucket `flowx profile` prices. They are the stable
target for the profiler's golden capture (see `../README.md`).

| Factory | IR | Data Flow |
|---|---|---|
| `flowx-profiler-test-adf` | default Azure IR (AutoResolve) | yes |
| `flowx-profiler-test-adf-vnet` | `ManagedVnetIR` (managed VNet, time-to-live 15 min) | no |

Both live in resource group `flowx-profiler-test-rg` (West Europe) and share one storage account.

## Pipelines

| Pipeline | Cost bucket |
|---|---|
| `pl_copy` | data movement (Copy, DIU-hours) |
| `pl_lookup_wait` | pipeline activity (Lookup) + Wait |
| `pl_web_databricks` | external activity (Web activity → Databricks REST API, managed identity); Azure IR factory only |
| `pl_databricks_job` | external activity (Databricks Job activity → serverless job, managed identity); Azure IR factory only |
| `pl_web_public` | external activity (two Web activities → public no-auth ARM metadata endpoint); managed-VNet factory only |
| `pl_foreach` | orchestration (ForEach × 2 of Lookup + Web) |
| `pl_dataflow` | Mapping Data Flow (8 General vCores); Azure IR factory only |

No triggers are defined: runs are started by hand so the golden capture window is controlled.

## Deploy

Run these in bash (zsh does not word-split `$COMMON`). Storage account: `flowxprofilertest`.

```bash
RG=flowx-profiler-test-rg
# The shared subscription these were built in deletes any resource group without both Owner and
# RemoveAfter (YYYY-MM-DD) tags, nightly around 00:00 UTC (it took the first copy on 2026-10-17).
az group create -n $RG -l westeurope --tags Owner=<you>@databricks.com RemoveAfter=<YYYY-MM-DD>
az storage account create -g $RG -n <storage> -l westeurope --sku Standard_LRS --kind StorageV2
az storage container create --account-name <storage> -n profiler --auth-mode login
az storage blob upload --account-name <storage> -c profiler -n input/sample.csv -f sample.csv --auth-mode login

COMMON="storageAccountName=<storage> databricksWorkspaceUrl=<workspace host> databricksWorkspaceResourceId=<workspace resource id> databricksJobId=<job id>"
az deployment group create -g $RG -f adf-arm-template.json -p factoryName=flowx-profiler-test-adf $COMMON
az deployment group create -g $RG -f adf-arm-template.json -p factoryName=flowx-profiler-test-adf-vnet managedVnet=true includeDataFlow=false $COMMON
```

After deploying:

- Approve the `pe-blob` managed private endpoint on the storage account (vnet factory only):
  `az network private-endpoint-connection list --id <storage resource id>` then `... approve --id <connection id>`.
- Add each factory's managed identity to the Databricks workspace as a service principal (by its
  application ID: `az ad sp show --id <factory principalId> --query appId`), and grant it
  `CAN_MANAGE_RUN` on the serverless job. This needs workspace admin, not Azure RBAC: deployers with
  plain `Contributor` on the subscription can't create role assignments, which is also why
  `ls_blob` uses the storage account key (read at deploy time with `listKeys`) instead of the
  factory's identity.

## Run

```bash
for p in pl_copy pl_lookup_wait pl_web_databricks pl_databricks_job pl_foreach pl_dataflow; do
  az datafactory pipeline create-run -g $RG --factory-name flowx-profiler-test-adf --name $p
done
```

Cost Management needs about 24–48 h to show the billed amounts.

## Databricks workspace

The Azure IR factory calls a Databricks workspace in the same Entra tenant as the factory's
subscription, so both Databricks activities authenticate with the factory's managed identity and no
secret is stored anywhere.

If the workspace has an IP access list, it must allow the factory's outbound addresses:

- **Azure IR:** calls come from the region's `DataFactory.<Region>` service tag (for West Europe,
  `40.74.26.0/23`, one entry). On shared internal Databricks workspaces, a custom entry like
  this is removed automatically by the workspace cleanup automation (it happened to this factory's entry
  on 2026-10-17, about 15 minutes after it was added), unless an exception is granted.
- **Managed-VNet IR:** the outbound address isn't fixed and isn't in any published range (several
  different addresses were seen within a day), so it can't be allowlisted. With `managedVnet=true` the
  template replaces the two Databricks pipelines with `pl_web_public`, and `pl_foreach`'s Web activity
  calls the same public endpoint. Same cost bucket, IR and time-to-live; no Databricks involved.

The golden capture of 2026-10-19 includes the Azure IR factory's Databricks runs, which failed from
2026-10-17 18:00 UTC on after the list entry was removed (failed runs are billed and costed like any
other). The plan is to switch the Azure IR factory to `pl_web_public` as well, so neither factory
depends on a workspace's IP access list.

### Fallback: a workspace in another tenant (e.g. a sandbox workspace)

If that workspace goes away, a workspace in another tenant can't be reached with the factory's
managed identity (a workspace only accepts Entra identities from its own tenant), and a sandbox often gives no
Azure rights on its subscription, so the factories can't move next to it. Instead:

- Create a short-lived Databricks personal access token (e.g. 30 days) in the target workspace and
  store it in a Key Vault in `flowx-profiler-test-rg`; grant each factory's identity
  `Key Vault Secrets User` on the vault.
- Switch `ls_databricks` to token auth: `"accessToken": {"type": "AzureKeyVaultSecret", "store":
  {"referenceName": "ls_keyvault", ...}, "secretName": "databricks-pat"}`, with an `AzureKeyVault`
  linked service `ls_keyvault`.
- The Web activity can't use a Key Vault reference, so add a first Web activity that GETs
  `https://<vault>.vault.azure.net/secrets/databricks-pat?api-version=7.4` with MSI auth
  (`resource: https://vault.azure.net`, `secureOutput: true`), and pass
  `Bearer @{activity('GetToken').output.value}` as the `Authorization` header of the Databricks call.
- The profiler prices both Web activities as external activities, so the cost buckets covered are
  unchanged.
