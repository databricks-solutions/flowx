# Azure live integration suite

This suite deploys an ephemeral Azure Data Factory and supporting resources, exports the factory
through the Azure REST API, and runs flowx against that live export. Expected-output cases under
`adf/cases/` exercise discover, convert, and package offline and against the live export. The workflow
validates translation and Databricks bundle structure; it never deploys a bundle.

## GitHub environment and secrets

Create a protected GitHub environment named `flowx-live-integration` and require approval. Same-repo
pull requests use `refs/pull/*/merge`, so do not add a deployment-branch restriction if PR runs should 
proceed; organizations that require trusted-branch-only environments should use the `workflow_dispatch` 
or post-merge `main` run instead. Add these environment secrets:

- `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, and `AZURE_SUBSCRIPTION_ID` for OIDC. Do not create a client secret.
- `RESOURCE_OWNER_EMAIL` for the required `owner` tag on live Azure resources.
- Either `CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY` for headless Claude Code. When neither is present, 
  deterministic conversion and bundle checks still run and the workflow reports that agentic resolution was 
  skipped. When either secret is configured, Claude must produce schema-valid merge-result JSON for every
  placeholder task in the translation report and the strict flowx merge must succeed; a Claude failure, timeout, invalid result, or
  unmatched result makes the job fail after deterministic validation completes.
- Optionally, `DATABRICKS_HOST` and `DATABRICKS_TOKEN` to run `databricks bundle validate`. They are not needed
  for the local structural validation. Validate-only runs supply placeholders for required variables; in
  particular, cross-bundle job IDs are set to `0` because sibling jobs are not deployed by this suite.

The workflow never prints secret values. Fork pull requests cannot reach the protected environment 
because both the `not-a-fork` job and the live job dependency reject them.

The agentic step loads the `flowx:flowx-convert` ADF guide, reads the translation report and sanitized ADF
export, and writes one result per `PlaceholderActivity` under `agentic_results/`. Claude runs in `dontAsk`
mode with read/search tools and file edits permitted only in that directory, and has no Bash access. Notebook
results carry source inline rather than creating separate executable files. The workflow validates each result's merge
schema, detects changes outside `agentic_results/` (including ignored files), and then invokes flowx's strict
`--merge-agentic` command. Missing Claude credentials explicitly skip this step; configured credentials make
any Claude, validation, or merge failure fail the job after the remaining deterministic checks run.
The workflow loads a read-only plugin copy from the runner's temporary directory because Claude Code denies
all writes beneath a plugin directory, which would otherwise include `agentic_results/` in the checkout.

## One-time Azure and GitHub setup

`make setup-integration` creates the Entra application and OIDC credential, registers providers, installs and assigns
the least-privilege custom role, protects the GitHub environment, and sets its secrets. It requires Bash,
`az`, `gh`, and `jq`, plus active Azure and GitHub CLI logins. The operator needs permission to create 
Entra applications, administer the repository environment, register providers, and create/assign roles 
(normally User Access Administrator or Owner for that last setup operation). The CI identity itself receives
only the custom deployer role, never Owner.

The setup registers `Microsoft.Resources`, `Microsoft.DataFactory`, `Microsoft.Storage`,
`Microsoft.KeyVault`, and `Microsoft.EventGrid`. Its custom role contains exactly these actions:

- `Microsoft.Resources/subscriptions/read`
- `Microsoft.Resources/subscriptions/resourceGroups/*`
- `Microsoft.DataFactory/factories/*`
- `Microsoft.Storage/storageAccounts/*`
- `Microsoft.KeyVault/vaults/*`
- `Microsoft.EventGrid/eventSubscriptions/read`
- `Microsoft.EventGrid/eventSubscriptions/write`
- `Microsoft.EventGrid/eventSubscriptions/delete`

The Event Grid permission covers the linked subscription created and removed with the stopped Blob Events
trigger; it grants no other Event Grid resource operations.

Existing installations must rerun `make setup-integration ARGS="providers"` and `make setup-integration ARGS="role"`
(or the full `make setup-integration ARGS="setup"`)
to register Event Grid and update the existing custom role. Role-definition changes can take a few minutes to
propagate, so wait a few minutes after `make setup-integration ARGS="role"` before rerunning the workflow.

Configuration comes from flags or these environment variables: `GITHUB_REPO`, `AZURE_SUBSCRIPTION_ID`, 
`APP_NAME`, `ENV_NAME`, `ROLE_NAME`, and the required comma-separated `REVIEWERS`. `RESOURCE_OWNER_EMAIL`
is also required when setting environment secrets. Defaults are derived
from the current CLI context or use `flowx-live-integration`. Claude and optional Databricks secret values 
may be exported as `CLAUDE_CODE_OAUTH_TOKEN`, `ANTHROPIC_API_KEY`, `DATABRICKS_HOST`, and `DATABRICKS_TOKEN`; 
otherwise the secrets step offers hidden interactive prompts.

```bash
export REVIEWERS='github-user-one,github-user-two'
make setup-integration ARGS="setup --dry-run"
make setup-integration ARGS="setup"
make setup-integration ARGS="verify"
```

Use `identity`, `providers`, `role`, `environment`, or `secrets` to rerun one step, and `--yes` for 
non-interactive automation. Running `make setup-integration` without `ARGS` defaults to `setup`. The OIDC credential
uses the protected-environment subject `repo:<owner>/<repo>:environment:flowx-live-integration`; no client secret
is created. Terraform provider auto-registration remains disabled.

Use the `cleanup` subcommand to inspect tagged cleanup candidates. Deletion requires both an exact resource-group
name and explicit confirmation; it never performs wildcard deletion:

```bash
make setup-integration ARGS="cleanup"
make setup-integration ARGS="cleanup --delete exact-resource-group-name"
```

## Run locally

Offline verification requires Python 3.12 and `uv` but no Azure credentials:

```bash
bash scripts/bootstrap.sh
PYTHONPATH=src uv run pytest tests/integration/test_adf_cases.py -v
```

### Expected-output cases

Each `adf/cases/<case>/case.json` names one or more fixture pipelines. The test constructs a temporary
ADF source containing those pipelines, every shared dataset/linked service/dataflow/integration runtime,
and only triggers that reference a selected pipeline. `expected/` records normalized inventory, per-pipeline
IR, gaps, and the complete packaged bundle tree.

To add a case, create its `case.json`, then regenerate expected artifacts offline:

```bash
PYTHONPATH=src uv run pytest tests/integration/test_adf_cases.py --update-expected -v
git diff -- tests/integration/azure/adf/cases
```

Always review the generated diff. Expected files come only from checked-in fixtures; update mode rejects a
live source directory. Live comparison reverses Terraform substitutions without printing their values and
runs flowx on the raw live definitions. It accepts only these empty defaults added by ADF: top-level
`annotations`, top-level `parameters` and `variables`, and activity `dependsOn` or `userProperties` arrays.
These values carry no behavior when empty; every other added or changed key fails with key paths only.
Pipeline `lastPublishTime` is also ignored only when it is an ISO-8601-like string: ADF sets this read-only
timestamp on GET responses and flowx does not read it. The same ignored fields are removed when comparing raw
JSON preserved in metadata, gaps, and placeholder notebooks.

Known issue: the Blob Events trigger currently produces `file_arrival.url: <STORAGE_ACCOUNT_ID>`, an ARM
resource ID rather than a storage URL. The expected output deliberately records today's behavior pending a
separate flowx fix.

To validate or deploy the Azure harness locally, authenticate using an OIDC-capable shell or another temporary
Azure CLI credential and set `TF_VAR_subscription_id`, `TF_VAR_tenant_id`, `TF_VAR_client_id`, and a lowercase
run-unique `TF_VAR_name_prefix`. Run Terraform from `terraform/`. Always run `terraform destroy` afterward.

Troubleshooting: if the first apply cannot read storage data-plane properties, set 
`features { storage { data_plane_available = false } }` or grant the deployer the required storage data-plane
read actions.

## Coverage matrix

| Area                     | Coverage                                                                                                                                                                                                  | Strategy or purpose                                          |
|--------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| Deterministic activities | Copy, DatabricksNotebook, DatabricksSparkJar, DatabricksSparkPython, ForEach, IfCondition, SetVariable, Switch, Lookup, WebActivity, Delete, ExecutePipeline, DatabricksJob, Wait, Filter, AppendVariable | All 16 registered deterministic types; 36 activity instances |
| Agentic activities       | ExecuteDataFlow, Until, SqlServerStoredProcedure, AzureFunction, WebHook, Custom, ExecuteSSISPackage, AzureMLExecutePipeline, GetMetadata, Validation, Fail, Script                                       | All 12 registered agentic types; 15 activity instances       |
| Complex orchestration    | Exhaustive activity pipeline, nested `ExecutePipeline`, nested control flow, and mixed deterministic/agentic dependencies                                                                                 | Exercises recursive inventory and cross-pipeline references  |
| Motifs                   | REST API pagination, incremental watermark load, metadata-driven bulk copy, activity-and-notify                                                                                                           | Dedicated pipeline for each production detector              |
| Formats                  | CSV, Parquet, JSON, Avro, ORC, Delta                                                                                                                                                                      | File and table dataset parsing                               |
| Stores                   | ADLS Gen2, Blob Storage, Azure SQL, MySQL, PostgreSQL, Databricks Delta                                                                                                                                   | Key Vault references or parameters only                      |
| Triggers                 | Schedule, TumblingWindow, BlobEvents                                                                                                                                                                      | All three supported trigger kinds                            |

The checked-in corpus currently classifies 51 activity instances: 36 deterministic, 15 agentic, and 0 unsupported.

Note: a real TumblingWindow export uses singular `properties.pipeline`. The current flowx loader reads only
`properties.pipelines`, so it loads the trigger but does not yet attach its pipeline reference. The fixture 
intentionally preserves the valid ADF shape; this is a known loader gap.

## Teardown and security

Every taggable base Azure resource is tagged `purpose=flowx-integration-test`, `ephemeral=true`,
`managedBy=terraform`, with its run prefix, and with `owner` set from the protected
`RESOURCE_OWNER_EMAIL` environment secret. The workflow runs `terraform destroy` under `if: always()` and
then deletes the exact
Terraform-output resource group as a safety net. It never performs a broad or wildcard deletion.

Fixtures contain no PII, credentials, tenant or subscription IDs, private endpoints, or usable connection 
strings. Values that would identify an environment use angle-bracket placeholders, dummy `example.invalid`
URLs, managed identity, parameters, or Key Vault references. Linked-service definitions reference a Key 
Vault secret named `integration-placeholder`; it intentionally does not exist because pipelines are never 
executed. The Key Vault contains no secret values. The workflow strips ARM `id` and `etag` fields before 
any export reaches flowx or Claude. Resources are isolated in a collision-resistant, run-scoped resource group.
