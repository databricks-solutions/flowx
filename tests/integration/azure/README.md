# Azure live integration suite

This suite deploys an ephemeral Azure Data Factory and supporting resources into one shared resource group, exports the factory
through the Azure REST API, and runs flowx against that live export. Expected-output cases under
`adf/cases/` exercise discover, convert, and package offline and against the live export. The workflow
validates translation and Databricks bundle structure; it never deploys a bundle.

## GitHub environment and secrets

Create a protected GitHub environment named `flowx-live-integration` and require approval. Same-repo
pull requests use `refs/pull/*/merge`, so do not add a deployment-branch restriction if PR runs should 
proceed; organizations that require trusted-branch-only environments should use the `workflow_dispatch` 
or post-merge `main` run instead. Add these environment secrets:

- `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, and `AZURE_SUBSCRIPTION_ID` for OIDC. Do not create a client secret.
- `AZURE_RESOURCE_GROUP`, the shared resource group that every run deploys into.
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

### What keeps a pull request from misusing Azure

The live job runs the pull request's own code, including its workflow file, scripts, and Terraform, with Azure
credentials. Two guards stand between a pull request and those credentials:

- **Approval by someone else.** The environment needs at least two required reviewers and prevents self-review,
  so a run always waits for approval from someone other than the person who triggered it. Approvers should read
  the pull request's changes to `.github/` and `tests/integration/azure/terraform/` before approving.
- **Checks before apply.** `apply-terraform.sh` runs these checks in order:
  - It refuses to start unless `.terraform.lock.hcl` matches its pinned SHA-256. The lock file lists only
    `hashicorp/azurerm` and `azure/azapi` at their pinned versions, and `init -lockfile=readonly` refuses any
    other provider.
  - It refuses any Terraform CLI configuration that could change where providers come from (`TF_CLI_CONFIG_FILE`,
    `TF_PLUGIN_CACHE_DIR`, or `~/.terraformrc`).
  - After `init`, it refuses `local-exec` or `remote-exec` provisioners (on any resource, including
    `null_resource` and `terraform_data`) and the `external` data source, because these run commands a plan can't
    show. It scans the root config and every module `init` recorded. In `.tf` files it refuses the words
    `provisioner` and `external` anywhere, even in comments, so layout can't hide them. It parses `.tf.json` files
    and refuses those keys however they're spaced or escaped.
  - It saves a plan, refuses it if any resource would be deleted or replaced, and applies that exact saved plan.

  When Dependabot updates the lock file, update `lock_sha256` in `apply-terraform.sh` in the same pull request.
  Teardown still runs a normal `terraform destroy`. These checks are lint, not a sandbox: a pull request that
  edits `apply-terraform.sh` can remove them, and approval is the guard for that.

The Azure role reaches only the shared resource group (see below), so even an approved run can't touch anything
else in the subscription.

The agentic step loads the `flowx:flowx-convert` ADF guide, reads the translation report and sanitized ADF
export, and writes one result per `PlaceholderActivity` under `agentic_results/`. Claude runs in `dontAsk`
mode with read/search tools and file edits permitted only in that directory, and has no Bash access. Its
`Read`, `Glob`, and `Grep` rules allow only the flowx output, the ADF export, the plugin copy, and
`agentic_results/`. `--restricted` keeps those tools inside the checkout and those directories. `/proc`, home
credential caches such as `~/.azure`, `.git/`, and the Terraform state are denied. Claude Code always lets these
tools read the checkout itself, which is the pull request's own code. The workflow also signs the
Azure CLI out and deletes `~/.azure` before Claude starts; Terraform teardown signs in through GitHub OIDC on its
own. Claude Code is installed with `npm ci --ignore-scripts` from `.github/claude-code/package-lock.json`. Notebook
results carry source inline rather than creating separate executable files. The workflow validates each result's merge
schema, detects changes outside `agentic_results/` (including ignored files), and then invokes flowx's strict
`--merge-agentic` command. Missing Claude credentials explicitly skip this step; configured credentials make
any Claude, validation, or merge failure fail the job after the remaining deterministic checks run.
The workflow loads a read-only plugin copy from the runner's temporary directory because Claude Code denies
all writes beneath a plugin directory, which would otherwise include `agentic_results/` in the checkout. CI
scripts are copied the same way so Claude can't change scripts that run after it. That copy only guards against
Claude: the scripts still come from the pull request.

Every tool is installed before the Azure sign-in, from a lock file with hashes or a pinned checksum: Python
packages with `uv sync --frozen`, Claude Code from its npm lock file, and Terraform and the Databricks CLI by
`install-pinned-tool.sh`, which checks each download against a SHA-256 pinned in the script. Each pin was copied
from that release's `SHA256SUMS` file, and keeping it in the repo means a changed release asset fails the check.
Terraform providers come from the committed `.terraform.lock.hcl` (`terraform init -lockfile=readonly`).

Live runs share one resource group, so the live job uses a single repository-wide concurrency group that never
cancels a run in progress. A new run waits until the current one has torn down.

## One-time Azure and GitHub setup

`make setup-integration` creates the Entra application and OIDC credential, registers providers, creates the shared
resource group, installs the custom role and assigns it on that resource group only, protects the GitHub environment,
and sets its secrets. It requires Bash,
`az`, `gh`, and `jq`, plus active Azure and GitHub CLI logins. The operator needs permission to create 
Entra applications, administer the repository environment, register providers, and create/assign roles 
(normally User Access Administrator or Owner for that last setup operation). The CI identity itself receives
only the custom deployer role, never Owner.

The setup registers `Microsoft.Resources`, `Microsoft.DataFactory`, `Microsoft.Storage`,
`Microsoft.KeyVault`, and `Microsoft.EventGrid`.

### The shared resource group

All test resources live in one resource group, `flowx-live-integration` by default (set `RESOURCE_GROUP` or
`--resource-group`, and `LOCATION` or `--location` for its region). The setup creates it once. Terraform reads it
with a data source and deploys each run's resources into it under run-unique names tagged
`purpose=flowx-integration-test` and `run=<prefix>`. Teardown deletes only that run's resources, never the group.

The custom role is assignable and assigned only on that resource group. It lists the exact actions
Terraform needs (`REQUIRED_ROLE_ACTIONS` in `.github/scripts/setup-integration.sh`): read the resource group;
read, write, and delete the storage account, its blob containers, the Key Vault, the data factory and its
child resources, and the Event Grid subscription the Blob Events trigger uses; and read the storage
account's blob, file, queue, and table service settings. It can't create or delete resource groups. Its
`NotActions` also block storage-key listing, account SAS, key regeneration, and Key Vault access-policy writes
(shared-key access is disabled in `main.tf`).

This role is narrower than before but isn't a full sandbox. A dedicated sandbox subscription is still
recommended. If a live run fails with an authorization error for an action not listed, add that exact action
to `REQUIRED_ROLE_ACTIONS` and rerun `role`.

Existing installations must rerun `make setup-integration ARGS="role"` (or the full
`make setup-integration ARGS="setup"`), then `ARGS="secrets"` to add `AZURE_RESOURCE_GROUP`, and
`ARGS="environment"` to turn on self-review prevention. The `role` step removes the old subscription-wide
assignment of the same role before narrowing it. Role-definition changes can take a few minutes to propagate, so
wait a few minutes after `make setup-integration ARGS="role"` before rerunning the workflow. Resource groups
left by older runs (`<prefix>-flowx-rg`) aren't managed by the new setup; delete them by hand.

Configuration comes from flags or these environment variables: `GITHUB_REPO`, `AZURE_SUBSCRIPTION_ID`, 
`APP_NAME`, `ENV_NAME`, `ROLE_NAME`, `RESOURCE_GROUP`, `LOCATION`, and the required comma-separated `REVIEWERS`,
which must name at least two GitHub users. `RESOURCE_OWNER_EMAIL`
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

Use the `cleanup` subcommand to list the run tags of test resources left in the shared resource group.
Deletion requires an exact run tag and explicit confirmation, deletes only that run's tagged resources, and never
deletes the resource group:

```bash
make setup-integration ARGS="cleanup"
make setup-integration ARGS="cleanup --delete exact-run-tag"
```

## Run locally

Offline verification requires Python 3.12 and `uv` but no Azure credentials:

```bash
bash scripts/bootstrap.sh
PYTHONPATH=src uv run pytest tests/integration/test_adf_cases.py -v
```

### Expected-output cases

Each `adf/cases/<case>/case.json` names one or more fixture pipelines. The test constructs a temporary
ADF source containing those pipelines, every shared dataset, linked service, and dataflow (integration
runtimes are left out because flowx doesn't load them), and only triggers that reference a selected pipeline. `expected/` records normalized inventory, per-pipeline
IR, gaps, and the complete packaged bundle tree.

Every case keeps its own complete `expected/` tree, even when that repeats another case's output, so a change to
one case never silently changes what another checks. `full-factory` runs every fixture pipeline together and
`motifs` runs every `motif-*` pipeline; add new fixture pipelines to the matching `case.json` too.

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
run-unique `TF_VAR_name_prefix`, and `TF_VAR_resource_group_name` for an existing resource group. Run Terraform
from `terraform/`. Always run `terraform destroy` afterward.

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
`RESOURCE_OWNER_EMAIL` environment secret. The workflow runs `terraform destroy` under `if: always()`, which
removes only this run's resources. Anything a failed teardown leaves behind shows up in
`make setup-integration ARGS="cleanup"`.

Fixtures contain no PII, credentials, tenant or subscription IDs, private endpoints, or usable connection 
strings. Values that would identify an environment use angle-bracket placeholders, dummy `example.invalid`
URLs, managed identity, parameters, or Key Vault references. Linked-service definitions reference a Key 
Vault secret named `integration-placeholder`; it intentionally does not exist because pipelines are never 
executed. The Key Vault contains no secret values. The workflow strips ARM `id` and `etag` fields before 
any export reaches flowx or Claude. Each run's resources have collision-resistant, run-unique names in the shared
resource group.

Accepted risk: the role includes `Microsoft.EventGrid/eventSubscriptions/write` on the shared resource group,
because the Blob Events trigger fixture needs it. An approved run could use it to send that group's storage events
to a webhook outside Azure. The events only describe test resources in that group, and approval is the guard.
