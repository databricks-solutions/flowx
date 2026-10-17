#!/usr/bin/env bash
set -euo pipefail

log() { printf '==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'EOF'
Usage: setup-integration.sh [subcommand] [options]

Subcommands:
  setup        run all setup steps (default)
  verify       check setup without making changes
  cleanup      list leftover test runs in the resource group; use --delete RUN to remove one
  identity     create or reuse the Entra app, SP, and OIDC credential
  providers    register required Azure providers
  role         create the shared resource group, then create/update the custom role and assign it there
  environment  create/update the protected GitHub environment
  secrets      set GitHub environment secrets

Options:
  --github-repo OWNER/REPO  --subscription-id ID  --app-name NAME
  --env-name NAME           --role-name NAME       --reviewers USER,USER[,USER...]
  --resource-group NAME     --location REGION
  --delete RUN              exact run tag whose resources to delete (cleanup only)
  --dry-run                 print writes without performing them
  --yes                     skip confirmation
  --help

Environment variables:
  GITHUB_REPO, AZURE_SUBSCRIPTION_ID, APP_NAME, ENV_NAME, ROLE_NAME, REVIEWERS
  RESOURCE_GROUP, LOCATION
  RESOURCE_OWNER_EMAIL, CLAUDE_CODE_OAUTH_TOKEN, ANTHROPIC_API_KEY
  DATABRICKS_HOST, DATABRICKS_TOKEN
EOF
}

COMMAND=setup
case "${1:-}" in
  setup|verify|cleanup|identity|providers|role|environment|secrets) COMMAND=$1; shift ;;
esac
DRY_RUN=false YES=false SHOW_HELP=false DELETE_RUN=''
GITHUB_REPO=${GITHUB_REPO:-}
AZURE_SUBSCRIPTION_ID=${AZURE_SUBSCRIPTION_ID:-}
APP_NAME=${APP_NAME:-flowx-live-integration}
ENV_NAME=${ENV_NAME:-flowx-live-integration}
ROLE_NAME=${ROLE_NAME:-flowx-live-integration-deployer}
REVIEWERS=${REVIEWERS:-}
RESOURCE_GROUP=${RESOURCE_GROUP:-flowx-live-integration}
LOCATION=${LOCATION:-eastus2}
TEST_TAG=purpose=flowx-integration-test
REQUIRED_PROVIDERS='Microsoft.Resources Microsoft.DataFactory Microsoft.Storage Microsoft.KeyVault Microsoft.EventGrid'
# The actions Terraform in tests/integration/azure/terraform uses, granted only on RESOURCE_GROUP.
REQUIRED_ROLE_ACTIONS='[
  "Microsoft.Resources/subscriptions/resourceGroups/read",
  "Microsoft.Storage/storageAccounts/read",
  "Microsoft.Storage/storageAccounts/write",
  "Microsoft.Storage/storageAccounts/delete",
  "Microsoft.Storage/storageAccounts/blobServices/read",
  "Microsoft.Storage/storageAccounts/blobServices/containers/read",
  "Microsoft.Storage/storageAccounts/blobServices/containers/write",
  "Microsoft.Storage/storageAccounts/blobServices/containers/delete",
  "Microsoft.Storage/storageAccounts/fileServices/read",
  "Microsoft.Storage/storageAccounts/queueServices/read",
  "Microsoft.Storage/storageAccounts/tableServices/read",
  "Microsoft.KeyVault/vaults/read",
  "Microsoft.KeyVault/vaults/write",
  "Microsoft.KeyVault/vaults/delete",
  "Microsoft.DataFactory/factories/read",
  "Microsoft.DataFactory/factories/write",
  "Microsoft.DataFactory/factories/delete",
  "Microsoft.DataFactory/factories/linkedservices/read",
  "Microsoft.DataFactory/factories/linkedservices/write",
  "Microsoft.DataFactory/factories/linkedservices/delete",
  "Microsoft.DataFactory/factories/integrationruntimes/read",
  "Microsoft.DataFactory/factories/integrationruntimes/write",
  "Microsoft.DataFactory/factories/integrationruntimes/delete",
  "Microsoft.DataFactory/factories/datasets/read",
  "Microsoft.DataFactory/factories/datasets/write",
  "Microsoft.DataFactory/factories/datasets/delete",
  "Microsoft.DataFactory/factories/dataflows/read",
  "Microsoft.DataFactory/factories/dataflows/write",
  "Microsoft.DataFactory/factories/dataflows/delete",
  "Microsoft.DataFactory/factories/pipelines/read",
  "Microsoft.DataFactory/factories/pipelines/write",
  "Microsoft.DataFactory/factories/pipelines/delete",
  "Microsoft.DataFactory/factories/triggers/read",
  "Microsoft.DataFactory/factories/triggers/write",
  "Microsoft.DataFactory/factories/triggers/delete",
  "Microsoft.EventGrid/eventSubscriptions/read",
  "Microsoft.EventGrid/eventSubscriptions/write",
  "Microsoft.EventGrid/eventSubscriptions/delete"
]'
# Shared-key access is disabled in Terraform; these keep key and access-policy changes out even if Actions widen.
REQUIRED_ROLE_NOT_ACTIONS='[
  "Microsoft.Storage/storageAccounts/listKeys/action",
  "Microsoft.Storage/storageAccounts/listAccountSas/action",
  "Microsoft.Storage/storageAccounts/regeneratekey/action",
  "Microsoft.KeyVault/vaults/accessPolicies/write"
]'
while (($#)); do
  case "$1" in
    --github-repo) [[ $# -ge 2 ]] || die "$1 requires a value"; GITHUB_REPO=$2; shift 2 ;;
    --subscription-id) [[ $# -ge 2 ]] || die "$1 requires a value"; AZURE_SUBSCRIPTION_ID=$2; shift 2 ;;
    --app-name) [[ $# -ge 2 ]] || die "$1 requires a value"; APP_NAME=$2; shift 2 ;;
    --env-name) [[ $# -ge 2 ]] || die "$1 requires a value"; ENV_NAME=$2; shift 2 ;;
    --role-name) [[ $# -ge 2 ]] || die "$1 requires a value"; ROLE_NAME=$2; shift 2 ;;
    --reviewers) [[ $# -ge 2 ]] || die "$1 requires a value"; REVIEWERS=$2; shift 2 ;;
    --resource-group) [[ $# -ge 2 ]] || die "$1 requires a value"; RESOURCE_GROUP=$2; shift 2 ;;
    --location) [[ $# -ge 2 ]] || die "$1 requires a value"; LOCATION=$2; shift 2 ;;
    --delete) [[ $# -ge 2 ]] || die "$1 requires one exact run tag"; DELETE_RUN=$2; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --yes) YES=true; shift ;;
    --help|-h) SHOW_HELP=true; shift ;;
    *) die "unknown option or subcommand: $1" ;;
  esac
done
$SHOW_HELP && { usage; exit 0; }
[[ "$COMMAND" == cleanup || -z "$DELETE_RUN" ]] || die "--delete is valid only with cleanup"
[[ "$RESOURCE_GROUP" =~ ^[A-Za-z0-9._()-]{1,90}$ ]] || die "RESOURCE_GROUP must be a valid Azure resource group name"
[[ "$LOCATION" =~ ^[a-z0-9]+$ ]] || die "LOCATION must be an Azure region name such as eastus2"

TEMP_FILES=()
cleanup_temp_files() {
  local path
  for path in ${TEMP_FILES[@]+"${TEMP_FILES[@]}"}; do rm -f "$path"; done
}
trap cleanup_temp_files EXIT
new_temp_file() { NEW_TEMP_FILE=$(mktemp); TEMP_FILES+=("$NEW_TEMP_FILE"); }

check_prereqs() {
  local command_name
  for command_name in az gh jq; do command -v "$command_name" >/dev/null || die "missing prerequisite: $command_name"; done
  az account show >/dev/null 2>&1 || die "Azure CLI is not logged in; run: az login"
  gh auth status >/dev/null 2>&1 || die "GitHub CLI is not logged in; run: gh auth login"
  [[ -n "$GITHUB_REPO" ]] || GITHUB_REPO=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
  [[ -n "$AZURE_SUBSCRIPTION_ID" ]] || AZURE_SUBSCRIPTION_ID=$(az account show --query id -o tsv)
  SUBSCRIPTION_NAME=$(az account show --subscription "$AZURE_SUBSCRIPTION_ID" --query name -o tsv) ||
    die "cannot access the requested Azure subscription"
}

confirm_target() {
  log "Target Azure subscription: $SUBSCRIPTION_NAME"
  log "Target GitHub repository: $GITHUB_REPO"
  $YES && return
  [[ -t 0 ]] || die "confirmation required; rerun interactively or pass --yes"
  read -r -p "Continue? [y/N] " answer
  [[ "$answer" == y || "$answer" == Y ]] || die "cancelled"
}

run_write() {
  if $DRY_RUN; then printf 'DRY-RUN:'; printf ' %q' "$@"; printf '\n'; else "$@"; fi
}

json_value() {
  local message=$1 filter=$2
  shift 2
  jq -r "$@" "$filter" 2>/dev/null || die "$message"
}

lookup_identity() {
  local attempt attempts
  [[ -n "${app_id:-}" && -n "${sp_id:-}" ]] && return
  if [[ -z "${app_id:-}" ]]; then
    app_id=$(az ad app list --display-name "$APP_NAME" --query "[?displayName=='$APP_NAME'] | [0].appId" -o tsv) ||
      die "cannot query Entra applications"
  fi
  [[ -n "$app_id" ]] || { sp_id=''; return; }
  sp_id=${sp_id:-}
  [[ -n "$sp_id" ]] && return
  if $DRY_RUN; then attempts=1; else attempts=4; fi
  attempt=1
  while ((attempt <= attempts)); do
    sp_id=$(az ad sp list --filter "appId eq '$app_id'" --query '[0].id' -o tsv) ||
      die "cannot query the Entra service principal"
    [[ -n "$sp_id" ]] && return
    ((attempt == attempts)) || sleep 1
    attempt=$((attempt + 1))
  done
}

step_identity() {
  lookup_identity
  if [[ -z "$app_id" ]]; then
    if $DRY_RUN; then
      app_id='<client-id>'; sp_id='<service-principal-object-id>'
      run_write az ad app create --display-name "$APP_NAME"
      run_write az ad sp create --id "$app_id"
    else
      app_id=$(az ad app create --display-name "$APP_NAME" --query appId -o tsv) ||
        die "cannot create Entra app; ask an Entra administrator for permission"
    fi
  else log "Reusing Entra application $APP_NAME"; fi
  if [[ -z "$sp_id" ]]; then
    if $DRY_RUN; then sp_id='<service-principal-object-id>'; run_write az ad sp create --id "$app_id"; else
      sp_id=$(az ad sp create --id "$app_id" --query id -o tsv) ||
        die "cannot create service principal; ask an Entra administrator for permission"
    fi
  fi

  subject="repo:${GITHUB_REPO}:environment:${ENV_NAME}"
  if [[ "$app_id" == '<client-id>' ]]; then credential='null'; else
    credential=$(az ad app federated-credential list --id "$app_id" --query "[?subject=='$subject'] | [0]" -o json)
  fi
  credential_id=$(json_value "invalid federated-credential response from Azure" \
    'if type == "object" then .id // empty else empty end' <<< "$credential")
  credential_matches=$(json_value "invalid federated-credential response from Azure" \
    'type == "object" and .issuer == "https://token.actions.githubusercontent.com" and ((.audiences // []) == ["api://AzureADTokenExchange"])' \
    <<< "${credential:-null}")
  if [[ -z "$credential_id" || "$credential_matches" != true ]]; then
    credential_name=$(json_value "invalid federated-credential response from Azure" \
      'if type == "object" then .name // empty else empty end' <<< "$credential")
    credential_name=${credential_name:-${ENV_NAME}-github}
    new_temp_file; credential_file=$NEW_TEMP_FILE
    jq -n --arg name "$credential_name" --arg subject "$subject" \
      '{name:$name,issuer:"https://token.actions.githubusercontent.com",subject:$subject,audiences:["api://AzureADTokenExchange"]}' > "$credential_file"
    if [[ -z "$credential_id" ]]; then
      run_write az ad app federated-credential create --id "$app_id" --parameters "@$credential_file"
    else
      run_write az ad app federated-credential update --id "$app_id" \
        --federated-credential-id "$credential_id" --parameters "@$credential_file"
    fi
  else log "Federated credential already matches $subject"; fi
  tenant_id=$(az account show --subscription "$AZURE_SUBSCRIPTION_ID" --query tenantId -o tsv)
  printf '\nAzure identity summary (IDs are intentionally shown here only):\nAZURE_CLIENT_ID=%s\nAZURE_TENANT_ID=%s\nAZURE_SUBSCRIPTION_ID=%s\n' \
    "$app_id" "$tenant_id" "$AZURE_SUBSCRIPTION_ID"
}

step_providers() {
  local provider state
  for provider in $REQUIRED_PROVIDERS; do
    state=$(az provider show --subscription "$AZURE_SUBSCRIPTION_ID" --namespace "$provider" \
      --query registrationState -o tsv 2>/dev/null || true)
    if [[ "$state" == Registered ]]; then log "$provider is registered"; else
      run_write az provider register --subscription "$AZURE_SUBSCRIPTION_ID" --namespace "$provider" --wait ||
        die "provider $provider did not register; verify provider-registration permission"
    fi
  done
}

step_role() {
  lookup_identity
  if [[ -z "$app_id" ]]; then
    if $DRY_RUN; then app_id='<client-id>'; sp_id='<service-principal-object-id>'; else
      die "Entra app $APP_NAME does not exist; run identity"
    fi
  elif [[ -z "$sp_id" ]]; then
    if $DRY_RUN; then sp_id='<service-principal-object-id>'; else die "service principal does not exist; run identity"; fi
  fi
  if az group show --subscription "$AZURE_SUBSCRIPTION_ID" --name "$RESOURCE_GROUP" >/dev/null 2>&1; then
    log "Reusing resource group $RESOURCE_GROUP"
  else
    run_write az group create --subscription "$AZURE_SUBSCRIPTION_ID" --name "$RESOURCE_GROUP" \
      --location "$LOCATION" --tags "$TEST_TAG" || die "cannot create resource group $RESOURCE_GROUP"
  fi
  scope="/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}"
  subscription_scope="/subscriptions/${AZURE_SUBSCRIPTION_ID}"
  new_temp_file; role_file=$NEW_TEMP_FILE
  jq -n --arg name "$ROLE_NAME" --arg scope "$scope" \
    --argjson actions "$REQUIRED_ROLE_ACTIONS" --argjson not_actions "$REQUIRED_ROLE_NOT_ACTIONS" \
    '{Name:$name,IsCustom:true,Description:"Deploy flowx live integration fixtures into one resource group",Actions:$actions,NotActions:$not_actions,AssignableScopes:[$scope]}' > "$role_file"
  role_id=$(az role definition list --name "$ROLE_NAME" --scope "$scope" --query '[0].id' -o tsv)
  if [[ -n "$role_id" && "$sp_id" != '<service-principal-object-id>' ]]; then
    # Older setups assigned this role to the whole subscription; Azure won't narrow the role while that exists.
    legacy_assignment=$(legacy_assignment_ids)
    # shellcheck disable=SC2086
    [[ -z "$legacy_assignment" ]] || run_write az role assignment delete --ids $legacy_assignment ||
      die "cannot remove the old subscription-wide role assignment"
  fi
  if [[ -z "$role_id" ]]; then
    if $DRY_RUN; then role_id='<role-definition-id>'; run_write az role definition create --role-definition "$role_file"; else
      role_id=$(az role definition create --role-definition "$role_file" --query id -o tsv) ||
        die "role creation failed; User Access Administrator or Owner is required"
    fi
  else
    run_write az role definition update --role-definition "$role_file" ||
      die "role update failed; User Access Administrator or Owner is required"
  fi
  if [[ "$role_id" == '<role-definition-id>' || "$sp_id" == '<service-principal-object-id>' ]]; then assignment=''; else
    assignment=$(az role assignment list --assignee-object-id "$sp_id" --scope "$scope" --role "$role_id" \
      --query '[0].id' -o tsv)
  fi
  [[ -n "$assignment" ]] || run_write az role assignment create --assignee-object-id "$sp_id" \
    --assignee-principal-type ServicePrincipal --role "$role_id" --scope "$scope" ||
    die "role assignment failed; User Access Administrator or Owner is required"
}

step_environment() {
  local reviewer reviewer_id reviewer_json
  reviewer_json='[]'
  IFS=',' read -r -a reviewer_names <<< "$REVIEWERS"
  for reviewer in ${reviewer_names[@]+"${reviewer_names[@]}"}; do
    reviewer=${reviewer//[[:space:]]/}; [[ -n "$reviewer" ]] || continue
    reviewer_id=$(gh api "users/$reviewer" --jq .id) || die "GitHub reviewer not found: $reviewer"
    # shellcheck disable=SC2016
    reviewer_json=$(json_value "could not build GitHub reviewer configuration" \
      '. + [{type:"User",id:$id}]' --argjson id "$reviewer_id" <<< "$reviewer_json")
  done
  new_temp_file; environment_file=$NEW_TEMP_FILE
  jq -n --argjson reviewers "$reviewer_json" \
    '{wait_timer:0,prevent_self_review:true,reviewers:$reviewers,deployment_branch_policy:null}' > "$environment_file"
  run_write gh api --method PUT -H 'Accept: application/vnd.github+json' \
    "repos/${GITHUB_REPO}/environments/${ENV_NAME}" --input "$environment_file" ||
    die "cannot configure environment; repository administrator permission is required"
}

set_secret() {
  local name=$1 value=$2
  if $DRY_RUN; then printf 'DRY-RUN: gh secret set %q --repo %q --env %q <stdin>\n' "$name" "$GITHUB_REPO" "$ENV_NAME"
  else printf '%s' "$value" | gh secret set "$name" --repo "$GITHUB_REPO" --env "$ENV_NAME"; fi
}
prompt_optional() {
  local variable_name=$1 prompt=$2
  [[ -n "${!variable_name:-}" || ! -t 0 ]] && return
  read -r -s -p "$prompt (optional; Enter to skip): " "${variable_name?}"; printf '\n'
}
step_secrets() {
  lookup_identity
  if [[ -z "$app_id" ]]; then if $DRY_RUN; then app_id='<client-id>'; else die "Entra app not found; run identity"; fi; fi
  if [[ -z "${RESOURCE_OWNER_EMAIL:-}" ]]; then
    [[ -t 0 ]] || die "RESOURCE_OWNER_EMAIL is required"
    read -r -s -p "Cloud resource owner email: " RESOURCE_OWNER_EMAIL; printf '\n'
  fi
  validate_owner_email
  tenant_id=$(az account show --subscription "$AZURE_SUBSCRIPTION_ID" --query tenantId -o tsv)
  set_secret AZURE_CLIENT_ID "$app_id"
  set_secret AZURE_TENANT_ID "$tenant_id"
  set_secret AZURE_SUBSCRIPTION_ID "$AZURE_SUBSCRIPTION_ID"
  set_secret RESOURCE_OWNER_EMAIL "$RESOURCE_OWNER_EMAIL"
  set_secret AZURE_RESOURCE_GROUP "$RESOURCE_GROUP"
  prompt_optional CLAUDE_CODE_OAUTH_TOKEN 'Claude Code OAuth token'
  prompt_optional ANTHROPIC_API_KEY 'Anthropic API key'
  prompt_optional DATABRICKS_HOST 'Databricks host'
  prompt_optional DATABRICKS_TOKEN 'Databricks token'
  for secret_name in CLAUDE_CODE_OAUTH_TOKEN ANTHROPIC_API_KEY DATABRICKS_HOST DATABRICKS_TOKEN; do
    if [[ -n "${!secret_name:-}" ]]; then set_secret "$secret_name" "${!secret_name}"; else log "Skipping optional secret $secret_name"; fi
  done
}

verify_setup() {
  local failures=0 provider state role_json role_matches role_id assignment environment reviewer_count self_review secret_names secret_name
  lookup_identity
  [[ -n "$app_id" ]] || die "Entra app $APP_NAME was not found"
  [[ -n "$sp_id" ]] || die "service principal for $APP_NAME was not found"
  subject="repo:${GITHUB_REPO}:environment:${ENV_NAME}"
  credential=$(az ad app federated-credential list --id "$app_id" \
    --query "[?subject=='$subject' && issuer=='https://token.actions.githubusercontent.com' && audiences[0]=='api://AzureADTokenExchange'].id | [0]" -o tsv)
  [[ -n "$credential" ]] || { echo "MISSING/MISMATCH: GitHub OIDC federated credential" >&2; failures=$((failures + 1)); }
  for provider in $REQUIRED_PROVIDERS; do
    state=$(az provider show --subscription "$AZURE_SUBSCRIPTION_ID" --namespace "$provider" --query registrationState -o tsv 2>/dev/null || true)
    [[ "$state" == Registered ]] || { echo "MISSING/MISMATCH: provider $provider is not registered" >&2; failures=$((failures + 1)); }
  done
  az group show --subscription "$AZURE_SUBSCRIPTION_ID" --name "$RESOURCE_GROUP" >/dev/null 2>&1 ||
    { echo "MISSING: resource group $RESOURCE_GROUP" >&2; failures=$((failures + 1)); }
  scope="/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}"
  subscription_scope="/subscriptions/${AZURE_SUBSCRIPTION_ID}"
  role_json=$(az role definition list --name "$ROLE_NAME" --scope "$scope" -o json)
  # shellcheck disable=SC2016
  role_matches=$(json_value "invalid role-definition response from Azure" \
    'if type == "array" then
       (length == 1) and
       (((.[0].permissions // []) | map(.actions // []) | add // [] | sort) == ($expected | sort)) and
       (((.[0].permissions // []) | map(.notActions // []) | add // [] | sort) == ($expected_not | sort)) and
       ((.[0].assignableScopes // []) == [$scope])
     else false end' --arg scope "$scope" --argjson expected "$REQUIRED_ROLE_ACTIONS" \
    --argjson expected_not "$REQUIRED_ROLE_NOT_ACTIONS" <<< "$role_json")
  [[ "$role_matches" == true ]] || { echo "MISSING/MISMATCH: custom role definition $ROLE_NAME" >&2; failures=$((failures + 1)); }
  role_id=$(json_value "invalid role-definition response from Azure" \
    'if type == "array" then .[0].id // empty else empty end' <<< "$role_json")
  if [[ -n "$role_id" ]]; then
    assignment=$(az role assignment list --assignee-object-id "$sp_id" --scope "$scope" --role "$role_id" --query '[0].id' -o tsv)
  else assignment=''; fi
  [[ -n "$assignment" ]] || { echo "MISSING/MISMATCH: role assignment $ROLE_NAME" >&2; failures=$((failures + 1)); }
  if [[ -n "$role_id" && -n "$(legacy_assignment_ids)" ]]; then
    echo "MISMATCH: role $ROLE_NAME is still assigned to the whole subscription; rerun role" >&2; failures=$((failures + 1))
  fi
  environment=$(gh api "repos/${GITHUB_REPO}/environments/${ENV_NAME}") || { echo "MISSING: GitHub environment $ENV_NAME" >&2; failures=$((failures + 1)); environment='{}'; }
  reviewer_count=$(json_value "invalid environment response from GitHub" \
    'if type == "object" then [.protection_rules[]? | select(.type=="required_reviewers") | .reviewers[]?] | unique_by(.reviewer.id) | length else 0 end' \
    <<< "$environment")
  ((reviewer_count >= 2)) || { echo 'MISSING/MISMATCH: environment needs at least 2 required reviewers' >&2; failures=$((failures + 1)); }
  self_review=$(json_value "invalid environment response from GitHub" \
    'if type == "object" then [.protection_rules[]? | select(.type=="required_reviewers") | .prevent_self_review] == [true] else false end' \
    <<< "$environment")
  [[ "$self_review" == true ]] || { echo 'MISMATCH: environment must prevent self-review' >&2; failures=$((failures + 1)); }
  secret_names=$(gh secret list --repo "$GITHUB_REPO" --env "$ENV_NAME" --json name --jq '.[].name')
  for secret_name in AZURE_CLIENT_ID AZURE_TENANT_ID AZURE_SUBSCRIPTION_ID AZURE_RESOURCE_GROUP RESOURCE_OWNER_EMAIL; do
    grep -Fxq "$secret_name" <<< "$secret_names" || { echo "MISSING: environment secret $secret_name" >&2; failures=$((failures + 1)); }
  done
  ((failures == 0)) || die "$failures verification check(s) failed"
  log 'Verification passed (secret names only; values were never read).'
}

legacy_assignment_ids() {
  az role assignment list --assignee-object-id "$sp_id" --scope "$subscription_scope" --role "$role_id" \
    --query "[?scope=='$subscription_scope'].id" -o tsv
}

# Lists the run tags of test resources left in the shared resource group, and deletes one run's resources.
# The resource group itself is never deleted.
cleanup_orphans() {
  local runs resource_ids
  runs=$(az resource list --subscription "$AZURE_SUBSCRIPTION_ID" --resource-group "$RESOURCE_GROUP" --tag "$TEST_TAG" \
    --query '[].tags.run' -o tsv | sort -u)
  if [[ -z "$runs" ]]; then log "No test resources left in $RESOURCE_GROUP"; else printf '%s\n' "$runs"; fi
  [[ -n "$DELETE_RUN" ]] || return 0
  grep -Fxq -- "$DELETE_RUN" <<< "$runs" || die "refusing to delete: $DELETE_RUN is not an exact run tag in $RESOURCE_GROUP"
  confirm_target
  if ! $YES; then
    read -r -p "Type the exact run tag to delete its resources: " confirmation
    [[ "$confirmation" == "$DELETE_RUN" ]] || die "run tag did not match; cancelled"
  fi
  resource_ids=$(az resource list --subscription "$AZURE_SUBSCRIPTION_ID" --resource-group "$RESOURCE_GROUP" \
    --tag "run=$DELETE_RUN" --query "[?tags.purpose=='flowx-integration-test'].id" -o tsv)
  # shellcheck disable=SC2086
  run_write az resource delete --ids $resource_ids
}

validate_owner_email() {
  [[ "${RESOURCE_OWNER_EMAIL:-}" =~ ^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$ ]] ||
    die "RESOURCE_OWNER_EMAIL must be a non-empty email-like value"
}

# GitHub logins are case-insensitive, so "Alice,alice" is one reviewer.
count_reviewers() {
  tr ',' '\n' <<< "$REVIEWERS" | sed 's/[[:space:]]//g' | grep . | sort -fu | wc -l
}

if [[ "$COMMAND" == setup || "$COMMAND" == environment ]] && (($(count_reviewers) < 2)); then
  die "REVIEWERS (or --reviewers) must list at least 2 GitHub users so nobody approves their own run"
fi
if [[ "$COMMAND" == setup ]]; then
  if [[ -n "${RESOURCE_OWNER_EMAIL:-}" ]]; then validate_owner_email
  elif [[ ! -t 0 ]]; then die "RESOURCE_OWNER_EMAIL is required before setup can begin"
  fi
fi
check_prereqs
case "$COMMAND" in
  verify) verify_setup ;;
  cleanup) cleanup_orphans ;;
  setup)
    confirm_target
    step_identity; step_providers; step_role; step_environment; step_secrets
    ;;
  identity) confirm_target; step_identity ;;
  providers) confirm_target; step_providers ;;
  role) confirm_target; step_role ;;
  environment) confirm_target; step_environment ;;
  secrets) confirm_target; step_secrets ;;
esac
