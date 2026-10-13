#!/usr/bin/env bash
set -euo pipefail

log() { printf '==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'EOF'
Usage: setup.sh [subcommand] [options]

Subcommands:
  setup        run all setup steps (default)
  verify       check setup without making changes
  cleanup      list tagged resource groups; use --delete NAME to remove one
  identity     create or reuse the Entra app, SP, and OIDC credential
  providers    register required Azure providers
  role         create/update and assign the least-privilege role
  environment  create/update the protected GitHub environment
  secrets      set GitHub environment secrets

Options:
  --github-repo OWNER/REPO  --subscription-id ID  --app-name NAME
  --env-name NAME           --role-name NAME       --reviewers USER[,USER...]
  --delete NAME             exact resource group (cleanup only)
  --dry-run                 print writes without performing them
  --yes                     skip confirmation
  --help

Environment variables:
  GITHUB_REPO, AZURE_SUBSCRIPTION_ID, APP_NAME, ENV_NAME, ROLE_NAME, REVIEWERS
  RESOURCE_OWNER_EMAIL, CLAUDE_CODE_OAUTH_TOKEN, ANTHROPIC_API_KEY
  DATABRICKS_HOST, DATABRICKS_TOKEN
EOF
}

COMMAND=setup
case "${1:-}" in
  setup|verify|cleanup|identity|providers|role|environment|secrets) COMMAND=$1; shift ;;
esac
DRY_RUN=false YES=false SHOW_HELP=false DELETE_GROUP=''
GITHUB_REPO=${GITHUB_REPO:-}
AZURE_SUBSCRIPTION_ID=${AZURE_SUBSCRIPTION_ID:-}
APP_NAME=${APP_NAME:-flowx-live-integration}
ENV_NAME=${ENV_NAME:-flowx-live-integration}
ROLE_NAME=${ROLE_NAME:-flowx-live-integration-deployer}
REVIEWERS=${REVIEWERS:-}
while (($#)); do
  case "$1" in
    --github-repo) [[ $# -ge 2 ]] || die "$1 requires a value"; GITHUB_REPO=$2; shift 2 ;;
    --subscription-id) [[ $# -ge 2 ]] || die "$1 requires a value"; AZURE_SUBSCRIPTION_ID=$2; shift 2 ;;
    --app-name) [[ $# -ge 2 ]] || die "$1 requires a value"; APP_NAME=$2; shift 2 ;;
    --env-name) [[ $# -ge 2 ]] || die "$1 requires a value"; ENV_NAME=$2; shift 2 ;;
    --role-name) [[ $# -ge 2 ]] || die "$1 requires a value"; ROLE_NAME=$2; shift 2 ;;
    --reviewers) [[ $# -ge 2 ]] || die "$1 requires a value"; REVIEWERS=$2; shift 2 ;;
    --delete) [[ $# -ge 2 ]] || die "$1 requires one exact name"; DELETE_GROUP=$2; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --yes) YES=true; shift ;;
    --help|-h) SHOW_HELP=true; shift ;;
    *) die "unknown option or subcommand: $1" ;;
  esac
done
$SHOW_HELP && { usage; exit 0; }
[[ "$COMMAND" == cleanup || -z "$DELETE_GROUP" ]] || die "--delete is valid only with cleanup"

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
  for provider in Microsoft.Resources Microsoft.DataFactory Microsoft.Storage Microsoft.KeyVault; do
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
  scope="/subscriptions/${AZURE_SUBSCRIPTION_ID}"
  new_temp_file; role_file=$NEW_TEMP_FILE
  jq -n --arg name "$ROLE_NAME" --arg scope "$scope" \
    '{Name:$name,IsCustom:true,Description:"Deploy flowx live integration fixtures",Actions:["Microsoft.Resources/subscriptions/read","Microsoft.Resources/subscriptions/resourceGroups/*","Microsoft.DataFactory/factories/*","Microsoft.Storage/storageAccounts/*","Microsoft.KeyVault/vaults/*"],NotActions:[],AssignableScopes:[$scope]}' > "$role_file"
  role_id=$(az role definition list --name "$ROLE_NAME" --scope "$scope" --query '[0].id' -o tsv)
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
    '{wait_timer:0,prevent_self_review:false,reviewers:$reviewers,deployment_branch_policy:null}' > "$environment_file"
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
  prompt_optional CLAUDE_CODE_OAUTH_TOKEN 'Claude Code OAuth token'
  prompt_optional ANTHROPIC_API_KEY 'Anthropic API key'
  prompt_optional DATABRICKS_HOST 'Databricks host'
  prompt_optional DATABRICKS_TOKEN 'Databricks token'
  for secret_name in CLAUDE_CODE_OAUTH_TOKEN ANTHROPIC_API_KEY DATABRICKS_HOST DATABRICKS_TOKEN; do
    if [[ -n "${!secret_name:-}" ]]; then set_secret "$secret_name" "${!secret_name}"; else log "Skipping optional secret $secret_name"; fi
  done
}

verify_setup() {
  local failures=0 provider state role_json role_matches role_id assignment environment reviewer_count secret_names secret_name
  lookup_identity
  [[ -n "$app_id" ]] || die "Entra app $APP_NAME was not found"
  [[ -n "$sp_id" ]] || die "service principal for $APP_NAME was not found"
  subject="repo:${GITHUB_REPO}:environment:${ENV_NAME}"
  credential=$(az ad app federated-credential list --id "$app_id" \
    --query "[?subject=='$subject' && issuer=='https://token.actions.githubusercontent.com' && audiences[0]=='api://AzureADTokenExchange'].id | [0]" -o tsv)
  [[ -n "$credential" ]] || { echo "MISSING/MISMATCH: GitHub OIDC federated credential" >&2; failures=$((failures + 1)); }
  for provider in Microsoft.Resources Microsoft.DataFactory Microsoft.Storage Microsoft.KeyVault; do
    state=$(az provider show --subscription "$AZURE_SUBSCRIPTION_ID" --namespace "$provider" --query registrationState -o tsv 2>/dev/null || true)
    [[ "$state" == Registered ]] || { echo "MISSING/MISMATCH: provider $provider is not registered" >&2; failures=$((failures + 1)); }
  done
  scope="/subscriptions/${AZURE_SUBSCRIPTION_ID}"
  role_json=$(az role definition list --name "$ROLE_NAME" --scope "$scope" -o json)
  required_actions='["Microsoft.Resources/subscriptions/read","Microsoft.Resources/subscriptions/resourceGroups/*","Microsoft.DataFactory/factories/*","Microsoft.Storage/storageAccounts/*","Microsoft.KeyVault/vaults/*"]'
  # shellcheck disable=SC2016
  role_matches=$(json_value "invalid role-definition response from Azure" \
    'if type == "array" then
       (length == 1) and
       (((.[0].permissions // []) | map(.actions // []) | add // [] | sort) == ($expected | sort)) and
       (((.[0].assignableScopes // []) | index($scope)) != null)
     else false end' --arg scope "$scope" --argjson expected "$required_actions" <<< "$role_json")
  [[ "$role_matches" == true ]] || { echo "MISSING/MISMATCH: custom role definition $ROLE_NAME" >&2; failures=$((failures + 1)); }
  role_id=$(json_value "invalid role-definition response from Azure" \
    'if type == "array" then .[0].id // empty else empty end' <<< "$role_json")
  if [[ -n "$role_id" ]]; then
    assignment=$(az role assignment list --assignee-object-id "$sp_id" --scope "$scope" --role "$role_id" --query '[0].id' -o tsv)
  else assignment=''; fi
  [[ -n "$assignment" ]] || { echo "MISSING/MISMATCH: role assignment $ROLE_NAME" >&2; failures=$((failures + 1)); }
  environment=$(gh api "repos/${GITHUB_REPO}/environments/${ENV_NAME}") || { echo "MISSING: GitHub environment $ENV_NAME" >&2; failures=$((failures + 1)); environment='{}'; }
  reviewer_count=$(json_value "invalid environment response from GitHub" \
    'if type == "object" then [.protection_rules[]? | select(.type=="required_reviewers") | .reviewers[]?] | length else 0 end' \
    <<< "$environment")
  ((reviewer_count > 0)) || { echo 'MISSING/MISMATCH: environment has no required reviewers' >&2; failures=$((failures + 1)); }
  secret_names=$(gh secret list --repo "$GITHUB_REPO" --env "$ENV_NAME" --json name --jq '.[].name')
  for secret_name in AZURE_CLIENT_ID AZURE_TENANT_ID AZURE_SUBSCRIPTION_ID RESOURCE_OWNER_EMAIL; do
    grep -Fxq "$secret_name" <<< "$secret_names" || { echo "MISSING: environment secret $secret_name" >&2; failures=$((failures + 1)); }
  done
  ((failures == 0)) || die "$failures verification check(s) failed"
  log 'Verification passed (secret names only; values were never read).'
}

cleanup_orphans() {
  groups=$(az group list --subscription "$AZURE_SUBSCRIPTION_ID" --tag purpose=flowx-integration-test --query '[].name' -o tsv)
  if [[ -z "$groups" ]]; then log "No resource groups tagged purpose=flowx-integration-test"; else printf '%s\n' "$groups"; fi
  [[ -n "$DELETE_GROUP" ]] || return 0
  grep -Fxq -- "$DELETE_GROUP" <<< "$groups" || die "refusing to delete: $DELETE_GROUP is not an exact tagged group name"
  confirm_target
  if ! $YES; then
    read -r -p "Type the exact resource-group name to delete: " confirmation
    [[ "$confirmation" == "$DELETE_GROUP" ]] || die "name did not match; cancelled"
  fi
  run_write az group delete --subscription "$AZURE_SUBSCRIPTION_ID" --name "$DELETE_GROUP" --yes
}

validate_owner_email() {
  [[ "${RESOURCE_OWNER_EMAIL:-}" =~ ^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$ ]] ||
    die "RESOURCE_OWNER_EMAIL must be a non-empty email-like value"
}

[[ "$COMMAND" == setup || "$COMMAND" == environment ]] && [[ -z "$REVIEWERS" ]] &&
  die "REVIEWERS (or --reviewers) is required before setup can begin"
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
