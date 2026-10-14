"""Shared integration-test configuration and fixtures."""

import shutil
from pathlib import Path

import pytest

from flowx.sources.adf.loader import load_adf_definitions

INTEGRATION_ROOT = Path(__file__).parent
ADF_FIXTURE_ROOT = INTEGRATION_ROOT / "azure" / "adf"


def pytest_addoption(parser):
    """Register expected-artifact regeneration for ADF case tests."""
    parser.addoption(
        "--update-expected",
        action="store_true",
        default=False,
        help="regenerate ADF case expected artifacts from checked-in fixtures",
    )


@pytest.fixture
def pipeline_by_name(adf_definitions):
    """Return a lookup helper that finds a pipeline by name."""

    def _find(name: str):
        for p in adf_definitions.pipelines:
            if p.name == name:
                return p
        raise KeyError(f"Pipeline '{name}' not found in fixtures")

    return _find


@pytest.fixture
def live_pipeline_by_name(live_definitions):
    """Return a lookup helper that finds a live ADF pipeline by name."""

    def _find(name: str):
        for p in live_definitions.pipelines:
            if p.name == name:
                return p
        raise KeyError(f"Pipeline '{name}' not found in live ADF export")

    return _find


@pytest.fixture(scope="module")
def definitions():
    """Load the checked-in definitions through the production ADF loader."""
    return load_adf_definitions(ADF_FIXTURE_ROOT)


@pytest.fixture
def stub_path(tmp_path: Path) -> tuple[Path, Path]:
    """Provide deterministic CLIs that reject unexpected writes."""
    jq = shutil.which("jq")
    if jq is None:
        pytest.skip("jq is required for the setup-script tests")
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    mutation_log = tmp_path / "mutations"
    az_stub = binary_dir / "az"
    az_stub.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  "account show "*"--query name"*) echo "Flowx Test Subscription" ;;
  "account show"*"--query id"*) echo "00000000-0000-0000-0000-000000000001" ;;
  "account show"*"--query tenantId"*) echo "00000000-0000-0000-0000-000000000002" ;;
  "account show"*) echo '{}' ;;
  *"ad app list"*) [[ "${STUB_FRESH:-}" == 1 ]] || echo "00000000-0000-0000-0000-000000000003" ;;
  *"ad sp list"*)
    [[ "${STUB_FRESH:-}" == 1 || "${STUB_NO_SP:-}" == 1 ]] ||
      echo "00000000-0000-0000-0000-000000000004" ;;
  *"federated-credential list"*) echo '{"id":"credential-id","name":"existing-name",
    "issuer":"https://token.actions.githubusercontent.com",
    "audiences":["api://AzureADTokenExchange"]}' ;;
  *"provider show"*) if [[ "${STUB_FRESH:-}" == 1 ]]; then echo "NotRegistered"; else echo "Registered"; fi ;;
  *"role definition list"*"-o json"*)
    if [[ "${STUB_ROLE_MISMATCH:-}" == 1 ]]; then actions='["Microsoft.Resources/subscriptions/read"]';
    else actions='["Microsoft.Resources/subscriptions/read","Microsoft.Resources/subscriptions/resourceGroups/*",
      "Microsoft.DataFactory/factories/*","Microsoft.Storage/storageAccounts/*","Microsoft.KeyVault/vaults/*",
      "Microsoft.EventGrid/eventSubscriptions/read","Microsoft.EventGrid/eventSubscriptions/write",
      "Microsoft.EventGrid/eventSubscriptions/delete"]'; fi
    printf '[{"assignableScopes":["/subscriptions/00000000-0000-0000-0000-000000000001"],
      "permissions":[{"actions":%s,"notActions":[],"dataActions":[],"notDataActions":[]}],
      "roleName":"flowx-live-integration-deployer","id":"role-id"}]\n' "$actions" ;;
  *"role definition list"*) [[ "${STUB_FRESH:-}" == 1 ]] || echo "role-id" ;;
  *"role assignment list"*)
    [[ "${STUB_NO_SP:-}" != 1 ]] || { echo 'placeholder assignment lookup' >> "$MUTATION_LOG"; exit 88; }
    echo "assignment-id" ;;
  *"role definition update"*)
    if [[ "${ALLOW_ROLE_UPDATE:-}" == 1 ]]; then
      previous=''
      for argument in "$@"; do
        if [[ "$previous" == --role-definition ]]; then cat "$argument" > "$ROLE_JSON_LOG"; fi
        previous=$argument
      done
    else echo "$*" >> "$MUTATION_LOG"; exit 91; fi ;;
  *"group list"*) echo "flowx-orphan-exact" ;;
  *" create"*|*" update"*|*" register"*|*" delete"*|*" account set"*)
    echo "$*" >> "$MUTATION_LOG"; exit 91 ;;
esac
""",
        encoding="utf-8",
    )
    gh_stub = binary_dir / "gh"
    gh_stub.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  "auth status") exit 0 ;;
  *"repo view"*) echo "databricks-solutions/flowx" ;;
  *"api users/"*) echo "12345" ;;
  *"api repos/"*"/environments/"*) echo '{"protection_rules":[
    {"type":"required_reviewers","reviewers":[{"type":"User","reviewer":{"login":"flowx-reviewer","id":12345}}]}
  ]}' ;;
  *"secret list"*)
    printf 'AZURE_CLIENT_ID\nAZURE_TENANT_ID\nAZURE_SUBSCRIPTION_ID\n'
    [[ "${STUB_MISSING_OWNER:-}" == 1 ]] || printf 'RESOURCE_OWNER_EMAIL\n' ;;
  *"secret set"*)
    if [[ "${ALLOW_SECRET_WRITE:-}" == 1 ]]; then
      printf '%s\n' "$*" >> "$GH_ARGV_LOG"
      input=$(cat)
      printf '%s\n' "$input" >> "$GH_STDIN_LOG"
    else echo "$*" >> "$MUTATION_LOG"; exit 91; fi ;;
  *"--method PUT"*) echo "$*" >> "$MUTATION_LOG"; exit 91 ;;
esac
""",
        encoding="utf-8",
    )
    jq_stub = binary_dir / "jq"
    jq_stub.write_text(f'#!/usr/bin/env bash\nexec "{jq}" "$@"\n', encoding="utf-8")
    for path in (az_stub, gh_stub, jq_stub):
        path.chmod(0o755)
    return binary_dir, mutation_log
