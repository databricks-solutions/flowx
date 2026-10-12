"""Offline safety checks for the Azure/GitHub setup script."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parent / "azure" / "setup.sh"
BASH = Path("/bin/bash")
SECRET = "must-never-appear-in-output"
SUBCOMMANDS = ("setup", "verify", "cleanup", "identity", "providers", "role", "environment", "secrets")
SENSITIVE_ENV = {
    "CLAUDE_CODE_OAUTH_TOKEN",
    "ANTHROPIC_API_KEY",
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
}


def clean_environment(binary_dir: Path, mutation_log: Path) -> dict[str, str]:
    """Build a deterministic environment without inheriting developer credentials."""
    environment = {
        key: value for key, value in os.environ.items() if key not in SENSITIVE_ENV and not key.startswith("AZURE_")
    }
    environment.update(
        {
            "PATH": f"{binary_dir}:{environment['PATH']}",
            "MUTATION_LOG": str(mutation_log),
            "GITHUB_REPO": "databricks-solutions/flowx",
            "AZURE_SUBSCRIPTION_ID": "00000000-0000-0000-0000-000000000001",
            "REVIEWERS": "flowx-reviewer",
        }
    )
    return environment


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
      "Microsoft.DataFactory/factories/*","Microsoft.Storage/storageAccounts/*","Microsoft.KeyVault/vaults/*"]'; fi
    printf '[{"assignableScopes":["/subscriptions/00000000-0000-0000-0000-000000000001"],
      "permissions":[{"actions":%s,"notActions":[],"dataActions":[],"notDataActions":[]}],
      "roleName":"flowx-live-integration-deployer","id":"role-id"}]\n' "$actions" ;;
  *"role definition list"*) [[ "${STUB_FRESH:-}" == 1 ]] || echo "role-id" ;;
  *"role assignment list"*)
    [[ "${STUB_NO_SP:-}" != 1 ]] || { echo 'placeholder assignment lookup' >> "$MUTATION_LOG"; exit 88; }
    echo "assignment-id" ;;
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
  *"secret list"*) printf 'AZURE_CLIENT_ID\nAZURE_TENANT_ID\nAZURE_SUBSCRIPTION_ID\n' ;;
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


def run_script(arguments: list[str], environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run the consolidated script without allowing interactive input."""
    return subprocess.run(
        [str(BASH), str(SCRIPT), *arguments],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        env=environment,
        check=False,
    )


@pytest.mark.parametrize("subcommand", SUBCOMMANDS)
def test_every_subcommand_dry_run_is_offline_and_secret_safe(subcommand: str, stub_path: tuple[Path, Path]) -> None:
    """Every subcommand succeeds without dispatching writes or leaking secrets."""
    binary_dir, mutation_log = stub_path
    environment = clean_environment(binary_dir, mutation_log)
    environment["ANTHROPIC_API_KEY"] = SECRET
    result = run_script([subcommand, "--dry-run", "--yes"], environment)
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert SECRET not in output
    assert not mutation_log.exists(), mutation_log.read_text() if mutation_log.exists() else ""


def test_fresh_tenant_setup_dry_run_skips_dependent_lookups(stub_path: tuple[Path, Path]) -> None:
    """A dry-run describes the complete setup when no Azure objects exist."""
    binary_dir, mutation_log = stub_path
    environment = clean_environment(binary_dir, mutation_log)
    environment["STUB_FRESH"] = "1"
    result = run_script(["setup", "--dry-run", "--yes"], environment)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "client-id" in result.stdout
    assert "role-definition-id" in result.stdout
    assert not mutation_log.exists()


def test_role_dry_run_skips_assignment_lookup_for_missing_sp(stub_path: tuple[Path, Path]) -> None:
    """A missing SP placeholder is never sent to the assignment-list API."""
    binary_dir, mutation_log = stub_path
    environment = clean_environment(binary_dir, mutation_log)
    environment["STUB_NO_SP"] = "1"
    result = run_script(["role", "--dry-run", "--yes"], environment)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "service-principal-object-id" in result.stdout
    assert not mutation_log.exists()


def test_verify_reports_mismatched_role_actions_clearly(stub_path: tuple[Path, Path]) -> None:
    """A real nested role response with wrong actions fails without a raw jq error."""
    binary_dir, mutation_log = stub_path
    environment = clean_environment(binary_dir, mutation_log)
    environment["STUB_ROLE_MISMATCH"] = "1"
    result = run_script(["verify"], environment)
    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert "MISSING/MISMATCH: custom role definition" in output
    assert "jq: error" not in output


def test_github_secret_value_uses_stdin_only(stub_path: tuple[Path, Path], tmp_path: Path) -> None:
    """The real secrets path omits --body and keeps secret values out of argv and output."""
    binary_dir, mutation_log = stub_path
    argv_log = tmp_path / "gh-argv"
    stdin_log = tmp_path / "gh-stdin"
    environment = clean_environment(binary_dir, mutation_log)
    environment.update(
        {
            "GH_ARGV_LOG": str(argv_log),
            "GH_STDIN_LOG": str(stdin_log),
            "ALLOW_SECRET_WRITE": "1",
            "ANTHROPIC_API_KEY": SECRET,
        }
    )
    result = run_script(["secrets", "--yes"], environment)
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert SECRET in stdin_log.read_text()
    assert SECRET not in argv_log.read_text()
    assert "--body" not in argv_log.read_text()
    assert SECRET not in output


def test_cleanup_without_delete_only_lists(stub_path: tuple[Path, Path]) -> None:
    """Cleanup without --delete cannot dispatch a resource-group deletion."""
    binary_dir, mutation_log = stub_path
    result = run_script(["cleanup", "--yes"], clean_environment(binary_dir, mutation_log))
    assert result.returncode == 0, result.stdout + result.stderr
    assert "flowx-orphan-exact" in result.stdout
    assert not mutation_log.exists()


def test_bash_syntax() -> None:
    """The consolidated setup script parses under the system Bash 3.2."""
    subprocess.run([str(BASH), "-n", str(SCRIPT)], check=True)
