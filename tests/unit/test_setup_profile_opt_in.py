"""The setup paths add the `profile` extra only when asked: bootstrap.sh's flag and the app deploy."""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).parents[2]
BOOTSTRAP = ROOT / "scripts" / "bootstrap.sh"


def _profile_extra() -> list[str]:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["optional-dependencies"]["profile"]


def test_bootstrap_rejects_unknown_flags_before_doing_any_work():
    completed = subprocess.run(["bash", str(BOOTSTRAP), "--bogus"], capture_output=True, text=True)
    assert completed.returncode == 2
    assert "Usage: bootstrap.sh [--with-profile]" in completed.stderr


def test_bootstrap_installs_the_local_checkout_with_the_profile_extra():
    # Installing from the checkout (not a published release) keeps the version in step with the source.
    assert '-m pip install -e "${PLUGIN_ROOT}[profile]"' in BOOTSTRAP.read_text()


def _app_requirements(with_profile: bool) -> str:
    sys.path.insert(0, str(ROOT / "app"))
    try:
        import deploy_helpers
    finally:
        sys.path.pop(0)
    return deploy_helpers.app_requirements(str(ROOT), with_profile).decode()


def test_app_requirements_leave_out_azure_by_default():
    requirements = _app_requirements(with_profile=False)
    assert requirements == (ROOT / "app" / "requirements.txt").read_text()
    assert "azure" not in requirements


def test_app_requirements_add_the_profile_extra_when_asked():
    lines = _app_requirements(with_profile=True).splitlines()
    for requirement in _profile_extra():
        assert requirement in lines
