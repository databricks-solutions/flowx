"""The setup paths add the `azure` extra only when asked through `source_deps`."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
BOOTSTRAP = ROOT / "scripts" / "bootstrap.sh"
DEPLOY = ROOT / "app" / "deploy.sh"


def _azure_extra() -> list[str]:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["optional-dependencies"]["azure"]


def _write_stub(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"#!/bin/sh\n{body}\n")
    path.chmod(0o755)


def _environment(stub_bin: Path, **extra: str) -> dict[str, str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in ("DATABRICKS_RUNTIME_VERSION", "DATABRICKS_PROFILE", "SOURCE_DEPS")
    }
    environment["PATH"] = f"{stub_bin}{os.pathsep}{environment['PATH']}"
    return environment | extra


def _run_bootstrap(tmp_path: Path, *args: str) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    plugin_root = tmp_path / "plugin"
    (plugin_root / "scripts").mkdir(parents=True)
    shutil.copy(BOOTSTRAP, plugin_root / "scripts" / "bootstrap.sh")
    (plugin_root / "requirements.txt").write_text("")
    pip_log = tmp_path / "pip.log"
    _write_stub(plugin_root / ".venv" / "bin" / "python", f'echo "$*" >> "{pip_log}"')
    _write_stub(tmp_path / "bin" / "python3", "exit 0")
    completed = subprocess.run(
        ["bash", str(plugin_root / "scripts" / "bootstrap.sh"), *args],
        capture_output=True,
        text=True,
        env=_environment(tmp_path / "bin"),
    )
    calls = pip_log.read_text().splitlines() if pip_log.exists() else []
    return completed, calls


def _azure_install(tmp_path: Path) -> str:
    return f"-m pip install -e {(tmp_path / 'plugin').resolve()}[azure]"


@pytest.mark.parametrize(
    "args",
    [
        ("--bogus",),
        ("--source-deps", "gcp"),
        ("--source-deps=gcp",),
        ("--source-deps",),
        ("--source-deps=",),
        ("--source-deps", ""),
    ],
)
def test_bootstrap_rejects_bad_arguments_before_doing_any_work(tmp_path: Path, args: tuple[str, ...]):
    completed, calls = _run_bootstrap(tmp_path, *args)
    assert completed.returncode == 2
    assert "Usage: bootstrap.sh [--source-deps azure]" in completed.stderr
    assert calls == []


@pytest.mark.parametrize(
    "args",
    [
        *[("--source-deps", value) for value in ("azure", "Azure", "AZURE")],
        *[(f"--source-deps={value}",) for value in ("azure", "Azure", "AZURE")],
    ],
)
def test_bootstrap_installs_the_azure_extra_with_the_flag(tmp_path: Path, args: tuple[str, ...]):
    completed, calls = _run_bootstrap(tmp_path, *args)
    assert completed.returncode == 0, completed.stderr
    assert _azure_install(tmp_path) in calls


def test_bootstrap_skips_the_azure_extra_by_default(tmp_path: Path):
    completed, calls = _run_bootstrap(tmp_path)
    assert completed.returncode == 0, completed.stderr
    assert calls
    assert not any("[azure]" in call for call in calls)


def _deploy(tmp_path: Path, **extra: str) -> tuple[subprocess.CompletedProcess[str], Path]:
    synced_requirements = tmp_path / "requirements.txt"
    _write_stub(
        tmp_path / "bin" / "databricks",
        f'shift 2\n[ "$1" = sync ] && cp "$3/requirements.txt" "{synced_requirements}"\necho "{{}}"',
    )
    environment = _environment(tmp_path / "bin", DATABRICKS_PROFILE="stub", **extra)
    completed = subprocess.run(["bash", str(DEPLOY)], capture_output=True, text=True, env=environment)
    return completed, synced_requirements


def _run_deploy(tmp_path: Path, **extra: str) -> list[str]:
    completed, synced_requirements = _deploy(tmp_path, **extra)
    assert completed.returncode == 0, completed.stderr
    return synced_requirements.read_text().splitlines()


@pytest.mark.parametrize("value", ["azure", "Azure", "AZURE"])
def test_deploy_appends_the_azure_extra_with_source_deps(tmp_path: Path, value: str):
    requirements = _run_deploy(tmp_path, SOURCE_DEPS=value)
    for requirement in _azure_extra():
        assert requirement in requirements


def test_deploy_leaves_out_the_azure_extra_by_default(tmp_path: Path):
    requirements = _run_deploy(tmp_path)
    assert requirements == (ROOT / "app" / "requirements.txt").read_text().splitlines()


def test_deploy_rejects_an_unknown_source_deps_before_syncing(tmp_path: Path):
    completed, synced_requirements = _deploy(tmp_path, SOURCE_DEPS="gcp")
    assert completed.returncode == 2
    assert "SOURCE_DEPS" in completed.stderr
    assert not synced_requirements.exists()


def test_deploy_needs_tomllib_for_source_deps(tmp_path: Path):
    old_python = 'case "$*" in *tomllib*) exit 1 ;; --version) echo "Python 3.10.4" ;; esac'
    _write_stub(tmp_path / "bin" / "python3", old_python)
    completed, synced_requirements = _deploy(tmp_path, SOURCE_DEPS="azure")
    assert completed.returncode == 1
    assert "SOURCE_DEPS=azure needs python3 >= 3.11" in completed.stderr
    assert "found Python 3.10.4" in completed.stderr
    assert not synced_requirements.exists()


def _app_requirements(source_deps: str) -> str:
    sys.path.insert(0, str(ROOT / "app"))
    try:
        import deploy_helpers
    finally:
        sys.path.pop(0)
    return deploy_helpers.app_requirements(str(ROOT), source_deps).decode()


def test_app_requirements_leave_out_azure_by_default():
    requirements = _app_requirements("")
    assert requirements == (ROOT / "app" / "requirements.txt").read_text()
    assert "azure" not in requirements


@pytest.mark.parametrize("value", ["azure", "Azure", "AZURE"])
def test_app_requirements_add_the_azure_extra_when_asked(value: str):
    lines = _app_requirements(value).splitlines()
    for requirement in _azure_extra():
        assert requirement in lines


def test_app_requirements_reject_an_unknown_source_deps():
    with pytest.raises(ValueError, match="azure"):
        _app_requirements("gcp")
