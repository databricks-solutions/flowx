"""Tests for the vendored airflow-to-dabs provider pin."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parents[2]
SCRIPT = ROOT / "scripts" / "sync_airflow_provider.py"
PROVIDER = ROOT / "skills" / "flowx-resolve-airflow-gaps" / "references" / "airflow-to-dabs-v0.2.1"


def _check(destination: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--check", "--destination", str(destination)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_committed_airflow_provider_pin_is_valid() -> None:
    result = _check(PROVIDER)

    assert result.returncode == 0, result.stderr
    pin = json.loads(result.stdout)
    assert pin == {
        "commit": "75196aef85ebb2736b926f2d4db13ec7d5c2551c",
        "content_sha256": "e1e7395204b3f2759722b9320cea08c6b58284a48fd8062686b36bf676b657fd",
        "tag": "v0.2.1",
    }


def test_airflow_provider_pin_rejects_modified_content(tmp_path: Path) -> None:
    destination = tmp_path / PROVIDER.name
    shutil.copytree(PROVIDER, destination)
    profile = destination / "providers" / "flowx-gap-resolver" / "PROFILE.md"
    profile.write_text(profile.read_text(encoding="utf-8") + "\nmodified\n", encoding="utf-8")

    result = _check(destination)

    assert result.returncode != 0
    assert "content digest does not match" in result.stderr


def test_airflow_provider_pin_rejects_noncanonical_json(tmp_path: Path) -> None:
    destination = tmp_path / PROVIDER.name
    shutil.copytree(PROVIDER, destination)
    manifest = destination / "providers" / "flowx-gap-resolver" / "provider.json"
    manifest.write_text(manifest.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    result = _check(destination)

    assert result.returncode != 0
    assert "JSON is not canonical" in result.stderr
