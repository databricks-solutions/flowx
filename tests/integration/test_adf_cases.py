"""Expected-output cases for the live and offline ADF integration corpus."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from difflib import unified_diff
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).parents[2]
FIXTURES = ROOT / "tests/integration/azure/adf"
CASES = FIXTURES / "cases"
PHASES = ("discover", "convert", "package")
SHARED_DIRS = ("datasets", "linked_services", "dataflows")
LIVE_DEFINITION_DIRS = (*SHARED_DIRS, "pipelines", "triggers")
LIVE_ENV = {
    "<STORAGE_ACCOUNT_ID>": "ADF_CASE_STORAGE_ACCOUNT_ID",
    "<KEY_VAULT_URI>": "ADF_CASE_KEY_VAULT_URI",
    "<ADLS_DFS_ENDPOINT>": "ADF_CASE_ADLS_DFS_ENDPOINT",
    "<LOCATION>": "ADF_CASE_LOCATION",
}


CASE_PHASES = [
    (case_dir, phase) for case_dir in sorted(path.parent for path in CASES.glob("*/case.json")) for phase in PHASES
]
CASE_IDS = [f"{case_dir.name}-{phase}" for case_dir, phase in CASE_PHASES]


@pytest.fixture(scope="module")
def case_results(tmp_path_factory: pytest.TempPathFactory, pytestconfig: pytest.Config):
    """Run each case's discover, convert, and package phases once and share the results across phase tests."""
    source_root = Path(os.getenv("ADF_CASE_SOURCE_DIR", str(FIXTURES)))
    if pytestconfig.getoption("--update-expected") and source_root.resolve() != FIXTURES.resolve():
        pytest.fail("--update-expected only accepts the checked-in fixture source")
    results: dict[Path, dict[str, Any]] = {}

    def get(case_dir: Path) -> dict[str, Any]:
        if case_dir not in results:
            results[case_dir] = _run_case(
                _read_json(case_dir / "case.json"), source_root, tmp_path_factory.mktemp(case_dir.name)
            )
        return results[case_dir]

    return get


@pytest.mark.parametrize(("case_dir", "phase"), CASE_PHASES, ids=CASE_IDS)
def test_adf_case(case_dir: Path, phase: str, case_results, pytestconfig: pytest.Config) -> None:
    """Check one expected-output phase for one selected pipeline case."""
    update = pytestconfig.getoption("--update-expected")
    result = case_results(case_dir)
    expected = case_dir / "expected"

    if phase == "discover":
        if update:
            _write_json(expected / "inventory.json", result["inventory"])
        else:
            assert result["inventory"] == _read_json(expected / "inventory.json")
    elif phase == "convert":
        if update:
            shutil.rmtree(expected / "ir", ignore_errors=True)
            for pipeline, value in result["ir"].items():
                _write_json(expected / "ir" / f"{pipeline}.json", value)
            _write_json(expected / "gaps.json", result["gaps"])
        else:
            assert result["ir"] == {path.stem: _read_json(path) for path in sorted((expected / "ir").glob("*.json"))}
            assert result["gaps"] == _read_json(expected / "gaps.json")
    elif update:
        _copy_tree(result["output"], expected / "bundles")
    else:
        _assert_tree(expected / "bundles", result["output"])


def test_live_export_matches_fixtures() -> None:
    """Show every unexpected GET field across the complete live export at once."""
    source_root = os.getenv("ADF_CASE_SOURCE_DIR")
    if not source_root:
        pytest.skip("ADF_CASE_SOURCE_DIR is not set; there is no live export to compare")
    _assert_live_export(Path(source_root))


def test_every_fixture_pipeline_is_in_a_case() -> None:
    """New fixture pipelines must be assigned to at least one expected-output case."""
    fixtures = {path.stem for path in (FIXTURES / "pipelines").glob("*.json")}
    covered = {pipeline for case_dir in _case_dirs() for pipeline in _read_json(case_dir / "case.json")["pipelines"]}
    assert covered == fixtures, f"uncovered={sorted(fixtures - covered)} unknown={sorted(covered - fixtures)}"
    assert set(_read_json(CASES / "full-factory/case.json")["pipelines"]) == fixtures
    motifs = {pipeline for pipeline in fixtures if pipeline.startswith("motif-")}
    assert set(_read_json(CASES / "motifs/case.json")["pipelines"]) == motifs


def test_blob_event_expected_url_documents_known_arm_id_bug() -> None:
    """Keep the current ARM-ID URL visible until the separate flowx bug is fixed."""
    expected = CASES / "full-factory/expected/bundles"
    matches = [path for path in expected.rglob("*.yml") if "file_arrival" in path.read_text(encoding="utf-8")]
    assert matches
    assert any("url: <STORAGE_ACCOUNT_ID>" in path.read_text(encoding="utf-8") for path in matches)


def test_live_definition_allowlist_rejects_translation_changes() -> None:
    """ADF defaults may be added, but translation-affecting activity policy may not."""
    fixture = {"properties": {"activities": [{"name": "wait", "type": "Wait"}]}}
    allowed = {"properties": {"annotations": [], "activities": [{"name": "wait", "type": "Wait", "dependsOn": []}]}}
    _validate_definition(fixture, allowed)

    changed = {
        "properties": {
            "activities": [{"name": "wait", "type": "Wait", "policy": {"retry": 3}}],
        }
    }
    with pytest.raises(AssertionError, match=r"\$\.properties\.activities\[0\]\.policy"):
        _validate_definition(fixture, changed)


def test_last_publish_time_allowlist_is_narrow() -> None:
    """Only a plausible read-only ADF pipeline timestamp is ignored."""
    fixture = {"properties": {}}
    _validate_definition(fixture, {"properties": {"lastPublishTime": "2026-10-13T12:34:56Z"}})
    with pytest.raises(AssertionError, match=r"\$\.properties\.lastPublishTime"):
        _validate_definition(fixture, {"properties": {"lastPublishTime": 123}})
    with pytest.raises(AssertionError, match=r"\$\.properties\.unexpected"):
        _validate_definition(fixture, {"properties": {"unexpected": "2026-10-13T12:34:56Z"}})


def test_whole_export_reports_all_files_and_paths(tmp_path: Path) -> None:
    """Aggregate validation does not stop at the first definition mismatch."""
    live = tmp_path / "live"
    for directory in LIVE_DEFINITION_DIRS:
        shutil.copytree(FIXTURES / directory, live / directory)
    first = live / "pipelines/agentic-activities.json"
    first_value = _read_json(first)
    first_value["properties"]["unexpectedOne"] = True
    _write_json(first, first_value)
    second = live / "datasets/csv-adls.json"
    second_value = _read_json(second)
    second_value["properties"]["unexpectedTwo"] = True
    _write_json(second, second_value)

    with pytest.raises(pytest.fail.Exception) as failure:
        _assert_live_export(live)
    message = str(failure.value)
    assert "pipelines/agentic-activities.json\n  $.properties.unexpectedOne" in message
    assert "datasets/csv-adls.json\n  $.properties.unexpectedTwo" in message


def test_expected_artifacts_are_not_subject_to_line_ending_conversion() -> None:
    """Generated CSV CRLF bytes must survive checkout unchanged."""
    sample = CASES / "full-factory/expected/bundles/metadata/profile_report.csv"
    result = subprocess.run(
        ["git", "check-attr", "text", "--", str(sample.relative_to(ROOT))],
        cwd=ROOT,
        capture_output=True,
        check=True,
        text=True,
    )
    assert result.stdout.rstrip().endswith(": text: unset")


def test_live_read_only_trigger_and_integration_runtime_fields(tmp_path: Path) -> None:
    """ADF-added trigger defaults are kept, and live-only pipeline and IR state doesn't change the output."""
    live = tmp_path / "live"
    for directory in (*SHARED_DIRS, "integration_runtimes", "pipelines", "triggers"):
        shutil.copytree(FIXTURES / directory, live / directory)

    runtime_path = live / "integration_runtimes/azure-ssis-placeholder.json"
    runtime = _read_json(runtime_path)
    runtime["properties"]["state"] = "Started"
    _write_json(runtime_path, runtime)
    for pipeline_path in (live / "pipelines").glob("*.json"):
        pipeline = _read_json(pipeline_path)
        pipeline["properties"]["lastPublishTime"] = "2026-10-13T12:34:56Z"
        _write_json(pipeline_path, pipeline)
    trigger_path = live / "triggers/schedule.json"
    trigger = _read_json(trigger_path)
    assert "annotations" not in trigger["properties"]
    trigger["properties"]["annotations"] = []
    _write_json(trigger_path, trigger)

    case_dir = CASES / "full-factory"
    result = _run_case(_read_json(case_dir / "case.json"), live, tmp_path)
    source = tmp_path / "source"
    assert not (source / "integration_runtimes").exists()
    copied_trigger = _read_json(source / "triggers/schedule.json")["properties"]
    assert copied_trigger["annotations"] == []
    assert copied_trigger["runtimeState"] == "Stopped"
    assert all(_read_json(path)["properties"]["runtimeState"] == "Stopped" for path in source.glob("triggers/*.json"))

    assert result["inventory"] == _read_json(case_dir / "expected/inventory.json")
    assert result["ir"] == {path.stem: _read_json(path) for path in sorted((case_dir / "expected/ir").glob("*.json"))}
    assert result["gaps"] == _read_json(case_dir / "expected/gaps.json")
    _assert_tree(case_dir / "expected/bundles", result["output"])


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _case_dirs() -> list[Path]:
    return sorted(path.parent for path in CASES.glob("*/case.json"))


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _substitutions() -> dict[str, str]:
    return {value: placeholder for placeholder, env_name in LIVE_ENV.items() if (value := os.getenv(env_name))}


def _replace_values(value: Any, substitutions: dict[str, str]) -> Any:
    if isinstance(value, str):
        return substitutions.get(value, value)
    if isinstance(value, list):
        return [_replace_values(item, substitutions) for item in value]
    if isinstance(value, dict):
        return {key: _replace_values(item, substitutions) for key, item in value.items()}
    return value


def _allow_added_key(path: str, value: Any) -> bool:
    """Allow only ADF GET fields that have no translation semantics."""
    # ADF stamps this read-only server timestamp on pipeline GET responses;
    # flowx does not consume it. Require a non-empty ISO-8601-like string.
    if path == "$.properties.lastPublishTime":
        return isinstance(value, str) and bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\S+Z", value))
    if path == "$.properties.annotations":
        return value == []
    if path in {"$.properties.parameters", "$.properties.variables"}:
        return value == {}
    if ".activities[" in path and path.rsplit(".", 1)[-1] in {"dependsOn", "userProperties"}:
        return value == []
    return False


def _definition_errors(expected: Any, actual: Any, path: str = "$") -> list[str]:
    """Return differing paths after accounting for documented ADF GET fields."""
    errors: list[str] = []

    def compare(want: Any, got: Any, current: str) -> None:
        if isinstance(want, dict):
            if not isinstance(got, dict):
                errors.append(current)
                return
            for key in want.keys() - got.keys():
                errors.append(f"{current}.{key}")
            for key in got.keys() - want.keys():
                added_path = f"{current}.{key}"
                if not _allow_added_key(added_path, got[key]):
                    errors.append(added_path)
            for key in want.keys() & got.keys():
                compare(want[key], got[key], f"{current}.{key}")
            return
        if isinstance(want, list):
            if not isinstance(got, list) or len(want) != len(got):
                errors.append(current)
                return
            for index, item in enumerate(want):
                compare(item, got[index], f"{current}[{index}]")
            return
        if got != want:
            errors.append(current)

    compare(expected, actual, path)
    return sorted(errors)


def _validate_definition(expected: Any, actual: Any, path: str = "$") -> None:
    """Require exact fixture content plus the documented no-op ADF defaults."""
    errors = _definition_errors(expected, actual, path)
    assert not errors, "live definition differs at key paths:\n" + "\n".join(sorted(errors))


def _assert_live_export(source_root: Path) -> None:
    """Report all live definition mismatches together, without printing values."""
    mismatches: dict[str, list[str]] = {}
    substitutions = _substitutions()
    for directory in LIVE_DEFINITION_DIRS:
        for fixture_path in sorted((FIXTURES / directory).glob("*.json")):
            relative = fixture_path.relative_to(FIXTURES)
            source_path = source_root / relative
            if not source_path.exists():
                mismatches[str(relative)] = ["$ (missing file)"]
                continue
            errors = _definition_errors(
                _read_json(fixture_path),
                _replace_values(_read_json(source_path), substitutions),
            )
            if errors:
                mismatches[str(relative)] = errors
    if mismatches:
        lines = ["live export differs from fixtures:"]
        for filename, paths in mismatches.items():
            lines.append(filename)
            lines.extend(f"  {path}" for path in paths)
        pytest.fail("\n".join(lines))


def _strip_allowed_defaults(value: Any, path: str = "$") -> Any:
    """Remove no-op defaults when comparing raw JSON preserved in outputs."""
    if isinstance(value, dict):
        return {
            key: _strip_allowed_defaults(item, f"{path}.{key}")
            for key, item in value.items()
            if not (
                (key in {"annotations", "dependsOn", "userProperties"} and item == [])
                or (key in {"parameters", "variables"} and item == {})
                or _allow_added_key(f"{path}.{key}", item)
            )
        }
    if isinstance(value, list):
        return [_strip_allowed_defaults(item, f"{path}[{index}]") for index, item in enumerate(value)]
    return value


def _pipeline_references(value: Any) -> set[str]:
    references: set[str] = set()
    if isinstance(value, dict):
        if value.get("type") == "PipelineReference" and isinstance(value.get("referenceName"), str):
            references.add(value["referenceName"])
        for child in value.values():
            references.update(_pipeline_references(child))
    elif isinstance(value, list):
        for child in value:
            references.update(_pipeline_references(child))
    return references


def _copy_definition(source_root: Path, relative: Path, destination_root: Path) -> None:
    fixture_path = FIXTURES / relative
    source_path = source_root / relative
    assert source_path.exists(), f"source export is missing {relative}"
    fixture = _read_json(fixture_path)
    actual = _replace_values(_read_json(source_path), _substitutions())
    _validate_definition(fixture, actual)
    _write_json(destination_root / relative, actual)


def _build_case_source(case: dict[str, Any], source_root: Path, destination: Path) -> None:
    selected = set(case["pipelines"])
    for directory in SHARED_DIRS:
        for fixture_path in sorted((FIXTURES / directory).glob("*.json")):
            _copy_definition(source_root, fixture_path.relative_to(FIXTURES), destination)
    for pipeline in sorted(selected):
        _copy_definition(source_root, Path("pipelines") / f"{pipeline}.json", destination)
    for fixture_path in sorted((FIXTURES / "triggers").glob("*.json")):
        trigger = _read_json(fixture_path)
        if _pipeline_references(trigger).intersection(selected):
            _copy_definition(source_root, fixture_path.relative_to(FIXTURES), destination)


def _run_case(case: dict[str, Any], source_root: Path, workdir: Path) -> dict[str, Any]:
    """Build a case source under workdir, then run discover, convert, and package on it once."""
    source = workdir / "source"
    output = workdir / "output"
    _build_case_source(case, source_root, source)
    _run("discover", source, output)
    inventory = _normalize_inventory(output / "metadata/inventory.json")
    _run("convert", source, output)
    ir, gaps = _capture_convert(output)
    _run("package", source, output)
    return {"inventory": inventory, "ir": ir, "gaps": gaps, "output": output}


def _run(phase: str, source: Path, output: Path) -> None:
    command = [
        sys.executable,
        "-m",
        "flowx.adapter",
        phase,
        "--source",
        "adf",
        "--output-dir",
        str(output),
    ]
    if phase != "package":
        command.extend(("--source-path", str(source)))
    else:
        command.extend(("--catalog", "main", "--schema", "default", "--no-download-workspace-files"))
    subprocess.run(command, cwd=ROOT, env={**os.environ, "PYTHONPATH": str(ROOT / "src")}, check=True)


def _normalize_inventory(path: Path) -> dict[str, Any]:
    inventory = _read_json(path)
    inventory.pop("generated_at", None)
    inventory.pop("source_dir", None)
    _write_json(path, inventory)
    return inventory


def _copy_tree(source: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(source, destination)


def _tree_contents(root: Path) -> dict[str, bytes]:
    return {str(path.relative_to(root)): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}


def _normalize_json_fences(content: str) -> str:
    def normalize(match: re.Match[str]) -> str:
        body = "\n".join(line.removeprefix("# MAGIC ") for line in match.group("body").splitlines())
        normalized = json.dumps(_strip_allowed_defaults(json.loads(body)), indent=2, sort_keys=True)
        prefixed = "\n".join(f"# MAGIC {line}" for line in normalized.splitlines())
        return f"{match.group('open')}{prefixed}\n{match.group('close')}"

    return re.sub(
        r"(?P<open># MAGIC ```json\n)(?P<body>.*?)(?P<close># MAGIC ```)",
        normalize,
        content,
        flags=re.DOTALL,
    )


def _normalized_content(relative: str, content: bytes) -> bytes:
    if relative.endswith(".json"):
        return (json.dumps(_strip_allowed_defaults(json.loads(content)), indent=2, sort_keys=True) + "\n").encode()
    if relative.endswith(".py") and b"# MAGIC ```json" in content:
        return _normalize_json_fences(content.decode()).encode()
    return content


def _assert_tree(expected: Path, actual: Path) -> None:
    expected_files = _tree_contents(expected)
    actual_files = _tree_contents(actual)
    assert actual_files.keys() == expected_files.keys(), (
        f"file set mismatch: missing={sorted(expected_files.keys() - actual_files.keys())}; "
        f"extra={sorted(actual_files.keys() - expected_files.keys())}"
    )
    for relative, expected_content in expected_files.items():
        want = _normalized_content(relative, expected_content)
        got = _normalized_content(relative, actual_files[relative])
        if got != want:
            try:
                diff = list(
                    unified_diff(
                        want.decode().splitlines(),
                        got.decode().splitlines(),
                        fromfile=f"expected/{relative}",
                        tofile=f"actual/{relative}",
                        lineterm="",
                    )
                )[:60]
            except UnicodeDecodeError:
                diff = ["binary content differs"]
            pytest.fail(f"content mismatch: {relative}\n" + "\n".join(diff))


def _capture_convert(output: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    ir = {
        path.stem: _strip_allowed_defaults(_read_json(path))
        for path in sorted((output / ".work").glob("*.json"))
        if path.name not in {"translation_report.json", "translation_report.stamped.json", "gaps.json"}
    }
    gaps_path = output / ".work/gaps.json"
    gaps = _strip_allowed_defaults(_read_json(gaps_path)) if gaps_path.exists() else []
    return ir, gaps
