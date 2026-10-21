"""Unit tests for the ADF extract phase: ARM transport, extraction, export layout, path checks and notebook."""

from __future__ import annotations

import ast
import json
import logging
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from flowx.adapter.__main__ import _run_phase
from flowx.sources import get_source
from flowx.sources.adf.extract import core
from flowx.sources.adf.extract.layout import inspect_source_path
from flowx.sources.adf.extract.notebook import embedded_source, render_notebook, source_fingerprint
from flowx.sources.adf.loader import load_adf_definitions

SUBSCRIPTION = "00000000-0000-0000-0000-000000000000"
RESOURCE_GROUP = "flowx-rg"
FACTORY = "flowx-adf"
BASE_URL = core.factory_url(SUBSCRIPTION, RESOURCE_GROUP, FACTORY)


def _url(collection: str | None = None) -> str:
    suffix = f"/{collection}" if collection else ""
    return f"{BASE_URL}{suffix}?api-version={core.API_VERSION}"


def _pipeline(name: str, dataset: str = "SourceDataset") -> dict[str, Any]:
    return {
        "id": f"{BASE_URL}/pipelines/{name}",
        "name": name,
        "type": "Microsoft.DataFactory/factories/pipelines",
        "properties": {
            "activities": [
                {
                    "name": "CopyData",
                    "type": "Copy",
                    "inputs": [{"referenceName": dataset, "type": "DatasetReference"}],
                    "outputs": [{"referenceName": dataset, "type": "DatasetReference"}],
                    "typeProperties": {"source": {"type": "DelimitedTextSource"}, "sink": {"type": "ParquetSink"}},
                }
            ]
        },
    }


def _factory_responses() -> dict[str, dict[str, Any]]:
    return {
        _url(): {
            "name": FACTORY,
            "location": "eastus2",
            "properties": {"globalParameters": {"environment": {"type": "String", "value": "dev"}}},
        },
        _url("pipelines"): {"value": [_pipeline("Load Sales")], "nextLink": f"{BASE_URL}/pipelines?page=2"},
        f"{BASE_URL}/pipelines?page=2": {"value": [_pipeline("Load_Sales")]},
        _url("datasets"): {
            "value": [
                {
                    "name": "SourceDataset",
                    "properties": {
                        "type": "DelimitedText",
                        "linkedServiceName": {"referenceName": "Storage", "type": "LinkedServiceReference"},
                    },
                }
            ]
        },
        _url("linkedservices"): {"value": [{"name": "Storage", "properties": {"type": "AzureBlobFS"}}]},
        _url("triggers"): {"value": []},
        _url("dataflows"): {"value": []},
        _url("integrationRuntimes"): {"value": [{"name": "AutoResolveIntegrationRuntime", "properties": {}}]},
        _url("credentials"): {"value": []},
    }


class ReplayTransport:
    """Answers GETs from a dict of URL -> payload, or raises the HttpError stored for that URL."""

    def __init__(self, responses: dict[str, Any]):
        self.responses = responses
        self.requested: list[str] = []

    def get_json(self, url: str) -> dict[str, Any]:
        self.requested.append(url)
        answer = self.responses[url]
        if isinstance(answer, core.HttpError):
            raise answer
        return answer


class FakeResponse:
    def __init__(self, status_code: int, payload: Any = None, headers: dict[str, str] | None = None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}

    def json(self) -> Any:
        return self._payload


class FakeSession:
    def __init__(self, answers: list[Any]):
        self.answers = list(answers)
        self.authorization_headers: list[str] = []

    def get(self, url: str, headers: dict[str, str], timeout: int) -> FakeResponse:
        self.authorization_headers.append(headers["Authorization"])
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


class FakeToken:
    def __init__(self, token: str):
        self.token = token
        self.expires_on = 4_102_444_800  # 2100-01-01


class FakeCredential:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def get_token(self, *scopes: str, **options: Any) -> FakeToken:
        self.calls.append(options)
        return FakeToken(f"token-{len(self.calls)}")


def _transport(answers: list[Any], **options: Any) -> tuple[core.ArmTransport, FakeSession, list[float]]:
    session = FakeSession(answers)
    sleeps: list[float] = []
    transport = core.ArmTransport(FakeCredential(), session=session, sleep=sleeps.append, **options)
    return transport, session, sleeps


# --------------------------------------------------------------------------------------
# ARM transport
# --------------------------------------------------------------------------------------


def test_transport_honours_retry_after_on_throttling():
    transport, _, sleeps = _transport(
        [FakeResponse(429, headers={"Retry-After": "3"}), FakeResponse(200, {"value": []})]
    )
    assert transport.get_json("https://example/x") == {"value": []}
    assert sleeps == [3.0]
    assert transport.retry_count == 1


def test_transport_backs_off_on_server_errors_then_gives_up():
    transport, _, sleeps = _transport([FakeResponse(503)] * 3, max_attempts=3)
    with pytest.raises(core.HttpError) as error:
        transport.get_json("https://example/x")
    assert error.value.status == 503
    assert len(sleeps) == 2


def test_transport_refreshes_token_once_on_401():
    transport, session, _ = _transport([FakeResponse(401), FakeResponse(200, {"ok": True})])
    assert transport.get_json("https://example/x") == {"ok": True}
    assert session.authorization_headers == ["Bearer token-1", "Bearer token-2"]


def test_transport_does_not_retry_forbidden():
    transport, _, sleeps = _transport(
        [FakeResponse(403, {"error": {"code": "AuthorizationFailed", "message": "no access"}})]
    )
    with pytest.raises(core.HttpError, match="AuthorizationFailed no access"):
        transport.get_json("https://example/x")
    assert sleeps == []


def test_transport_retries_connection_errors():
    transport, _, sleeps = _transport([ConnectionError("reset"), FakeResponse(200, {})])
    assert transport.get_json("https://example/x") == {}
    assert len(sleeps) == 1


def test_transport_requests_token_for_the_given_tenant():
    credential = FakeCredential()
    transport = core.ArmTransport(credential, tenant_id="tenant-1", session=FakeSession([FakeResponse(200, {})]))
    transport.get_json("https://example/x")
    assert credential.calls == [{"tenant_id": "tenant-1"}]


def test_transport_records_remaining_read_budget():
    transport, _, _ = _transport(
        [FakeResponse(200, {}, headers={"x-ms-ratelimit-remaining-subscription-reads": "11998"})]
    )
    transport.get_json("https://example/x")
    assert transport.last_remaining_reads == "11998"


def test_missing_requests_package_is_a_setup_error_not_retried(monkeypatch):
    monkeypatch.setitem(sys.modules, "requests", None)
    sleeps: list[float] = []
    transport = core.ArmTransport(FakeCredential(), sleep=sleeps.append)
    with pytest.raises(core.SetupError, match="pip install"):
        transport.get_json("https://example/x")
    assert sleeps == []


class NotJsonResponse(FakeResponse):
    def json(self) -> Any:
        raise ValueError("Expecting value")


def test_transport_reports_a_response_that_is_not_json():
    transport, _, _ = _transport([NotJsonResponse(200)])
    with pytest.raises(core.HttpError, match="not JSON"):
        transport.get_json("https://example/x")


# --------------------------------------------------------------------------------------
# Extraction
# --------------------------------------------------------------------------------------


def test_list_collection_follows_next_link():
    transport = ReplayTransport(_factory_responses())
    items = core.list_collection(transport, f"{BASE_URL}/pipelines")
    assert [item["name"] for item in items] == ["Load Sales", "Load_Sales"]


def test_list_collection_stops_when_next_link_repeats():
    responses = _factory_responses()
    responses[f"{BASE_URL}/pipelines?page=2"] = {"value": [], "nextLink": f"{BASE_URL}/pipelines?page=2"}
    with pytest.raises(core.HttpError, match="same nextLink twice"):
        core.list_collection(ReplayTransport(responses), f"{BASE_URL}/pipelines")


def test_extract_factory_lists_each_type_once():
    transport = ReplayTransport(_factory_responses())
    export = core.extract_factory(
        transport, subscription_id=SUBSCRIPTION, resource_group=RESOURCE_GROUP, factory_name=FACTORY
    )
    assert len(export.resources["pipeline"]) == 2
    assert export.warnings == []
    # factory + one call per type + one extra page of pipelines
    assert len(transport.requested) == 1 + len(core.RESOURCE_TYPES) + 1


def test_extract_factory_reports_missing_access_clearly():
    responses = _factory_responses()
    responses[_url()] = core.HttpError(403, _url(), "AuthorizationFailed")
    with pytest.raises(core.AzureAuthenticationError, match="cannot read factory"):
        core.extract_factory(
            ReplayTransport(responses),
            subscription_id=SUBSCRIPTION,
            resource_group=RESOURCE_GROUP,
            factory_name=FACTORY,
        )


def test_extract_factory_reports_unknown_factory():
    responses = _factory_responses()
    responses[_url()] = core.HttpError(404, _url())
    with pytest.raises(core.ExtractError, match="was not found"):
        core.extract_factory(
            ReplayTransport(responses),
            subscription_id=SUBSCRIPTION,
            resource_group=RESOURCE_GROUP,
            factory_name=FACTORY,
        )


def test_optional_type_failure_is_a_warning():
    responses = _factory_responses()
    responses[_url("credentials")] = core.HttpError(403, _url("credentials"))
    export = core.extract_factory(
        ReplayTransport(responses), subscription_id=SUBSCRIPTION, resource_group=RESOURCE_GROUP, factory_name=FACTORY
    )
    assert "credential" not in export.resources
    assert export.warnings and "credentials" in export.warnings[0]


def test_required_type_failure_stops_the_run():
    responses = _factory_responses()
    responses[_url("datasets")] = core.HttpError(500, _url("datasets"))
    with pytest.raises(core.ExtractError, match="Could not list datasets"):
        core.extract_factory(
            ReplayTransport(responses),
            subscription_id=SUBSCRIPTION,
            resource_group=RESOURCE_GROUP,
            factory_name=FACTORY,
        )


def test_optional_type_unreachable_is_a_warning():
    responses = _factory_responses()
    responses[_url("credentials")] = core.ExtractError("Could not reach credentials: timed out")
    transport = ReplayTransport(responses)
    original_get_json = transport.get_json

    def get_json(url: str) -> dict[str, Any]:
        answer = transport.responses[url]
        if isinstance(answer, core.ExtractError):
            raise answer
        return original_get_json(url)

    transport.get_json = get_json  # type: ignore[method-assign]
    export = core.extract_factory(
        transport, subscription_id=SUBSCRIPTION, resource_group=RESOURCE_GROUP, factory_name=FACTORY
    )
    assert "pipeline" in export.resources
    assert any("credentials" in warning for warning in export.warnings)


def test_required_type_without_access_is_a_permission_problem():
    responses = _factory_responses()
    responses[_url("linkedservices")] = core.HttpError(403, _url("linkedservices"), "AuthorizationFailed")
    with pytest.raises(core.AzureAuthenticationError, match="cannot list linkedservices"):
        core.extract_factory(
            ReplayTransport(responses),
            subscription_id=SUBSCRIPTION,
            resource_group=RESOURCE_GROUP,
            factory_name=FACTORY,
        )


# --------------------------------------------------------------------------------------
# Writing the export
# --------------------------------------------------------------------------------------


def test_assign_filenames_never_collides():
    filenames = core.assign_filenames(["Load Sales", "Load_Sales", "Report", "report", "Plain"])
    assert len(set(name.lower() for name in filenames.values())) == 5
    assert filenames["Plain"] == "Plain.json"
    assert all(" " not in name for name in filenames.values())


def _write_sample(tmp_path: Path, **options: Any) -> tuple[Path, dict[str, Any]]:
    output = tmp_path / "export"
    manifest = core.run_extraction(
        subscription_id=SUBSCRIPTION,
        resource_group=RESOURCE_GROUP,
        factory_name=FACTORY,
        output_dir=output,
        transport=ReplayTransport(_factory_responses()),
        **options,
    )
    return output, manifest


def test_export_round_trips_through_the_loader(tmp_path):
    output, manifest = _write_sample(tmp_path)
    definitions = load_adf_definitions(output)
    assert sorted(pipeline.name for pipeline in definitions.pipelines) == ["Load Sales", "Load_Sales"]
    assert set(definitions.datasets) == {"SourceDataset"}
    assert set(definitions.linked_services) == {"Storage"}
    assert definitions.global_parameters == {"environment": {"type": "String", "value": "dev"}}
    assert manifest["complete"] is True
    assert manifest["counts"]["pipeline"] == 2
    on_disk = json.loads((output / core.MANIFEST_NAME).read_text())
    assert on_disk["factory"]["name"] == FACTORY


def test_export_refuses_to_replace_without_overwrite(tmp_path):
    output, _ = _write_sample(tmp_path)
    with pytest.raises(core.ExtractError, match="already exists"):
        _write_sample(tmp_path)
    _write_sample(tmp_path, overwrite=True)
    assert (output / core.MANIFEST_NAME).is_file()


def test_failed_write_leaves_no_partial_export(tmp_path, monkeypatch):
    def broken_manifest(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise OSError("disk full")

    monkeypatch.setattr(core, "build_manifest", broken_manifest)
    with pytest.raises(OSError):
        _write_sample(tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_failed_overwrite_keeps_the_previous_export(tmp_path, monkeypatch):
    output, _ = _write_sample(tmp_path)
    previous_manifest = (output / core.MANIFEST_NAME).read_text()
    real_move = core._move_tree

    def fail_when_installing(source: Path, target: Path) -> None:
        if target == output and ".previous-" not in source.name:
            raise OSError("volume unavailable")
        real_move(source, target)

    monkeypatch.setattr(core, "_move_tree", fail_when_installing)
    with pytest.raises(OSError):
        _write_sample(tmp_path, overwrite=True)
    monkeypatch.undo()
    assert (output / core.MANIFEST_NAME).read_text() == previous_manifest
    assert sorted(path.name for path in tmp_path.iterdir()) == ["export"]


def test_overwrite_keeps_folders_flowx_did_not_create(tmp_path):
    output, _ = _write_sample(tmp_path)
    unrelated = tmp_path / ".export.previous"
    unrelated.mkdir()
    (unrelated / "notes.txt").write_text("keep me")
    _write_sample(tmp_path, overwrite=True)
    assert (unrelated / "notes.txt").read_text() == "keep me"
    assert sorted(path.name for path in tmp_path.iterdir()) == [".export.previous", "export"]


def test_overwrite_warns_when_the_backup_cannot_be_removed(tmp_path, monkeypatch, caplog):
    _write_sample(tmp_path)
    real_rmtree = core.shutil.rmtree

    def locked_backup(path, ignore_errors=False):
        if ".previous-" not in Path(path).name:
            return real_rmtree(path, ignore_errors=ignore_errors)
        if not ignore_errors:
            raise OSError("backup is locked")

    monkeypatch.setattr(core.shutil, "rmtree", locked_backup)
    with caplog.at_level(logging.WARNING, logger=core.logger.name):
        output, manifest = _write_sample(tmp_path, overwrite=True)
    assert json.loads((output / core.MANIFEST_NAME).read_text()) == manifest
    (backup,) = tmp_path.glob(".export.previous-*")
    assert any(f"Couldn't remove export backup at {backup}" in rec.getMessage() for rec in caplog.records)


# --------------------------------------------------------------------------------------
# Path checks
# --------------------------------------------------------------------------------------


def _arm_export_folder(root: Path) -> Path:
    """Mimics the unzipped ADF Studio export: a template at the top and a factory-only factory/ folder."""
    root.mkdir(parents=True)
    template = {
        "$schema": "https://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#",
        "parameters": {"factoryName": {"type": "string", "defaultValue": FACTORY}},
        "resources": [
            {
                "name": "[concat(parameters('factoryName'), '/Daily Load')]",
                "type": "Microsoft.DataFactory/factories/pipelines",
                "apiVersion": "2018-06-01",
                "properties": {"activities": [{"name": "Wait", "type": "Wait", "typeProperties": {}}]},
            }
        ],
    }
    (root / "ARMTemplateForFactory.json").write_text(json.dumps(template))
    (root / "factory").mkdir()
    (root / "factory" / f"{FACTORY}_ARMTemplateForFactory.json").write_text(
        json.dumps({"resources": [{"type": "Microsoft.DataFactory/factories", "name": FACTORY}]})
    )
    return root


def test_arm_export_folder_points_at_the_template(tmp_path):
    folder = _arm_export_folder(tmp_path / "arm_template")
    assert load_adf_definitions(folder).pipelines == []
    report = inspect_source_path(folder)
    assert report.kind == "arm_export_folder"
    assert report.discover_path == str(folder / "ARMTemplateForFactory.json")
    assert report.counts["pipelines"] == 1
    assert report.usable


def test_publish_branch_template_is_found_one_level_down(tmp_path):
    repository = tmp_path / "repo"
    _arm_export_folder(repository / FACTORY)
    report = inspect_source_path(repository)
    assert report.discover_path == str(repository / FACTORY / "ARMTemplateForFactory.json")


def test_shared_publish_branch_asks_which_factory(tmp_path):
    """Uncommon setup: two factories publishing to the same repository."""
    repository = tmp_path / "adf_publish"
    _arm_export_folder(repository / "sales-factory")
    _arm_export_folder(repository / "finance-factory")
    report = inspect_source_path(repository)
    assert report.kind == "multiple_factories"
    assert not report.usable
    assert report.candidates == [
        str(repository / "finance-factory" / "ARMTemplateForFactory.json"),
        str(repository / "sales-factory" / "ARMTemplateForFactory.json"),
    ]
    assert "2 factories" in report.warnings[0]


def test_git_root_folder_is_found(tmp_path):
    output, _ = _write_sample(tmp_path)
    repository = tmp_path / "repo"
    repository.mkdir()
    output.rename(repository / "adf")
    report = inspect_source_path(repository)
    assert report.kind == "native"
    assert report.discover_path == str(repository / "adf")
    assert report.counts["pipelines"] == 2


def test_incomplete_manifest_is_flagged(tmp_path):
    output, _ = _write_sample(tmp_path)
    manifest_path = output / core.MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text())
    manifest.update(complete=False, warnings=["Skipped credentials: HTTP 403"])
    manifest["counts"]["pipeline"] = 3
    manifest_path.write_text(json.dumps(manifest))
    warnings = " ".join(inspect_source_path(output).warnings)
    assert "incomplete" in warnings and "lists 3 pipeline" in warnings


def test_unusable_paths_are_explained(tmp_path):
    (tmp_path / "export.zip").write_bytes(b"PK")
    (tmp_path / "empty").mkdir()
    assert inspect_source_path(tmp_path / "export.zip").kind == "zip"
    assert inspect_source_path(tmp_path / "empty").kind == "unknown"
    assert inspect_source_path(tmp_path / "missing").kind == "missing"
    assert not inspect_source_path(tmp_path / "empty").usable
    (tmp_path / "notes.txt").write_text("not a template")
    report = inspect_source_path(tmp_path / "notes.txt")
    assert report.kind == "unknown"
    assert "not a .json file" in report.warnings[0]


# --------------------------------------------------------------------------------------
# Notebook
# --------------------------------------------------------------------------------------


def test_core_is_self_contained():
    tree = ast.parse(embedded_source())
    imported = {
        alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
    } | {node.module.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module}
    assert "flowx" not in imported


def test_notebook_embeds_the_current_extractor_and_parses():
    notebook = render_notebook(tenant_id='ten"ant', factory_name="my-factory")
    assert notebook.startswith("# Databricks notebook source")
    assert embedded_source().rstrip("\n") in notebook
    assert source_fingerprint(embedded_source()) in notebook
    ast.parse(notebook)
    assert 'dbutils.widgets.text("tenant_id", "ten\\"ant"' in notebook


def test_notebook_restarts_python_in_its_own_cell():
    cells = render_notebook().split("# COMMAND ----------")
    install_cell = next(cell for cell in cells if "%pip install" in cell)
    assert all(line.startswith("# MAGIC") for line in install_cell.strip().splitlines())
    assert any(cell.strip() == "dbutils.library.restartPython()" for cell in cells)


def test_notebook_never_asks_for_a_secret_in_a_widget():
    widget_lines = [line for line in render_notebook().splitlines() if line.startswith("dbutils.widgets.")]
    assert widget_lines
    assert not any('"client_secret"' in line or '"password"' in line for line in widget_lines)


# --------------------------------------------------------------------------------------
# Adapter wiring
# --------------------------------------------------------------------------------------


def test_extract_is_registered_for_adf_only():
    assert get_source("adf").extract_module == "flowx.sources.adf.extract.runner"
    assert get_source("airflow").extract_module is None
    assert _run_phase("extract", ["--source", "airflow"]) == 2


def test_validate_path_through_the_adapter(tmp_path, capsys):
    folder = _arm_export_folder(tmp_path / "arm_template")
    assert _run_phase("extract", ["--source", "adf", "--validate-path", str(folder), "--json"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["kind"] == "arm_export_folder" and result["usable"] is True
    assert _run_phase("extract", ["--source", "adf", "--validate-path", str(tmp_path / "missing")]) == 1


def test_emit_notebook_through_the_adapter(tmp_path):
    destination = tmp_path / "nb" / "extract.py"
    exit_code = _run_phase(
        "extract", ["--source", "adf", "--emit-notebook", str(destination), "--factory-name", "my-factory"]
    )
    assert exit_code == 0
    assert 'dbutils.widgets.text("factory_name", "my-factory"' in destination.read_text()


def test_extraction_requires_factory_coordinates(capsys):
    assert _run_phase("extract", ["--source", "adf", "--factory-name", "x"]) == 2
    assert "--subscription-id" in capsys.readouterr().err


def _extract_args(output_dir: Path) -> list[str]:
    return [
        "--source", "adf", "--subscription-id", SUBSCRIPTION, "--resource-group", RESOURCE_GROUP,
        "--factory-name", FACTORY, "--output-dir", str(output_dir),
    ]  # fmt: skip


def test_extraction_with_skipped_optional_types_is_still_ready(monkeypatch, tmp_path, capsys):
    responses = _factory_responses()
    responses[_url("credentials")] = core.HttpError(403, _url("credentials"))
    original_run_extraction = core.run_extraction

    def run_with_replay(**options: Any) -> dict[str, Any]:
        return original_run_extraction(**options, transport=ReplayTransport(responses))

    monkeypatch.setattr(core, "run_extraction", run_with_replay)
    assert _run_phase("extract", _extract_args(tmp_path / "export")) == 0
    assert "Skipped credentials" in capsys.readouterr().out


def test_missing_packages_exit_with_a_setup_code(monkeypatch, tmp_path, capsys):
    def missing_packages(**options: Any) -> dict[str, Any]:
        raise core.SetupError("The extract phase needs the optional Azure packages.")

    monkeypatch.setattr(core, "run_extraction", missing_packages)
    assert _run_phase("extract", _extract_args(tmp_path / "export")) == 2
    assert "optional Azure packages" in capsys.readouterr().err


def test_unwritable_output_is_reported_not_raised(monkeypatch, tmp_path, capsys):
    original_run_extraction = core.run_extraction

    def run_with_replay(**options: Any) -> dict[str, Any]:
        return original_run_extraction(**options, transport=ReplayTransport(_factory_responses()))

    monkeypatch.setattr(core, "run_extraction", run_with_replay)
    not_a_folder = tmp_path / "export.json"
    not_a_folder.write_text("{}")
    assert _run_phase("extract", _extract_args(not_a_folder)) == 1
    assert "could not write" in capsys.readouterr().err


# --------------------------------------------------------------------------------------
# Unity Catalog service credential sign-in
# --------------------------------------------------------------------------------------


def _fake_dbutils_module(monkeypatch, provider: Any = None, error: Exception | None = None) -> list[str]:
    requested: list[str] = []

    class Credentials:
        def getServiceCredentialsProvider(self, name: str) -> Any:  # noqa: N802 -- mirrors the dbutils API
            requested.append(name)
            if error is not None:
                raise error
            return provider

    class DBUtils:
        credentials = Credentials()

    module = types.ModuleType("databricks.sdk.runtime")
    module.dbutils = DBUtils()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", module)
    return requested


def test_service_credential_comes_from_dbutils(monkeypatch):
    provider = FakeCredential()
    requested = _fake_dbutils_module(monkeypatch, provider=provider)
    credential = core.build_credential(auth_mode="service-credential", service_credential="flowx_adf_reader")
    assert credential is provider
    assert requested == ["flowx_adf_reader"]


def test_service_credential_needs_a_name():
    with pytest.raises(core.ExtractError, match="needs the name"):
        core.build_credential(auth_mode="service-credential")


def test_service_credential_errors_explain_access(monkeypatch):
    _fake_dbutils_module(monkeypatch, error=RuntimeError("PERMISSION_DENIED"))
    with pytest.raises(core.AzureAuthenticationError, match="ACCESS on it"):
        core.build_credential(auth_mode="service-credential", service_credential="flowx_adf_reader")


def test_service_credential_tokens_are_requested_without_a_tenant(monkeypatch, tmp_path):
    credential = FakeCredential()
    _fake_dbutils_module(monkeypatch, provider=credential)
    session = FakeSession([FakeResponse(200, payload) for payload in _ordered_payloads()])
    monkeypatch.setattr(core.ArmTransport, "_http", lambda self: session)
    manifest = core.run_extraction(
        subscription_id=SUBSCRIPTION,
        resource_group=RESOURCE_GROUP,
        factory_name=FACTORY,
        output_dir=tmp_path / "export",
        tenant_id="tenant-1",
        auth_mode="service-credential",
        service_credential="flowx_adf_reader",
    )
    assert manifest["complete"] is True
    assert credential.calls and all(call == {} for call in credential.calls)


def _ordered_payloads() -> list[dict[str, Any]]:
    responses = _factory_responses()
    order = [_url()]
    for resource_type in core.RESOURCE_TYPES:
        order.append(_url(resource_type.collection))
        if resource_type.collection == "pipelines":
            order.append(f"{BASE_URL}/pipelines?page=2")
    return [responses[url] for url in order]


def test_notebook_defaults_to_the_service_credential_when_given():
    notebook = render_notebook(service_credential="flowx_adf_reader")
    assert '"auth_mode", "service_credential", [' in notebook
    assert 'dbutils.widgets.text("service_credential", "flowx_adf_reader"' in notebook
    assert '"auth_mode", "device_code", [' in render_notebook()


def test_extraction_with_service_credential_requires_its_name(capsys):
    exit_code = _run_phase(
        "extract",
        [
            "--source",
            "adf",
            "--subscription-id",
            "s",
            "--resource-group",
            "r",
            "--factory-name",
            "f",
            "--output-dir",
            "/tmp/unused",
            "--auth",
            "service-credential",
        ],
    )
    assert exit_code == 2
    assert "--service-credential" in capsys.readouterr().err
