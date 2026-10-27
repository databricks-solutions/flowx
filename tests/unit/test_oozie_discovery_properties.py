"""Finding Oozie app directories, resolving the files actions reference, and reading job.properties."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowx.sources.oozie.loader.discovery import discover_apps, resolve_app_file
from flowx.sources.oozie.loader.properties import PropertyScope, parameter_name, parse_properties


def _app(root: Path, relative: str, *, coordinator: bool = False, properties: str | None = None) -> Path:
    directory = root / relative
    (directory / "lib").mkdir(parents=True)
    (directory / "workflow.xml").write_text("<workflow-app/>")
    (directory / "lib" / "etl.jar").write_bytes(b"jar")
    if coordinator:
        (directory / "coordinator.xml").write_text("<coordinator-app/>")
    if properties is not None:
        (directory / "job.properties").write_text(properties)
    return directory


def test_discovers_every_workflow_directory_with_stable_names(tmp_path: Path):
    _app(tmp_path, "finance/daily", coordinator=True, properties="a=1")
    _app(tmp_path, "ops/cleanup")
    apps = discover_apps(tmp_path)
    assert [app.name for app in apps] == ["finance_daily", "ops_cleanup"]
    assert apps[0].coordinator_paths == [tmp_path / "finance/daily/coordinator.xml"]
    assert apps[0].properties_path == tmp_path / "finance/daily/job.properties"
    assert apps[1].properties_path is None


def test_skips_flowx_copies_in_an_output_dir_inside_the_source(tmp_path: Path):
    _app(tmp_path, "daily")
    _app(tmp_path, "flowx_output/metadata/daily")
    apps = discover_apps(tmp_path, output_dir=tmp_path / "flowx_output")
    assert [app.name for app in apps] == ["daily"]


def test_single_app_at_source_root_is_named_after_the_directory(tmp_path: Path):
    root = tmp_path / "daily_bookings"
    root.mkdir()
    (root / "workflow.xml").write_text("<workflow-app/>")
    assert [app.name for app in discover_apps(root)] == ["daily_bookings"]


@pytest.mark.parametrize(
    "reference",
    [
        "lib/etl.jar",
        "${nameNode}/user/etl/apps/daily/lib/etl.jar",
        "hdfs://ns1:8020/user/etl/apps/daily/lib/etl.jar",
        "${wf:appPath()}/lib/etl.jar",
        "etl.jar",
        "lib/etl.jar#etl",
    ],
)
def test_resolves_references_to_the_local_copy(tmp_path: Path, reference: str):
    directory = _app(tmp_path, "daily")
    app = discover_apps(tmp_path)[0]
    assert resolve_app_file(app, reference) == directory / "lib" / "etl.jar"


@pytest.mark.parametrize(
    "reference", ["lib/missing.jar", "../../etc/passwd", "/etc/passwd", "", "lib/etl.jar\x00", "lib/\x00etl.jar"]
)
def test_unresolvable_or_escaping_references_return_none(tmp_path: Path, reference: str):
    _app(tmp_path, "daily")
    assert resolve_app_file(discover_apps(tmp_path)[0], reference) is None


def test_references_escaping_the_app_directory_return_none_even_when_the_target_exists(tmp_path: Path):
    export = tmp_path / "export"
    directory = _app(export, "daily")
    secret = export / "secret.txt"
    secret.write_text("not part of the app")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "inner.jar").write_bytes(b"jar")
    (directory / "lib" / "dirlink").symlink_to(outside, target_is_directory=True)
    app = discover_apps(export)[0]
    references = [
        "../secret.txt",
        str(secret),
        "lib/../../secret.txt#x",
        f"/{secret}",
        "lib/dirlink/inner.jar",
    ]
    for reference in references:
        assert resolve_app_file(app, reference) is None, reference


def test_symlink_pointing_outside_the_app_directory_is_not_resolved(tmp_path: Path):
    outside = tmp_path / "secret.txt"
    outside.write_text("not part of the app")
    directory = _app(tmp_path / "source", "daily")
    (directory / "lib" / "leak.txt").symlink_to(outside)
    app = discover_apps(tmp_path / "source")[0]
    assert resolve_app_file(app, "lib/leak.txt") is None


def test_colliding_app_names_get_numeric_suffixes(tmp_path: Path):
    _app(tmp_path, "a/b_c")
    _app(tmp_path, "a_b/c")
    assert [app.name for app in discover_apps(tmp_path)] == ["a_b_c", "a_b_c_2"]


def test_parses_java_properties_syntax():
    text = (
        "# comment\n! also a comment\n"
        "nameNode=hdfs://ns1\n"
        "queue : etl\n"
        "spaced value\n"
        "long=first \\\n    second\n"
        "escaped=a\\tb\\u0041\n"
        "empty=\n"
    )
    assert parse_properties(text) == {
        "nameNode": "hdfs://ns1",
        "queue": "etl",
        "spaced": "value",
        "long": "first second",
        "escaped": "a\tbA",
        "empty": "",
    }


def test_comment_lines_are_not_continued():
    assert parse_properties("# c:\\temp\\\nnext=1\nafter=2\n") == {"next": "1", "after": "2"}


def test_truncated_unicode_escape_stays_literal():
    assert parse_properties("tail=value\\u00\nother=x\\uZZZZ\n") == {"tail": "value\\u00", "other": "x\\uZZZZ"}


def test_scope_lookup_respects_precedence():
    scope = PropertyScope(layers=({"a": "global"}, {"a": "job", "b": "job"}))
    assert scope.lookup("a") == "global" and scope.lookup("b") == "job" and scope.lookup("c") is None
    assert scope.with_layer({"a": "action"}).lookup("a") == "action"


def test_parameter_names_are_reference_safe():
    assert parameter_name("oozie.wf.application.path") == "oozie_wf_application_path"
    assert parameter_name("run-date") == "run_date"
    assert parameter_name("runDate") == "runDate"
