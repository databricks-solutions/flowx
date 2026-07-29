"""Unit tests for the dbt manifest reader (flowx.dbt.manifest).

Uses synthetic manifests so the suite runs on a fresh clone with no dbt install.
"""

from __future__ import annotations

import pytest

from flowx.dbt.manifest import explode_manifest


def _model(name, fqn, deps=None):
    return {
        "resource_type": "model",
        "name": name,
        "fqn": fqn,
        "depends_on": {"nodes": deps or []},
    }


def _seed(name, fqn):
    return {"resource_type": "seed", "name": name, "fqn": fqn, "depends_on": {"nodes": []}}


def _test(name, fqn, deps=None):
    return {"resource_type": "test", "name": name, "fqn": fqn, "depends_on": {"nodes": deps or []}}


def _unit_test(name, fqn, model_uid):
    return {"resource_type": "unit_test", "name": name, "fqn": fqn, "depends_on": {"nodes": [model_uid]}}


def _manifest(nodes, unit_tests=None):
    return {"nodes": nodes, "unit_tests": unit_tests or {}}


def test_explodes_each_runnable_resource_type():
    manifest = _manifest(
        {
            "model.p.stg": _model("stg", ["p", "staging", "stg"]),
            "seed.p.codes": _seed("codes", ["p", "codes"]),
            "test.p.t": _test("t", ["p", "staging", "t"], deps=["model.p.stg"]),
        }
    )
    nodes = explode_manifest(manifest)
    by_key = {n.task_key: n for n in nodes}
    assert by_key["model_stg"].command == "run"
    assert by_key["seed_codes"].command == "seed"
    assert by_key["test_t"].command == "test"


def test_explosion_can_limit_resource_types_to_airflow_command_scope():
    manifest = _manifest(
        {
            "model.p.stg": _model("stg", ["p", "staging", "stg"]),
            "seed.p.codes": _seed("codes", ["p", "codes"]),
            "test.p.t": _test("t", ["p", "staging", "t"], deps=["model.p.stg"]),
        }
    )

    nodes = explode_manifest(manifest, resource_types={"model"})

    assert [(node.resource_type, node.name) for node in nodes] == [("model", "stg")]


def test_fqn_selector_built_from_components():
    manifest = _manifest({"model.p.stg": _model("stg", ["p", "staging", "stg"])})
    (node,) = explode_manifest(manifest)
    assert node.selector == "fqn:p.staging.stg"


def test_dependency_edges_pruned_to_exploded_set():
    # The model depends on a source (not runnable) and another model (runnable).
    manifest = _manifest(
        {
            "model.p.stg": _model("stg", ["p", "stg"], deps=["source.p.raw.raw_orders"]),
            "model.p.fct": _model("fct", ["p", "fct"], deps=["model.p.stg", "source.p.raw.x"]),
        }
    )
    by_key = {n.task_key: n for n in explode_manifest(manifest)}
    assert by_key["model_stg"].depends_on == []  # source edge dropped
    assert by_key["model_fct"].depends_on == ["model_stg"]  # source edge dropped, model kept


def test_downstream_model_waits_for_tests_on_its_upstream_model():
    manifest = _manifest(
        {
            "model.p.stg": _model("stg", ["p", "stg"]),
            "test.p.stg_not_null": _test("stg_not_null", ["p", "stg_not_null"], deps=["model.p.stg"]),
            "model.p.fct": _model("fct", ["p", "fct"], deps=["model.p.stg"]),
        }
    )

    by_key = {node.task_key: node for node in explode_manifest(manifest)}

    assert by_key["model_fct"].depends_on == ["model_stg", "test_stg_not_null"]


def test_non_runnable_resource_types_skipped():
    manifest = _manifest(
        {
            "model.p.stg": _model("stg", ["p", "stg"]),
            "source.p.raw": {"resource_type": "source", "name": "raw", "fqn": ["p", "raw"]},
            "operation.p.hook": {"resource_type": "operation", "name": "hook", "fqn": ["p", "hook"]},
        }
    )
    keys = {n.task_key for n in explode_manifest(manifest)}
    assert keys == {"model_stg"}


def test_output_is_sorted_by_task_key():
    manifest = _manifest(
        {
            "model.p.zeta": _model("zeta", ["p", "zeta"]),
            "model.p.alpha": _model("alpha", ["p", "alpha"]),
        }
    )
    keys = [n.task_key for n in explode_manifest(manifest)]
    assert keys == sorted(keys)


def test_unit_tests_explode_into_their_own_test_command_tasks():
    manifest = _manifest(
        {"model.p.stg": _model("stg", ["p", "staging", "stg"])},
        unit_tests={
            "unit_test.p.stg.check_amount": _unit_test(
                "check_amount", ["p", "staging", "stg", "check_amount"], "model.p.stg"
            )
        },
    )

    by_key = {node.task_key: node for node in explode_manifest(manifest)}

    unit = by_key["unit_test_check_amount"]
    assert unit.command == "test"
    assert unit.selector == "fqn:p.staging.stg.check_amount"
    # The unit test gates on the model it targets, like a data test.
    assert unit.depends_on == ["model_stg"]


def test_downstream_model_waits_for_unit_tests_on_its_upstream_model():
    manifest = _manifest(
        {
            "model.p.stg": _model("stg", ["p", "stg"]),
            "model.p.fct": _model("fct", ["p", "fct"], deps=["model.p.stg"]),
        },
        unit_tests={
            "unit_test.p.stg.check": _unit_test("check", ["p", "stg", "check"], "model.p.stg"),
        },
    )

    by_key = {node.task_key: node for node in explode_manifest(manifest)}

    assert by_key["model_fct"].depends_on == ["model_stg", "unit_test_check"]


def test_unit_tests_dropped_when_test_scope_excluded():
    # `dbt run` (resource_types={"model"}) does not run tests, so unit tests are out of scope too.
    manifest = _manifest(
        {"model.p.stg": _model("stg", ["p", "stg"])},
        unit_tests={"unit_test.p.stg.check": _unit_test("check", ["p", "stg", "check"], "model.p.stg")},
    )

    keys = {node.task_key for node in explode_manifest(manifest, resource_types={"model"})}

    assert keys == {"model_stg"}


def test_rejects_unsafe_fqn_characters():
    manifest = _manifest({"model.p.bad": _model("bad", ["p", "foo,bar"])})
    with pytest.raises(ValueError, match="Unsafe fqn"):
        explode_manifest(manifest)


def test_rejects_task_key_collision():
    # Distinct unique_ids whose (resource_type, name) sanitize to one key.
    manifest = _manifest(
        {
            "model.p.a": _model("foo bar", ["p", "a"]),
            "model.q.b": _model("foo_bar", ["q", "b"]),
        }
    )
    with pytest.raises(ValueError, match="collide"):
        explode_manifest(manifest)
