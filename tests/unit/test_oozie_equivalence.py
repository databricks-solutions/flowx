"""Every fixture, and random valid workflows, behave the same on Oozie and as the generated Lakeflow job."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import HealthCheck, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from flowx.bundler.dab_writer import write_bundle  # noqa: E402
from flowx.preparer.workflow_preparer import prepare_workflow  # noqa: E402
from flowx.sources.oozie.equivalence import Scenario, check_equivalence, run_lakeflow, run_oozie  # noqa: E402
from flowx.sources.oozie.loader import discover_apps, load_app  # noqa: E402
from flowx.sources.oozie.loader.discovery import OozieApp  # noqa: E402
from flowx.sources.oozie.loader.graph import assign_task_keys  # noqa: E402
from flowx.sources.oozie.loader.parse import parse_workflow  # noqa: E402

_APPS = Path(__file__).resolve().parents[1] / "resources" / "oozie" / "apps"


def _job_tasks(pipeline, bundle_dir: Path) -> list[dict]:
    write_bundle(prepare_workflow(pipeline), bundle_dir)
    document = yaml.safe_load(next((bundle_dir / "resources").glob("*.yml")).read_text())
    return next(iter(document["resources"]["jobs"].values()))["tasks"]


def _check(app: OozieApp, bundle_dir: Path) -> list[str]:
    workflow = parse_workflow(app.workflow_path.read_text())
    keys = assign_task_keys(workflow)
    tasks = _job_tasks(load_app(app), bundle_dir)
    return check_equivalence(workflow, tasks, keys, fail_task_keys=set(keys.kills.values()))


def test_every_fixture_is_equivalent(tmp_path: Path):
    failures = {app.name: _check(app, tmp_path / app.name)[:3] for app in discover_apps(_APPS)}
    assert {name: mismatches for name, mismatches in failures.items() if mismatches} == {}


def test_shared_error_handler_not_triggered_by_kill_path(tmp_path: Path):
    (app,) = [app for app in discover_apps(_APPS) if app.name == "error_handlers"]
    workflow = parse_workflow(app.workflow_path.read_text())
    keys = assign_task_keys(workflow)
    tasks = _job_tasks(load_app(app), tmp_path / "bundle")
    scenario = Scenario(action_succeeds={"step1": False}, decision_choice={})
    lakeflow = run_lakeflow(tasks, scenario, keys, fail_task_keys=set(keys.kills.values()))
    assert "notify" not in lakeflow.executed_actions
    assert run_oozie(workflow, scenario).executed_actions == {"step1"}


@st.composite
def _workflows(draw) -> str:
    """Random valid workflows: sequences of actions, forks, and decisions, with varied error targets.

    Fork paths and decision branches are sequences, so failures partway along a multi-action path get tested.
    Inside a fork an error may go to the kill node, to the shared handler (which goes on to kill), or to a
    recovery action that rejoins the path; only ``end`` is off limits there. Oozie's fork/join validation
    (``LiteWorkflowValidator.validateForkJoin``) rejects reaching end inside a fork, and a join paired with a
    different fork, which the nesting here never produces.
    """
    counter = iter(range(1, 1000))
    nodes: list[str] = []

    def pig_action(name: str, ok_to: str, error_to: str) -> None:
        nodes.append(
            f'<action name="{name}"><pig><script>x</script></pig><ok to="{ok_to}"/><error to="{error_to}"/></action>'
        )

    def action(next_name: str, in_fork: bool) -> str:
        name = f"a{next(counter)}"
        error = draw(st.sampled_from(["fail", "handler", "recover"] if in_fork else ["fail", "end", "handler"]))
        if error == "recover":
            error = f"r{next(counter)}"
            pig_action(error, next_name, "fail")
        pig_action(name, next_name, error)
        return name

    def block(next_name: str, depth: int, in_fork: bool) -> str:
        kind = draw(st.sampled_from(["action", "action", "fork", "decision"] if depth < 2 else ["action"]))
        if kind == "action":
            return action(next_name, in_fork)
        if kind == "fork":
            join = f"j{next(counter)}"
            nodes.append(f'<join name="{join}" to="{next_name}"/>')
            paths = [sequence(join, depth + 1, True) for _ in range(draw(st.integers(2, 3)))]
            fork = f"f{next(counter)}"
            nodes.append(f'<fork name="{fork}">' + "".join(f'<path start="{path}"/>' for path in paths) + "</fork>")
            return fork
        decision = f"d{next(counter)}"
        targets = [sequence(next_name, depth + 1, in_fork) for _ in range(draw(st.integers(1, 3)))]
        default = draw(st.sampled_from([next_name, *targets]))
        cases = "".join(f'<case to="{target}">${{p{index}}}</case>' for index, target in enumerate(targets))
        nodes.append(f'<decision name="{decision}"><switch>{cases}<default to="{default}"/></switch></decision>')
        return decision

    def sequence(next_name: str, depth: int, in_fork: bool) -> str:
        current = next_name
        for _ in range(draw(st.integers(1, 3 if depth == 0 else 2))):
            current = block(current, depth, in_fork)
        return current

    start = sequence("end", 0, False)
    nodes.append('<action name="handler"><pig><script>h</script></pig><ok to="fail"/><error to="fail"/></action>')
    return (
        f'<workflow-app name="random" xmlns="uri:oozie:workflow:1.0"><start to="{start}"/>'
        + "".join(nodes)
        + '<kill name="fail"><message>x</message></kill><end name="end"/></workflow-app>'
    )


@settings(
    max_examples=150, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow]
)
@given(workflow_xml=_workflows())
def test_random_workflows_are_equivalent(tmp_path_factory, workflow_xml: str):
    directory = tmp_path_factory.mktemp("random_app")
    (directory / "workflow.xml").write_text(workflow_xml)
    app = OozieApp(name="random", directory=directory, workflow_path=directory / "workflow.xml")
    mismatches = _check(app, directory / "bundle")
    assert mismatches == [], f"{workflow_xml}\n" + "\n".join(mismatches[:3])
