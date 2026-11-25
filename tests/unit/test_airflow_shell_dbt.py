"""Tests for dbt commands run from an Airflow BashOperator becoming native Databricks dbt tasks."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from flowx.bundler.dab_writer import main as package_main
from flowx.bundler.dab_writer import pipeline_dict_to_ir, write_bundle
from flowx.ir_serde import pipeline_to_dict
from flowx.models.ir import DbtCommandActivity, NotebookActivity, Pipeline, PlaceholderActivity
from flowx.preparer.workflow_preparer import prepare_workflow
from flowx.sources.airflow import shell_dbt
from flowx.sources.airflow.convert import main as convert_main
from flowx.sources.airflow.discover import main as discover_main
from flowx.sources.airflow.loader import load_pipelines


@pytest.mark.parametrize(
    ("command", "working_directory", "commands", "project_dir"),
    [
        ("$DBT_PATH snapshot --select snp", "/srv/proj", ["dbt snapshot --select snp"], "/srv/proj"),
        ("${DBT_BIN} run-operation merge_dim", "/srv/proj", ["dbt run-operation merge_dim"], "/srv/proj"),
        ("/opt/venv/bin/dbt build", "/srv/proj", ["dbt build"], "/srv/proj"),
        ("cd /opt/proj && dbt deps && dbt run", None, ["dbt deps", "dbt run"], "/opt/proj"),
        ("cd proj && dbt run", "/srv", ["dbt run"], "/srv/proj"),
        ("cd /p &&\ndbt run", None, ["dbt run"], "/p"),
        ("dbt run \\\n  --select a", "/p", ["dbt run --select a"], "/p"),
        ("cd /opt/proj &&\n  dbt deps &&\n  dbt run", None, ["dbt deps", "dbt run"], "/opt/proj"),
        ("\n    dbt run --select a\n", "/srv", ["dbt run --select a"], "/srv"),
        ("dbt run --project-dir=../other", "/srv/proj", ["dbt run"], "/srv/other"),
        ("dbt run --vars '{\"day\": 1}'", "/p", ["dbt run --vars '{\"day\": 1}'"], "/p"),
        ("dbt test", None, ["dbt test"], None),
    ],
)
def test_dbt_only_commands_are_recognized(
    command: str, working_directory: str | None, commands: list[str], project_dir: str | None
) -> None:
    parsed = shell_dbt.parse(command, working_directory)

    assert parsed is not None
    assert parsed.commands == commands
    assert parsed.project_dir == project_dir


def test_attached_short_target_option_is_dropped() -> None:
    parsed = shell_dbt.parse("dbt run -tprod --select a", "/p")

    assert parsed is not None
    assert parsed.commands == ["dbt run --select a"]
    assert parsed.dropped_options == ["-t prod"]


def test_home_folder_path_is_not_joined_onto_cwd() -> None:
    parsed = shell_dbt.parse("cd ~/proj && dbt run", "/opt")

    assert parsed is not None
    assert parsed.project_dir == "~/proj"


def test_profile_options_are_dropped_and_reported() -> None:
    parsed = shell_dbt.parse("dbt run --target prod --profiles-dir ~/.dbt --select a", "/p")

    assert parsed is not None
    assert parsed.commands == ["dbt run --select a"]
    assert parsed.dropped_options == ["--target prod", "--profiles-dir ~/.dbt"]


@pytest.mark.parametrize(
    "command",
    [
        "dbt run; dbt test",
        "cd /missing; dbt run",
        "cd proj\ndbt run",
        "dbt run &&",
        "&& dbt run",
        "dbt run && && dbt test",
        "dbt run --vars 'x\ny'",
        "dbt run -threads=4",
        'dbt run --vars "{\n  \\"day\\": 1}"',
        "dbt run\ndbt test",
        "dbt run || true",
        "dbt run | tee run.log",
        "dbt run > run.log",
        "dbt run &",
        "dbt run --select $MODEL",
        "dbt run --select {{ params.model }}",
        "dbt run $(cat args)",
        "source venv/bin/activate && dbt run",
        "dbt frobnicate",
        "dbt run && echo done",
        "dbt run --project-dir $PROJECT",
        "dbt run --target $ENV",
        "dbt run --project-dir /p/a && dbt test --project-dir /p/b",
        "echo hi",
    ],
)
def test_commands_doing_more_than_dbt_are_not_recognized(command: str) -> None:
    assert shell_dbt.parse(command, "/p") is None


def _load(tmp_path: Path, task: str) -> Pipeline:
    dag_path = tmp_path / "dags" / "dag.py"
    dag_path.parent.mkdir(exist_ok=True)
    dag_path.write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "with DAG(dag_id='d') as dag:\n"
        f"    t = {task}\n",
        encoding="utf-8",
    )
    return load_pipelines(tmp_path)[0]


def _task(pipeline: Pipeline):
    return next(task for task in pipeline.tasks if task.task_key == "t")


def test_bash_dbt_task_becomes_a_dbt_task_with_cwd_as_its_project(tmp_path: Path) -> None:
    project = _project(tmp_path)
    pipeline = _load(
        tmp_path,
        f"BashOperator(task_id='t', bash_command='$DBT_PATH run --select dim', cwd={str(project)!r}, retries=2)",
    )

    task = _task(pipeline)
    assert isinstance(task, DbtCommandActivity)
    assert task.commands == ["dbt run --select dim"]
    assert task.project_dir == str(project)
    assert task.max_retries == 2
    assert pipeline.reconciliation_status == "verified"
    disclosure = next(
        item for item in pipeline.audit["transformations"] if item["code"] == "shell_dbt_lowered_to_dbt_task"
    )
    assert disclosure["executable"] == "$DBT_PATH"
    arguments = next(
        item
        for item in pipeline.audit["transformations"]
        if item["code"] == "operator_arguments_classified" and item["capture_id"] == "t"
    )["arguments"]
    assert {"name": "cwd", "status": "consumed", "rationale": "operator_adapter"} in arguments


def test_project_on_the_airflow_machine_is_found_by_folder_name(tmp_path: Path) -> None:
    project = _project(tmp_path)

    pipeline = _load(
        tmp_path, "BashOperator(task_id='t', bash_command='dbt run', cwd='/home/ec2-user/airflow/intelsat_ca')"
    )

    task = _task(pipeline)
    assert isinstance(task, DbtCommandActivity)
    assert task.project_dir == str(project)
    disclosure = next(
        item for item in pipeline.audit["transformations"] if item["code"] == "shell_dbt_lowered_to_dbt_task"
    )
    assert disclosure["project_dir"] == "/home/ec2-user/airflow/intelsat_ca"


def test_discover_folder_under_a_target_folder_still_finds_the_project(tmp_path: Path) -> None:
    root = tmp_path / "target" / "repo"
    project = _project(root)

    pipeline = _load(
        root, "BashOperator(task_id='t', bash_command='dbt run', cwd='/home/ec2-user/airflow/intelsat_ca')"
    )

    task = _task(pipeline)
    assert isinstance(task, DbtCommandActivity)
    assert task.project_dir == str(project)


def test_two_same_named_projects_under_the_discover_folder_are_a_gap(tmp_path: Path) -> None:
    _project(tmp_path, parent="dags/team_a")
    _project(tmp_path, parent="dags/team_b")

    pipeline = _load(
        tmp_path, "BashOperator(task_id='t', bash_command='dbt run', cwd='/home/ec2-user/airflow/intelsat_ca')"
    )

    assert isinstance(_task(pipeline), PlaceholderActivity)


def test_relative_project_path_says_it_must_be_absolute(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "BashOperator(task_id='t', bash_command='dbt run', cwd='projects/intelsat_ca')")

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert "is relative to a working directory flowx does not know" in task.comment


def test_project_not_found_where_flowx_runs_is_a_gap(tmp_path: Path) -> None:
    pipeline = _load(
        tmp_path, "BashOperator(task_id='t', bash_command='dbt run', cwd='/home/ec2-user/airflow/missing')"
    )

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert "was not found" in task.comment
    assert pipeline.reconciliation_status == "verified_with_gaps"


def test_cwd_stays_a_gap_for_a_command_that_is_not_dbt(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "BashOperator(task_id='t', bash_command='make all', cwd='/srv/proj')")

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert "cwd" in task.comment


def test_dbt_without_a_known_project_is_a_gap(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "BashOperator(task_id='t', bash_command='dbt run')")

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert "cannot tell which dbt project" in task.comment
    assert task.raw_definition["dbt_commands"] == ["dbt run"]


def test_dbt_with_a_dynamic_cwd_is_not_lowered(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "BashOperator(task_id='t', bash_command='dbt run', cwd=PROJECT_DIR)")

    assert isinstance(_task(pipeline), PlaceholderActivity)


def test_dbt_with_other_unrepresented_arguments_is_a_gap(tmp_path: Path) -> None:
    project = _project(tmp_path)
    pipeline = _load(
        tmp_path, f"BashOperator(task_id='t', bash_command='dbt run', cwd={str(project)!r}, env={{'A': '1'}})"
    )

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert "cwd, env" in task.comment
    assert not any(item["code"] == "shell_dbt_lowered_to_dbt_task" for item in pipeline.audit["transformations"])
    arguments = next(
        item
        for item in pipeline.audit["transformations"]
        if item["code"] == "operator_arguments_classified" and item["capture_id"] == "t"
    )["arguments"]
    assert {"name": "cwd", "status": "unconsumed", "rationale": "no_declared_semantics"} in arguments


def _bundle(tmp_path: Path, activity: DbtCommandActivity) -> tuple[dict, Path]:
    workflow = prepare_workflow(Pipeline(name="p", tasks=[activity]))
    out = tmp_path / "bundle"
    write_bundle(workflow, out, catalog="main", schema="a")
    job = yaml.safe_load(next((out / "resources").glob("*.yml")).read_text())
    return next(iter(job["resources"]["jobs"].values())), out


def _project(tmp_path: Path, files: dict[str, str] | None = None, parent: str = "dags") -> Path:
    project = tmp_path / parent / "intelsat_ca"
    (project / "models").mkdir(parents=True)
    (project / "dbt_packages" / "dbt_utils").mkdir(parents=True)
    (project / "dbt_packages" / "dbt_utils" / "dbt_project.yml").write_text("name: dbt_utils\n", encoding="utf-8")
    (project / "profiles.yml").write_text("intelsat_ca:\n  outputs: {}\n", encoding="utf-8")
    (project / "dbt_project.yml").write_text("name: intelsat_ca\n", encoding="utf-8")
    (project / "models" / "dim.sql").write_text("select 1\n", encoding="utf-8")
    for name, content in (files or {}).items():
        (project / name).write_text(content, encoding="utf-8")
    return project


def test_dbt_task_runs_on_the_warehouse_with_the_project_in_the_bundle(tmp_path: Path) -> None:
    project = _project(tmp_path)
    job, out = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    task = job["tasks"][0]
    folder = task["dbt_task"]["project_directory"]
    assert folder.startswith("../src/dbt_projects/intelsat_ca_")
    assert task["dbt_task"] == {
        "project_directory": folder,
        "commands": ["dbt run"],
        "warehouse_id": "${var.warehouse_id}",
        "catalog": "${var.catalog}",
        "schema": "${var.schema}",
        "source": "WORKSPACE",
    }
    assert task["job_cluster_key"] == "default_cluster"
    copied = out / folder.removeprefix("../")
    assert (copied / "models" / "dim.sql").is_file()
    assert not (copied / "profiles.yml").exists()
    assert not (copied / "dbt_packages").exists()
    assert "warehouse_id" in yaml.safe_load((out / "databricks.yml").read_text())["variables"]


def test_project_with_packages_installs_them_first(tmp_path: Path) -> None:
    project = _project(tmp_path, {"packages.yml": "packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n"})
    job, _ = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    assert job["tasks"][0]["dbt_task"]["commands"] == ["dbt deps", "dbt run"]


def test_unparseable_package_file_still_adds_deps(tmp_path: Path) -> None:
    project = _project(tmp_path, {"packages.yml": "packages: [\n"})
    job, _ = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    assert job["tasks"][0]["dbt_task"]["commands"] == ["dbt deps", "dbt run"]


def test_package_file_declaring_no_packages_adds_no_deps(tmp_path: Path) -> None:
    project = _project(tmp_path, {"packages.yml": "packages: []\n"})
    job, _ = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    assert job["tasks"][0]["dbt_task"]["commands"] == ["dbt run"]


def test_bundle_folder_follows_the_source_path_not_the_local_checkout(tmp_path: Path) -> None:
    first = _project(tmp_path, parent="checkout_a")
    second = _project(tmp_path, parent="checkout_b")
    source = "/home/ec2-user/airflow/intelsat_ca"

    folders = {
        task["dbt_task"]["project_directory"]
        for project in (first, second)
        for task in prepare_workflow(
            Pipeline(
                name="p",
                tasks=[
                    DbtCommandActivity(
                        name="t",
                        task_key="t",
                        commands=["dbt run"],
                        project_dir=str(project),
                        source_project_dir=source,
                    )
                ],
            )
        ).tasks
    }

    assert len(folders) == 1


def test_missing_project_becomes_a_failing_setup_task(tmp_path: Path) -> None:
    job, out = _bundle(
        tmp_path,
        DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir="/home/ec2-user/airflow/proj"),
    )

    task = job["tasks"][0]
    assert "dbt_task" not in task
    notebook = out / "src" / "notebooks" / "t_dbt_project_required.py"
    assert "Make it available at that path" in notebook.read_text()


def test_projects_with_the_same_folder_name_get_separate_folders(tmp_path: Path) -> None:
    first = _project(tmp_path, parent="team_a")
    second = _project(tmp_path, parent="team_b")
    workflow = prepare_workflow(
        Pipeline(
            name="p",
            tasks=[
                DbtCommandActivity(name="a", task_key="a", commands=["dbt run"], project_dir=str(first)),
                DbtCommandActivity(name="b", task_key="b", commands=["dbt run"], project_dir=str(second)),
            ],
        )
    )

    folders = {task["dbt_task"]["project_directory"] for task in workflow.tasks}
    assert len(folders) == 2


def test_dbt_command_activity_round_trips_through_the_report() -> None:
    pipeline = Pipeline(
        name="p",
        tasks=[
            NotebookActivity(name="a", task_key="a", notebook_path="notebooks/a.py"),
            DbtCommandActivity(
                name="t",
                task_key="t",
                commands=["dbt deps", "dbt run"],
                project_dir="/srv/proj",
                source_project_dir="/home/ec2-user/airflow/proj",
            ),
        ],
    )

    rehydrated, _ = pipeline_dict_to_ir(pipeline_to_dict(pipeline))

    task = rehydrated.tasks[1]
    assert isinstance(task, DbtCommandActivity)
    assert task.commands == ["dbt deps", "dbt run"]
    assert task.project_dir == "/srv/proj"
    assert task.source_project_dir == "/home/ec2-user/airflow/proj"


def test_only_the_files_dbt_reads_are_copied_into_the_bundle(tmp_path: Path) -> None:
    project = _project(tmp_path, {".env": "DBT_PASSWORD=secret\n", "service_account.json": "{}\n"})
    (project / "macros").mkdir()
    (project / "macros" / "merge.sql").write_text("{% macro merge() %}{% endmacro %}\n", encoding="utf-8")
    (project / "models" / ".secrets").write_text("token\n", encoding="utf-8")
    _, out = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    copied = next((out / "src" / "dbt_projects").iterdir())
    files = sorted(path.relative_to(copied).as_posix() for path in copied.rglob("*") if path.is_file())
    assert files == ["dbt_project.yml", "macros/merge.sql", "models/dim.sql"]


def test_folders_named_in_dbt_project_yml_are_copied(tmp_path: Path) -> None:
    project = _project(tmp_path)
    (project / "dbt_project.yml").write_text("name: intelsat_ca\nmodel-paths: ['transform']\n", encoding="utf-8")
    (project / "transform").mkdir()
    (project / "transform" / "orders.sql").write_text("select 1\n", encoding="utf-8")
    _, out = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    copied = next((out / "src" / "dbt_projects").iterdir())
    assert (copied / "transform" / "orders.sql").is_file()
    assert not (copied / "models").exists()


def test_home_folder_project_is_a_gap(tmp_path: Path) -> None:
    pipeline = _load(tmp_path, "BashOperator(task_id='t', bash_command='cd ~/proj && dbt run', cwd='/opt')")

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert "home folder of the source machine" in task.comment


def test_project_reading_env_var_is_a_gap(tmp_path: Path) -> None:
    project = _project(tmp_path)
    (project / "models" / "dim.sql").write_text(
        "select * from {{ env_var('DBT_SOURCE_SCHEMA') }}.orders\n", encoding="utf-8"
    )

    pipeline = _load(tmp_path, f"BashOperator(task_id='t', bash_command='dbt run', cwd={str(project)!r})")

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert "$DBT_SOURCE_SCHEMA" in task.comment
    assert task.raw_definition["environment_variables"] == ["DBT_SOURCE_SCHEMA"]


def test_relative_discover_folder_stores_an_absolute_project_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _project(tmp_path)
    _load(tmp_path, "BashOperator(task_id='t', bash_command='dbt run', cwd='/home/ec2-user/airflow/intelsat_ca')")
    monkeypatch.chdir(tmp_path)

    task = _task(load_pipelines(Path("dags"))[0])

    assert isinstance(task, DbtCommandActivity)
    assert Path(task.project_dir).is_absolute()


def test_deps_runs_after_a_leading_clean(tmp_path: Path) -> None:
    project = _project(tmp_path, {"packages.yml": "packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n"})
    job, _ = _bundle(
        tmp_path,
        DbtCommandActivity(name="t", task_key="t", commands=["dbt clean", "dbt build"], project_dir=str(project)),
    )

    assert job["tasks"][0]["dbt_task"]["commands"] == ["dbt clean", "dbt deps", "dbt build"]


def test_project_with_models_at_its_root_copies_root_resource_files(tmp_path: Path) -> None:
    project = tmp_path / "flat"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: flat\nmodel-paths: ['.']\n", encoding="utf-8")
    (project / "orders.sql").write_text("select 1\n", encoding="utf-8")
    (project / "schema.yml").write_text("version: 2\n", encoding="utf-8")
    (project / "profiles.yml").write_text("flat: {}\n", encoding="utf-8")
    (project / "service_account.json").write_text("{}\n", encoding="utf-8")
    (project / "target").mkdir()
    (project / "target" / "compiled.sql").write_text("select 1\n", encoding="utf-8")
    _, out = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    copied = next((out / "src" / "dbt_projects").iterdir())
    files = sorted(path.relative_to(copied).as_posix() for path in copied.rglob("*") if path.is_file())
    assert files == ["dbt_project.yml", "orders.sql", "schema.yml"]


def test_jinja_cwd_is_not_lowered(tmp_path: Path) -> None:
    _project(tmp_path)

    pipeline = _load(
        tmp_path, "BashOperator(task_id='t', bash_command='dbt run', cwd='/opt/{{ var.value.env }}/intelsat_ca')"
    )

    assert not isinstance(_task(pipeline), DbtCommandActivity)


def test_env_var_with_a_default_is_not_a_gap(tmp_path: Path) -> None:
    project = _project(tmp_path)
    (project / "models" / "dim.sql").write_text(
        "select * from {{ env_var('DBT_SOURCE_SCHEMA', 'raw') }}.orders\n", encoding="utf-8"
    )

    pipeline = _load(tmp_path, f"BashOperator(task_id='t', bash_command='dbt run', cwd={str(project)!r})")

    assert isinstance(_task(pipeline), DbtCommandActivity)


def test_local_package_inside_the_project_is_copied(tmp_path: Path) -> None:
    project = _project(tmp_path, {"packages.yml": "packages:\n  - local: internal_pkg\n"})
    (project / "internal_pkg").mkdir()
    (project / "internal_pkg" / "dbt_project.yml").write_text("name: internal_pkg\n", encoding="utf-8")
    _, out = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    copied = next((out / "src" / "dbt_projects").iterdir())
    assert (copied / "internal_pkg" / "dbt_project.yml").is_file()


def test_local_package_outside_the_project_is_a_gap(tmp_path: Path) -> None:
    project = _project(tmp_path, {"packages.yml": "packages:\n  - local: ../shared\n"})

    pipeline = _load(tmp_path, f"BashOperator(task_id='t', bash_command='dbt run', cwd={str(project)!r})")

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert task.raw_definition["local_packages"] == ["../shared"]


def test_more_commands_than_a_dbt_task_runs_is_a_gap(tmp_path: Path) -> None:
    project = _project(tmp_path, {"packages.yml": "packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n"})
    command = " && ".join(["dbt run"] * 10)

    pipeline = _load(tmp_path, f"BashOperator(task_id='t', bash_command={command!r}, cwd={str(project)!r})")

    task = _task(pipeline)
    assert isinstance(task, PlaceholderActivity)
    assert "11 dbt commands counting the dbt deps" in task.comment


def test_root_model_path_skips_build_output_and_virtual_environments(tmp_path: Path) -> None:
    project = tmp_path / "flat"
    (project / "build").mkdir(parents=True)
    (project / "dbt_project.yml").write_text("name: flat\nmodel-paths: ['.']\ntarget-path: build\n", encoding="utf-8")
    (project / "orders.sql").write_text("select 1\n", encoding="utf-8")
    (project / "build" / "compiled.sql").write_text("select 1\n", encoding="utf-8")
    (project / "venv" / "lib").mkdir(parents=True)
    (project / "venv" / "pyvenv.cfg").write_text("home = /usr/bin\n", encoding="utf-8")
    (project / "venv" / "lib" / "module.py").write_text("x = 1\n", encoding="utf-8")
    _, out = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    copied = next((out / "src" / "dbt_projects").iterdir())
    files = sorted(path.relative_to(copied).as_posix() for path in copied.rglob("*") if path.is_file())
    assert files == ["dbt_project.yml", "orders.sql"]


def test_setup_lists_the_warehouse_a_dbt_task_needs(tmp_path: Path) -> None:
    project = _project(tmp_path)
    _, out = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    assert "| `warehouse_id` |" in (out / "SETUP.md").read_text(encoding="utf-8")


def test_two_dags_running_dbt_point_at_their_own_project_copy(tmp_path: Path) -> None:
    dags = tmp_path / "dags"
    project = _project(tmp_path)
    for dag_id, task in (
        ("dag_a", f"BashOperator(task_id='run_dbt', bash_command='cd {project} && dbt run')"),
        ("dag_b", f"BashOperator(task_id='test_dbt', bash_command='dbt test', cwd={str(project)!r})"),
    ):
        (dags / f"{dag_id}.py").write_text(
            "from airflow import DAG\n"
            "from airflow.operators.bash import BashOperator\n"
            f"with DAG(dag_id={dag_id!r}) as dag:\n"
            f"    {task}\n",
            encoding="utf-8",
        )
    output = tmp_path / "out"
    discover_main(["--source-dir", str(dags), "--output-dir", str(output)])
    convert_main(["--source-dir", str(dags), "--output-dir", str(output)])

    assert package_main(["--output-dir", str(output), "--no-download-workspace-files"]) == 0

    for dag_id in ("dag_a", "dag_b"):
        job = yaml.safe_load((output / "resources" / f"{dag_id}.yml").read_text(encoding="utf-8"))
        folder = job["resources"]["jobs"][dag_id]["tasks"][0]["dbt_task"]["project_directory"]
        assert (output / folder.removeprefix("../") / "models" / "dim.sql").is_file()


def test_root_model_path_exclusions_are_paths_from_the_project_root(tmp_path: Path) -> None:
    project = tmp_path / "flat"
    (project / "build" / "target").mkdir(parents=True)
    (project / "dbt_project.yml").write_text(
        "name: flat\nmodel-paths: ['.']\ntarget-path: ./build/target\n", encoding="utf-8"
    )
    (project / "build" / "target" / "compiled.sql").write_text("select 1\n", encoding="utf-8")
    (project / "models" / "env").mkdir(parents=True)
    (project / "models" / "env" / "orders.sql").write_text("select 1\n", encoding="utf-8")
    (project / "env").mkdir()
    (project / "env" / "pyvenv.cfg").write_text("home = /usr/bin\n", encoding="utf-8")
    (project / "env" / "module.py").write_text("x = 1\n", encoding="utf-8")
    _, out = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    copied = next((out / "src" / "dbt_projects").iterdir())
    files = sorted(path.relative_to(copied).as_posix() for path in copied.rglob("*") if path.is_file())
    assert files == ["dbt_project.yml", "models/env/orders.sql"]


def test_preparer_refuses_more_commands_than_a_dbt_task_runs(tmp_path: Path) -> None:
    project = _project(tmp_path)
    job, out = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"] * 11, project_dir=str(project))
    )

    task = job["tasks"][0]
    assert "dbt_task" not in task
    assert "at most 10" in (out / "src" / "notebooks" / "t_dbt_too_many_commands.py").read_text()


def test_preparer_refuses_a_local_package_outside_the_project(tmp_path: Path) -> None:
    project = _project(tmp_path, {"packages.yml": "packages:\n  - local: ../shared\n"})
    job, out = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    assert "dbt_task" not in job["tasks"][0]
    assert "../shared" in (out / "src" / "notebooks" / "t_dbt_local_packages.py").read_text()


def test_root_model_path_skips_profiles_packages_credentials_and_conda_at_any_depth(tmp_path: Path) -> None:
    project = tmp_path / "flat"
    for folder in ("config", "sub/dbt_packages/utils", "conda/conda-meta", "models"):
        (project / folder).mkdir(parents=True)
    (project / "dbt_project.yml").write_text("name: flat\nmodel-paths: ['.']\n", encoding="utf-8")
    (project / "config" / "profiles.yml").write_text("flat: {}\n", encoding="utf-8")
    (project / "sub" / "dbt_packages" / "utils" / "macro.sql").write_text("select 1\n", encoding="utf-8")
    (project / "conda" / "module.py").write_text("x = 1\n", encoding="utf-8")
    (project / "secrets.yml").write_text("token: x\n", encoding="utf-8")
    (project / "models" / "secret_sauce.sql").write_text("select 1\n", encoding="utf-8")
    _, out = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    copied = next((out / "src" / "dbt_projects").iterdir())
    files = sorted(path.relative_to(copied).as_posix() for path in copied.rglob("*") if path.is_file())
    assert files == ["dbt_project.yml", "models/secret_sauce.sql"]


def test_root_copy_keeps_yaml_whose_name_only_mentions_credentials(tmp_path: Path) -> None:
    project = tmp_path / "flat"
    (project / "models").mkdir(parents=True)
    (project / "dbt_project.yml").write_text("name: flat\nmodel-paths: ['.']\n", encoding="utf-8")
    (project / "credentials.yml").write_text("token: x\n", encoding="utf-8")
    for name in ("customer_credentials.sql", "customer_credentials.yml", "secret_santa.yml"):
        (project / "models" / name).write_text("version: 2\n", encoding="utf-8")
    _, out = _bundle(
        tmp_path, DbtCommandActivity(name="t", task_key="t", commands=["dbt run"], project_dir=str(project))
    )

    copied = next((out / "src" / "dbt_projects").iterdir())
    files = sorted(path.relative_to(copied).as_posix() for path in copied.rglob("*") if path.is_file())
    assert files == [
        "dbt_project.yml",
        "models/customer_credentials.sql",
        "models/customer_credentials.yml",
        "models/secret_santa.yml",
    ]
