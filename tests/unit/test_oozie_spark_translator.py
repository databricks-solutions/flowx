"""Translating Oozie <spark> actions into spark_jar_task / spark_python_task with faithful compute."""

from __future__ import annotations

from pathlib import Path

import pytest

from flowx.models.ir import PlaceholderActivity, SparkJarActivity, SparkPythonActivity
from flowx.sources.oozie.loader.discovery import OozieApp
from flowx.sources.oozie.loader.el_render import ElEnvironment
from flowx.sources.oozie.loader.parse import parse_workflow
from flowx.sources.oozie.loader.properties import PropertyScope
from flowx.sources.oozie.loader.report import AppReport
from flowx.sources.oozie.translators import ActionContext, translate_action


def _context(tmp_path: Path, **properties: str) -> ActionContext:
    (tmp_path / "lib").mkdir(exist_ok=True)
    (tmp_path / "lib" / "etl.jar").write_bytes(b"jar")
    (tmp_path / "lib" / "deps.jar").write_bytes(b"jar")
    (tmp_path / "job.py").write_text("print('hi')\n")
    app = OozieApp(name="app", directory=tmp_path, workflow_path=tmp_path / "workflow.xml")
    environment = ElEnvironment(
        properties=PropertyScope(layers=({"nameNode": "hdfs://ns1", **properties},)),
        workflow_name="daily",
        action_task_keys={"load": "load"},
    )
    return ActionContext(task_key="load", app=app, environment=environment, report=AppReport())


def _spark(inner: str):
    workflow = parse_workflow(
        '<workflow-app name="w"><start to="load"/><action name="load">'
        f'<spark xmlns="uri:oozie:spark-action:1.0">{inner}</spark><ok to="end"/><error to="end"/></action>'
        '<end name="end"/></workflow-app>'
    )
    return workflow.nodes["load"]


def test_jar_action_with_conf_sizing_and_libraries(tmp_path: Path):
    context = _context(tmp_path, runDate="2024-01-01")
    translated = translate_action(
        _spark(
            "<master>yarn</master><mode>cluster</mode><class>com.ek.Load</class>"
            "<jar>${nameNode}/apps/daily/lib/etl.jar</jar>"
            "<spark-opts>--num-executors 4 --executor-cores 2 --executor-memory 8g --driver-memory 4g "
            "--conf spark.sql.shuffle.partitions=64 --conf spark.yarn.maxAppAttempts=1 --queue etl "
            "--jars lib/deps.jar --packages org.apache.spark:spark-avro_2.12:3.3.0</spark-opts>"
            "<arg>--date</arg><arg>${runDate}</arg>"
            "<configuration><property><name>fs.s3a.connection.maximum</name><value>50</value></property>"
            "<property><name>oozie.launcher.mapreduce.map.memory.mb</name><value>2048</value></property></configuration>"
        ),
        context,
    )
    task = translated.main
    assert isinstance(task, SparkJarActivity)
    assert task.main_class_name == "com.ek.Load"
    assert task.parameters == ["--date", "{{job.parameters.runDate}}"]
    assert task.libraries == [
        {"jar": f"file:{tmp_path / 'lib' / 'etl.jar'}"},
        {"jar": f"file:{tmp_path / 'lib' / 'deps.jar'}"},
        {"maven": {"coordinates": "org.apache.spark:spark-avro_2.12:3.3.0"}},
    ]
    spec = task.cluster["_job_cluster_spec"]
    assert spec["num_workers"] == 4
    assert spec["spark_conf"] == {
        "spark.sql.shuffle.partitions": "64",
        "spark.hadoop.fs.s3a.connection.maximum": "50",
    }
    dropped = {entry["key"] for entry in context.report.dropped_spark_conf}
    assert dropped >= {"spark.yarn.maxAppAttempts", "--queue", "oozie.launcher.mapreduce.map.memory.mb"}
    assert "executor-memory=8g" in (task.description or "")
    assert {entry["rule"] for entry in context.report.transformations} >= {
        "SPARK-JAR",
        "SPARK-CONF",
        "SPARK-CONF-DROP",
        "SPARK-SIZING",
        "SPARK-MAVEN",
    }
    assert context.report.hdfs_paths[0]["field"] == "jar"


def test_dynamic_allocation_becomes_autoscale(tmp_path: Path):
    context = _context(tmp_path)
    translated = translate_action(
        _spark(
            "<class>c.M</class><jar>lib/etl.jar</jar><spark-opts>--conf spark.dynamicAllocation.enabled=true "
            "--conf spark.dynamicAllocation.minExecutors=2 --conf spark.dynamicAllocation.maxExecutors=10</spark-opts>"
        ),
        context,
    )
    spec = translated.main.cluster["_job_cluster_spec"]
    assert spec["autoscale"] == {"min_workers": 2, "max_workers": 10} and "num_workers" not in spec
    assert "spark_conf" not in spec or not any(key.startswith("spark.dynamicAllocation") for key in spec["spark_conf"])
    assert "SPARK-AUTOSCALE" in {entry["rule"] for entry in context.report.transformations}


def test_python_action_with_local_master(tmp_path: Path):
    context = _context(tmp_path)
    translated = translate_action(_spark("<master>local[*]</master><jar>job.py</jar><arg>x</arg>"), context)
    task = translated.main
    assert isinstance(task, SparkPythonActivity)
    assert task.generated_source == "print('hi')\n" and task.parameters == ["x"]
    spec = task.cluster["_job_cluster_spec"]
    assert spec["num_workers"] == 0 and spec["spark_conf"]["spark.master"] == "local[*]"


def test_quoted_spark_opts_and_args(tmp_path: Path):
    context = _context(tmp_path)
    translated = translate_action(
        _spark(
            "<class>c.M</class><jar>lib/etl.jar</jar>"
            "<spark-opts>--conf \"spark.driver.extraJavaOptions=-Da=1 -Db=2\" --conf spark.x='a b'</spark-opts>"
            "<arg>--filter</arg><arg>name = 'x y'</arg>"
        ),
        context,
    )
    task = translated.main
    assert task.cluster["_job_cluster_spec"]["spark_conf"] == {
        "spark.driver.extraJavaOptions": "-Da=1 -Db=2",
        "spark.x": "a b",
    }
    assert task.parameters == ["--filter", "name = 'x y'"]


def test_missing_jar_is_reported_not_fatal(tmp_path: Path):
    context = _context(tmp_path)
    translated = translate_action(_spark("<class>c.M</class><jar>${nameNode}/apps/lib/gone.jar</jar>"), context)
    assert isinstance(translated.main, SparkJarActivity)
    assert translated.main.libraries == [
        {"jar": "/apps/lib/gone.jar"},
        {"jar": f"file:{tmp_path / 'lib' / 'deps.jar'}"},
        {"jar": f"file:{tmp_path / 'lib' / 'etl.jar'}"},
    ]
    assert context.report.missing_files == [{"action": "load", "reference": "${nameNode}/apps/lib/gone.jar"}]


def test_default_executors_and_deferred_features_are_reported(tmp_path: Path):
    context = _context(tmp_path)
    translated = translate_action(
        _spark("<class>c.M</class><jar>lib/etl.jar</jar><prepare><delete path='/tmp/x'/></prepare><file>a.csv</file>"),
        context,
    )
    assert translated.main.cluster["_job_cluster_spec"]["num_workers"] == 2
    codes = {finding["code"] for finding in context.report.findings}
    assert "spark_deferred_feature" in codes
    assert "SPARK-DEFERRED" in {entry["rule"] for entry in context.report.transformations}


@pytest.mark.parametrize(
    "inner, reason_fragment",
    [
        ("<jar>lib/etl.jar</jar>", "main class"),
        ("<class>c.M</class><jar>lib/etl.jar</jar><spark-opts>--conf 'unterminated</spark-opts>", "spark-opts"),
        ("<class>c.M</class><jar>lib/etl.jar</jar><arg>${fs:exists('/x')}</arg>", "argument"),
    ],
)
def test_untranslatable_spark_actions_fall_back_to_agentic(tmp_path: Path, inner: str, reason_fragment: str):
    translated = translate_action(_spark(inner), _context(tmp_path))
    assert isinstance(translated.main, PlaceholderActivity) and reason_fragment in (translated.main.comment or "")


def _jar(tmp_path: Path, options: str, **properties: str):
    context = _context(tmp_path, **properties)
    translated = translate_action(
        _spark(f"<class>c.M</class><jar>lib/etl.jar</jar><spark-opts>{options}</spark-opts>"), context
    )
    return translated.main, context


def test_local_master_with_cores_still_uses_star_and_records_original(tmp_path: Path):
    context = _context(tmp_path)
    task = translate_action(_spark("<master>local[2]</master><jar>job.py</jar>"), context).main
    spec = task.cluster["_job_cluster_spec"]
    assert spec["spark_conf"]["spark.master"] == "local[*]"
    assert "original master: local[2]" in (task.description or "")


def test_single_node_spec_is_not_shared_between_actions(tmp_path: Path):
    first = translate_action(_spark("<master>local</master><jar>job.py</jar>"), _context(tmp_path)).main
    first.cluster["_job_cluster_spec"]["custom_tags"]["x"] = "y"
    second = translate_action(_spark("<master>local</master><jar>job.py</jar>"), _context(tmp_path)).main
    assert "x" not in second.cluster["_job_cluster_spec"]["custom_tags"]


def test_sizing_values_go_through_el(tmp_path: Path):
    task, _ = _jar(tmp_path, "--num-executors ${n}", n="5")
    assert task.cluster["_job_cluster_spec"]["num_workers"] == 5


def test_non_integer_sizing_falls_back_to_agentic(tmp_path: Path):
    translated = translate_action(
        _spark("<class>c.M</class><jar>lib/etl.jar</jar><spark-opts>--num-executors ${n}</spark-opts>"),
        _context(tmp_path, n="many"),
    )
    assert isinstance(translated.main, PlaceholderActivity) and "whole number" in (translated.main.comment or "")


def test_unknown_flag_does_not_swallow_the_next_flag(tmp_path: Path):
    task, context = _jar(tmp_path, "--foo --conf a=b --bar baz")
    assert task.cluster["_job_cluster_spec"]["spark_conf"] == {"a": "b"}
    details = [f["details"] for f in context.report.findings if f["code"] == "spark_unknown_option"]
    assert {"option": "--bar", "consumed_value": "baz"} in details


def test_trailing_value_flag_without_value_is_agentic(tmp_path: Path):
    translated = translate_action(
        _spark("<class>c.M</class><jar>lib/etl.jar</jar><spark-opts>--conf</spark-opts>"), _context(tmp_path)
    )
    assert isinstance(translated.main, PlaceholderActivity) and "has no value" in (translated.main.comment or "")


def test_sizing_through_conf_and_cli_precedence(tmp_path: Path):
    task, context = _jar(
        tmp_path,
        "--conf spark.executor.instances=7 --conf spark.executor.memory=6g --conf spark.cores.max=20 "
        "--conf spark.driver.cores=2 --conf spark.executor.cores=3 --conf spark.driver.memory=2g",
    )
    spec = task.cluster["_job_cluster_spec"]
    assert spec["num_workers"] == 7 and "spark_conf" not in spec
    assert "executor-memory=6g" in task.description and "total-executor-cores=20" in task.description
    both, _ = _jar(tmp_path, "--conf spark.executor.instances=7 --num-executors 3")
    assert both.cluster["_job_cluster_spec"]["num_workers"] == 3


def test_cli_java_options_win_regardless_of_order(tmp_path: Path):
    for options in (
        "--driver-java-options -Dcli=1 --conf spark.driver.extraJavaOptions=-Dconf=1",
        "--conf spark.driver.extraJavaOptions=-Dconf=1 --driver-java-options -Dcli=1",
    ):
        task, _ = _jar(tmp_path, options)
        assert task.cluster["_job_cluster_spec"]["spark_conf"] == {"spark.driver.extraJavaOptions": "-Dcli=1"}


def test_security_and_cdp_settings_are_dropped(tmp_path: Path):
    context = _context(tmp_path)
    translated = translate_action(
        _spark(
            "<class>c.M</class><jar>lib/etl.jar</jar><spark-opts>--conf spark.authenticate=true "
            "--conf spark.ssl.enabled=true --conf spark.ui.port=4050 --conf spark.sql.warehouse.dir=/w "
            "--conf spark.executor.extraClassPath=/x --conf spark.keep=1 "
            '--conf "spark.executor.extraJavaOptions=-Djava.security.krb5.conf=/etc/krb5.conf -Dkeep=1 '
            '-Djavax.security.auth.useSubjectCredsOnly=false" '
            '--conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/j.conf"</spark-opts>'
            "<configuration><property><name>fs.defaultFS</name><value>hdfs://x</value></property>"
            "<property><name>dfs.nameservices</name><value>ns</value></property>"
            "<property><name>hadoop.security.authentication</name><value>kerberos</value></property>"
            "</configuration>"
        ),
        context,
    )
    assert translated.main.cluster["_job_cluster_spec"]["spark_conf"] == {
        "spark.keep": "1",
        "spark.executor.extraJavaOptions": "-Dkeep=1",
    }
    dropped = [(entry["key"], entry["value"]) for entry in context.report.dropped_spark_conf]
    for key in (
        "spark.authenticate",
        "spark.ssl.enabled",
        "spark.ui.port",
        "spark.sql.warehouse.dir",
        "spark.executor.extraClassPath",
        "spark.hadoop.fs.defaultFS",
        "spark.hadoop.dfs.nameservices",
        "spark.hadoop.hadoop.security.authentication",
    ):
        assert any(dropped_key == key for dropped_key, _ in dropped), key
    assert ("spark.executor.extraJavaOptions", "-Djava.security.krb5.conf=/etc/krb5.conf") in dropped
    assert ("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=/j.conf") in dropped


def test_unused_dynamic_allocation_is_reported(tmp_path: Path):
    task, context = _jar(tmp_path, "--conf spark.dynamicAllocation.enabled=true")
    assert task.cluster["_job_cluster_spec"]["num_workers"] == 2
    assert "spark_dynamic_allocation_ignored" in {finding["code"] for finding in context.report.findings}


def test_property_driven_conf_is_expanded_to_its_value(tmp_path: Path):
    task, context = _jar(tmp_path, "--conf spark.sql.shuffle.partitions=${parts}", parts="200")
    assert task.cluster["_job_cluster_spec"]["spark_conf"] == {"spark.sql.shuffle.partitions": "200"}
    assert context.environment.job_parameters == {}


def test_conf_built_from_a_derived_property_is_fully_expanded(tmp_path: Path):
    task, _ = _jar(tmp_path, "--conf spark.local.dir=${scratch}", base="/tmp", scratch="${base}/spark")
    assert task.cluster["_job_cluster_spec"]["spark_conf"] == {"spark.local.dir": "/tmp/spark"}


@pytest.mark.parametrize(
    "options",
    [
        "--conf spark.sql.shuffle.partitions=${undefinedParts}",
        "--conf spark.app.id=${wf:id()}",
        "--driver-java-options -Drun=${wf:id()}",
        "--driver-class-path ${undefinedPath}",
    ],
)
def test_conf_that_stays_dynamic_is_agentic(tmp_path: Path, options: str):
    context = _context(tmp_path)
    translated = translate_action(
        _spark(f"<class>c.M</class><jar>lib/etl.jar</jar><spark-opts>{options}</spark-opts>"), context
    )
    assert isinstance(translated.main, PlaceholderActivity)
    assert "can't be resolved at convert time" in (translated.main.comment or "")
    assert context.environment.job_parameters == {} and context.environment.unresolved_properties == set()


def test_configuration_value_from_a_property_is_static(tmp_path: Path):
    context = _context(tmp_path, maxConnections="50")
    translated = translate_action(
        _spark(
            "<class>c.M</class><jar>lib/etl.jar</jar><configuration><property><name>fs.s3a.connection.maximum</name>"
            "<value>${maxConnections}</value></property></configuration>"
        ),
        context,
    )
    assert translated.main.cluster["_job_cluster_spec"]["spark_conf"] == {
        "spark.hadoop.fs.s3a.connection.maximum": "50"
    }


@pytest.mark.parametrize(
    "inner, what",
    [
        ("<class>${mainClass}</class><jar>lib/etl.jar</jar>", "main class"),
        ("<class>c.M</class><jar>lib/etl.jar</jar><spark-opts>--jars ${depsDir}/gone.jar</spark-opts>", "library path"),
        ("<jar>${scriptDir}/gone.py</jar>", "python file"),
    ],
)
def test_task_fields_without_dynamic_references_need_static_values(tmp_path: Path, inner: str, what: str):
    translated = translate_action(_spark(inner), _context(tmp_path))
    assert isinstance(translated.main, PlaceholderActivity) and what in (translated.main.comment or "")
    static = translate_action(_spark(inner), _context(tmp_path, mainClass="c.M", depsDir="/d", scriptDir="/s")).main
    assert not isinstance(static, PlaceholderActivity)


def test_arguments_still_use_job_parameter_references(tmp_path: Path):
    context = _context(tmp_path, runDate="2024-01-01")
    translated = translate_action(_spark("<class>c.M</class><jar>lib/etl.jar</jar><arg>${runDate}</arg>"), context)
    assert translated.main.parameters == ["{{job.parameters.runDate}}"]
    assert context.environment.job_parameters == {"runDate": "2024-01-01"}


def test_other_lib_jars_are_attached_once_in_sorted_order(tmp_path: Path):
    context = _context(tmp_path)
    (tmp_path / "lib" / "aaa.jar").write_bytes(b"jar")
    (tmp_path / "lib" / "notes.txt").write_text("not a jar")
    translated = translate_action(_spark("<class>c.M</class><jar>lib/etl.jar</jar>"), context)
    assert translated.main.libraries == [
        {"jar": f"file:{tmp_path / 'lib' / 'etl.jar'}"},
        {"jar": f"file:{tmp_path / 'lib' / 'aaa.jar'}"},
        {"jar": f"file:{tmp_path / 'lib' / 'deps.jar'}"},
    ]
    notes = [entry for entry in context.report.transformations if entry["rule"] == "SPARK-LIB"]
    assert notes == [{"rule": "SPARK-LIB", "node": "load", "detail": "aaa.jar, deps.jar"}]


def test_python_action_gets_the_lib_jars_too(tmp_path: Path):
    context = _context(tmp_path)
    translated = translate_action(_spark("<jar>job.py</jar>"), context)
    assert translated.main.libraries == [
        {"jar": f"file:{tmp_path / 'lib' / 'deps.jar'}"},
        {"jar": f"file:{tmp_path / 'lib' / 'etl.jar'}"},
    ]


def test_no_lib_directory_means_no_extra_libraries(tmp_path: Path):
    context = _context(tmp_path)
    for jar in (tmp_path / "lib").iterdir():
        jar.unlink()
    (tmp_path / "lib").rmdir()
    translated = translate_action(_spark("<jar>job.py</jar>"), context)
    assert translated.main.libraries is None
    assert "SPARK-LIB" not in {entry["rule"] for entry in context.report.transformations}


def test_hdfs_paths_reached_through_a_property_are_recorded(tmp_path: Path):
    context = _context(tmp_path, inputDir="${nameNode}/data/raw", alias="${inputDir}")
    translate_action(_spark("<class>c.M</class><jar>lib/etl.jar</jar><arg>${alias}/day=1</arg>"), context)
    assert {"action": "load", "field": "arg", "path": "${nameNode}/data/raw/day=1"} in context.report.hdfs_paths
