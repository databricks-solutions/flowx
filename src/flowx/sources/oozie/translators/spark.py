"""Translate an Oozie ``<spark>`` action into a Spark task on its own job-cluster spec.

The aim is to stay faithful to what spark-submit did on YARN (compute is classic job clusters; a separate
agent handles serverless later): executor counts and ``--conf`` settings carry over, YARN- and CDP-only
settings are dropped and listed, and node types are left as a bundle variable rather than guessed.
"""

from __future__ import annotations

import copy
import re
import shlex
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from flowx.models.ir import SparkJarActivity, SparkPythonActivity
from flowx.sources.oozie.loader.discovery import resolve_app_file
from flowx.sources.oozie.loader.el_render import render_reference
from flowx.sources.oozie.loader.graph import TranslatedAction

if TYPE_CHECKING:
    from flowx.sources.oozie.loader.ast import ActionNode, SparkAction
    from flowx.sources.oozie.translators import ActionContext

_DROPPED_CONF_PREFIXES = (
    "spark.yarn.",
    "spark.shuffle.service.",
    "spark.eventLog.",
    "spark.history.",
    "spark.kerberos.",
    "spark.sql.hive.metastore.",
    "spark.hadoop.hive.metastore.",
    "spark.security.credentials.",
    "spark.authenticate",
    "spark.network.crypto.",
    "spark.ssl.",
    "spark.hadoop.dfs.",
    "spark.hadoop.yarn.",
    "spark.hadoop.hadoop.security.",
)
_DROPPED_CONF_KEYS = frozenset(
    {
        "spark.master",
        "spark.submit.deployMode",
        "spark.driver.host",
        "spark.ui.filters",
        "spark.ui.port",
        "spark.sql.catalogImplementation",
        "spark.sql.warehouse.dir",
        "spark.executor.extraClassPath",
        "spark.driver.extraClassPath",
        "spark.hadoop.fs.defaultFS",
    }
)
_JAVA_OPTION_KEYS = ("spark.driver.extraJavaOptions", "spark.executor.extraJavaOptions")
_KERBEROS_JAVA_OPTION = re.compile(
    r"^-D(java\.security\.krb5\.|java\.security\.auth\.login\.config=|sun\.security\.krb5\.|"
    r"javax\.security\.auth\.useSubjectCredsOnly=)"
)
_SIZING_CONF_KEYS = {
    "spark.executor.instances": "num-executors",
    "spark.executor.memory": "executor-memory",
    "spark.executor.cores": "executor-cores",
    "spark.driver.memory": "driver-memory",
    "spark.driver.cores": "driver-cores",
    "spark.cores.max": "total-executor-cores",
}
_DROPPED_CONFIGURATION_PREFIXES = ("oozie.", "mapred.job.queue", "mapreduce.job.queuename")
_DROPPED_FLAGS = frozenset(
    {"--queue", "--master", "--deploy-mode", "--principal", "--keytab", "--name", "--proxy-user"}
)
_FLAGS_WITHOUT_VALUE = frozenset({"--verbose", "--supervise", "--help", "--version", "-v", "--kill", "--status"})
_SIZING_FLAGS = {
    "--num-executors": "num-executors",
    "--executor-cores": "executor-cores",
    "--executor-memory": "executor-memory",
    "--driver-memory": "driver-memory",
    "--driver-cores": "driver-cores",
    "--total-executor-cores": "total-executor-cores",
}
_DEFERRED_FLAGS = frozenset({"--py-files", "--files", "--archives", "--properties-file"})
_SPARK_SUBMIT_DEFAULT_EXECUTORS = 2
_SINGLE_NODE: dict[str, Any] = {
    "num_workers": 0,
    "spark_conf": {"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]"},
    "custom_tags": {"ResourceClass": "SingleNode"},
}
_VALUE_FLAGS = (
    frozenset({"--conf", "--jars", "--packages", "--repositories", "--driver-java-options", "--driver-class-path"})
    | frozenset(_SIZING_FLAGS)
    | _DROPPED_FLAGS
    | _DEFERRED_FLAGS
)


def _split_flag(token: str) -> tuple[str, str | None]:
    """Splits ``--flag=value`` into ``("--flag", "value")``; a bare ``--flag`` gives ``("--flag", None)``."""
    if token.startswith("--") and "=" in token:
        flag, _, value = token.partition("=")
        return flag, value
    return token, None


def _reference(text: str, context: ActionContext, what: str) -> str:
    from flowx.sources.oozie.translators import AgenticAction

    rendered = render_reference(text, context.environment)
    if rendered is None:
        raise AgenticAction(f"{what} {text!r} needs EL computed at run time (value bridges are planned for M2)")
    return rendered


def _cluster_value(text: str, context: ActionContext, what: str) -> str:
    """Renders *text* for a field Databricks never substitutes ``{{...}}`` in (cluster settings, main class, paths).

    Every property the workflow defines is expanded to its value here and now, instead of becoming a job
    parameter, because a job parameter would reach the cluster as the literal text ``{{job.parameters.x}}``.
    Anything still dynamic after that (an undefined property, ``${wf:id()}``) makes the action agentic.
    """
    from flowx.sources.oozie.translators import AgenticAction

    environment = context.environment
    defined = frozenset(name for layer in environment.properties.layers for name in layer)
    # Fresh registries, so a value that turns out to be unresolvable doesn't leave a job parameter behind.
    static_environment = replace(
        environment,
        inline_properties=defined,
        job_parameters=dict(environment.job_parameters),
        unresolved_properties=set(environment.unresolved_properties),
        parameter_names=dict(environment.parameter_names),
    )
    rendered = render_reference(text, static_environment)
    if rendered is None or "{{" in rendered:
        raise AgenticAction(
            f"{what} {text!r} can't be resolved at convert time (cluster settings can't read job parameters)"
        )
    return rendered


_PROPERTY_REFERENCE = re.compile(r"\$\{([^}]+)\}")


def _sizing_value(text: str, context: ActionContext, what: str) -> str:
    """Resolves ``${property}`` to its actual value, since cluster sizing can't wait for a run-time parameter."""
    substituted = _PROPERTY_REFERENCE.sub(
        lambda match: context.environment.properties.lookup(match.group(1)) or match.group(0), text
    )
    return _cluster_value(substituted, context, what) if "${" in substituted else substituted


def _record_hdfs_paths(action: ActionNode, context: ActionContext, field_name: str, text: str) -> None:
    context.report.record_hdfs_paths(action.name, field_name, text, context.environment.properties)


def _is_sizing_key(key: str) -> bool:
    return key in _SIZING_CONF_KEYS or key.startswith("spark.dynamicAllocation.")


def _whole_number(text: str, what: str) -> int:
    from flowx.sources.oozie.translators import AgenticAction

    try:
        return int(text.strip())
    except ValueError:
        raise AgenticAction(f"{what} {text!r} isn't a whole number") from None


def _library_path(reference: str, action: ActionNode, context: ActionContext) -> str:
    local = resolve_app_file(context.app, reference)
    if local is not None:
        return f"file:{local}"
    context.report.missing_files.append({"action": action.name, "reference": reference})
    return _cluster_value(reference, context, "library path")


def _lib_directory_jars(
    action: ActionNode, context: ActionContext, listed: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Every other JAR in the app's ``lib/``, which Oozie puts on the action's classpath without being asked."""
    lib_directory = context.app.directory / "lib"
    if not lib_directory.is_dir():
        return []
    app_root = context.app.directory.resolve()
    attached: list[dict[str, Any]] = []
    for path in sorted(lib_directory.glob("*.jar")):
        resolved = path.resolve()
        if not resolved.is_file() or not resolved.is_relative_to(app_root):
            continue
        library = {"jar": f"file:{resolved}"}
        if library not in listed and library not in attached:
            attached.append(library)
    if attached:
        context.report.note(
            "SPARK-LIB", action.name, ", ".join(Path(library["jar"].removeprefix("file:")).name for library in attached)
        )
    return attached


def _parse_options(
    payload: SparkAction, action: ActionNode, context: ActionContext
) -> tuple[dict[str, str], dict[str, str], list[dict[str, Any]]]:
    """Splits spark-opts into (spark_conf, sizing, extra libraries), recording what gets dropped or deferred."""
    from flowx.sources.oozie.translators import AgenticAction

    try:
        tokens = shlex.split(payload.spark_opts)
    except ValueError as error:
        raise AgenticAction(f"spark-opts can't be parsed: {error}", hints={"spark_opts": payload.spark_opts}) from error
    spark_conf: dict[str, str] = {}
    sizing: dict[str, str] = {}
    command_line_conf: dict[str, str] = {}
    libraries: list[dict[str, Any]] = []
    repositories: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        flag, inline_value = _split_flag(token)
        if inline_value is not None:
            value = inline_value
            index += 1
        elif flag in _FLAGS_WITHOUT_VALUE:
            value = ""
            index += 1
        elif flag in _VALUE_FLAGS:
            if index + 1 >= len(tokens):
                raise AgenticAction(f"{flag} has no value in spark-opts", hints={"spark_opts": payload.spark_opts})
            value = tokens[index + 1]
            index += 2
        elif token.startswith("-") and index + 1 < len(tokens) and not tokens[index + 1].startswith("-"):
            value = tokens[index + 1]
            index += 2
        else:
            value = ""
            index += 1
        if flag == "--conf":
            key, _, conf_value = value.partition("=")
            resolve = _sizing_value if _is_sizing_key(key) else _cluster_value
            spark_conf[key] = resolve(conf_value, context, f"--conf {key}")
            _record_hdfs_paths(action, context, f"conf:{key}", conf_value)
        elif flag in _SIZING_FLAGS:
            sizing[_SIZING_FLAGS[flag]] = _sizing_value(value, context, flag)
        elif flag == "--jars":
            libraries.extend({"jar": _library_path(item, action, context)} for item in value.split(",") if item)
        elif flag == "--packages":
            libraries.extend({"maven": {"coordinates": item}} for item in value.split(",") if item)
            context.report.note("SPARK-MAVEN", action.name, value)
        elif flag == "--repositories":
            repositories.extend(item for item in value.split(",") if item)
        elif flag == "--driver-java-options":
            command_line_conf["spark.driver.extraJavaOptions"] = _cluster_value(value, context, flag)
        elif flag == "--driver-class-path":
            command_line_conf["spark.driver.extraClassPath"] = _cluster_value(value, context, flag)
        elif flag in _DROPPED_FLAGS or flag in _FLAGS_WITHOUT_VALUE:
            context.report.dropped_spark_conf.append({"action": action.name, "key": flag, "value": value})
            context.report.note("SPARK-CONF-DROP", action.name, flag)
        elif flag in _DEFERRED_FLAGS:
            context.report.gap(
                "spark_deferred_feature", f"action {action.name!r}: {flag} is planned for M2", {"value": value}
            )
            context.report.note("SPARK-DEFERRED", action.name, flag)
        else:
            context.report.gap(
                "spark_unknown_option",
                f"action {action.name!r}: unrecognised spark-submit option {token!r}",
                {"option": token, "consumed_value": value or None},
            )
    if repositories:
        for library in libraries:
            if "maven" in library:
                library["maven"]["repo"] = repositories[0]
    spark_conf.update(command_line_conf)
    return spark_conf, sizing, libraries


def _strip_kerberos_java_options(key: str, value: str, action: ActionNode, context: ActionContext) -> str | None:
    """Removes Kerberos/JAAS ``-D`` options from a JVM options string; ``None`` means nothing is left."""
    try:
        tokens = shlex.split(value)
    except ValueError:
        return value
    kept: list[str] = []
    for token in tokens:
        if _KERBEROS_JAVA_OPTION.match(token):
            context.report.dropped_spark_conf.append({"action": action.name, "key": key, "value": token})
            context.report.note("SPARK-CONF-DROP", action.name, f"{key}: {token}")
        else:
            kept.append(token)
    return " ".join(shlex.quote(token) for token in kept) if kept else None


def _split_conf(
    spark_conf: dict[str, str], sizing: dict[str, str], action: ActionNode, context: ActionContext
) -> tuple[dict[str, str], dict[str, str]]:
    """Separates settings that belong on Databricks from sizing, YARN/CDP ones (dropped) and dynamic allocation."""
    kept: dict[str, str] = {}
    dynamic: dict[str, str] = {}
    for key, value in spark_conf.items():
        if key.startswith("spark.dynamicAllocation."):
            dynamic[key.removeprefix("spark.dynamicAllocation.")] = value
        elif key in _SIZING_CONF_KEYS:
            sizing.setdefault(_SIZING_CONF_KEYS[key], value)
            context.report.note("SPARK-SIZING", action.name, key)
        elif key in _DROPPED_CONF_KEYS or key.startswith(_DROPPED_CONF_PREFIXES):
            context.report.dropped_spark_conf.append({"action": action.name, "key": key, "value": value})
            context.report.note("SPARK-CONF-DROP", action.name, key)
        elif key in _JAVA_OPTION_KEYS:
            remaining = _strip_kerberos_java_options(key, value, action, context)
            if remaining is not None:
                kept[key] = remaining
        else:
            kept[key] = value
    return kept, dynamic


def _cluster_spec(
    payload: SparkAction,
    spark_conf: dict[str, str],
    dynamic: dict[str, str],
    sizing: dict[str, str],
    action: ActionNode,
    context: ActionContext,
) -> dict[str, Any]:
    if (payload.master or "").startswith("local"):
        context.report.note("SPARK-LOCAL-MASTER", action.name, payload.master or "")
        spec = copy.deepcopy(_SINGLE_NODE)
        spec["spark_conf"] = {**spark_conf, **spec["spark_conf"]}
        if dynamic:
            context.report.gap(
                "spark_dynamic_allocation_ignored",
                f"action {action.name!r}: dynamicAllocation settings don't apply to a single-node cluster",
                {"settings": dynamic},
            )
        return spec
    spec = {}
    if dynamic.get("enabled", "").lower() == "true" and "maxExecutors" in dynamic:
        spec["autoscale"] = {
            "min_workers": max(_whole_number(dynamic.get("minExecutors", "1"), "minExecutors"), 1),
            "max_workers": _whole_number(dynamic["maxExecutors"], "maxExecutors"),
        }
        context.report.note("SPARK-AUTOSCALE", action.name, str(spec["autoscale"]))
    else:
        if dynamic:
            context.report.gap(
                "spark_dynamic_allocation_ignored",
                f"action {action.name!r}: dynamicAllocation needs enabled=true and maxExecutors to become autoscale",
                {"settings": dynamic},
            )
        spec["num_workers"] = _whole_number(
            sizing.get("num-executors", str(_SPARK_SUBMIT_DEFAULT_EXECUTORS)), "num-executors"
        )
        detail = "num-executors" if "num-executors" in sizing else "spark-submit default of 2 executors"
        context.report.note("SPARK-SIZING", action.name, detail)
    if spark_conf:
        spec["spark_conf"] = spark_conf
    return spec


def translate_spark(action: ActionNode, context: ActionContext) -> TranslatedAction:
    """Translates a ``<spark>`` action; raises ``AgenticAction`` when it can't do so faithfully."""
    from flowx.sources.oozie.translators import AgenticAction

    payload: SparkAction = action.payload  # type: ignore[assignment]
    scoped = context.environment
    if payload.configuration:
        context = replace(
            context, environment=replace(scoped, properties=scoped.properties.with_layer(payload.configuration))
        )

    is_python = payload.jar.endswith(".py")
    if not is_python and not payload.main_class:
        raise AgenticAction("spark action has a JAR but no <class> (main class) to run")
    _record_hdfs_paths(action, context, "jar", payload.jar)
    option_conf, sizing, extra_libraries = _parse_options(payload, action, context)
    for key, value in payload.configuration.items():
        if key.startswith(_DROPPED_CONFIGURATION_PREFIXES):
            context.report.dropped_spark_conf.append({"action": action.name, "key": key, "value": value})
            context.report.note("SPARK-CONF-DROP", action.name, key)
        else:
            resolve = _sizing_value if _is_sizing_key(key) else _cluster_value
            option_conf.setdefault(
                key if key.startswith("spark.") else f"spark.hadoop.{key}", resolve(value, context, key)
            )
    spark_conf, dynamic = _split_conf(option_conf, sizing, action, context)
    if spark_conf:
        context.report.note("SPARK-CONF", action.name, ", ".join(sorted(spark_conf)))
    for deferred, values in (
        ("<prepare>", payload.prepare),
        ("<file>", payload.files),
        ("<archive>", payload.archives),
    ):
        if values:
            context.report.gap("spark_deferred_feature", f"action {action.name!r}: {deferred} is planned for M2")
            context.report.note("SPARK-DEFERRED", action.name, deferred)

    arguments = [_reference(argument, context, "argument") for argument in payload.arguments]
    for argument in payload.arguments:
        _record_hdfs_paths(action, context, "arg", argument)
    spec = _cluster_spec(payload, spark_conf, dynamic, sizing, action, context)
    sizing_summary = ", ".join(f"{name}={value}" for name, value in sizing.items())
    originals = []
    if sizing_summary:
        originals.append(f"original sizing: {sizing_summary}")
    if (payload.master or "").startswith("local"):
        originals.append(f"original master: {payload.master}")
    description = f"Oozie spark action {action.name!r}" + (f" ({'; '.join(originals)})" if originals else "")

    if is_python:
        script = resolve_app_file(context.app, payload.jar)
        if script is None:
            context.report.missing_files.append({"action": action.name, "reference": payload.jar})
        context.report.note("SPARK-PYTHON", action.name)
        python_libraries = [*extra_libraries, *_lib_directory_jars(action, context, extra_libraries)]
        python_task = SparkPythonActivity(
            name=action.name,
            task_key=context.task_key,
            description=description,
            python_file=script.name if script else _cluster_value(payload.jar, context, "python file"),
            parameters=arguments or None,
            generated_source=script.read_text(encoding="utf-8") if script else None,
            libraries=python_libraries or None,
            cluster={"_job_cluster_spec": spec},
        )
        return TranslatedAction(main=python_task)

    if payload.main_class is None:
        raise AgenticAction("spark action has a JAR but no <class> (main class) to run")
    context.report.note("SPARK-JAR", action.name)
    listed = [{"jar": _library_path(payload.jar, action, context)}, *extra_libraries]
    main = SparkJarActivity(
        name=action.name,
        task_key=context.task_key,
        description=description,
        main_class_name=_cluster_value(payload.main_class, context, "main class"),
        parameters=arguments or None,
        libraries=[*listed, *_lib_directory_jars(action, context, listed)],
        cluster={"_job_cluster_spec": spec},
    )
    return TranslatedAction(main=main)
