"""Unit tests for individual activity translators and the translation engine."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any

from orchestra.models.adf_ast import (
    AdfActivity,
    AdfDatasetReference,
    AdfDefinitions,
    AdfDependency,
    AdfLinkedServiceReference,
    AdfPolicy,
)
from orchestra.models.ir import (
    AppendVariableActivity,
    CopyActivity,
    DeleteActivity,
    ExecutePipelineActivity,
    FilterActivity,
    ForEachActivity,
    IfConditionActivity,
    LookupActivity,
    NotebookActivity,
    Pipeline,
    PlaceholderActivity,
    RunJobActivity,
    SetVariableActivity,
    SparkJarActivity,
    SparkPythonActivity,
    SwitchActivity,
    TranslationContext,
    WaitActivity,
    WebActivity,
)
from orchestra.translator.engine import translate_pipeline

_EMPTY_DEFS = AdfDefinitions(pipelines=[], datasets={}, linked_services={}, triggers=[])


def _base_kwargs(name: str = "test_activity") -> dict[str, Any]:
    """Minimal base_kwargs for translator functions."""
    return {
        "name": name,
        "task_key": name.replace(" ", "_"),
        "description": None,
        "timeout_seconds": None,
        "max_retries": None,
        "min_retry_interval_millis": None,
        "depends_on": None,
        "cluster": None,
        "existing_cluster_id": None,
    }


def _context() -> TranslationContext:
    return TranslationContext(
        activity_cache=MappingProxyType({}),
        registry=MappingProxyType({}),
        variable_cache=MappingProxyType({}),
    )


def _make_activity(
    name: str,
    adf_type: str,
    type_properties: dict[str, Any] | None = None,
    *,
    depends_on: list[AdfDependency] | None = None,
    inputs: list[AdfDatasetReference] | None = None,
    outputs: list[AdfDatasetReference] | None = None,
    linked_service_name: AdfLinkedServiceReference | None = None,
    if_true_activities: list[AdfActivity] | None = None,
    if_false_activities: list[AdfActivity] | None = None,
    activities: list[AdfActivity] | None = None,
    policy: AdfPolicy | None = None,
) -> AdfActivity:
    return AdfActivity(
        name=name,
        type=adf_type,
        type_properties=type_properties,
        depends_on=depends_on,
        inputs=inputs,
        outputs=outputs,
        linked_service_name=linked_service_name,
        if_true_activities=if_true_activities,
        if_false_activities=if_false_activities,
        activities=activities,
        policy=policy,
    )


class TestCopyTranslator:
    def test_translate_copy_basic(self):
        from orchestra.translator.activity_translators.copy import translate

        activity = _make_activity(
            "Copy Data",
            "Copy",
            {
                "source": {"type": "BlobSource", "recursive": True},
                "sink": {"type": "DeltaSink", "writeBatchSize": 10000},
            },
        )
        result = translate(activity, _base_kwargs("Copy Data"), _context(), _EMPTY_DEFS)
        assert isinstance(result, CopyActivity)
        assert result.source_type == "BlobSource"
        assert result.sink_type == "DeltaSink"
        assert result.source_properties["recursive"] is True
        assert result.sink_properties["writeBatchSize"] == 10000

    def test_translate_copy_with_column_mapping(self):
        from orchestra.translator.activity_translators.copy import translate

        activity = _make_activity(
            "Copy Mapped",
            "Copy",
            {
                "source": {"type": "AzureSqlSource"},
                "sink": {"type": "DeltaSink"},
                "translator": {
                    "type": "TabularTranslator",
                    "mappings": [
                        {
                            "source": {"name": "id", "type": "Int32"},
                            "sink": {"name": "id", "type": "Int64"},
                        },
                        {
                            "source": {"name": "name", "type": "String"},
                            "sink": {"name": "full_name", "type": "String"},
                        },
                    ],
                },
            },
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, CopyActivity)
        assert result.column_mapping is not None
        assert len(result.column_mapping) == 2
        assert result.column_mapping[0]["source_name"] == "id"
        assert result.column_mapping[1]["sink_name"] == "full_name"

    def test_translate_copy_empty_type_properties(self):
        from orchestra.translator.activity_translators.copy import translate

        activity = _make_activity("Empty Copy", "Copy", {})
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, CopyActivity)
        assert result.source_type is None
        assert result.sink_type is None


class TestNotebookTranslator:
    def test_translate_notebook_basic(self):
        from orchestra.translator.activity_translators.notebook import translate

        activity = _make_activity(
            "Run Notebook",
            "DatabricksNotebook",
            {"notebookPath": "/Shared/ETL/transform", "baseParameters": {"env": "dev"}},
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, NotebookActivity)
        assert result.notebook_path == "/Shared/ETL/transform"
        assert result.base_parameters == {"env": "dev"}

    def test_translate_notebook_no_params(self):
        from orchestra.translator.activity_translators.notebook import translate

        activity = _make_activity(
            "Run Notebook",
            "DatabricksNotebook",
            {"notebookPath": "/Shared/ETL/simple"},
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, NotebookActivity)
        assert result.base_parameters == {}

    def test_translate_notebook_resolves_library_with_globals(self):
        """C-01 (NB-ITER2-1, LSC2-004): @concat of literals collapses to a
        literal jar path so the library install succeeds."""
        from orchestra.translator.activity_translators.notebook import translate

        activity = _make_activity(
            "Run NB",
            "DatabricksNotebook",
            {
                "notebookPath": "/Shared/x",
                "libraries": [{"jar": ("@concat('/Volumes/x/', pipeline().globalParameters.libFileName)")}],
            },
        )
        # Context with global parameter so the @concat resolves.
        ctx = TranslationContext(
            global_parameters=MappingProxyType({"libFileName": "my-job.jar"}),
        )
        result = translate(activity, _base_kwargs(), ctx, _EMPTY_DEFS)
        assert isinstance(result, NotebookActivity)
        # With C-01 the concat collapses to a literal string -- the bundle
        # YAML carries the resolved jar path directly instead of a Python
        # source string.
        assert result.libraries == [{"jar": "/Volumes/x/my-job.jar"}]

    def test_translate_notebook_resolves_pipeline_param_in_library(self):
        """Library entry referencing a single pipeline parameter resolves to a literal."""
        from orchestra.translator.activity_translators.notebook import translate

        activity = _make_activity(
            "Run NB",
            "DatabricksNotebook",
            {
                "notebookPath": "/Shared/x",
                "libraries": [
                    {"jar": "@pipeline().globalParameters.libPath"},
                ],
            },
        )
        ctx = TranslationContext(
            global_parameters=MappingProxyType({"libPath": "/Volumes/my.jar"}),
        )
        result = translate(activity, _base_kwargs(), ctx, _EMPTY_DEFS)
        assert isinstance(result, NotebookActivity)
        assert result.libraries == [{"jar": "/Volumes/my.jar"}]

    def test_translate_notebook_passes_libraries_through(self):
        from orchestra.translator.activity_translators.notebook import translate

        libraries = [
            {"jar": "dbfs:/libs/util.jar"},
            {"whl": "dbfs:/libs/pkg-1.0-py3-none-any.whl"},
            {"pypi": {"package": "requests==2.31.0"}},
            {"maven": {"coordinates": "org.jsoup:jsoup:1.7.2", "exclusions": ["slf4j:slf4j"]}},
            {"cran": {"package": "ada", "repo": "https://cran.us.r-project.org"}},
        ]
        activity = _make_activity(
            "Run Notebook",
            "DatabricksNotebook",
            {"notebookPath": "/Shared/nb", "libraries": libraries},
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, NotebookActivity)
        assert result.libraries == libraries

    def test_translate_notebook_dynamic_path_marks_unresolved(self):
        """C-28 (NB-ITER4-001): an expression notebookPath is captured as
        ``notebook_path_unresolved`` so the preparer emits a dispatch stub."""
        from orchestra.translator.activity_translators.notebook import translate

        activity = _make_activity(
            "Dispatch",
            "DatabricksNotebook",
            {
                "notebookPath": {
                    "value": "@trim(json(activity('cfg').output.firstRow).notebook_path)",
                    "type": "Expression",
                },
                "baseParameters": {"env": "dev"},
            },
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, NotebookActivity)
        assert result.notebook_path_unresolved is True
        assert result.notebook_path == ""
        assert "@trim" in (result.notebook_path_expression or "")

    def test_translate_notebook_unresolved_library_captured(self):
        """C-30 (NB-ITER4-003): library jar/whl entries whose @concat
        references a missing globalParameter surface as
        ``unresolved_libraries`` so SETUP.md can flag them."""
        from orchestra.translator.activity_translators.notebook import translate

        activity = _make_activity(
            "Run NB",
            "DatabricksNotebook",
            {
                "notebookPath": "/Shared/x",
                "libraries": [{"jar": ("@concat('/Volumes/x/', pipeline().globalParameters.proj4jLibFileName)")}],
            },
        )
        ctx = TranslationContext()
        result = translate(activity, _base_kwargs(), ctx, _EMPTY_DEFS)
        assert isinstance(result, NotebookActivity)
        # The unresolved entry is captured with the missing identifier.
        assert len(result.unresolved_libraries) == 1
        entry = result.unresolved_libraries[0]
        assert entry["type"] == "jar"
        assert "proj4jLibFileName" in entry["expression"]
        assert "proj4jLibFileName" in entry["missing"]

    def test_translate_notebook_captures_utcnow_approximation(self):
        from orchestra.translator.activity_translators.notebook import translate

        activity = _make_activity(
            "Score",
            "DatabricksNotebook",
            {
                "notebookPath": "/Shared/score",
                "baseParameters": {
                    "scoring_date": {"value": "@formatDateTime(utcnow(), 'yyyy-MM-dd')", "type": "Expression"},
                    "env": "dev",
                },
            },
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, NotebookActivity)
        assert result.base_parameters["scoring_date"] == "{{job.start_time.iso_date}}"
        assert result.base_parameters["env"] == "dev"
        assert len(result.parameter_approximations) == 1
        approximation = result.parameter_approximations[0]
        assert approximation["widget_name"] == "scoring_date"
        assert approximation["raw_expression"] == "@formatDateTime(utcnow(), 'yyyy-MM-dd')"
        assert approximation["replacement"] == "{{job.start_time.iso_date}}"
        assert "utcnow" in approximation["note"].lower()


class TestCommonAttributes:
    def test_existing_cluster_id_extracted_from_linked_service(self):
        from orchestra.models.adf_ast import AdfLinkedService
        from orchestra.translator.engine import _build_base_kwargs

        linked_service = AdfLinkedService(
            name="AzureDatabricks_LS",
            type="AzureDatabricks",
            properties={
                "typeProperties": {
                    "existingClusterId": "1234-567890-abcde123",
                },
            },
        )
        definitions = AdfDefinitions(
            pipelines=[],
            datasets={},
            linked_services={"AzureDatabricks_LS": linked_service},
            triggers=[],
        )
        activity = _make_activity(
            "Run Notebook",
            "DatabricksNotebook",
            {"notebookPath": "/Shared/nb"},
            linked_service_name=AdfLinkedServiceReference(reference_name="AzureDatabricks_LS"),
        )
        kwargs = _build_base_kwargs(activity, definitions)
        assert kwargs["existing_cluster_id"] == "1234-567890-abcde123"
        assert kwargs["cluster"] == {"existing_cluster_id": "1234-567890-abcde123"}

    def test_existing_cluster_id_none_when_linked_service_uses_new_cluster(self):
        from orchestra.models.adf_ast import AdfLinkedService
        from orchestra.translator.engine import _build_base_kwargs

        linked_service = AdfLinkedService(
            name="AzureDatabricks_LS",
            type="AzureDatabricks",
            properties={
                "typeProperties": {
                    "newClusterSparkVersion": "15.4.x-scala2.12",
                    "newClusterNumOfWorker": 2,
                },
            },
        )
        definitions = AdfDefinitions(
            pipelines=[],
            datasets={},
            linked_services={"AzureDatabricks_LS": linked_service},
            triggers=[],
        )
        activity = _make_activity(
            "Run Notebook",
            "DatabricksNotebook",
            {"notebookPath": "/Shared/nb"},
            linked_service_name=AdfLinkedServiceReference(reference_name="AzureDatabricks_LS"),
        )
        kwargs = _build_base_kwargs(activity, definitions)
        assert kwargs["existing_cluster_id"] is None

    def test_linked_service_parameter_overrides_cluster_version(self):
        """Change linked-service-parameter-resolution (P0): NB-4, LSC-001."""
        from orchestra.models.adf_ast import AdfLinkedService
        from orchestra.translator.engine import _build_base_kwargs

        linked_service = AdfLinkedService(
            name="CLI0010_ls_databricks",
            type="AzureDatabricks",
            properties={
                "parameters": {
                    "clusterVersion": {
                        "type": "string",
                        "defaultValue": "16.4.x-scala2.12",
                    },
                },
                "typeProperties": {
                    "newClusterVersion": "@linkedService().clusterVersion",
                    "newClusterNumOfWorker": "1",
                    "newClusterNodeType": "Standard_D4s_v3",
                },
            },
        )
        definitions = AdfDefinitions(
            pipelines=[],
            datasets={},
            linked_services={"CLI0010_ls_databricks": linked_service},
            triggers=[],
        )
        activity = _make_activity(
            "Run Notebook",
            "DatabricksNotebook",
            {"notebookPath": "/Shared/nb"},
            linked_service_name=AdfLinkedServiceReference(
                reference_name="CLI0010_ls_databricks",
                parameters={"clusterVersion": "16.4.x-scala2.12"},
            ),
        )
        kwargs = _build_base_kwargs(activity, definitions)
        assert kwargs["cluster"] is not None
        # No literal ADF expression should leak into the cluster spec.
        assert kwargs["cluster"]["spark_version"] == "16.4.x-scala2.12"
        # num_workers='1' must coerce to int.
        assert kwargs["cluster"]["num_workers"] == 1
        assert isinstance(kwargs["cluster"]["num_workers"], int)

    def test_parameter_default_coerces_bool_string_to_real_bool(self):
        """Change expression-resolver-bool-and-numeric-coercion (P1): VAR-006."""
        from orchestra.translator.engine import _coerce_parameter_default

        assert _coerce_parameter_default("false", "Bool") is False
        assert _coerce_parameter_default("True", "Bool") is True
        assert _coerce_parameter_default("FALSE", "boolean") is False

    def test_parameter_default_coerces_int_string_to_int(self):
        from orchestra.translator.engine import _coerce_parameter_default

        assert _coerce_parameter_default("42", "Int") == 42
        assert _coerce_parameter_default(42, "Int") == 42

    def test_parameter_default_string_left_alone(self):
        from orchestra.translator.engine import _coerce_parameter_default

        assert _coerce_parameter_default("hello", "String") == "hello"

    def test_dependency_multi_condition_succeeded_and_failed_maps_to_completed(self):
        """Change dependency-multi-condition-mapping (P1): CF-004."""
        from orchestra.translator.engine import _map_dependency_conditions

        assert _map_dependency_conditions(["Succeeded"]) == "Succeeded"
        assert _map_dependency_conditions(["Failed"]) == "Failed"
        assert _map_dependency_conditions(["Completed"]) == "Completed"
        assert _map_dependency_conditions(["Skipped"]) == "Skipped"
        # [Succeeded, Failed] semantics = "run regardless" -> Completed
        assert _map_dependency_conditions(["Succeeded", "Failed"]) == "Completed"
        # [Succeeded, Skipped] -> Skipped wins (ALL_DONE downstream)
        assert _map_dependency_conditions(["Succeeded", "Skipped"]) == "Skipped"
        # [Failed] (multi-element with same) handled in single-item branch.
        assert _map_dependency_conditions([]) is None
        assert _map_dependency_conditions(None) is None

    def test_ls_param_expression_wrapper_unwrapped_in_custom_tags(self):
        """C-02 (NB-ITER2-2 / LSC2-003): Expression-dict-wrapped LS params
        must collapse to plain scalars in cluster fields like custom_tags."""
        from orchestra.models.adf_ast import AdfLinkedService
        from orchestra.translator.engine import _build_base_kwargs

        linked_service = AdfLinkedService(
            name="LS",
            type="AzureDatabricks",
            properties={
                "parameters": {
                    "digitalCase": {"type": "string", "defaultValue": "CLI0010"},
                },
                "typeProperties": {
                    "newClusterVersion": "16.4.x-scala2.12",
                    "newClusterNumOfWorker": 0,
                    "newClusterNodeType": "Standard_D4s_v3",
                    "newClusterCustomTags": {
                        "DigitalCase": "@linkedService().digitalCase",
                    },
                },
            },
        )
        definitions = AdfDefinitions(
            pipelines=[],
            datasets={},
            linked_services={"LS": linked_service},
            triggers=[],
        )
        # Activity supplies the LS param as the {value, type:'Expression'}
        # wrapper shape -- the same shape the ADF JSON corpus ships.
        activity = _make_activity(
            "Run Notebook",
            "DatabricksNotebook",
            {"notebookPath": "/Shared/nb"},
            linked_service_name=AdfLinkedServiceReference(
                reference_name="LS",
                parameters={"digitalCase": {"value": "CLI0010", "type": "Expression"}},
            ),
        )
        cluster = _build_base_kwargs(activity, definitions)["cluster"]
        assert cluster is not None
        # custom_tags must be Map[String, String] -- no dict wrapper survives.
        assert cluster["custom_tags"] == {"DigitalCase": "CLI0010"}
        # spark_env_vars likewise stays scalar-valued.
        assert "DigitalCase" in cluster["custom_tags"]
        assert not isinstance(cluster["custom_tags"]["DigitalCase"], dict)

    def test_ls_param_resolved_against_factory_global_parameters(self):
        """C-03 (NB-ITER2-3 / LSC2-002): activity-supplied LS param values
        that reference @pipeline().globalParameters.X must collapse to the
        factory-provided literal so cluster.spark_version is a real DBR."""
        from orchestra.models.adf_ast import AdfLinkedService
        from orchestra.translator.engine import _build_base_kwargs

        linked_service = AdfLinkedService(
            name="LS",
            type="AzureDatabricks",
            properties={
                "parameters": {
                    "clusterVersion": {"type": "string", "defaultValue": "15.4.x-scala2.12"},
                },
                "typeProperties": {
                    "newClusterVersion": "@linkedService().clusterVersion",
                    "newClusterNumOfWorker": 0,
                    "newClusterNodeType": "Standard_D4s_v3",
                },
            },
        )
        definitions = AdfDefinitions(
            pipelines=[],
            datasets={},
            linked_services={"LS": linked_service},
            triggers=[],
        )
        activity = _make_activity(
            "Run Notebook",
            "DatabricksNotebook",
            {"notebookPath": "/Shared/nb"},
            linked_service_name=AdfLinkedServiceReference(
                reference_name="LS",
                parameters={
                    "clusterVersion": {
                        "value": "@pipeline().globalParameters.clusterVersion",
                        "type": "Expression",
                    },
                },
            ),
        )
        context = TranslationContext(
            global_parameters=MappingProxyType({"clusterVersion": "16.4.x-scala2.12"}),
        )
        cluster = _build_base_kwargs(activity, definitions, context=context)["cluster"]
        assert cluster is not None
        assert cluster["spark_version"] == "16.4.x-scala2.12"
        # The raw @pipeline() expression must not leak into the cluster spec.
        assert not cluster["spark_version"].startswith("@")

    def test_ls_param_resolved_against_pipeline_parameters_as_dab_ref(self):
        """C-13 (NB-ITER3-002 / LSC3-003 / VAREX3-006): activity-supplied LS
        param values that reference @pipeline().parameters.X must collapse to
        {{job.parameters.X}} (a dab_ref), valid in custom_tags map values."""
        from orchestra.models.adf_ast import AdfLinkedService
        from orchestra.translator.engine import _build_base_kwargs

        linked_service = AdfLinkedService(
            name="LS",
            type="AzureDatabricks",
            properties={
                "parameters": {
                    "digitalCase": {"type": "string", "defaultValue": "CLI0010"},
                },
                "typeProperties": {
                    "newClusterVersion": "16.4.x-scala2.12",
                    "newClusterNumOfWorker": 0,
                    "newClusterNodeType": "Standard_D4s_v3",
                    "newClusterCustomTags": {
                        "DigitalCase": "@linkedService().digitalCase",
                    },
                },
            },
        )
        definitions = AdfDefinitions(
            pipelines=[],
            datasets={},
            linked_services={"LS": linked_service},
            triggers=[],
        )
        activity = _make_activity(
            "Run Notebook",
            "DatabricksNotebook",
            {"notebookPath": "/Shared/nb"},
            linked_service_name=AdfLinkedServiceReference(
                reference_name="LS",
                parameters={
                    "digitalCase": {
                        "value": "@pipeline().parameters.digitalCaseCode",
                        "type": "Expression",
                    },
                },
            ),
        )
        # Pipeline parameters resolver routes via dab_ref kind, not literal.
        context = TranslationContext()
        cluster = _build_base_kwargs(activity, definitions, context=context)["cluster"]
        assert cluster is not None
        # The resolver should now substitute the dab_ref so the raw @pipeline
        # expression does not leak.
        assert cluster["custom_tags"]["DigitalCase"] == "{{job.parameters.digitalCaseCode}}"
        assert not cluster["custom_tags"]["DigitalCase"].startswith("@")

    def test_notebook_library_resolves_pipeline_param_dab_ref(self):
        """C-13 (NB-ITER3-004): a jar path referencing @pipeline().parameters.X
        collapses to {{job.parameters.X}} in the emitted library entry."""
        from orchestra.translator.activity_translators.notebook import translate

        activity = _make_activity(
            "Run NB",
            "DatabricksNotebook",
            {
                "notebookPath": "/Shared/x",
                "libraries": [
                    {"jar": "@pipeline().parameters.libName"},
                ],
            },
        )
        ctx = TranslationContext()
        result = translate(activity, _base_kwargs(), ctx, _EMPTY_DEFS)
        assert isinstance(result, NotebookActivity)
        assert result.libraries == [{"jar": "{{job.parameters.libName}}"}]

    def test_extended_cluster_fields_propagated(self):
        """Change linked-service-cluster-field-coverage (P1): NB-3, LSC-003."""
        from orchestra.models.adf_ast import AdfLinkedService
        from orchestra.translator.engine import _build_base_kwargs

        linked_service = AdfLinkedService(
            name="LS",
            type="AzureDatabricks",
            properties={
                "typeProperties": {
                    "newClusterVersion": "16.4.x-scala2.12",
                    "newClusterNumOfWorker": 0,
                    "newClusterNodeType": "Standard_D4s_v3",
                    "newClusterDriverNodeType": "Standard_D8s_v3",
                    "newClusterSparkEnvVars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
                    "newClusterCustomTags": {"DigitalCase": "MyCase"},
                    "newClusterInitScripts": [{"workspace": {"destination": "/init.sh"}}],
                    "dataSecurityMode": "SINGLE_USER",
                    "clusterLogConf": {"dbfs": {"destination": "dbfs:/cluster-logs"}},
                },
            },
        )
        definitions = AdfDefinitions(
            pipelines=[],
            datasets={},
            linked_services={"LS": linked_service},
            triggers=[],
        )
        activity = _make_activity(
            "Run Notebook",
            "DatabricksNotebook",
            {"notebookPath": "/Shared/nb"},
            linked_service_name=AdfLinkedServiceReference(reference_name="LS"),
        )
        cluster = _build_base_kwargs(activity, definitions)["cluster"]
        assert cluster is not None
        assert cluster["driver_node_type_id"] == "Standard_D8s_v3"
        assert cluster["spark_env_vars"]["PYSPARK_PYTHON"] == "/databricks/python3/bin/python3"
        assert cluster["custom_tags"]["DigitalCase"] == "MyCase"
        assert cluster["init_scripts"] == [{"workspace": {"destination": "/init.sh"}}]
        assert cluster["data_security_mode"] == "SINGLE_USER"
        assert cluster["cluster_log_conf"] == {"dbfs": {"destination": "dbfs:/cluster-logs"}}


class TestSparkJarTranslator:
    def test_translate_spark_jar(self):
        from orchestra.translator.activity_translators.spark_jar import translate

        activity = _make_activity(
            "Run Jar",
            "DatabricksSparkJar",
            {
                "mainClassName": "com.example.MainJob",
                "parameters": ["--input", "/mnt/data"],
                "libraries": [{"jar": "dbfs:/libs/my-job.jar"}],
            },
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, SparkJarActivity)
        assert result.main_class_name == "com.example.MainJob"
        assert result.parameters == ["--input", "/mnt/data"]
        assert result.libraries == [{"jar": "dbfs:/libs/my-job.jar"}]


class TestSparkPythonTranslator:
    def test_translate_spark_python(self):
        from orchestra.translator.activity_translators.spark_python import translate

        activity = _make_activity(
            "Run Python",
            "DatabricksSparkPython",
            {"pythonFile": "dbfs:/scripts/etl.py", "parameters": ["--mode", "batch"]},
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, SparkPythonActivity)
        assert result.python_file == "dbfs:/scripts/etl.py"
        assert result.parameters == ["--mode", "batch"]

    def test_translate_spark_python_passes_libraries_through(self):
        from orchestra.translator.activity_translators.spark_python import translate

        libraries = [
            {"egg": "dbfs:/libs/util.egg"},
            {"pypi": {"package": "pandas", "repo": "https://pypi.example.com"}},
        ]
        activity = _make_activity(
            "Run Python",
            "DatabricksSparkPython",
            {"pythonFile": "dbfs:/scripts/etl.py", "libraries": libraries},
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, SparkPythonActivity)
        assert result.libraries == libraries


class TestLookupTranslator:
    def test_translate_lookup_first_row(self):
        from orchestra.translator.activity_translators.lookup import translate

        activity = _make_activity(
            "Lookup Config",
            "Lookup",
            {
                "source": {"type": "AzureSqlSource", "sqlReaderQuery": "SELECT TOP 1 * FROM config"},
                "firstRowOnly": True,
            },
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, LookupActivity)
        assert result.first_row_only is True
        assert result.source_type == "AzureSqlSource"
        assert result.source_query == "SELECT TOP 1 * FROM config"

    def test_translate_lookup_all_rows(self):
        from orchestra.translator.activity_translators.lookup import translate

        activity = _make_activity(
            "Lookup All",
            "Lookup",
            {
                "source": {"type": "AzureSqlSource", "sqlReaderQuery": "SELECT * FROM tables"},
                "firstRowOnly": False,
            },
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, LookupActivity)
        assert result.first_row_only is False

    def test_translate_lookup_resolves_json_file_dataset(self):
        """Change lookup-file-dataset-support (P0)."""
        from orchestra.models.adf_ast import AdfDataset
        from orchestra.translator.activity_translators.lookup import translate

        json_dataset = AdfDataset(
            name="ConfigDataset",
            type="Json",
            properties={
                "typeProperties": {
                    "location": {
                        "type": "AzureBlobFSLocation",
                        "fileSystem": "configs",
                        "folderPath": "lookup",
                        "fileName": "tables.json",
                    },
                    "formatSettings": {"multiLineJson": True},
                },
            },
            linked_service_name="LS",
        )
        definitions = AdfDefinitions(
            pipelines=[],
            datasets={"ConfigDataset": json_dataset},
            linked_services={},
            triggers=[],
        )
        activity = _make_activity(
            "Read_Configuration",
            "Lookup",
            {
                "source": {"type": "JsonSource"},
                "dataset": {
                    "referenceName": "ConfigDataset",
                    "type": "DatasetReference",
                },
                "firstRowOnly": False,
            },
        )
        result = translate(activity, _base_kwargs(), _context(), definitions)
        assert isinstance(result, LookupActivity)
        assert result.source_type == "JsonSource"
        assert result.first_row_only is False
        # Source properties carry the dataset type plus location bits.
        assert result.source_properties is not None
        assert result.source_properties["dataset_type"] == "Json"
        assert result.source_properties["container"] == "configs"
        assert result.source_properties["file_name"] == "tables.json"
        assert result.source_properties.get("multiLineJson") is True

    def test_translate_lookup_substitutes_dataset_parameter_refs(self):
        """C-47 (LSC5-001): a file Lookup whose dataset folderPath references
        ``dataset().X`` substitutes the dataset reference's parameter bindings
        so the baked path carries no literal ``dataset(`` expression."""
        from orchestra.models.adf_ast import AdfDataset
        from orchestra.translator.activity_translators.lookup import translate

        ds = AdfDataset(
            name="arq_ds",
            type="Json",
            properties={
                "typeProperties": {
                    "location": {
                        "type": "AzureBlobFSLocation",
                        "fileSystem": "configs",
                        "folderPath": {
                            "value": "@toLower(dataset().digitalCase)",
                            "type": "Expression",
                        },
                        "fileName": {"value": "@dataset().fileName", "type": "Expression"},
                    },
                },
            },
            linked_service_name="LS",
        )
        definitions = AdfDefinitions(
            pipelines=[],
            datasets={"arq_ds": ds},
            linked_services={},
            triggers=[],
        )
        activity = _make_activity(
            "Read_Arq",
            "Lookup",
            {
                "source": {"type": "JsonSource"},
                "dataset": {
                    "referenceName": "arq_ds",
                    "type": "DatasetReference",
                    "parameters": {
                        "digitalCase": "@pipeline().parameters.digitalCaseCode",
                        "fileName": "@pipeline().parameters.fileName",
                    },
                },
                "firstRowOnly": True,
            },
        )
        result = translate(activity, _base_kwargs(), _context(), definitions)
        assert isinstance(result, LookupActivity)
        assert result.source_properties is not None
        folder = result.source_properties.get("folder_path", "")
        filename = result.source_properties.get("file_name", "")
        # The dataset() reference is gone; the pipeline-param binding takes over.
        assert "dataset(" not in folder
        assert "dataset(" not in filename
        assert "{{job.parameters.digitalCaseCode}}" in folder


class TestLookupCaseInsensitiveAndLinkedService:
    """Change fix-dataset-and-linked-service-case-insensitive-lookup (P1): LSC3-005."""

    def test_lookup_resolves_dataset_case_insensitively(self):
        """ADF identifiers are case-insensitive; a pipeline referencing
        'cli0010_a_ds_conf_json' must resolve dataset 'CLI0010_a_ds_conf_json'."""
        from orchestra.models.adf_ast import AdfDataset, AdfLinkedService
        from orchestra.translator.activity_translators.lookup import translate

        ds = AdfDataset(
            name="CLI0010_a_ds_conf_json",
            type="Json",
            properties={
                "typeProperties": {
                    "location": {
                        "type": "AzureBlobFSLocation",
                        "fileSystem": "configext",
                        "folderPath": "settings",
                        "fileName": "tables.json",
                    },
                },
            },
            linked_service_name="LS_ABFSS",
        )
        ls = AdfLinkedService(
            name="LS_ABFSS",
            type="AzureBlobFS",
            properties={
                "typeProperties": {
                    "url": "abfss://configext@myacct.dfs.core.windows.net",
                },
            },
        )
        definitions = AdfDefinitions(
            pipelines=[],
            datasets={"CLI0010_a_ds_conf_json": ds},
            linked_services={"LS_ABFSS": ls},
            triggers=[],
        )
        # NOTE: lowercase reference name in the activity.
        activity = _make_activity(
            "Read_Conf",
            "Lookup",
            {
                "source": {"type": "JsonSource"},
                "dataset": {
                    "referenceName": "cli0010_a_ds_conf_json",
                    "type": "DatasetReference",
                },
                "firstRowOnly": True,
            },
        )
        result = translate(activity, _base_kwargs(), _context(), definitions)
        assert isinstance(result, LookupActivity)
        assert result.source_properties is not None
        assert result.source_properties["dataset_type"] == "Json"
        assert result.source_properties["container"] == "configext"
        # Linked-service URL surfaces so the code generator can build abfss://...
        assert result.source_properties["linked_service_url"] == ("abfss://configext@myacct.dfs.core.windows.net")

    def test_generated_file_lookup_notebook_uses_abfss_path(self):
        """LSC3-005 end-to-end: generated file-lookup notebook ships a real
        abfss:// default path instead of an empty widget fallback."""
        from orchestra.preparer.code_generator import generate_lookup_notebook

        base = _base_kwargs("Read_Conf")
        base.pop("existing_cluster_id", None)
        activity = LookupActivity(
            **base,
            source_type="JsonSource",
            source_properties={
                "dataset_type": "Json",
                "container": "configext",
                "folder_path": "settings",
                "file_name": "tables.json",
                "linked_service_url": "abfss://configext@myacct.dfs.core.windows.net",
            },
            first_row_only=True,
        )
        content = generate_lookup_notebook(activity)
        assert "abfss://configext@myacct.dfs.core.windows.net" in content
        assert "tables.json" in content
        # spark.sql('') sentinel must not appear for file-source lookups.
        assert "spark.sql('')" not in content


class TestWebActivityTranslator:
    def test_translate_web_activity_get(self):
        from orchestra.translator.activity_translators.web_activity import translate

        activity = _make_activity(
            "Call API",
            "WebActivity",
            {"url": "https://api.example.com/data", "method": "GET"},
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, WebActivity)
        assert result.url == "https://api.example.com/data"
        assert result.method == "GET"

    def test_translate_web_activity_post(self):
        from orchestra.translator.activity_translators.web_activity import translate

        activity = _make_activity(
            "Post Data",
            "WebActivity",
            {
                "url": "https://api.example.com/submit",
                "method": "POST",
                "body": {"key": "value"},
                "headers": {"Content-Type": "application/json"},
                "authentication": {"type": "ServicePrincipal", "resource": "https://api.example.com"},
            },
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, WebActivity)
        assert result.method == "POST"
        assert result.body == {"key": "value"}
        assert result.headers == {"Content-Type": "application/json"}
        assert result.authentication["type"] == "ServicePrincipal"


class TestDeleteTranslator:
    def test_translate_delete(self):
        from orchestra.translator.activity_translators.delete import translate

        activity = _make_activity(
            "Delete Files",
            "Delete",
            {"recursive": True},
            inputs=[AdfDatasetReference(reference_name="ds_staging_folder")],
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, DeleteActivity)
        assert result.dataset_name == "ds_staging_folder"
        assert result.recursive is True


class TestExecutePipelineTranslator:
    def test_translate_execute_pipeline(self):
        from orchestra.translator.activity_translators.execute_pipeline import translate

        activity = _make_activity(
            "Run Child",
            "ExecutePipeline",
            {
                "pipeline": {"referenceName": "child_pipeline", "type": "PipelineReference"},
                "parameters": {"date": "2024-01-01"},
                "waitOnCompletion": True,
            },
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, ExecutePipelineActivity)
        assert result.pipeline_name == "child_pipeline"
        assert result.parameters == {"date": "2024-01-01"}
        assert result.wait_on_completion is True

    def test_translate_execute_pipeline_drops_notebook_code_parameters(self):
        """C-09 (VAREX-001): an ExecutePipeline parameter value that resolves
        to notebook_code (e.g. @concat('x', pipeline().parameters.Y)) must NOT
        ride through as a literal Python source string -- it's dropped from
        the parameters dict and surfaced via parameter_approximations."""
        from orchestra.translator.activity_translators.execute_pipeline import translate

        activity = _make_activity(
            "Run Child",
            "ExecutePipeline",
            {
                "pipeline": {"referenceName": "child_pipeline", "type": "PipelineReference"},
                "parameters": {
                    "ok_value": "@pipeline().parameters.env",  # dab_ref -- kept
                    "bad_value": {
                        "value": "@concat('json: ', pipeline().parameters.configFile)",
                        "type": "Expression",
                    },
                },
                "waitOnCompletion": True,
            },
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, ExecutePipelineActivity)
        assert result.parameters == {"ok_value": "{{job.parameters.env}}"}
        # bad_value surfaced as a parameter_approximation for SETUP.md.
        approximations = result.parameter_approximations
        assert any(a.get("widget_name") == "bad_value" for a in approximations)
        # Literal Python source must NOT leak into the parameters dict.
        assert "dbutils.widgets.get" not in str(result.parameters)


class TestDatabricksJobTranslator:
    def test_translate_databricks_job(self):
        from orchestra.translator.activity_translators.databricks_job import translate

        activity = _make_activity(
            "Run Job",
            "DatabricksJob",
            {"jobName": "nightly-agg", "jobId": "12345"},
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, RunJobActivity)
        assert result.job_name == "nightly-agg"
        assert result.existing_job_id == "12345"


class TestWaitTranslator:
    def test_translate_wait(self):
        from orchestra.translator.activity_translators.wait import translate

        activity = _make_activity(
            "Pause",
            "Wait",
            {"waitTimeInSeconds": 60},
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, WaitActivity)
        assert result.wait_time_seconds == 60

    def test_translate_wait_defaults_to_zero(self):
        from orchestra.translator.activity_translators.wait import translate

        activity = _make_activity("Pause", "Wait", {})
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, WaitActivity)
        assert result.wait_time_seconds == 0


class TestFilterTranslator:
    def test_translate_filter(self):
        from orchestra.translator.activity_translators.filter import translate

        activity = _make_activity(
            "Filter Items",
            "Filter",
            {
                "items": {"type": "Expression", "value": "@variables('myList')"},
                "condition": {"type": "Expression", "value": "@not(empty(item()))"},
            },
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, FilterActivity)
        assert result.items_expression is not None
        assert result.condition_expression is not None

    def test_translate_filter_lowers_simple_condition(self):
        """``@equals(item().X, 'Y')`` lowers to a Python expression with item.get(X)."""
        from orchestra.translator.activity_translators.filter import translate

        activity = _make_activity(
            "Filter Active",
            "Filter",
            {
                "items": {"type": "Expression", "value": "@activity('Lookup').output.value"},
                "condition": {"type": "Expression", "value": "@equals(item().status, 'active')"},
            },
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert result.condition_code is not None
        assert "item.get('status')" in result.condition_code
        assert "dbutils.widgets.get" not in result.condition_code

    def test_translate_filter_falls_back_to_placeholder_for_unresolvable(self):
        """A condition that doesn't lower cleanly leaves condition_code=None."""
        from orchestra.translator.activity_translators.filter import translate

        activity = _make_activity(
            "Filter Mystery",
            "Filter",
            {
                "items": {"type": "Expression", "value": "@activity('X').output.value"},
                "condition": {"type": "Expression", "value": "@nonexistent_function(item())"},
            },
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert result.condition_code is None


class TestForEachTranslator:
    def test_translate_foreach_basic(self):
        from orchestra.translator.activity_translators.for_each import translate

        inner_activity = _make_activity(
            "InnerCopy", "Copy", {"source": {"type": "BlobSource"}, "sink": {"type": "DeltaSink"}}
        )
        activity = _make_activity(
            "Loop Items",
            "ForEach",
            {
                "items": {"type": "Expression", "value": "@activity('GetList').output.value"},
                "isSequential": False,
                "batchCount": 5,
            },
            activities=[inner_activity],
        )
        result, context = translate(activity, _base_kwargs("Loop_Items"), _context(), _EMPTY_DEFS)
        assert isinstance(result, ForEachActivity)
        # The translator now resolves to a DAB ref
        assert result.items_expression == "{{tasks.GetList.values.result}}"
        assert result.concurrency == 5

    def test_translate_foreach_sequential(self):
        from orchestra.translator.activity_translators.for_each import translate

        inner_activity = _make_activity("InnerWait", "Wait", {"waitTimeInSeconds": 1})
        activity = _make_activity(
            "Seq Loop",
            "ForEach",
            {"items": "@items", "isSequential": True},
            activities=[inner_activity],
        )
        result, context = translate(activity, _base_kwargs("Seq_Loop"), _context(), _EMPTY_DEFS)
        assert isinstance(result, ForEachActivity)
        assert result.concurrency == 1

    def test_translate_foreach_propagates_globals_to_child_context(self):
        """C-13 (NB-ITER3-001 / CF3-002 / LSC3-004): ForEach child context
        must carry global_parameters and linked_service_parameters so inner
        notebooks resolve @pipeline().globalParameters.X to literals."""
        from orchestra.translator.activity_translators.for_each import translate

        # Inner notebook whose library jar references a global parameter.
        inner_activity = _make_activity(
            "InnerNB",
            "DatabricksNotebook",
            {
                "notebookPath": "/Shared/nb",
                "libraries": [{"jar": "@pipeline().globalParameters.libPath"}],
            },
        )
        activity = _make_activity(
            "Loop",
            "ForEach",
            {"items": "@activity('GetList').output.value"},
            activities=[inner_activity],
        )

        # The parent context carries the global parameter the inner notebook
        # needs.  We use the real notebook translator inside our mock callback
        # so the inner activity is processed exactly as the engine would.
        from orchestra.translator.activity_translators.notebook import translate as translate_nb

        def _mock_translate(activities, ctx, defs):
            results: list[Any] = []
            for child in activities:
                results.append(translate_nb(child, _base_kwargs(child.name), ctx, defs))
            return results, ctx

        ctx = TranslationContext(
            global_parameters=MappingProxyType({"libPath": "/Volumes/my.jar"}),
        )
        result, _ = translate(
            activity,
            _base_kwargs("Loop"),
            ctx,
            _EMPTY_DEFS,
            translate_activities_fn=_mock_translate,
        )
        assert isinstance(result, ForEachActivity)
        inner = result.inner_activities[0]
        assert isinstance(inner, NotebookActivity)
        # The jar should resolve to the literal from global_parameters, not the
        # raw @pipeline() expression.
        assert inner.libraries == [{"jar": "/Volumes/my.jar"}]


class TestIfConditionTranslator:
    def test_translate_if_condition_equals(self):
        from orchestra.translator.activity_translators.if_condition import translate

        true_act = _make_activity("TrueAct", "Wait", {"waitTimeInSeconds": 1})
        false_act = _make_activity("FalseAct", "Wait", {"waitTimeInSeconds": 2})

        def _mock_translate(activities, context, definitions):
            """Mock translate callback that wraps ADF activities as WaitActivity IRs."""
            results = []
            for child in activities:
                results.append(WaitActivity(name=child.name, task_key=child.name, wait_time_seconds=1))
            return results, context

        activity = _make_activity(
            "Branch",
            "IfCondition",
            {"expression": {"type": "Expression", "value": "@equals(pipeline().parameters.env, 'prod')"}},
            if_true_activities=[true_act],
            if_false_activities=[false_act],
        )
        result, context = translate(
            activity,
            _base_kwargs("Branch"),
            _context(),
            _EMPTY_DEFS,
            translate_activities_fn=_mock_translate,
        )
        assert isinstance(result, IfConditionActivity)
        assert result.op == "EQUAL_TO"
        assert len(result.if_true_activities) == 1
        assert len(result.if_false_activities) == 1

    def test_translate_if_condition_greater(self):
        from orchestra.translator.activity_translators.if_condition import translate

        activity = _make_activity(
            "Check Count",
            "IfCondition",
            {"expression": {"type": "Expression", "value": "@greater(activity('Copy').output.rowsCopied, 0)"}},
        )
        result, context = translate(activity, _base_kwargs("Check_Count"), _context(), _EMPTY_DEFS)
        assert isinstance(result, IfConditionActivity)
        assert result.op == "GREATER_THAN"
        assert "tasks.Copy.values.rowsCopied" in result.left
        assert result.right == "0"

    def test_translate_if_condition_empty_bridges_via_notebook(self):
        """C-07 (CF-iter2-001 / VAREX-003): @empty(...) operand routes through
        a bridge SetVariable task rather than shipping as a raw ADF expression."""
        from orchestra.translator.activity_translators.if_condition import translate

        activity = _make_activity(
            "Branch",
            "IfCondition",
            {
                "expression": {
                    "type": "Expression",
                    "value": "@empty(pipeline().parameters.X)",
                }
            },
        )
        result, _ = translate(activity, _base_kwargs("Branch"), _context(), _EMPTY_DEFS)
        assert isinstance(result, IfConditionActivity)
        # Bridge populated and right operand is False (no longer the legacy '0').
        assert result.bridge_notebook_code is not None
        assert "len(" in result.bridge_notebook_code  # @empty -> (len(X) == 0)
        assert result.op == "NOT_EQUAL"
        assert result.right == "False"
        # Left operand is a translator placeholder the preparer rewrites.
        assert result.left.startswith("__BRIDGE__::")

    def test_translate_if_condition_boolean_variable_uses_lowercase_false(self):
        """C-32 (CF4-002): the truthy fallback path emits ``right='false'`` (not
        ``'0'``) when the operand is a known-Boolean variable, since C-21
        SetVariable now writes lowercase ``'true'/'false'`` strings."""
        from orchestra.translator.activity_translators.if_condition import translate

        # Seed the context with a Boolean default-valued variable so the
        # truthy fallback knows the operand renders as 'true'/'false'.
        ctx = _context().with_variable("continue", "_init_continue", dab_ref_value="true")
        activity = _make_activity(
            "Branch",
            "IfCondition",
            {"expression": {"type": "Expression", "value": "@variables('continue')"}},
        )
        result, _ = translate(activity, _base_kwargs("Branch"), ctx, _EMPTY_DEFS)
        assert isinstance(result, IfConditionActivity)
        assert result.op == "NOT_EQUAL"
        # C-32: lowercase 'false' (compatible with C-21 SetVariable output);
        # was '0' before this change.
        assert result.right == "false"

    def test_translate_if_condition_boolean_variable_by_declared_type(self):
        """C-41 (CF5-001): a Boolean variable seeded only by a literal default
        init task never populates variable_value_cache as a dab_ref, so the
        IfCondition fallback must fall back to the declared type and still emit
        ``right='false'`` (not the always-true ``'0'``)."""
        from orchestra.translator.activity_translators.if_condition import translate

        # No dab_ref value cached — only the declared Boolean type is known.
        ctx = _context().with_variable_types({"continue": "Boolean"})
        activity = _make_activity(
            "Branch",
            "IfCondition",
            {"expression": {"type": "Expression", "value": "@variables('continue')"}},
        )
        result, _ = translate(activity, _base_kwargs("Branch"), ctx, _EMPTY_DEFS)
        assert isinstance(result, IfConditionActivity)
        assert result.op == "NOT_EQUAL"
        assert result.right == "false"

    def test_translate_if_condition_boolean_variable_bridges_when_default_literal_known(self):
        """C-43 (CF5-001): when a Boolean variable carries a seeded literal
        default, the IfCondition operand is recomputed locally via a
        BridgeRequest (``left='__BRIDGE__::...'`` + ``bridge_notebook_code``)
        rather than left as a parent-job task-value ref the bundler would
        blank.  This keeps the operand local so an inner-ForEach condition
        survives the dangling-ref safety net."""
        from orchestra.translator.activity_translators.if_condition import translate

        # Declared Boolean type AND a seeded literal default -> bridge.
        ctx = _context().with_variable_types({"continue": "Boolean"}, default_literals={"continue": "true"})
        activity = _make_activity(
            "Branch",
            "IfCondition",
            {"expression": {"type": "Expression", "value": "@variables('continue')"}},
        )
        result, _ = translate(activity, _base_kwargs("Branch"), ctx, _EMPTY_DEFS)
        assert isinstance(result, IfConditionActivity)
        assert result.op == "NOT_EQUAL"
        assert result.left.startswith("__BRIDGE__::")
        assert result.right == "False"
        assert result.bridge_notebook_code == "True"

    def test_translate_if_condition_not_of_function_uses_false_right(self):
        """C-15 (CF3-003 / VAREX3-004): @not(<function-call>) produces a bridge
        task value compared against 'False', not '' or '0', so the condition
        can actually evaluate to FALSE against the Python bool the bridge writes."""
        from orchestra.translator.activity_translators.if_condition import translate

        activity = _make_activity(
            "Branch",
            "IfCondition",
            {
                "expression": {
                    "type": "Expression",
                    # @not(empty(...)) — the bridge writes a Python bool for
                    # the comparison; right operand must be 'False'.
                    "value": "@not(empty(pipeline().parameters.X))",
                }
            },
        )
        result, _ = translate(activity, _base_kwargs("Branch"), _context(), _EMPTY_DEFS)
        assert isinstance(result, IfConditionActivity)
        assert result.op == "NOT_EQUAL"
        # When bridged, right must be 'False' (was '' under the legacy code path).
        assert result.right == "False"
        assert result.left.startswith("__BRIDGE__::") or result.bridge_notebook_code is not None

    def test_translate_if_condition_truthy_fallback_bridges_with_false_right(self):
        """C-15 (CF3-003 / VAREX3-004): the legacy truthy fallback path emits
        right='False' when the resolved operand is a bridge placeholder.
        Previously emitted right='0', which the bridge's Python bool output
        can never satisfy."""
        from orchestra.translator.activity_translators.if_condition import translate

        # An expression with a function call that bridges (e.g. @toUpper).
        activity = _make_activity(
            "Branch",
            "IfCondition",
            {
                "expression": {
                    "type": "Expression",
                    "value": "@toUpper(pipeline().parameters.X)",
                }
            },
        )
        result, _ = translate(activity, _base_kwargs("Branch"), _context(), _EMPTY_DEFS)
        assert isinstance(result, IfConditionActivity)
        assert result.op == "NOT_EQUAL"
        # Either bridge=task value with right='False', or the truthy path -
        # both legitimate; ensure right is not '0'.
        assert result.right != "0"


class TestIfConditionPreparer:
    """C-07: preparer rewrites bridge placeholder to the real task value."""

    def test_prepare_if_condition_emits_bridge_task(self):
        from orchestra.preparer.activity_preparers.if_condition import prepare

        if_act = IfConditionActivity(
            name="Branch",
            task_key="branch",
            op="NOT_EQUAL",
            left="__BRIDGE__::result",
            right="False",
            bridge_notebook_code="(len(dbutils.widgets.get('X')) == 0)",
            bridge_required_parameters={"X": "{{job.parameters.X}}"},
        )
        prepared = prepare(if_act)
        # A bridge task is prepended ahead of the condition.
        bridge_tasks = [t for t in prepared.extra_tasks if t.get("task_key", "").endswith("_bridge")]
        assert len(bridge_tasks) == 1
        bridge_task = bridge_tasks[0]
        assert "notebook_task" in bridge_task
        assert bridge_task["notebook_task"]["base_parameters"] == {"X": "{{job.parameters.X}}"}
        # Condition operand now references the bridge task value.
        cond = prepared.task["condition_task"]
        assert cond["left"] == "{{tasks.branch_bridge.values.result}}"
        assert cond["right"] == "False"
        # Condition depends on the bridge task.
        assert any(dep.get("task_key") == "branch_bridge" for dep in prepared.task.get("depends_on") or [])


class TestSetVariableTranslator:
    def test_translate_set_variable_literal(self):
        from orchestra.translator.activity_translators.set_variable import translate

        activity = _make_activity(
            "Set Status",
            "SetVariable",
            {"variableName": "status", "value": "completed"},
        )
        result, context = translate(activity, _base_kwargs("Set_Status"), _context(), _EMPTY_DEFS)
        assert isinstance(result, SetVariableActivity)
        assert result.variable_name == "status"
        assert result.variable_value == "completed"
        assert result.value_kind == "literal"
        assert result.notebook_code is None
        # Context should have the variable mapped
        assert context.get_variable_task_key("status") == "Set_Status"

    def test_translate_set_variable_return_value_pairs_resolves_inner(self):
        """C-42 (VAREX5-001): a Set Pipeline Return Value list-of-pairs value
        whose inner expression references a resolvable variable lowers to a
        dab_ref task-value reference instead of being stringified and blanked."""
        from orchestra.translator.activity_translators.set_variable import translate

        # Seed the referenced variable so @variables('executionOutputs')
        # resolves to its setter task value.
        ctx = _context().with_variable("executionOutputs", "set_outputs")
        activity = _make_activity(
            "Set Return",
            "SetVariable",
            {
                "variableName": "result",
                "value": [
                    {
                        "key": "result",
                        "value": {
                            "type": "Expression",
                            "content": "@variables('executionOutputs')",
                        },
                    }
                ],
            },
        )
        result, _ = translate(activity, _base_kwargs("Set_Return"), ctx, _EMPTY_DEFS)
        assert isinstance(result, SetVariableActivity)
        assert result.value_kind == "dab_ref"
        assert "{{tasks." in result.variable_value

    def test_translate_set_variable_utcnow(self):
        from orchestra.translator.activity_translators.set_variable import translate

        # ``utcNow('yyyy-MM-dd')`` now maps to a DAB dynamic value, so the
        # SetVariable result is dab_ref rather than notebook_code.
        activity = _make_activity(
            "SetRunDate",
            "SetVariable",
            {"variableName": "runDate", "value": {"type": "Expression", "value": "@utcNow('yyyy-MM-dd')"}},
        )
        result, context = translate(activity, _base_kwargs("SetRunDate"), _context(), _EMPTY_DEFS)
        assert isinstance(result, SetVariableActivity)
        assert result.value_kind == "dab_ref"
        assert result.variable_value == "{{job.start_time.iso_date}}"

    def test_translate_set_variable_split_subscript_lowers_to_notebook_code(self):
        """C-33 (VAREX4-001): ``split(...)[N]`` previously left value_kind
        stamped as 'literal' with the raw @concat text; now it lowers to
        notebook_code so the SetVariable notebook computes the value."""
        from orchestra.translator.activity_translators.set_variable import translate

        activity = _make_activity(
            "SetPart",
            "SetVariable",
            {
                "variableName": "year",
                "value": {
                    "type": "Expression",
                    "value": "@split(pipeline().parameters.referenceDate,'/')[0]",
                },
            },
        )
        result, _ = translate(activity, _base_kwargs("SetPart"), _context(), _EMPTY_DEFS)
        assert isinstance(result, SetVariableActivity)
        assert result.value_kind == "notebook_code"
        assert result.notebook_code is not None
        assert ".split(str('/'))" in result.notebook_code

    def test_translate_set_variable_unresolved_expression_blanks_value(self):
        """C-33 (VAREX4-001 / CF4-003): an ADF expression the resolver
        cannot lower no longer ships as value_kind='literal' with the raw
        @-expression.  The value is blanked, value_kind='unresolved', and
        raw_expression captures the original text for SETUP.md."""
        from orchestra.translator.activity_translators.set_variable import translate

        activity = _make_activity(
            "SetX",
            "SetVariable",
            {
                "variableName": "x",
                "value": {
                    "type": "Expression",
                    # No handler exists for foo(...) so the resolver returns None.
                    "value": "@foo(pipeline().parameters.bar)",
                },
            },
        )
        result, _ = translate(activity, _base_kwargs("SetX"), _context(), _EMPTY_DEFS)
        assert isinstance(result, SetVariableActivity)
        assert result.value_kind == "unresolved"
        assert result.variable_value == ""
        assert result.raw_expression == "@foo(pipeline().parameters.bar)"

    def test_translate_set_variable_utcnow_unknown_format(self):
        from orchestra.translator.activity_translators.set_variable import translate

        # Unrecognised format falls back to the legacy notebook_code path.
        activity = _make_activity(
            "SetRunDate",
            "SetVariable",
            {"variableName": "runDate", "value": {"type": "Expression", "value": "@utcNow('yyyyMMdd')"}},
        )
        result, context = translate(activity, _base_kwargs("SetRunDate"), _context(), _EMPTY_DEFS)
        assert isinstance(result, SetVariableActivity)
        assert result.value_kind == "notebook_code"
        assert result.notebook_code is not None
        assert "strftime" in result.notebook_code
        assert "datetime" in result.notebook_imports[0]

    def test_translate_set_variable_pipeline_param(self):
        from orchestra.translator.activity_translators.set_variable import translate

        activity = _make_activity(
            "Set Env",
            "SetVariable",
            {"variableName": "env", "value": "@pipeline().parameters.environment"},
        )
        result, context = translate(activity, _base_kwargs("Set_Env"), _context(), _EMPTY_DEFS)
        assert isinstance(result, SetVariableActivity)
        assert result.value_kind == "dab_ref"
        assert result.variable_value == "{{job.parameters.environment}}"
        assert result.notebook_code is None


class TestAppendVariableTranslator:
    def test_translate_append_variable(self):
        from orchestra.translator.activity_translators.append_variable import translate

        activity = _make_activity(
            "Append Log",
            "AppendVariable",
            {"variableName": "logEntries", "value": "step1 done"},
        )
        result, context = translate(activity, _base_kwargs("Append_Log"), _context(), _EMPTY_DEFS)
        assert isinstance(result, AppendVariableActivity)
        assert result.variable_name == "logEntries"
        assert result.value_kind == "literal"
        assert result.append_value == "step1 done"
        assert context.get_variable_task_key("logEntries") == "Append_Log"


class TestSwitchTranslator:
    def test_translate_switch_with_cases(self):
        from orchestra.translator.activity_translators.switch import translate

        case_act = _make_activity("CaseWait", "Wait", {"waitTimeInSeconds": 1})
        default_act = _make_activity("DefaultWait", "Wait", {"waitTimeInSeconds": 2})

        def _mock_translate(activities, context, definitions):
            results = []
            for child in activities:
                results.append(WaitActivity(name=child.name, task_key=child.name, wait_time_seconds=1))
            return results, context

        activity = _make_activity(
            "Route",
            "Switch",
            {
                "on": {"type": "Expression", "value": "@item().load_type"},
                "cases": [
                    {"value": "full", "activities": [case_act]},
                    {"value": "incremental", "activities": [case_act]},
                ],
                "defaultActivities": [default_act],
            },
        )
        result, context = translate(
            activity,
            _base_kwargs("Route"),
            _context(),
            _EMPTY_DEFS,
            translate_activities_fn=_mock_translate,
        )
        assert isinstance(result, SwitchActivity)
        assert result.on_expression == "{{input.load_type}}"
        assert len(result.cases) == 2
        assert result.cases[0].value == "full"
        assert result.cases[1].value == "incremental"
        assert len(result.default_activities) == 1

    def test_translate_switch_function_call_routes_through_bridge(self):
        """C-07 (CF-iter2-001 / CF-iter2-003): @toUpper(coalesce(...)) on the
        Switch on-expression lowers to a bridge SetVariable task rather than
        shipping as a raw ADF expression."""
        from orchestra.translator.activity_translators.switch import translate

        activity = _make_activity(
            "Route",
            "Switch",
            {
                "on": {
                    "type": "Expression",
                    "value": "@toUpper(coalesce(item().type, 'default'))",
                },
                "cases": [],
                "defaultActivities": [],
            },
        )
        result, _ = translate(activity, _base_kwargs("Route"), _context(), _EMPTY_DEFS)
        assert isinstance(result, SwitchActivity)
        assert result.bridge_notebook_code is not None
        assert ".upper()" in result.bridge_notebook_code
        # The on-expression carries the translator placeholder so the
        # preparer can rewrite it to the bridge task value.
        assert result.on_expression.startswith("__BRIDGE__::")


class TestVariableInitTasks:
    """C-05 (VAREX-002): init SetVariable tasks for default-valued variables."""

    def test_default_valued_variable_yields_init_task(self):
        from orchestra.models.adf_ast import AdfPipeline, AdfVariable

        pipeline = AdfPipeline(
            name="pl_with_var_default",
            activities=[
                _make_activity(
                    "Echo",
                    "DatabricksNotebook",
                    {
                        "notebookPath": "/Shared/nb",
                        "baseParameters": {
                            "uuid": {"value": "@variables('uuid')", "type": "Expression"},
                        },
                    },
                ),
            ],
            variables={"uuid": AdfVariable(type="String", default_value="seed-value")},
        )
        definitions = AdfDefinitions(pipelines=[pipeline], datasets={}, linked_services={}, triggers=[])
        report = translate_pipeline(pipeline, definitions)
        # An init task is prepended before the regular activities.
        task_keys = [t.task_key for t in report.pipeline.tasks]
        assert "_init_uuid" in task_keys
        init_task = next(t for t in report.pipeline.tasks if t.task_key == "_init_uuid")
        assert isinstance(init_task, SetVariableActivity)
        assert init_task.variable_name == "uuid"
        # Downstream @variables('uuid') routes through the init task value.
        notebook_task = next(t for t in report.pipeline.tasks if t.name == "Echo")
        assert isinstance(notebook_task, NotebookActivity)
        assert notebook_task.base_parameters["uuid"] == "{{tasks._init_uuid.values.uuid}}"

    def test_default_valued_boolean_variable_renders_lowercase(self):
        """VAREX3-002: Boolean variable default ``True`` must serialise as
        the lowercase string 'true' so downstream ADF
        ``@equals(variables('continue'), true)`` evaluates consistently.
        Python's title-case ``'True'`` silently inverted comparisons."""
        from orchestra.models.adf_ast import AdfPipeline, AdfVariable

        pipeline = AdfPipeline(
            name="pl_bool_var",
            activities=[],
            variables={
                "continue_t": AdfVariable(type="Boolean", default_value=True),
                "continue_f": AdfVariable(type="Boolean", default_value=False),
            },
        )
        definitions = AdfDefinitions(pipelines=[pipeline], datasets={}, linked_services={}, triggers=[])
        report = translate_pipeline(pipeline, definitions)
        init_true = next(t for t in report.pipeline.tasks if t.task_key == "_init_continue_t")
        init_false = next(t for t in report.pipeline.tasks if t.task_key == "_init_continue_f")
        assert isinstance(init_true, SetVariableActivity)
        assert isinstance(init_false, SetVariableActivity)
        assert init_true.variable_value == "true"
        assert init_false.variable_value == "false"

    def test_set_variable_with_raw_bool_value_renders_lowercase(self):
        """VAREX3-002: a SetVariable activity carrying a raw Python ``False``
        as its typeProperties.value must serialise as 'false' (lowercase),
        not 'False' (title-case)."""
        from orchestra.models.adf_ast import AdfPipeline

        pipeline = AdfPipeline(
            name="pl_set_var_bool",
            activities=[
                _make_activity(
                    "Reset",
                    "SetVariable",
                    {"variableName": "flag", "value": False},
                ),
            ],
        )
        definitions = AdfDefinitions(pipelines=[pipeline], datasets={}, linked_services={}, triggers=[])
        report = translate_pipeline(pipeline, definitions)
        set_var = next(t for t in report.pipeline.tasks if t.name == "Reset")
        assert isinstance(set_var, SetVariableActivity)
        assert set_var.variable_value == "false"

    def test_default_valued_variable_with_concat_expression(self):
        """An @concat defaultValue resolves like a SetVariable value would."""
        from orchestra.models.adf_ast import AdfPipeline, AdfVariable

        pipeline = AdfPipeline(
            name="pl_var_default_concat",
            activities=[],
            variables={
                "fullPath": AdfVariable(
                    type="String",
                    default_value="@concat('/Volumes/', pipeline().globalParameters.env)",
                )
            },
        )
        definitions = AdfDefinitions(
            pipelines=[pipeline],
            datasets={},
            linked_services={},
            triggers=[],
            global_parameters={"env": "prod"},
        )
        report = translate_pipeline(pipeline, definitions)
        init_task = next(t for t in report.pipeline.tasks if t.task_key == "_init_fullPath")
        assert isinstance(init_task, SetVariableActivity)
        # All-literal concat collapses to a literal (C-01 interplay).
        assert init_task.value_kind == "literal"
        assert init_task.variable_value == "/Volumes/prod"


class TestScheduleCompilation:
    """C-10 (SCHED-001): map AdfTrigger objects onto Pipeline.schedule."""

    def _build_definitions(self, trigger_props, *, runtime_state="Started", trigger_type="ScheduleTrigger"):
        from orchestra.models.adf_ast import AdfPipeline, AdfTrigger

        pipeline = AdfPipeline(name="pl_with_trigger", activities=[])
        props = dict(trigger_props)
        props["runtimeState"] = runtime_state
        trigger = AdfTrigger(
            name="trg",
            type=trigger_type,
            properties=props,
            pipelines=[{"pipelineReference": {"referenceName": "pl_with_trigger"}}],
        )
        definitions = AdfDefinitions(pipelines=[pipeline], datasets={}, linked_services={}, triggers=[trigger])
        return pipeline, definitions

    def test_schedule_trigger_daily_at_specific_time(self):
        pipeline, definitions = self._build_definitions(
            {
                "typeProperties": {
                    "recurrence": {
                        "frequency": "Day",
                        "interval": 1,
                        "schedule": {"hours": [4], "minutes": [30]},
                        "timeZone": "UTC",
                    }
                }
            }
        )
        report = translate_pipeline(pipeline, definitions)
        assert report.pipeline.schedule is not None
        assert report.pipeline.schedule["kind"] == "schedule"
        assert report.pipeline.schedule["quartz_cron_expression"] == "0 30 4 * * ?"
        assert report.pipeline.schedule["timezone_id"] == "UTC"
        assert report.pipeline.schedule["pause_status"] == "UNPAUSED"

    def test_schedule_trigger_derives_time_of_day_from_start_time(self):
        """C-44 (SCHED5-001): a Day recurrence with no schedule block derives
        the cron hour/minute from ``startTime`` instead of silently defaulting
        to midnight."""
        pipeline, definitions = self._build_definitions(
            {
                "typeProperties": {
                    "recurrence": {
                        "frequency": "Day",
                        "interval": 1,
                        "startTime": "2023-03-15T21:00:00Z",
                        "timeZone": "UTC",
                    }
                }
            }
        )
        report = translate_pipeline(pipeline, definitions)
        assert report.pipeline.schedule is not None
        assert report.pipeline.schedule["quartz_cron_expression"] == "0 0 21 * * ?"

    def test_schedule_trigger_interval_3_days_emits_periodic(self):
        """SCHED3-002 + C-36 (SCHED4-001): Day/Week/Month with interval > 1
        emits periodic, AND the time-of-day from the schedule block is
        captured as ``time_of_day_note`` so SETUP.md can surface it."""
        pipeline, definitions = self._build_definitions(
            {
                "typeProperties": {
                    "recurrence": {
                        "frequency": "Day",
                        "interval": 3,
                        "schedule": {"hours": [2]},
                    }
                }
            }
        )
        report = translate_pipeline(pipeline, definitions)
        assert report.pipeline.schedule is not None
        assert report.pipeline.schedule["kind"] == "periodic"
        assert report.pipeline.schedule["interval"] == 3
        assert report.pipeline.schedule["unit"] == "DAYS"
        # C-36: the schedule block (``hours: [2]``) is captured as
        # ``time_of_day_note`` rather than silently dropped.
        assert report.pipeline.schedule["time_of_day_note"] == {"hours": [2]}

    def test_schedule_trigger_interval_2_months_does_not_emit_months_unit(self):
        """C-45 (SCHED5-002): a Month recurrence with interval > 1 must never
        emit a periodic spec with the invalid DAB unit 'MONTHS' (the
        PeriodicTriggerConfigurationTimeUnit enum only has DAYS/HOURS/WEEKS).
        Instead it surfaces a manual setup note."""
        pipeline, definitions = self._build_definitions(
            {
                "typeProperties": {
                    "recurrence": {
                        "frequency": "Month",
                        "interval": 2,
                        "schedule": {"monthDays": [1]},
                    }
                }
            }
        )
        report = translate_pipeline(pipeline, definitions)
        assert report.pipeline.schedule is not None
        assert report.pipeline.schedule.get("unit") != "MONTHS"
        # Either a manual setup note or a cron expr, never a MONTHS periodic.
        assert report.pipeline.schedule["kind"] in ("manual_setup", "schedule")

    def test_schedule_trigger_interval_2_weeks_emits_periodic(self):
        pipeline, definitions = self._build_definitions(
            {
                "typeProperties": {
                    "recurrence": {
                        "frequency": "Week",
                        "interval": 2,
                    }
                }
            }
        )
        report = translate_pipeline(pipeline, definitions)
        assert report.pipeline.schedule["kind"] == "periodic"
        assert report.pipeline.schedule["unit"] == "WEEKS"
        assert report.pipeline.schedule["interval"] == 2

    def test_trigger_carries_per_pipeline_parameter_overrides(self):
        """SCHED3-003: parameters on the trigger's pipelineReference entry
        must surface on the schedule spec so the bundler can mutate the
        matching job.parameter defaults for scheduled runs."""
        from orchestra.models.adf_ast import AdfParameter, AdfPipeline, AdfTrigger

        pipeline = AdfPipeline(
            name="pl_with_overrides",
            activities=[],
            parameters={
                "negocio": AdfParameter(type="String", default_value="DEFAULT"),
                "applicationName": AdfParameter(type="String", default_value="DEFAULT"),
            },
        )
        trigger = AdfTrigger(
            name="nightly",
            type="ScheduleTrigger",
            properties={
                "runtimeState": "Started",
                "typeProperties": {
                    "recurrence": {
                        "frequency": "Day",
                        "interval": 1,
                        "schedule": {"hours": [2]},
                    }
                },
            },
            pipelines=[
                {
                    "pipelineReference": {"referenceName": "pl_with_overrides"},
                    "parameters": {
                        "negocio": "GLP",
                        "applicationName": "cli0010",
                    },
                }
            ],
        )
        definitions = AdfDefinitions(pipelines=[pipeline], datasets={}, linked_services={}, triggers=[trigger])
        report = translate_pipeline(pipeline, definitions)
        assert report.pipeline.schedule is not None
        overrides = report.pipeline.schedule.get("parameter_overrides") or {}
        assert overrides["negocio"] == "GLP"
        assert overrides["applicationName"] == "cli0010"

    def test_schedule_trigger_interval_1_day_still_cron(self):
        """Interval == 1 stays on the cron path so we keep timezone/hour spec."""
        pipeline, definitions = self._build_definitions(
            {
                "typeProperties": {
                    "recurrence": {
                        "frequency": "Day",
                        "interval": 1,
                        "schedule": {"hours": [4], "minutes": [30]},
                    }
                }
            }
        )
        report = translate_pipeline(pipeline, definitions)
        assert report.pipeline.schedule["kind"] == "schedule"
        assert "quartz_cron_expression" in report.pipeline.schedule

    def test_schedule_trigger_runtime_state_stopped_pauses(self):
        pipeline, definitions = self._build_definitions(
            {
                "typeProperties": {
                    "recurrence": {
                        "frequency": "Day",
                        "interval": 1,
                        "schedule": {"hours": [0], "minutes": [0]},
                        "timeZone": "UTC",
                    }
                }
            },
            runtime_state="Stopped",
        )
        report = translate_pipeline(pipeline, definitions)
        assert report.pipeline.schedule["pause_status"] == "PAUSED"

    def test_schedule_trigger_normalises_romance_standard_time(self):
        pipeline, definitions = self._build_definitions(
            {
                "typeProperties": {
                    "recurrence": {
                        "frequency": "Day",
                        "interval": 1,
                        "schedule": {"hours": [8], "minutes": [0]},
                        "timeZone": "Romance Standard Time",
                    }
                }
            }
        )
        report = translate_pipeline(pipeline, definitions)
        # Romance Standard Time -> Europe/Madrid per the IANA map.
        assert report.pipeline.schedule["timezone_id"] == "Europe/Madrid"

    def test_schedule_trigger_weekly_with_week_days(self):
        pipeline, definitions = self._build_definitions(
            {
                "typeProperties": {
                    "recurrence": {
                        "frequency": "Week",
                        "interval": 1,
                        "schedule": {
                            "hours": [9],
                            "minutes": [0],
                            "weekDays": ["Monday", "Wednesday", "Friday"],
                        },
                        "timeZone": "UTC",
                    }
                }
            }
        )
        report = translate_pipeline(pipeline, definitions)
        assert report.pipeline.schedule["quartz_cron_expression"] == "0 0 9 ? * MON,WED,FRI"

    def test_tumbling_window_trigger_surfaces_setup_note(self):
        pipeline, definitions = self._build_definitions(
            {
                "typeProperties": {
                    "frequency": "Hour",
                    "interval": 1,
                }
            },
            trigger_type="TumblingWindowTrigger",
        )
        report = translate_pipeline(pipeline, definitions)
        assert report.pipeline.schedule is not None
        assert report.pipeline.schedule["kind"] == "schedule"
        assert report.pipeline.schedule["tumbling"] is True

    def test_blob_events_trigger_maps_to_file_arrival(self):
        pipeline, definitions = self._build_definitions(
            {
                "typeProperties": {
                    "scope": "/subscriptions/x/y",
                    "events": ["Microsoft.Storage.BlobCreated"],
                }
            },
            trigger_type="BlobEventsTrigger",
        )
        report = translate_pipeline(pipeline, definitions)
        assert report.pipeline.schedule is not None
        assert report.pipeline.schedule["kind"] == "file_arrival"
        assert report.pipeline.schedule["url"] == "/subscriptions/x/y"

    def test_custom_events_trigger_routed_to_manual_setup(self):
        pipeline, definitions = self._build_definitions(
            {"typeProperties": {}},
            trigger_type="CustomEventsTrigger",
        )
        report = translate_pipeline(pipeline, definitions)
        assert report.pipeline.schedule["kind"] == "manual_setup"


class TestTranslateEngine:
    def test_translate_pipeline_produces_report(self, adf_definitions):
        """translate_pipeline returns a TranslationReport for every pipeline."""
        for pipeline in adf_definitions.pipelines:
            report = translate_pipeline(pipeline, adf_definitions)
            assert report.pipeline is not None
            assert isinstance(report.pipeline, Pipeline)
            assert report.pipeline.name == pipeline.name
            total = report.deterministic_count + report.agentic_count + report.unsupported_count
            assert total > 0

    def test_translate_pipeline_gaps_tracked(self, adf_definitions):
        """Agentic and unsupported types produce gap entries."""
        # pipeline_mixed_agentic has ExecuteDataFlow, SqlServerStoredProcedure, etc.
        mixed = next(pl for pl in adf_definitions.pipelines if pl.name == "pipeline_mixed_agentic")
        report = translate_pipeline(mixed, adf_definitions)
        assert report.agentic_count > 0 or report.unsupported_count > 0
        assert len(report.gaps) > 0

    def test_translate_pipeline_deterministic_only(self, adf_definitions):
        """Pipeline with only deterministic types has zero gaps."""
        basic = next(pl for pl in adf_definitions.pipelines if pl.name == "pipeline_notebook_basic")
        report = translate_pipeline(basic, adf_definitions)
        assert report.deterministic_count > 0
        assert report.agentic_count == 0
        assert report.unsupported_count == 0
        assert len(report.gaps) == 0

    def test_translate_pipeline_preserves_parameters(self, adf_definitions):
        """Pipeline parameters are forwarded to the IR."""
        csv = next(pl for pl in adf_definitions.pipelines if pl.name == "pipeline_copy_csv_to_delta")
        report = translate_pipeline(csv, adf_definitions)
        param_names = {param["name"] for param in report.pipeline.parameters} if report.pipeline.parameters else set()
        assert "sourceFolderPath" in param_names
        assert "triggerDate" in param_names

    def test_translate_pipeline_placeholder_for_agentic(self, adf_definitions):
        """Agentic activities produce PlaceholderActivity in the IR."""
        mixed = next(pl for pl in adf_definitions.pipelines if pl.name == "pipeline_mixed_agentic")
        report = translate_pipeline(mixed, adf_definitions)
        placeholders = [task for task in report.pipeline.tasks if isinstance(task, PlaceholderActivity)]
        assert len(placeholders) > 0
        for placeholder in placeholders:
            assert placeholder.original_type is not None
