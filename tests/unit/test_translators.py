"""Unit tests for individual activity translators and the translation engine."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any

from flowx.models.adf_ast import (
    AdfActivity,
    AdfDatasetReference,
    AdfDefinitions,
    AdfDependency,
    AdfLinkedServiceReference,
    AdfPolicy,
)
from flowx.models.ir import (
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
from flowx.translator.engine import translate_pipeline

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
        from flowx.translator.activity_translators.copy import translate

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
        from flowx.translator.activity_translators.copy import translate

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
        from flowx.translator.activity_translators.copy import translate

        activity = _make_activity("Empty Copy", "Copy", {})
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, CopyActivity)
        assert result.source_type is None
        assert result.sink_type is None


class TestNotebookTranslator:
    def test_translate_notebook_basic(self):
        from flowx.translator.activity_translators.notebook import translate

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
        from flowx.translator.activity_translators.notebook import translate

        activity = _make_activity(
            "Run Notebook",
            "DatabricksNotebook",
            {"notebookPath": "/Shared/ETL/simple"},
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, NotebookActivity)
        assert result.base_parameters == {}

    def test_translate_notebook_passes_libraries_through(self):
        from flowx.translator.activity_translators.notebook import translate

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

    def test_translate_notebook_captures_utcnow_approximation(self):
        from flowx.translator.activity_translators.notebook import translate

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
        from flowx.models.adf_ast import AdfLinkedService
        from flowx.translator.engine import _build_base_kwargs

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
        from flowx.models.adf_ast import AdfLinkedService
        from flowx.translator.engine import _build_base_kwargs

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


class TestSparkJarTranslator:
    def test_translate_spark_jar(self):
        from flowx.translator.activity_translators.spark_jar import translate

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
        from flowx.translator.activity_translators.spark_python import translate

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
        from flowx.translator.activity_translators.spark_python import translate

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
        from flowx.translator.activity_translators.lookup import translate

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
        from flowx.translator.activity_translators.lookup import translate

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


class TestWebActivityTranslator:
    def test_translate_web_activity_get(self):
        from flowx.translator.activity_translators.web_activity import translate

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
        from flowx.translator.activity_translators.web_activity import translate

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
        from flowx.translator.activity_translators.delete import translate

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
        from flowx.translator.activity_translators.execute_pipeline import translate

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


class TestDatabricksJobTranslator:
    def test_translate_databricks_job(self):
        from flowx.translator.activity_translators.databricks_job import translate

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
        from flowx.translator.activity_translators.wait import translate

        activity = _make_activity(
            "Pause",
            "Wait",
            {"waitTimeInSeconds": 60},
        )
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, WaitActivity)
        assert result.wait_time_seconds == 60

    def test_translate_wait_defaults_to_zero(self):
        from flowx.translator.activity_translators.wait import translate

        activity = _make_activity("Pause", "Wait", {})
        result = translate(activity, _base_kwargs(), _context(), _EMPTY_DEFS)
        assert isinstance(result, WaitActivity)
        assert result.wait_time_seconds == 0


class TestFilterTranslator:
    def test_translate_filter(self):
        from flowx.translator.activity_translators.filter import translate

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
        from flowx.translator.activity_translators.filter import translate

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
        from flowx.translator.activity_translators.filter import translate

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
        from flowx.translator.activity_translators.for_each import translate

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
        from flowx.translator.activity_translators.for_each import translate

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


class TestIfConditionTranslator:
    def test_translate_if_condition_equals(self):
        from flowx.translator.activity_translators.if_condition import translate

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
        from flowx.translator.activity_translators.if_condition import translate

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


class TestSetVariableTranslator:
    def test_translate_set_variable_literal(self):
        from flowx.translator.activity_translators.set_variable import translate

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

    def test_translate_set_variable_utcnow(self):
        from flowx.translator.activity_translators.set_variable import translate

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

    def test_translate_set_variable_utcnow_unknown_format(self):
        from flowx.translator.activity_translators.set_variable import translate

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
        from flowx.translator.activity_translators.set_variable import translate

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
        from flowx.translator.activity_translators.append_variable import translate

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
        from flowx.translator.activity_translators.switch import translate

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
