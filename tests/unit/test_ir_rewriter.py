"""Unit tests for the whole-IR expression rewriter."""

from __future__ import annotations

from orchestra.models.ir import (
    CopyActivity,
    Dependency,
    ForEachActivity,
    IfConditionActivity,
    LookupActivity,
    NotebookActivity,
    Pipeline,
    SetVariableActivity,
    SwitchActivity,
    SwitchCase,
    WebActivity,
)
from orchestra.parser.ir_rewriter import rewrite_pipeline_expressions


def _base(task_key: str, name: str | None = None) -> dict[str, object]:
    return {"name": name or task_key, "task_key": task_key}


class TestRewritePipelineExpressions:
    def test_rewrites_sql_inside_source_properties_dict(self):
        """Raw @{} tokens inside Copy's source_properties must be rewritten."""
        copy = CopyActivity(
            **_base("copy_orders"),
            source_type="AzureSqlSource",
            source_properties={
                "sql": "SELECT * FROM dbo.orders WHERE modified_dt >= '@{pipeline().parameters.watermark}'"
            },
            sink_type="ParquetSink",
        )
        pipeline = Pipeline(name="p", parameters=[{"name": "watermark", "default": None}], tasks=[copy])
        rewritten = rewrite_pipeline_expressions(pipeline)
        sql = rewritten.tasks[0].source_properties["sql"]
        assert "@{" not in sql
        assert "{{job.parameters.watermark}}" in sql

    def test_rewrites_strings_inside_lists(self):
        """@{} tokens inside list-valued fields are rewritten too."""
        # CopyActivity has no list-of-strings field, so use a list inside a dict
        copy = CopyActivity(
            **_base("copy"),
            source_type="AzureSqlSource",
            source_properties={
                "partitions": ["p_@{pipeline().parameters.region}_1", "p_@{pipeline().parameters.region}_2"]
            },
        )
        pipeline = Pipeline(name="p", tasks=[copy])
        rewritten = rewrite_pipeline_expressions(pipeline)
        partitions = rewritten.tasks[0].source_properties["partitions"]
        assert all("@{" not in p for p in partitions)
        assert all("{{job.parameters.region}}" in p for p in partitions)

    def test_rewrites_web_activity_url_body_and_headers(self):
        """Web activities embed @{} tokens in url/body/headers."""
        web = WebActivity(
            **_base("call_api"),
            url="https://api.example.com/jobs/@{pipeline().parameters.job_id}",
            method="POST",
            body='{"run":"@{pipeline().RunId}"}',
            headers={"X-Tenant": "@{pipeline().parameters.tenant}"},
        )
        pipeline = Pipeline(name="p", tasks=[web])
        rewritten = rewrite_pipeline_expressions(pipeline)
        task = rewritten.tasks[0]
        assert "{{job.parameters.job_id}}" in task.url
        assert "{{job.run_id}}" in task.body
        assert "{{job.parameters.tenant}}" in task.headers["X-Tenant"]

    def test_recurses_into_foreach_inner_activities(self):
        inner = CopyActivity(
            **_base("inner_copy"),
            source_type="AzureSqlSource",
            source_properties={"sql": "SELECT * FROM @{item().table_name}"},
        )
        for_each = ForEachActivity(
            **_base("loop"),
            items_expression="@activity('lookup').output.value",
            inner_activities=[inner],
        )
        pipeline = Pipeline(name="p", tasks=[for_each])
        rewritten = rewrite_pipeline_expressions(pipeline)
        inner_rewritten = rewritten.tasks[0].inner_activities[0]
        assert "@{" not in inner_rewritten.source_properties["sql"]
        assert "{{input.table_name}}" in inner_rewritten.source_properties["sql"]

    def test_recurses_into_if_condition_branches(self):
        true_branch = NotebookActivity(
            **_base("true_nb"),
            notebook_path="/Shared/promote",
            base_parameters={"score": "@{activity('lookup').output.firstRow.quality_score}"},
        )
        false_branch = NotebookActivity(
            **_base("false_nb"),
            notebook_path="/Shared/remediate",
            base_parameters={"score": "@{activity('lookup').output.firstRow.quality_score}"},
        )
        ifc = IfConditionActivity(
            **_base("gate"),
            op="greaterOrEquals",
            left="@activity('lookup').output.firstRow.quality_score",
            right="0.95",
            if_true_activities=[true_branch],
            if_false_activities=[false_branch],
        )
        pipeline = Pipeline(name="p", tasks=[ifc])
        rewritten = rewrite_pipeline_expressions(pipeline)
        ifc_out = rewritten.tasks[0]
        for nb in (ifc_out.if_true_activities[0], ifc_out.if_false_activities[0]):
            assert "@{" not in nb.base_parameters["score"]

    def test_recurses_into_switch_cases(self):
        case_nb = NotebookActivity(
            **_base("case_nb"),
            notebook_path="/Shared/h",
            base_parameters={"d": "@{pipeline().parameters.dt}"},
        )
        default_nb = NotebookActivity(
            **_base("default_nb"),
            notebook_path="/Shared/d",
            base_parameters={"d": "@{pipeline().parameters.dt}"},
        )
        switch = SwitchActivity(
            **_base("sw"),
            on_expression="@pipeline().parameters.mode",
            cases=[SwitchCase(value="full", activities=[case_nb])],
            default_activities=[default_nb],
        )
        pipeline = Pipeline(name="p", tasks=[switch])
        rewritten = rewrite_pipeline_expressions(pipeline)
        case_out = rewritten.tasks[0].cases[0].activities[0]
        default_out = rewritten.tasks[0].default_activities[0]
        assert "{{job.parameters.dt}}" in case_out.base_parameters["d"]
        assert "{{job.parameters.dt}}" in default_out.base_parameters["d"]

    def test_skips_linked_service_definition_raw_field(self):
        """linked_service_definition holds raw ADF input and must not be touched."""
        raw_ls = {"type": "AzureSqlDatabase", "connectionString": "@{pipeline().parameters.cs}"}
        nb = NotebookActivity(
            **_base("nb"),
            notebook_path="/Shared/x",
            linked_service_definition=raw_ls,
        )
        pipeline = Pipeline(name="p", tasks=[nb])
        rewritten = rewrite_pipeline_expressions(pipeline)
        # linked_service_definition stays verbatim — raw ADF passthrough
        assert rewritten.tasks[0].linked_service_definition == raw_ls

    def test_unresolved_tokens_remain_and_surface_as_warning(self):
        """An expression the parser can't resolve must (a) stay in the output and
        (b) be recorded as a warning so the user sees the gap."""
        copy = CopyActivity(
            **_base("copy"),
            source_type="AzureSqlSource",
            source_properties={"sql": "SELECT * FROM @{activity('NonexistentActivity').output.unknownField}"},
        )
        pipeline = Pipeline(name="p", tasks=[copy])
        warnings: list[str] = []
        rewritten = rewrite_pipeline_expressions(pipeline, warnings=warnings)
        # Unresolved tokens get replaced by their best-effort dab_ref by the
        # parser (activity-output → tasks.X.values...).  When even the parser
        # cannot produce *anything* it leaves the original @{} verbatim; in
        # that case we must surface a warning.  The activity-output regex
        # *does* match unknownField, so this particular case resolves
        # silently.  Confirm the more pathological "completely unknown
        # function" case raises a warning instead.
        del rewritten

        weird_copy = CopyActivity(
            **_base("weird"),
            source_type="AzureSqlSource",
            source_properties={"sql": "SELECT @{nonsense('foo')} FROM t"},
        )
        warnings = []
        rewritten = rewrite_pipeline_expressions(Pipeline(name="p", tasks=[weird_copy]), warnings=warnings)
        assert "@{" in rewritten.tasks[0].source_properties["sql"]
        assert any("Unresolved ADF expression" in w for w in warnings)

    def test_identifier_fields_are_left_alone(self):
        """task_key and name must never be rewritten — they are reference identifiers."""
        nb = NotebookActivity(
            name="@{pipeline().parameters.foo}",  # intentionally bizarre
            task_key="weird_name_with_@{x}_token",
            notebook_path="/Shared/x",
        )
        pipeline = Pipeline(name="p", tasks=[nb])
        rewritten = rewrite_pipeline_expressions(pipeline)
        assert rewritten.tasks[0].name == "@{pipeline().parameters.foo}"
        assert rewritten.tasks[0].task_key == "weird_name_with_@{x}_token"

    def test_pipeline_with_no_tokens_returns_equivalent_pipeline(self):
        nb = NotebookActivity(
            **_base("nb"),
            notebook_path="/Shared/etl",
            base_parameters={"date": "2026-05-30"},
        )
        pipeline = Pipeline(name="p", tasks=[nb])
        rewritten = rewrite_pipeline_expressions(pipeline)
        # Same logical content
        assert rewritten.tasks[0].base_parameters == {"date": "2026-05-30"}

    def test_variable_tokens_resolve_via_set_variable_setter(self):
        """@{variables('x')} should pick up the SetVariableActivity's task_key."""
        setter = SetVariableActivity(
            **_base("set_x"),
            variable_name="x",
            variable_value="42",
        )
        consumer = NotebookActivity(
            **_base("nb"),
            notebook_path="/Shared/q",
            base_parameters={"x_value": "@{variables('x')}"},
            depends_on=[Dependency(task_key="set_x")],
        )
        pipeline = Pipeline(name="p", tasks=[setter, consumer])
        rewritten = rewrite_pipeline_expressions(pipeline)
        consumer_out = rewritten.tasks[1]
        assert "@{" not in consumer_out.base_parameters["x_value"]
        # Resolves to task value reference for the setter
        assert "tasks.set_x.values.x" in consumer_out.base_parameters["x_value"]

    def test_lookup_source_query_rewritten(self):
        lookup = LookupActivity(
            **_base("lookup_w"),
            source_type="AzureSqlSource",
            source_query="SELECT MAX(modified_dt) FROM dbo.@{pipeline().parameters.table_name}",
        )
        pipeline = Pipeline(name="p", tasks=[lookup])
        rewritten = rewrite_pipeline_expressions(pipeline)
        assert "{{job.parameters.table_name}}" in rewritten.tasks[0].source_query
