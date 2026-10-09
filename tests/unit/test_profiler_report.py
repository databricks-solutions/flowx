"""The report writer emits cost_comparison files under metadata/tco/."""

from __future__ import annotations

from flowx.sources.adf.profiler import report
from flowx.sources.adf.profiler.models import ActualCost, PipelineCost, ProfileResult


def _pipeline(name: str, factory: str = "demo-factory", **costs: float) -> PipelineCost:
    values = {"orchestration_cost": 0.0, "data_movement_cost": 0.0, "pipeline_activity_cost": 0.0, "external_cost": 0.0}
    values.update(costs)
    return PipelineCost(
        factory_name=factory, pipeline_name=name, total_runs=5, total_cost=round(sum(values.values()), 4), **values
    )


def _result(**overrides) -> ProfileResult:
    fields = {
        "region": "eastus",
        "pricing_source": "default list rates",
        "days": 90,
        "total_pipeline_runs": 5,
        "pipeline_costs": [_pipeline("copy_pipeline", orchestration_cost=0.005, data_movement_cost=0.08)],
        "actual_costs": [
            ActualCost(factory_name="demo-factory", meter_subcategory="", cost=12.34, currency="USD"),
            ActualCost(factory_name="demo-factory", meter_subcategory="Managed Airflow", cost=99.0, currency="USD"),
        ],
        "factory_subscriptions": {"demo-factory": "Demo Subscription"},
    }
    fields.update(overrides)
    return ProfileResult(**fields)


def test_write_reports_creates_tco_files(tmp_path):
    paths = report.write_reports(_result(), tmp_path)
    tco = tmp_path / "metadata" / "tco"
    assert paths == {
        "cost_comparison_csv": str(tco / "cost_comparison.csv"),
        "cost_comparison_md": str(tco / "cost_comparison.md"),
    }
    md = (tco / "cost_comparison.md").read_text()
    assert "copy_pipeline" in md
    assert "demo-factory" in md


def test_write_reports_overwrites_not_timestamps(tmp_path):
    report.write_reports(_result(), tmp_path)
    report.write_reports(_result(), tmp_path)
    tco = tmp_path / "metadata" / "tco"
    assert sorted(path.name for path in tco.iterdir()) == ["cost_comparison.csv", "cost_comparison.md"]


def test_csv_matches_original_layout(tmp_path):
    report.write_reports(_result(), tmp_path)
    csv_text = (tmp_path / "metadata" / "tco" / "cost_comparison.csv").read_text()
    assert csv_text == (
        "## Factory-Level Costs (Estimated vs Actual)\n"
        "subscription,factory,est_orchestration,est_data_movement,est_pipeline_activity,est_external,est_total,"
        "actual_compute,actual_total\n"
        # Only empty-MeterSubcategory rows count as actual compute; the $99 infra row is excluded.
        "Demo Subscription,demo-factory,$0.0050,$0.0800,$0.0000,$0.0000,$0.0850,$12.3400,$12.3400\n"
        "\n"
        "## Pipeline-Level Costs (Estimates Only)\n"
        "subscription,factory,pipeline,runs,est_orchestration,est_data_movement,est_pipeline_activity,est_external,"
        "est_total\n"
        "Demo Subscription,demo-factory,copy_pipeline,5,$0.0050,$0.0800,$0.0000,$0.0000,$0.0850\n"
    )


def test_factory_with_only_actuals_prints_integer_estimates(tmp_path):
    # Ported quirk: missing estimates default to int 0, which the original prints without "$".
    result = _result(
        pipeline_costs=[],
        actual_costs=[ActualCost(factory_name="billed-only", meter_subcategory="", cost=5.0, currency="USD")],
        factory_subscriptions={},
    )
    report.write_reports(result, tmp_path)
    lines = (tmp_path / "metadata" / "tco" / "cost_comparison.csv").read_text().splitlines()
    assert lines[2] == "Unknown,billed-only,0,0,0,0,0,$5.0000,$5.0000"


def test_zero_cost_pipelines_and_factories_are_omitted(tmp_path):
    result = _result(pipeline_costs=[_pipeline("idle", factory="idle-factory")], actual_costs=[])
    report.write_reports(result, tmp_path)
    csv_text = (tmp_path / "metadata" / "tco" / "cost_comparison.csv").read_text()
    assert "idle" not in csv_text


def test_rows_sorted_by_cost_descending(tmp_path):
    result = _result(
        pipeline_costs=[_pipeline("cheap", orchestration_cost=0.001), _pipeline("pricey", orchestration_cost=0.5)]
    )
    report.write_reports(result, tmp_path)
    md = (tmp_path / "metadata" / "tco" / "cost_comparison.md").read_text()
    assert md.index("| pricey |") < md.index("| cheap |")


def test_markdown_has_context_summary_and_difference(tmp_path):
    report.write_reports(_result(), tmp_path)
    md = (tmp_path / "metadata" / "tco" / "cost_comparison.md").read_text()
    assert md.startswith("# Cost Comparison: Estimated vs Actual\n")
    assert "| Profiling Window | 90 days |" in md
    assert "| Pricing Source | default list rates |" in md
    assert "| Demo Subscription | $0.0850 | $12.3400 | $12.2550 |" in md
    assert "| **TOTAL** | **$0.0850** | **$12.3400** | **$12.2550** |" in md


def test_markdown_omits_context_when_no_runs(tmp_path):
    report.write_reports(_result(total_pipeline_runs=0, pipeline_costs=[]), tmp_path)
    md = (tmp_path / "metadata" / "tco" / "cost_comparison.md").read_text()
    assert "Profiling Context" not in md
    assert "Pipeline-Level Costs" not in md


def test_markdown_lists_denied_subscriptions(tmp_path):
    result = _result(cost_management_denied=["SUB", "NAMELESS"], subscription_names={"SUB": "Demo Subscription"})
    report.write_reports(result, tmp_path)
    md = (tmp_path / "metadata" / "tco" / "cost_comparison.md").read_text()
    assert "- **Demo Subscription (SUB)**: Cost Management access denied" in md
    assert "- **NAMELESS**: Cost Management access denied" in md


def test_whole_number_actuals_still_print_as_dollars(tmp_path):
    # Cost Management can return an integer PreTaxCost; the original summed into a float default.
    result = _result(
        actual_costs=[ActualCost(factory_name="demo-factory", meter_subcategory="", cost=12, currency="USD")]
    )
    report.write_reports(result, tmp_path)
    lines = (tmp_path / "metadata" / "tco" / "cost_comparison.csv").read_text().splitlines()
    assert lines[2].endswith(",$12.0000,$12.0000")


def test_live_compute_meter_counts_as_actual_compute(tmp_path):
    # Live Cost Management tags ADF compute "Azure Data Factory v2", not an empty subcategory.
    # Infrastructure meters (even ones sharing that prefix) stay out of the comparison.
    result = _result(
        actual_costs=[
            ActualCost(
                factory_name="demo-factory", meter_subcategory="Azure Data Factory v2", cost=26.80, currency="USD"
            ),
            ActualCost(
                factory_name="demo-factory",
                meter_subcategory="Azure Data Factory v2 - Managed Airflow",
                cost=654.45,
                currency="USD",
            ),
            ActualCost(
                factory_name="demo-factory", meter_subcategory="Virtual Network Private Link", cost=6.52, currency="USD"
            ),
        ]
    )
    report.write_reports(result, tmp_path)
    lines = (tmp_path / "metadata" / "tco" / "cost_comparison.csv").read_text().splitlines()
    assert lines[2].endswith(",$26.8000,$26.8000")
