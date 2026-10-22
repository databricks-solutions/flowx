"""The report writer emits cost_comparison files under metadata/tco/."""

from __future__ import annotations

import csv
from pathlib import Path

from flowx.sources.adf.profile import report
from flowx.sources.adf.profile.models import ActualCost, PipelineCost, ProfileResult


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


def test_csv_is_one_table_with_a_level_column(tmp_path):
    report.write_reports(_result(), tmp_path)
    csv_text = (tmp_path / "metadata" / "tco" / "cost_comparison.csv").read_text()
    assert csv_text == (
        "level,subscription,factory,pipeline,runs,est_orchestration,est_data_movement,est_pipeline_activity,"
        "est_external,est_total,actual_compute,actual_total\n"
        "factory,Demo Subscription,demo-factory,,,0.0050,0.0800,0.0000,0.0000,0.0850,12.3400,12.3400\n"
        "pipeline,Demo Subscription,demo-factory,copy_pipeline,5,0.0050,0.0800,0.0000,0.0000,0.0850,,\n"
    )


def test_csv_quotes_values_that_contain_commas(tmp_path):
    result = _result(
        pipeline_costs=[_pipeline('copy, "daily"', orchestration_cost=0.5)],
        factory_subscriptions={"demo-factory": "Sub, Prod"},
    )
    report.write_reports(result, tmp_path)
    with (tmp_path / "metadata" / "tco" / "cost_comparison.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [(row["level"], row["subscription"], row["pipeline"]) for row in rows] == [
        ("factory", "Sub, Prod", ""),
        ("pipeline", "Sub, Prod", 'copy, "daily"'),
    ]


def test_factory_with_only_actuals_has_zero_estimates(tmp_path):
    result = _result(
        pipeline_costs=[],
        actual_costs=[ActualCost(factory_name="billed-only", meter_subcategory="", cost=5.0, currency="USD")],
        factory_subscriptions={},
    )
    report.write_reports(result, tmp_path)
    lines = (tmp_path / "metadata" / "tco" / "cost_comparison.csv").read_text().splitlines()
    assert lines[1] == "factory,Unknown,billed-only,,,0.0000,0.0000,0.0000,0.0000,0.0000,5.0000,5.0000"


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
    assert lines[1].endswith(",12.0000,12.0000")


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
    assert lines[1].endswith(",26.8000,26.8000")


def test_mapping_data_flow_meters_count_as_actual_compute(tmp_path):
    # Live Cost Management bills Mapping Data Flow vCores under their own subcategory per compute tier.
    # The estimate prices ExecuteDataFlow, so those count; wrangling data flows aren't estimated, so they
    # stay out on both sides.
    def billed(subcategory: str, cost: float) -> ActualCost:
        return ActualCost(factory_name="demo-factory", meter_subcategory=subcategory, cost=cost, currency="USD")

    result = _result(
        actual_costs=[
            billed("Azure Data Factory v2", 0.25),
            billed("Azure Data Factory v2 Data Flow - General Purpose", 0.5),
            billed("Azure Data Factory v2 Data Flow - Memory Optimized", 1.0),
            billed("Azure Data Factory v2 Data Flow - Compute Optimized", 2.0),
            billed("Azure Data Factory v2 Wrangling Data Flow - General Purpose", 40.0),
        ]
    )
    report.write_reports(result, tmp_path)
    lines = (tmp_path / "metadata" / "tco" / "cost_comparison.csv").read_text().splitlines()
    assert lines[1].endswith(",3.7500,3.7500")


def test_markdown_ends_with_every_warning(tmp_path):
    warnings = ["f: Cannot query pipeline runs (need Data Factory Contributor)", "first line\nsecond line"]
    report.write_reports(_result(warnings=warnings), tmp_path)
    md = (tmp_path / "metadata" / "tco" / "cost_comparison.md").read_text()
    assert md.endswith(
        "## Warnings\n\n- f: Cannot query pipeline runs (need Data Factory Contributor)\n- first line\n  second line\n"
    )


def test_markdown_has_no_warnings_section_without_warnings(tmp_path):
    report.write_reports(_result(), tmp_path)
    md = (tmp_path / "metadata" / "tco" / "cost_comparison.md").read_text()
    assert "## Warnings" not in md


def test_warnings_do_not_change_the_csv(tmp_path):
    report.write_reports(_result(), tmp_path / "plain")
    report.write_reports(_result(warnings=["westeurope: couldn't fetch live Azure prices"]), tmp_path / "warned")
    csv_path = Path("metadata") / "tco" / "cost_comparison.csv"
    assert (tmp_path / "warned" / csv_path).read_text() == (tmp_path / "plain" / csv_path).read_text()
