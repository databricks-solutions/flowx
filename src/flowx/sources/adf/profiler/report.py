"""Writes the profiler's cost report: estimated ADF spend vs Azure-billed actuals.

Ported from the standalone script's `_export_cost_comparison`, reading the typed ProfileResult
instead of the script's stats. Output goes to `<output_dir>/metadata/tco/` and is overwritten on
every run (no timestamped folders), so the skill and MCP tools always find the current report.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from flowx.sources.adf.profiler.models import ProfileResult

# Meter subcategories Cost Management uses for ADF compute/orchestration. Live data tags it
# "Azure Data Factory v2"; the original script assumed an empty subcategory, so both count. Matched
# exactly: infrastructure meters such as "Azure Data Factory v2 - Managed Airflow" share the prefix.
_COMPUTE_METER_SUBCATEGORIES = frozenset({"", "Azure Data Factory v2"})

_ESTIMATE_BUCKETS = ("orchestration", "data_movement", "pipeline_activity", "external")
_FACTORY_COLUMNS = [
    "subscription",
    "factory",
    "est_orchestration",
    "est_data_movement",
    "est_pipeline_activity",
    "est_external",
    "est_total",
    "actual_compute",
    "actual_total",
]
_PIPELINE_COLUMNS = [
    "subscription",
    "factory",
    "pipeline",
    "runs",
    "est_orchestration",
    "est_data_movement",
    "est_pipeline_activity",
    "est_external",
    "est_total",
]


def _factory_rows(result: ProfileResult) -> list[dict[str, Any]]:
    """One row per factory: estimate (rolled up from pipelines) next to billed compute."""
    # Defaults stay int 0 rather than 0.0: the original prints ints without "$", and so do we.
    estimates: dict[str, dict[str, Any]] = {}
    for pipeline in result.pipeline_costs:
        bucket = estimates.setdefault(pipeline.factory_name, dict.fromkeys(_ESTIMATE_BUCKETS, 0))
        bucket["orchestration"] += pipeline.orchestration_cost
        bucket["data_movement"] += pipeline.data_movement_cost
        bucket["pipeline_activity"] += pipeline.pipeline_activity_cost
        bucket["external"] += pipeline.external_cost

    # Only compute/orchestration meters count; infrastructure (Managed Airflow, Private Link, ...)
    # would skew an apples-to-apples comparison with the estimate.
    actual_compute: dict[str, Any] = {}
    for actual in result.actual_costs:
        if actual.meter_subcategory in _COMPUTE_METER_SUBCATEGORIES:
            actual_compute[actual.factory_name] = actual_compute.get(actual.factory_name, 0.0) + actual.cost

    rows = []
    for factory in sorted(set(estimates) | set(actual_compute)):
        estimate = estimates.get(factory, dict.fromkeys(_ESTIMATE_BUCKETS, 0))
        estimate_total = sum(estimate.values())
        compute = actual_compute.get(factory, 0)
        if estimate_total == 0 and compute == 0:
            continue
        rows.append(
            {
                "subscription": result.factory_subscriptions.get(factory) or "Unknown",
                "factory": factory,
                "est_orchestration": estimate["orchestration"],
                "est_data_movement": estimate["data_movement"],
                "est_pipeline_activity": estimate["pipeline_activity"],
                "est_external": estimate["external"],
                "est_total": estimate_total,
                "actual_compute": compute,
                "actual_total": compute,
            }
        )
    rows.sort(key=lambda row: -row["actual_total"])
    return rows


def _pipeline_rows(result: ProfileResult) -> list[dict[str, Any]]:
    """One row per pipeline with a non-zero estimate, most expensive first."""
    rows = []
    for pipeline in sorted(result.pipeline_costs, key=lambda cost: cost.total_cost, reverse=True):
        if pipeline.total_cost == 0:
            continue
        rows.append(
            {
                "subscription": result.factory_subscriptions.get(pipeline.factory_name) or "Unknown",
                "factory": pipeline.factory_name,
                "pipeline": pipeline.pipeline_name,
                "runs": pipeline.total_runs,
                "est_orchestration": pipeline.orchestration_cost,
                "est_data_movement": pipeline.data_movement_cost,
                "est_pipeline_activity": pipeline.pipeline_activity_cost,
                "est_external": pipeline.external_cost,
                "est_total": pipeline.total_cost,
            }
        )
    return rows


def _csv_line(row: dict[str, Any], columns: list[str]) -> str:
    return ",".join(f"${row[column]:.4f}" if isinstance(row[column], float) else str(row[column]) for column in columns)


def _render_csv(factory_rows: list[dict[str, Any]], pipeline_rows: list[dict[str, Any]]) -> str:
    lines = ["## Factory-Level Costs (Estimated vs Actual)\n"]
    if factory_rows:
        lines.append(",".join(_FACTORY_COLUMNS) + "\n")
        lines.extend(_csv_line(row, _FACTORY_COLUMNS) + "\n" for row in factory_rows)
    lines.append("\n")
    lines.append("## Pipeline-Level Costs (Estimates Only)\n")
    if pipeline_rows:
        lines.append(",".join(_PIPELINE_COLUMNS) + "\n")
        lines.extend(_csv_line(row, _PIPELINE_COLUMNS) + "\n" for row in pipeline_rows)
    return "".join(lines)


def _render_markdown(
    result: ProfileResult, factory_rows: list[dict[str, Any]], pipeline_rows: list[dict[str, Any]]
) -> str:
    lines = ["# Cost Comparison: Estimated vs Actual\n"]

    # The original only records profiling context once at least one pipeline run was found.
    if result.total_pipeline_runs:
        lines += [
            "## Profiling Context\n",
            "| Setting | Value |",
            "| --- | --- |",
            f"| Profiling Window | {result.days} days |",
            f"| Region | {result.region} |",
            f"| Pricing Source | {result.pricing_source} |",
            f"| Total Pipeline Runs | {result.total_pipeline_runs} |",
            "| Note | Estimates use Azure list/retail rates before discounts, reserved capacity, or enterprise "
            "agreements. Actual per-pipeline billing requires opt-in via Factory Settings. |",
            "",
        ]

    if result.cost_management_denied:
        lines.append("## Cost Management Access Issues\n")
        for subscription_id in result.cost_management_denied:
            display_name = result.subscription_names.get(subscription_id, "")
            label = f"{display_name} ({subscription_id})" if display_name else subscription_id
            lines.append(
                f"- **{label}**: Cost Management access denied - need Billing Reader or Cost Management Reader role"
            )
        lines.append("")

    totals_by_subscription: dict[str, dict[str, float]] = {}
    for row in factory_rows:
        totals = totals_by_subscription.setdefault(row["subscription"], {"est": 0.0, "act": 0.0})
        totals["est"] += row["est_total"]
        totals["act"] += row["actual_total"]

    lines += [
        "## Summary by Subscription\n",
        "| Subscription | Estimated Total | Actual Total | Difference |",
        "| --- | --- | --- | --- |",
    ]
    grand_estimate = grand_actual = 0.0
    for subscription, totals in sorted(totals_by_subscription.items(), key=lambda item: -item[1]["act"]):
        difference = totals["act"] - totals["est"]
        lines.append(f"| {subscription} | ${totals['est']:.4f} | ${totals['act']:.4f} | ${difference:.4f} |")
        grand_estimate += totals["est"]
        grand_actual += totals["act"]
    grand_difference = grand_actual - grand_estimate
    lines.append(f"| **TOTAL** | **${grand_estimate:.4f}** | **${grand_actual:.4f}** | **${grand_difference:.4f}** |")
    lines.append("")

    lines += [
        "## Factory-Level Costs (Estimated vs Actual)\n",
        "| Subscription | Factory | Est. Orch | Est. Data Mvmt | Est. Pipeline Act | Est. External | Est. Total "
        "| Act. Compute | Act. Total | Difference |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in factory_rows:
        difference = row["actual_total"] - row["est_total"]
        lines.append(
            f"| {row['subscription']} | {row['factory']} "
            f"| ${row['est_orchestration']:.4f} | ${row['est_data_movement']:.4f} "
            f"| ${row['est_pipeline_activity']:.4f} | ${row['est_external']:.4f} "
            f"| ${row['est_total']:.4f} | ${row['actual_compute']:.4f} "
            f"| ${row['actual_total']:.4f} "
            f"| ${difference:.4f} |"
        )
    lines.append("")

    if pipeline_rows:
        lines += [
            "## Pipeline-Level Costs (Estimates Only)\n",
            "| Subscription | Factory | Pipeline | Runs | Orchestration | Data Movement | Pipeline Activity "
            "| External | Total |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for row in pipeline_rows:
            lines.append(
                f"| {row['subscription']} | {row['factory']} "
                f"| {row['pipeline']} | {row['runs']} "
                f"| ${row['est_orchestration']:.4f} | ${row['est_data_movement']:.4f} "
                f"| ${row['est_pipeline_activity']:.4f} | ${row['est_external']:.4f} "
                f"| ${row['est_total']:.4f} |"
            )
        lines.append("")

    return "\n".join(lines)


def write_reports(result: ProfileResult, output_dir: str | Path) -> dict[str, str]:
    """Write cost_comparison.csv and cost_comparison.md under `<output_dir>/metadata/tco/`.

    Returns the written paths keyed by report name.
    """
    tco_dir = Path(output_dir) / "metadata" / "tco"
    tco_dir.mkdir(parents=True, exist_ok=True)
    factory_rows = _factory_rows(result)
    pipeline_rows = _pipeline_rows(result)

    csv_path = tco_dir / "cost_comparison.csv"
    csv_path.write_text(_render_csv(factory_rows, pipeline_rows))
    md_path = tco_dir / "cost_comparison.md"
    md_path.write_text(_render_markdown(result, factory_rows, pipeline_rows))
    return {"cost_comparison_csv": str(csv_path), "cost_comparison_md": str(md_path)}
