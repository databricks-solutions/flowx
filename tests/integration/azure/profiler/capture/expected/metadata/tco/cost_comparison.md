# Cost Comparison: Estimated vs Actual

## Profiling Context

| Setting | Value |
| --- | --- |
| Profiling Window | 3 days |
| Region | westeurope |
| Pricing Source | Azure Retail Prices API (westeurope) |
| Total Pipeline Runs | 96 |
| Note | Estimates use Azure list/retail rates before discounts, reserved capacity, or enterprise agreements. Actual per-pipeline billing requires opt-in via Factory Settings. |

## Summary by Subscription

| Subscription | Estimated Total | Actual Total | Difference |
| --- | --- | --- | --- |
| demo-subscription | $4.1883 | $4.0055 | $-0.1828 |
| **TOTAL** | **$4.1883** | **$4.0055** | **$-0.1828** |

## Factory-Level Costs (Estimated vs Actual)

| Subscription | Factory | Est. Orch | Est. Data Mvmt | Est. Pipeline Act | Est. External | Est. Total | Act. Compute | Act. Total | Difference |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| demo-subscription | flowx-profiler-test-adf-vnet | $0.1360 | $0.3500 | $0.9834 | $1.2000 | $2.6694 | $3.8284 | $3.8284 | $1.1590 |
| demo-subscription | flowx-profiler-test-adf | $0.1630 | $0.1500 | $0.0002 | $1.2057 | $1.5189 | $0.1771 | $0.1771 | $-1.3418 |

## Pipeline-Level Costs (Estimates Only)

| Subscription | Factory | Pipeline | Runs | Orchestration | Data Movement | Pipeline Activity | External | Total |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| demo-subscription | flowx-profiler-test-adf-vnet | pl_foreach | 10 | $0.0600 | $0.0000 | $0.6334 | $0.7000 | $1.3934 |
| demo-subscription | flowx-profiler-test-adf | pl_dataflow | 9 | $0.0180 | $0.0000 | $0.0000 | $1.2056 | $1.2236 |
| demo-subscription | flowx-profiler-test-adf-vnet | pl_web_public | 9 | $0.0270 | $0.0000 | $0.0000 | $0.4667 | $0.4937 |
| demo-subscription | flowx-profiler-test-adf-vnet | pl_copy | 9 | $0.0180 | $0.3500 | $0.0000 | $0.0000 | $0.3680 |
| demo-subscription | flowx-profiler-test-adf-vnet | pl_lookup_wait | 9 | $0.0270 | $0.0000 | $0.3167 | $0.0000 | $0.3437 |
| demo-subscription | flowx-profiler-test-adf | pl_copy | 9 | $0.0180 | $0.1500 | $0.0000 | $0.0000 | $0.1680 |
| demo-subscription | flowx-profiler-test-adf | pl_foreach | 10 | $0.0600 | $0.0000 | $0.0001 | $0.0001 | $0.0602 |
| demo-subscription | flowx-profiler-test-adf-vnet | pl_web_databricks | 1 | $0.0020 | $0.0000 | $0.0000 | $0.0333 | $0.0353 |
| demo-subscription | flowx-profiler-test-adf-vnet | pl_databricks_job | 1 | $0.0020 | $0.0000 | $0.0333 | $0.0000 | $0.0353 |
| demo-subscription | flowx-profiler-test-adf | pl_lookup_wait | 9 | $0.0270 | $0.0000 | $0.0001 | $0.0000 | $0.0271 |
| demo-subscription | flowx-profiler-test-adf | pl_web_databricks | 10 | $0.0200 | $0.0000 | $0.0000 | $0.0000 | $0.0200 |
| demo-subscription | flowx-profiler-test-adf | pl_databricks_job | 10 | $0.0200 | $0.0000 | $0.0000 | $0.0000 | $0.0200 |
