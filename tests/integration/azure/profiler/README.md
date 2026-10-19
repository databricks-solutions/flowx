# Profiler golden reference

`flowx profile` scans Azure live: it reads pipeline and activity run history and Cost Management billing.
Neither exists for a factory that was just deployed, so unlike the translation suite in `../adf/`, this suite
can't deploy fixtures and test live in CI. Instead it replays one live capture offline, in
`tests/integration/test_profiler_golden.py`, as part of `make integration`.

## What's in `capture/`

| File | Contents |
|------|----------|
| `exchanges.jsonl` | Every Azure REST request the port made during one live scan, with its response, in call order |
| `pricing.json` | The Azure Retail Prices rate card fetched during that scan |
| `capture.json` | The scan's arguments and its pinned window end (`now`) |
| `golden/pipeline_runs_by_pipeline.csv` | The original standalone script's per-pipeline costs, from a scan seconds earlier over the same runs |
| `expected/metadata/tco/` | The port's report for the capture. Regenerate with `--update-expected` and review the diff |

The test checks that the port's per-pipeline estimates equal the original script's within 1.5e-4, and that
its report equals `expected/`. Where the port deliberately differs from the original, the test lists the
pipeline in `INTENDED_DIFFERENCES` with the reason, still requires the unaffected columns to match, and fails
if the difference disappears.

## The capture

- **Factories:** `flowx-profiler-test-adf` (Azure IR) and `flowx-profiler-test-adf-vnet` (managed VNet IR,
  TTL 15 minutes) in resource group `flowx-profiler-test-rg`, West Europe. Their ARM template and runbook are
  in `factory/`.
- **Runs:** 96 pipeline runs from 2026-10-17 to 2026-10-19, from manual runs and a 6-hourly schedule
  trigger. The Azure IR factory's two Databricks pipelines failed from 2026-10-17 18:00 UTC on (the
  workspace's IP access list entry for ADF was removed). Failed runs are billed and costed like any other.
- **Captured 2026-10-19:** the original script ran from 13:40:37 to 13:41:06 UTC, and the port's recorder
  started at 13:41:06 UTC. Both used `--days 3`. No runs happened in between.

## Intended differences from the original

- **Managed-VNet pipeline and external activity rate (#12 A).** The original looks the IR up by the run's
  `ManagedVnetIR (West Europe)` name, misses because of the region suffix, and prices these activities at
  Azure IR rates ($0.005/h and $0.00025/h). The port prices them at the managed-VNet rate ($1.00/h list).
- **Data flow priced from what Azure billed (A).** The original prices a data flow as 8 vCores × its
  duration ($1.2056 for `pl_dataflow`); the port prices the vCore-hours each run's `billingReference`
  reports ($0.9212, close to the 3.29 vCore-hours Cost Management billed). The other Azure IR activities
  bill whole minutes, which the duration estimate already matched, so they don't change. Separately, the
  port counts `DatabricksJob` as an external activity (Azure bills it as one), which moves the vnet
  factory's `pl_databricks_job` from pipeline activity to external; its total is unchanged.
- **Actuals.** The original reports $0.00 actual compute (it counts only an empty meter subcategory, #11),
  and attributes billing by substring match on the factory name (#1). The port also counts the Mapping Data
  Flow meters (`Azure Data Factory v2 Data Flow - <tier>`), which adds the Azure IR factory's $0.59 of data
  flow vCores. The golden file doesn't carry actuals, so these show only in `expected/`.

## Recapturing

The capture can be redone while the factories' run history (45 days) still covers the runs. Run the two
scans back to back with no pipeline runs in between, and choose `--days` so the window covers every run.

```bash
# 1. The original script (branch feat_profiler), in its own virtual environment.
pip install azure-identity azure-mgmt-datafactory 'azure-mgmt-resource<24' aiohttp pandas tqdm requests certifi
python extract_pipelines.py --subscription-id <SUBSCRIPTION_ID> --resource-group flowx-profiler-test-rg \
  --days <DAYS> --no-synapse --no-fabric --output-dir /tmp/flowx-golden/original/out

# 2. Immediately after: the port, recording every exchange (unredacted; keep it outside the repo).
PYTHONPATH=src python tests/integration/azure/profiler/record.py --subscription-id <SUBSCRIPTION_ID> \
  --resource-group flowx-profiler-test-rg --days <DAYS> --out /tmp/flowx-golden/port

# 3. Redact into capture/, then regenerate expected/ and review both diffs.
python tests/integration/azure/profiler/redact.py --raw /tmp/flowx-golden/port \
  --original /tmp/flowx-golden/original/out/<timestamp> --out tests/integration/azure/profiler/capture
PYTHONPATH=src uv run --frozen pytest tests/integration/test_profiler_golden.py --update-expected
```

`redact.py` gives every distinct GUID its own alias (`<SUBSCRIPTION_1>`, `<ID_0001>`, ...) in URLs and
bodies alike, so replayed run URLs stay distinct. It also replaces emails, Databricks hosts, numeric
workspace/job/run IDs (with fake numbers of the same length), IP addresses, and the subscription's name,
and drops Cost Management rows for other resource groups. `test_capture_is_redacted` fails on any GUID left
behind; check the rest of the diff by eye before committing.
