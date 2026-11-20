# flowx skill trigger evals

One case per skill checks that a request the skill's description covers makes Claude load that skill, two cases
check that non-ADF sources (Airflow, Fabric Data Factory) still reach `flowx-migrate`, and two check that no flowx
skill loads for a request that isn't a migration. Run them when a skill description changes:

```bash
claude plugin eval . --ablation none --no-publish                    # all cases, 3 runs each
claude plugin eval . --ablation none --tag trigger --runs 1 --no-publish   # a quick pass
```

Each run is a real Claude session on your own credentials, so these are not part of `make test`. The structure
checks that run on every change are in `tests/unit/test_skills_standard.py`.
