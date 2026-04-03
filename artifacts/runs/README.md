# Run Artifacts

Each upstream research run writes into:

`artifacts/runs/exp_YYYYMMDD_name/`

Minimum standardized outputs:

- `run_config.yaml`
- `metrics.json`
- `summary.md`
- `tables/baseline_comparison.csv`
- `tables/walkforward.csv`
- `signals.parquet` or `signals.csv`
- `signals_manifest.json`
- `figures/`

Generated run directories are local research outputs by default. Keep this README as contract documentation; avoid committing per-run outputs unless you intentionally want to publish a frozen example handoff.
