# Cross Project Conventions (News-to-Signal)

## Scope

This doc defines reusable conventions for downstream quant research projects.

## 1) Directory Structure

- Keep a shared structure:
  - `src/data`, `src/features`, `src/labels`, `src/models`,
    `src/validation`, `src/pipelines`, `src/utils`
  - `configs/*.yaml` for experiment parameters
  - `artifacts/runs/exp_YYYYMMDD_name/` for per-run outputs
  - `data/{raw,interim,processed}`

## 2) Signal Schema (Required)

- Standard columns:
  - `timestamp`
  - `asset`
  - `signal_value`
  - `signal_name`
  - `signal_version`
  - `horizon`
  - `source`
- Export default format: `parquet`
- CSV is mandatory fallback when parquet is unavailable

## 3) Artifact Layout

Every run must include at least:

- `run_config.yaml`
- `metrics.json`
- `summary.md`
- `tables/` (metrics tables)
- `figures/` (if generated)

## 4) Config Convention

- Keep all experiment-relevant parameters in:
  - `configs/data.yaml`
  - `configs/features.yaml`
  - `configs/baseline.yaml`
  - `configs/experiment.yaml`
- CLI flags only override YAML defaults when explicitly provided.

## 5) Experiment Naming

- Use `exp_YYYYMMDD_name` format.
- Prefer deterministic names for reproducible baselines, e.g.:
  - `exp_20260319_baseline`
  - `exp_20260319_signal_decay`
  - `exp_20260319_cost_x2`
