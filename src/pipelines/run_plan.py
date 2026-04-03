from __future__ import annotations

from typing import Any, Dict

from src.pipelines.phase1_pipeline import run_experiment


def run_from_cli(args) -> Dict[str, Any]:
    overrides = {}
    if args.universe is not None:
        overrides.setdefault("data", {})["universe_type"] = args.universe
    if args.lookback_days is not None:
        overrides.setdefault("data", {})["lookback_days"] = args.lookback_days
    if args.horizon is not None:
        overrides.setdefault("data", {})["holding_horizon"] = args.horizon
    if args.signal_version is not None:
        overrides.setdefault("features", {})["signal_version"] = args.signal_version

    return run_experiment(
        experiment_name=args.experiment_name,
        run_name=args.run_name,
        synthetic=args.synthetic,
        skip_preprocess=args.skip_preprocess,
        config_dir=args.config_dir,
        config_overrides=overrides,
        output_root=args.run_root,
    )
