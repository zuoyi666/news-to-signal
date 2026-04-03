#!/usr/bin/env python3
"""Backward compatible entrypoint for legacy `run_phase1.py` usage."""

from __future__ import annotations

import argparse

from src.pipelines.phase1_pipeline import run_experiment


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Phase 1 optimized runner")
    parser.add_argument("--synthetic", action="store_true", help="Use synthetic data instead of live APIs")
    parser.add_argument("--universe", choices=["original", "sp500", "russell1000"], default="russell1000")
    parser.add_argument("--skip-preprocess", action="store_true", help="Skip preprocessing step")
    parser.add_argument("--run-name", default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = run_experiment(
        experiment_name="phase1",
        run_name=args.run_name,
        synthetic=args.synthetic,
        skip_preprocess=args.skip_preprocess,
        config_overrides={"data": {"universe_type": args.universe}},
    )
    print(f"Run output written to: {result['run_dir']}")


if __name__ == "__main__":
    main()
