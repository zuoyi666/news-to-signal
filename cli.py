from __future__ import annotations

import argparse
import json

from src.pipelines.run_plan import run_from_cli


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="News-to-Signal research runner")
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Run a full research experiment")
    run_p.add_argument("--experiment-name", default="baseline")
    run_p.add_argument("--run-name", default=None)
    run_p.add_argument("--config-dir", default="configs")
    run_p.add_argument("--run-root", default=None)
    run_p.add_argument("--universe", choices=["original", "sp500", "russell1000"])
    run_p.add_argument("--lookback-days", type=int, default=None)
    run_p.add_argument("--horizon", type=int, default=None)
    run_p.add_argument("--signal-version", default=None)
    run_p.add_argument("--synthetic", action="store_true")
    run_p.add_argument("--skip-preprocess", action="store_true")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "run":
        result = run_from_cli(args)
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
