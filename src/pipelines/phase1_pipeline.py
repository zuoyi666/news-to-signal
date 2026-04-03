from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml

from config import (
    DATA_SOURCE_PRIORITY,
    HOLDING_HORIZON,
    MAX_NEWS_PER_TICKER,
    SIGNAL_COLS,
    UNIVERSE_PATH,
    UNIVERSE_PATHS,
)
from config import BASE_DIR, RATE_LIMITS
from src.data.preprocess import run_preprocess_v2
from src.features.engineering import run_feature_engineering
from src.models.signal_builder import build_signals
from src.utils.configuration import load_pipeline_config
from src.utils.experiment import make_run_dir, make_run_name
from src.utils.signal_schema import write_signal_snapshot
from src.validation.evaluation import run_baseline, run_walkforward

import config as project_config
from src.kaggle_data_loader import generate_synthetic_historical_data


def _apply_runtime_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    data_cfg = cfg.get("data", {})
    baseline_cfg = cfg.get("baseline", {})
    features_cfg = cfg.get("features", {})

    universe_type = data_cfg.get("universe_type", getattr(project_config, "UNIVERSE_TYPE", "russell1000"))
    if universe_type not in UNIVERSE_PATHS:
        universe_type = "russell1000"

    project_config.UNIVERSE_TYPE = universe_type
    project_config.UNIVERSE_PATH = UNIVERSE_PATHS.get(universe_type, project_config.UNIVERSE_PATHS["original"])
    project_config.HOLDING_HORIZON = int(data_cfg.get("holding_horizon", HOLDING_HORIZON))
    project_config.LOOKBACK_DAYS = int(data_cfg.get("lookback_days", getattr(project_config, "LOOKBACK_DAYS", 730)))
    project_config.MAX_NEWS_PER_TICKER = int(data_cfg.get("max_news_per_ticker", MAX_NEWS_PER_TICKER))

    if "sources" in data_cfg and isinstance(data_cfg["sources"], dict):
        priority = data_cfg["sources"].get("priority")
        if priority:
            project_config.DATA_SOURCE_PRIORITY = list(priority)
        if data_cfg["sources"].get("finnhub_enabled") is not None:
            project_config.ENABLE_Finnhub = bool(data_cfg["sources"].get("finnhub_enabled"))
        if data_cfg["sources"].get("fmp_enabled") is not None:
            project_config.ENABLE_FMP = bool(data_cfg["sources"].get("fmp_enabled"))

    if "rate_limits" in data_cfg:
        for k, v in data_cfg["rate_limits"].items():
            RATE_LIMITS[k] = int(v)

    # Keep explicit env/API-key state from runtime
    project_config.ENABLE_Finnhub = project_config.ENABLE_Finnhub and bool(os.getenv("FINNHUB_API_KEY"))
    project_config.ENABLE_FMP = project_config.ENABLE_FMP and bool(os.getenv("FMP_API_KEY"))

    grouping = baseline_cfg.get("grouping", {})
    if grouping:
        project_config.MIN_SAMPLE_DROP = int(grouping.get("min_sample_drop", project_config.MIN_SAMPLE_DROP))
        project_config.MIN_SAMPLE_TERCILE = int(grouping.get("min_sample_tercile", project_config.MIN_SAMPLE_TERCILE))

    if features_cfg.get("event_keywords"):
        project_config.EVENT_KEYWORDS = list(features_cfg["event_keywords"])

    if features_cfg.get("signal_cols"):
        project_config.SIGNAL_COLS = list(features_cfg["signal_cols"])

    return {
        "applied_universe_path": project_config.UNIVERSE_PATH,
        "applied_universe_type": project_config.UNIVERSE_TYPE,
        "holding_horizon": project_config.HOLDING_HORIZON,
    }


def _run_synthetic(universe_type: str) -> pd.DataFrame:
    universe_df = pd.read_csv(project_config.UNIVERSE_PATH)
    tickers = universe_df["ticker"].tolist()

    synthetic = generate_synthetic_historical_data(
        tickers=tickers,
        start_date="2024-01-01",
        end_date="2025-03-01",
        avg_news_per_day=3,
    )
    synthetic.to_csv(project_config.RAW_DATA_PATH, index=False)
    return synthetic


def _safe_df_to_dict_list(df: pd.DataFrame):
    if df is None or len(df) == 0:
        return []
    return json.loads(df.to_json(orient="records", date_format="iso"))


def run_experiment(
    *,
    experiment_name: str = "baseline",
    run_name: str | None = None,
    synthetic: bool = False,
    skip_preprocess: bool = False,
    config_dir: str = "configs",
    config_overrides: Dict[str, Any] | None = None,
    output_root: str | None = None,
) -> Dict[str, Any]:
    cfg = load_pipeline_config(config_dir=config_dir, overrides=config_overrides or {})
    exp_cfg = cfg.get("experiment", {})

    # CLI/runtime wins over YAML
    if exp_cfg.get("synthetic") is not None:
        synthetic = bool(exp_cfg.get("synthetic"))
    if exp_cfg.get("skip_preprocess") is not None:
        skip_preprocess = bool(exp_cfg.get("skip_preprocess"))
    if output_root is None:
        output_root = exp_cfg.get("output", {}).get("root", "artifacts/runs")

    run_meta = _apply_runtime_config(cfg)
    run_name = run_name or exp_cfg.get("run_name") or make_run_name(experiment_name)
    run_dir = make_run_dir(output_root, run_name)

    pipeline_stamp = datetime.utcnow().isoformat()
    cfg.setdefault("run", {})
    cfg["run"] = {
        "name": experiment_name,
        "run_name": run_name,
        "timestamp_utc": pipeline_stamp,
        "project_root": str(Path(BASE_DIR).resolve()),
        "skip_preprocess": skip_preprocess,
        "synthetic": synthetic,
    }
    cfg["applied"] = run_meta

    try:
        (run_dir / "run_config.yaml").write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    except Exception:
        (run_dir / "run_config.yaml").write_text(json.dumps(cfg, indent=2), encoding="utf-8")

    if not skip_preprocess:
        if synthetic:
            preprocess_df = _run_synthetic(project_config.UNIVERSE_TYPE)
        else:
            preprocess_out = run_preprocess_v2()
            preprocess_df = preprocess_out["dataframe"]
    else:
        preprocess_df = pd.read_csv(project_config.RAW_DATA_PATH) if Path(project_config.RAW_DATA_PATH).exists() else pd.DataFrame()

    feature_df = run_feature_engineering(preprocess_df)
    signal_df = build_signals(feature_df)

    baseline_df = run_baseline(signal_df)
    walk_cfg = cfg.get("baseline", {}).get("metrics", {}).get("walkforward", {})
    walkforward_df, robustness = run_walkforward(
        signal_df,
        train_days=int(walk_cfg.get("train_days", 180)),
        val_days=int(walk_cfg.get("val_days", 90)),
        test_days=int(walk_cfg.get("test_days", 95)),
    )

    baseline_path = run_dir / "tables" / "baseline_comparison.csv"
    walkforward_path = run_dir / "tables" / "walkforward.csv"
    baseline_df.to_csv(baseline_path, index=False)
    walkforward_df.to_csv(walkforward_path, index=False)

    metrics_path = run_dir / "metrics.json"
    with metrics_path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "baseline": _safe_df_to_dict_list(baseline_df),
                "walkforward": _safe_df_to_dict_list(walkforward_df),
                "robustness": robustness,
                "run_metadata": cfg.get("run", {}),
            },
            f,
            indent=2,
        )

    signal_artifacts = write_signal_snapshot(
        signal_df,
        signal_cols=project_config.SIGNAL_COLS,
        output_dir=run_dir,
        signal_version=cfg.get("features", {}).get("signal_version", "v1"),
        horizon=project_config.HOLDING_HORIZON,
        source="news-to-signal",
    )

    summary_path = run_dir / "summary.md"
    if len(baseline_df) > 0:
        best_signal = baseline_df.loc[baseline_df["mean_ic"].abs().idxmax()]
        summary = [
            "# Experiment Summary",
            "",
            f"- Run: `{run_name}`",
            f"- Timestamp (UTC): `{pipeline_stamp}`",
            "",
            "## Best signal (by |mean_ic|)",
            f"- signal: `{best_signal['signal']}`",
            f"- mean_ic: `{best_signal['mean_ic']:.6f}`",
            f"- annualized_ic: `{best_signal['annualized_ir']:.6f}`",
        ]
    else:
        summary = [
            "# Experiment Summary",
            "",
            f"- Run: `{run_name}`",
            "- No baseline metrics were produced.",
        ]
    summary_path.write_text("\\n".join(summary), encoding="utf-8")

    return {
        "run_dir": str(run_dir),
        "run_name": run_name,
        "run_config": str(run_dir / "run_config.yaml"),
        "metrics": str(metrics_path),
        "summary": str(summary_path),
        "baseline_table": str(baseline_path),
        "walkforward_table": str(walkforward_path),
        "signals": signal_artifacts,
        "signal_count": int(len(signal_df)),
    }
