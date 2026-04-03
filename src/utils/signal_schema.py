from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable

import pandas as pd

REQUIRED_SIGNAL_COLUMNS = [
    "timestamp",
    "asset",
    "signal_value",
    "signal_name",
    "signal_version",
    "horizon",
    "source",
]


def validate_standard_signal_schema(df: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in REQUIRED_SIGNAL_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required signal columns: {missing}")

    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=False).dt.tz_localize(None)
    out["asset"] = out["asset"].astype("string").str.strip()
    out["signal_name"] = out["signal_name"].astype("string").str.strip()
    out["signal_version"] = out["signal_version"].astype("string").str.strip()
    out["horizon"] = out["horizon"].astype("string").str.strip()
    out["source"] = out["source"].astype("string").str.strip()
    out["signal_value"] = pd.to_numeric(out["signal_value"], errors="coerce")

    invalid_mask = out[REQUIRED_SIGNAL_COLUMNS].isnull().any(axis=1)
    if invalid_mask.any():
        raise ValueError(
            "Signal export contains null or invalid values in required columns."
        )

    duplicate_keys = ["timestamp", "asset", "signal_name", "signal_version", "horizon", "source"]
    if out.duplicated(duplicate_keys).any():
        raise ValueError("Signal export contains duplicate downstream signal keys.")

    return out.sort_values(["timestamp", "asset", "signal_name"]).reset_index(drop=True)[REQUIRED_SIGNAL_COLUMNS]


def to_standard_signal_long_format(
    df: pd.DataFrame,
    signal_cols: Iterable[str],
    signal_name: str = "signal_full",
    signal_version: str = "v1",
    horizon: int = 5,
    source: str = "news-to-signal",
) -> pd.DataFrame:
    cols = list(signal_cols)
    base = df.copy()
    missing_signal_cols = [column for column in cols if column not in base.columns]
    if "date" not in base.columns or "ticker" not in base.columns:
        raise ValueError("Wide signal export expects `date` and `ticker` columns.")
    if missing_signal_cols:
        raise ValueError(f"Missing signal columns for export: {missing_signal_cols}")
    base["date"] = pd.to_datetime(base["date"]).dt.tz_localize(None)

    long_df = (
        base
        .melt(
            id_vars=["date", "ticker"],
            value_vars=cols,
            var_name="signal_name",
            value_name="signal_value",
        )
        .rename(columns={"date": "timestamp", "ticker": "asset"})
    )

    long_df["signal_version"] = signal_version
    long_df["horizon"] = str(horizon)
    long_df["source"] = source
    long_df["signal_name"] = long_df["signal_name"].astype(str)
    return validate_standard_signal_schema(long_df)


def write_signal_snapshot(
    df: pd.DataFrame,
    signal_cols: Iterable[str],
    output_dir: Path,
    signal_version: str = "v1",
    horizon: int = 5,
    source: str = "news-to-signal",
) -> Dict[str, str]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    signal_df = to_standard_signal_long_format(
        df,
        signal_cols=signal_cols,
        signal_version=signal_version,
        horizon=horizon,
        source=source,
    )

    parquet_path = output_dir / "signals.parquet"
    csv_path = output_dir / "signals.csv"

    # Prefer parquet; fallback to csv if parquet backend is unavailable.
    try:
        signal_df.to_parquet(parquet_path, index=False)
        used_path = str(parquet_path)
        used_format = "parquet"
        csv_path.unlink(missing_ok=True)
    except Exception:
        signal_df.to_csv(csv_path, index=False)
        used_path = str(csv_path)
        used_format = "csv"

    schema_manifest = {
        "schema": {
            "required_columns": REQUIRED_SIGNAL_COLUMNS,
            "signal_names": sorted(signal_df["signal_name"].unique().tolist()),
            "signal_versions": sorted(signal_df["signal_version"].unique().tolist()),
            "horizons": sorted(signal_df["horizon"].unique().tolist()),
            "sources": sorted(signal_df["source"].unique().tolist()),
        },
        "file": {
            "format": used_format,
            "path": used_path,
        },
        "record_count": int(len(signal_df)),
    }

    manifest_path = output_dir / "signals_manifest.json"
    manifest_path.write_text(json.dumps(schema_manifest, indent=2), encoding="utf-8")
    return {"signal_file": used_path, "manifest": str(manifest_path)}
