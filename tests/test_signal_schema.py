import json
from pathlib import Path

import pandas as pd
import pytest

from src.utils.signal_schema import REQUIRED_SIGNAL_COLUMNS, to_standard_signal_long_format, validate_standard_signal_schema, write_signal_snapshot


def test_standard_signal_schema_shape():
    df = pd.DataFrame(
        [
            {"date": "2026-01-02", "ticker": "AAPL", "signal_sentiment_only": 0.1, "signal_full": 0.5},
            {"date": "2026-01-03", "ticker": "MSFT", "signal_sentiment_only": -0.2, "signal_full": 0.3},
        ]
    )
    out = to_standard_signal_long_format(
        df,
        signal_cols=["signal_sentiment_only", "signal_full"],
        signal_version="v1",
        horizon=5,
        source="test",
    )
    expected = {
        "timestamp",
        "asset",
        "signal_value",
        "signal_name",
        "signal_version",
        "horizon",
        "source",
    }
    assert expected.issubset(set(out.columns))
    assert len(out) == 4


def test_write_signal_snapshot_emits_downstream_contract(tmp_path):
    df = pd.DataFrame(
        [
            {"date": "2026-01-02", "ticker": "AAPL", "signal_sentiment_only": 0.1, "signal_full": 0.5},
            {"date": "2026-01-03", "ticker": "MSFT", "signal_sentiment_only": -0.2, "signal_full": 0.3},
        ]
    )

    artifacts = write_signal_snapshot(
        df,
        signal_cols=["signal_sentiment_only", "signal_full"],
        output_dir=tmp_path,
        signal_version="v2",
        horizon=5,
        source="news-to-signal",
    )

    signal_path = Path(artifacts["signal_file"])
    if signal_path.suffix == ".parquet":
        exported = pd.read_parquet(signal_path)
    else:
        exported = pd.read_csv(signal_path)

    manifest = json.loads(Path(artifacts["manifest"]).read_text(encoding="utf-8"))

    assert exported.columns.tolist() == REQUIRED_SIGNAL_COLUMNS
    assert set(exported["horizon"]) == {"5"}
    assert manifest["schema"]["required_columns"] == REQUIRED_SIGNAL_COLUMNS
    assert sorted(manifest["schema"]["signal_names"]) == ["signal_full", "signal_sentiment_only"]


def test_validate_standard_signal_schema_rejects_invalid_signal_value():
    df = pd.DataFrame(
        [
            {
                "timestamp": "2026-01-02",
                "asset": "AAPL",
                "signal_value": "not-a-number",
                "signal_name": "signal_full",
                "signal_version": "v1",
                "horizon": "5",
                "source": "news-to-signal",
            }
        ]
    )

    with pytest.raises(ValueError):
        validate_standard_signal_schema(df)
