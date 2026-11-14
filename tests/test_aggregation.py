"""Headline aggregation keeps every headline of a ticker-day for the text features."""
import pandas as pd

from src import feature_engineering
from src.preprocess_v2 import aggregate_headlines


def _three_headlines():
    return pd.DataFrame({
        "ticker": ["AAA"] * 3,
        "date": ["2025-03-03"] * 3,
        "source": "test",
        "future_return_5d": 0.01,
        "headline": ["Company beats estimates and raises guidance",
                     "Strong growth and record profit",
                     "CEO resigns amid fraud probe"],
    })


def test_every_headline_reaches_the_features():
    out = aggregate_headlines(_three_headlines())
    assert len(out) == 1
    assert out.loc[0, "headline_count"] == 3
    for text in ("beats estimates", "record profit", "fraud probe"):
        assert text in out.loc[0, "headline"]


def test_sentiment_uses_all_headlines_not_only_the_last():
    lmd = feature_engineering.load_lmd_dictionary()
    positive, negative, _ = feature_engineering.parse_lmd_categories(lmd)
    aggregated = aggregate_headlines(_three_headlines()).loc[0, "headline"]
    last_only = "CEO resigns amid fraud probe"
    score_all = feature_engineering.compute_sentiment_score(aggregated, positive, negative)
    score_last = feature_engineering.compute_sentiment_score(last_only, positive, negative)
    assert score_all > score_last
