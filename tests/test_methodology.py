"""Label timing, boundary purging, Newey-West IC statistics and the run summary."""
import numpy as np
import pandas as pd
import pytest
from scipy.stats import ttest_1samp

from config import HOLDING_HORIZON
from src import evaluation, preprocess, preprocess_v2, walkforward
from src.pipelines.phase1_pipeline import render_summary


def price_frame(periods=20):
    days = pd.bdate_range("2024-01-01", periods=periods)
    return pd.DataFrame({"Date": days, "Close": 100.0 + np.arange(periods)})


def label_v2(news_date, prices, horizon):
    return preprocess_v2.compute_single_return(news_date, prices, horizon)


def label_legacy(news_date, prices, horizon):
    return preprocess.compute_forward_return("TEST", news_date, prices, horizon)


@pytest.mark.parametrize("label", [label_v2, label_legacy])
def test_label_spans_exactly_horizon_intervals(label):
    prices = price_frame()
    # News on index 2 -> T' = index 2; entry Close[3] = 103, exit Close[3 + 5] = 108.
    assert label(prices["Date"][2], prices, 5) == pytest.approx(np.log(108.0 / 103.0))


@pytest.mark.parametrize("label", [label_v2, label_legacy])
def test_weekend_news_maps_to_the_next_trading_day(label):
    prices = price_frame()
    saturday = pd.Timestamp("2024-01-06")  # T' = Monday 2024-01-08, index 5
    assert label(saturday, prices, 5) == pytest.approx(np.log(111.0 / 106.0))


def test_label_is_missing_when_the_horizon_runs_past_the_data():
    prices = price_frame()
    assert np.isnan(label_v2(prices["Date"][14], prices, 5))
    assert label_legacy(prices["Date"][14], prices, 5) is None


def test_split_purges_rows_whose_labels_reach_the_next_period():
    dates = pd.bdate_range("2024-01-01", periods=60)
    df = pd.DataFrame({"date": np.repeat(dates, 2), "ticker": ["A", "B"] * 60})
    splits = walkforward.split_data_by_date(df, train_days=30, val_days=15, test_days=15)
    gap = HOLDING_HORIZON + 2
    assert splits["train"]["date"].max() == dates[30 - gap]
    assert splits["validation"]["date"].max() == dates[45 - gap]
    assert splits["purged_train_dates"] == gap - 1
    assert splits["purged_val_dates"] == gap - 1
    assert splits["test_dates"] == 15
    # The last kept train label ends at Close[i + 1 + horizon], before the first
    # validation label starts at Close[30 + 1].
    last_train = dates.get_loc(splits["train"]["date"].max())
    assert last_train + 1 + HOLDING_HORIZON < 30 + 1


def test_newey_west_with_zero_lags_is_mean_over_population_se():
    x = np.array([0.1, -0.05, 0.2, 0.0, 0.05])
    expected = x.mean() / np.sqrt(x.var() / len(x))
    assert evaluation.newey_west_tstat(x, 0) == pytest.approx(expected)


def overlapping_ic(n=600, horizon=5, seed=0):
    shocks = np.random.default_rng(seed).normal(0.01, 0.05, size=n + horizon - 1)
    return pd.Series(np.convolve(shocks, np.ones(horizon) / horizon, mode="valid"))


def test_newey_west_shrinks_the_t_statistic_for_overlapping_labels():
    ic = overlapping_ic()
    naive = ttest_1samp(ic, 0).statistic
    assert abs(evaluation.newey_west_tstat(ic, HOLDING_HORIZON)) < abs(naive)


def test_ic_statistics_report_newey_west_as_primary():
    ic = overlapping_ic()
    stats = evaluation.compute_ic_statistics(ic)
    assert stats["nw_lags"] == HOLDING_HORIZON
    assert stats["t_statistic"] == pytest.approx(evaluation.newey_west_tstat(ic, HOLDING_HORIZON))
    assert stats["t_statistic_naive"] == pytest.approx(ttest_1samp(ic, 0).statistic)
    assert stats["significant_5pct"] == (stats["p_value"] < 0.05)


def test_summary_reports_every_signal_and_the_limitations():
    baseline = pd.DataFrame(
        {
            "signal": ["signal_sentiment_only", "signal_full"],
            "mean_ic": [0.02, -0.01],
            "ic_t_statistic": [1.2, -0.4],
            "annualized_ir": [0.5, -0.2],
        }
    )
    text = render_summary("exp_test", "2026-10-24T00:00:00Z", baseline, 5)
    assert "`signal_sentiment_only`" in text and "`signal_full`" in text
    assert "Best signal" not in text
    assert "\\n" not in text  # real line breaks, not an escaped backslash-n
    assert "survivorship" in text
    assert "Close[T'+1+5]" in text
