"""
evaluation.py

This module handles evaluation of signals for the news-to-signal case study.
All evaluation is cross-sectional (per trading date), never pooled across dates.
"""

from typing import Optional

import numpy as np
import pandas as pd
from scipy.stats import norm, spearmanr, ttest_1samp, skew, kurtosis

from config import HOLDING_HORIZON, MIN_SAMPLE_DROP, MIN_SAMPLE_TERCILE, SIGNAL_COLS


def get_grouping_config(n_obs: int) -> tuple[int, str]:
    """
    Determine grouping configuration based on number of observations.

    Args:
        n_obs: Number of valid observations for a date.

    Returns:
        Tuple of (n_groups, method) where method is 'tercile' or 'quintile'.
        Returns (0, 'drop') if date should be dropped.
    """
    if n_obs < MIN_SAMPLE_DROP:
        return 0, "drop"
    elif n_obs < MIN_SAMPLE_TERCILE:
        return 3, "tercile"
    else:
        return 5, "quintile"


def assign_group_labels(
    df_date: pd.DataFrame,
    signal_col: str,
    n_groups: int,
) -> pd.Series:
    """
    Assign group labels (1 to n_groups) based on signal ranking.

    Group 1 = lowest signal values (short candidates)
    Group n_groups = highest signal values (long candidates)

    Args:
        df_date: DataFrame for a single date.
        signal_col: Signal column to rank on.
        n_groups: Number of groups (3 or 5).

    Returns:
        Series with group labels.
    """
    # Rank the signal (1 = lowest, N = highest)
    ranks = df_date[signal_col].rank(method="first")

    # Assign to groups
    # Use pd.qcut for quantile-based grouping
    try:
        groups = pd.qcut(ranks, q=n_groups, labels=range(1, n_groups + 1))
    except ValueError:
        # Fallback if equal-sized bins not possible
        groups = pd.cut(ranks, bins=n_groups, labels=range(1, n_groups + 1))

    return groups


def compute_grouped_returns(df: pd.DataFrame, signal_col: str) -> pd.DataFrame:
    """
    Compute mean forward returns per group for each date.

    For each date:
    - Assign group labels using fallback logic
    - Compute mean future_return_5d per group

    Args:
        df: DataFrame with date, signal_col, and future_return_5d columns.
        signal_col: Name of the signal column to evaluate.

    Returns:
        DataFrame with columns: date, group, mean_return, signal_col.
    """
    results = []

    for date, group_df in df.groupby("date"):
        # Filter to valid observations
        valid_df = group_df[group_df[signal_col].notna() & group_df["future_return_5d"].notna()]

        if len(valid_df) == 0:
            continue

        n_obs = len(valid_df)
        n_groups, method = get_grouping_config(n_obs)

        if method == "drop":
            continue

        # Assign group labels
        valid_df = valid_df.copy()
        valid_df["group"] = assign_group_labels(valid_df, signal_col, n_groups)

        # Compute mean return per group
        group_means = valid_df.groupby("group")["future_return_5d"].mean().reset_index()
        group_means["date"] = date
        group_means["n_groups"] = n_groups
        group_means = group_means.rename(columns={"future_return_5d": "mean_return"})

        results.append(group_means)

    if not results:
        return pd.DataFrame(columns=["date", "group", "mean_return", "n_groups"])

    result_df = pd.concat(results, ignore_index=True)
    result_df["signal"] = signal_col

    return result_df[["date", "group", "mean_return", "n_groups", "signal"]]


def compute_daily_spread(df: pd.DataFrame, signal_col: str) -> pd.Series:
    """
    Compute Top-minus-Bottom spread for each date.

    For each date: highest group mean - lowest group mean.

    Args:
        df: DataFrame with date, signal_col, and future_return_5d columns.
        signal_col: Name of the signal column to evaluate.

    Returns:
        Series indexed by date with daily spread values.
    """
    grouped_returns = compute_grouped_returns(df, signal_col)

    if grouped_returns.empty:
        return pd.Series(dtype=float)

    spreads = []
    for date, date_df in grouped_returns.groupby("date"):
        max_return = date_df["mean_return"].max()
        min_return = date_df["mean_return"].min()
        spread = max_return - min_return
        spreads.append({"date": date, "spread": spread})

    spread_df = pd.DataFrame(spreads)
    spread_df = spread_df.set_index("date")["spread"]
    spread_df.index = pd.to_datetime(spread_df.index)

    return spread_df


def compute_daily_ic(df: pd.DataFrame, signal_col: str) -> pd.Series:
    """
    Compute Spearman rank correlation (IC) between signal and forward return for each date.

    Args:
        df: DataFrame with date, signal_col, and future_return_5d columns.
        signal_col: Name of the signal column to evaluate.

    Returns:
        Series indexed by date with daily IC values.
    """
    ics = []

    for date, group_df in df.groupby("date"):
        valid_df = group_df[group_df[signal_col].notna() & group_df["future_return_5d"].notna()]

        if len(valid_df) < 3:  # Need at least 3 observations for correlation
            continue

        signal_values = valid_df[signal_col].values
        returns = valid_df["future_return_5d"].values

        try:
            ic, _ = spearmanr(signal_values, returns)
            if not np.isnan(ic):
                ics.append({"date": date, "ic": ic})
        except Exception:
            continue

    if not ics:
        return pd.Series(dtype=float)

    ic_df = pd.DataFrame(ics)
    ic_df = ic_df.set_index("date")["ic"]
    ic_df.index = pd.to_datetime(ic_df.index)

    return ic_df


def summarize_metrics(spread_series: pd.Series, ic_series: pd.Series) -> dict:
    """
    Summarize evaluation metrics.

    Args:
        spread_series: Series of daily spreads.
        ic_series: Series of daily ICs.

    Returns:
        Dictionary with:
            - mean_spread: Mean of daily spreads
            - spread_hit_rate: Fraction of positive spreads
            - mean_ic: Mean of daily ICs
            - ic_hit_rate: Fraction of positive ICs
    """
    metrics = {}

    if len(spread_series) > 0:
        metrics["mean_spread"] = spread_series.mean()
        metrics["spread_hit_rate"] = (spread_series > 0).mean()
        metrics["spread_std"] = spread_series.std()
    else:
        metrics["mean_spread"] = np.nan
        metrics["spread_hit_rate"] = np.nan
        metrics["spread_std"] = np.nan

    if len(ic_series) > 0:
        metrics["mean_ic"] = ic_series.mean()
        metrics["ic_hit_rate"] = (ic_series > 0).mean()
        metrics["ic_std"] = ic_series.std()
    else:
        metrics["mean_ic"] = np.nan
        metrics["ic_hit_rate"] = np.nan
        metrics["ic_std"] = np.nan

    return metrics


def compute_monthly_stability(spread_series: pd.Series) -> pd.Series:
    """
    Resample spread series by month and compute mean spread per month.

    Args:
        spread_series: Series of daily spreads indexed by date.

    Returns:
        Series indexed by month with mean spread values.
    """
    if spread_series.empty:
        return pd.Series(dtype=float)

    spread_series = spread_series.copy()
    spread_series.index = pd.to_datetime(spread_series.index)

    # Resample by month
    monthly = spread_series.resample("ME").mean()

    return monthly


def newey_west_tstat(values, lags: int) -> float:
    """
    t-statistic of the mean using a Newey-West (Bartlett kernel) long-run variance.

    Daily ICs computed on overlapping multi-day labels are autocorrelated, so a
    plain t-test overstates significance. Lags of at least horizon - 1 cover the
    overlap; lags=0 reduces to the mean over sqrt(population variance / n).
    """
    x = np.asarray(values, dtype=float)
    n = len(x)
    if n < 2:
        return np.nan
    e = x - x.mean()
    long_run_var = e @ e / n
    for k in range(1, min(lags, n - 1) + 1):
        weight = 1.0 - k / (lags + 1.0)
        long_run_var += 2.0 * weight * (e[k:] @ e[:-k]) / n
    if long_run_var <= 0:
        return np.nan
    return float(x.mean() / np.sqrt(long_run_var / n))


def compute_ic_statistics(ic_series: pd.Series, nw_lags: Optional[int] = None) -> dict:
    """
    Compute statistical significance tests for IC series.

    Tests the null hypothesis that the true mean IC is zero. The primary
    t-statistic and p-value are Newey-West (HAC) because consecutive daily
    labels overlap; the plain t-test is kept as `*_naive` for comparison.

    Args:
        ic_series: Series of daily Information Coefficients.
        nw_lags: Newey-West lags; defaults to HOLDING_HORIZON.

    Returns:
        Dictionary with statistical metrics:
            - mean_ic: Mean of daily ICs
            - ic_std: Standard deviation of daily ICs
            - t_statistic: Newey-West t-statistic for the mean IC
            - p_value: Two-tailed normal p-value of the Newey-West t-statistic
            - t_statistic_naive / p_value_naive: plain one-sample t-test
            - nw_lags: Newey-West lags used
            - annualized_ir: Information Ratio annualized (IC_mean / IC_std * sqrt(252))
            - significant_5pct: Boolean, True if the Newey-West p < 0.05
            - skewness: Skewness of IC distribution
            - kurtosis: Excess kurtosis of IC distribution
            - n_observations: Number of valid IC observations
    """
    lags = HOLDING_HORIZON if nw_lags is None else int(nw_lags)
    if len(ic_series) < 2:
        return {
            "mean_ic": np.nan,
            "ic_std": np.nan,
            "t_statistic": np.nan,
            "p_value": np.nan,
            "t_statistic_naive": np.nan,
            "p_value_naive": np.nan,
            "nw_lags": lags,
            "annualized_ir": np.nan,
            "significant_5pct": False,
            "skewness": np.nan,
            "kurtosis": np.nan,
            "n_observations": 0,
        }

    # Remove NaN values
    clean_ic = ic_series.dropna()
    n = len(clean_ic)

    if n < 2:
        return {
            "mean_ic": np.nan,
            "ic_std": np.nan,
            "t_statistic": np.nan,
            "p_value": np.nan,
            "t_statistic_naive": np.nan,
            "p_value_naive": np.nan,
            "nw_lags": lags,
            "annualized_ir": np.nan,
            "significant_5pct": False,
            "skewness": np.nan,
            "kurtosis": np.nan,
            "n_observations": n,
        }

    mean_ic = clean_ic.mean()
    ic_std = clean_ic.std(ddof=1)  # Sample standard deviation

    # Plain t-test, kept for comparison; it ignores the label overlap.
    t_naive, p_naive = ttest_1samp(clean_ic, 0)
    # Primary test: Newey-West t-statistic with a two-sided normal p-value.
    t_stat = newey_west_tstat(clean_ic, lags)
    p_val = float(2.0 * norm.sf(abs(t_stat))) if np.isfinite(t_stat) else np.nan

    # Annualized Information Ratio (assuming 252 trading days)
    annualized_ir = mean_ic / ic_std * np.sqrt(252) if ic_std > 0 else np.nan

    return {
        "mean_ic": mean_ic,
        "ic_std": ic_std,
        "t_statistic": t_stat,
        "p_value": p_val,
        "t_statistic_naive": t_naive,
        "p_value_naive": p_naive,
        "nw_lags": lags,
        "annualized_ir": annualized_ir,
        "significant_5pct": bool(p_val < 0.05) if np.isfinite(p_val) else False,
        "skewness": skew(clean_ic),
        "kurtosis": kurtosis(clean_ic),
        "n_observations": n,
    }


def compute_risk_adjusted_metrics(returns: pd.Series, freq: int = 252) -> dict:
    """
    Compute risk-adjusted performance metrics.

    Args:
        returns: Series of returns (daily frequency assumed).
        freq: Annualization factor (252 for trading days).

    Returns:
        Dictionary with risk metrics:
            - sharpe_ratio: Return/risk ratio (annualized)
            - sortino_ratio: Return/downside deviation (annualized)
            - max_drawdown: Maximum peak-to-trough decline
            - calmar_ratio: Annualized return / abs(max_drawdown)
            - volatility: Annualized standard deviation
            - downside_deviation: Annualized downside deviation
            - win_rate: Fraction of positive returns
    """
    if len(returns) < 2:
        return {
            "sharpe_ratio": np.nan,
            "sortino_ratio": np.nan,
            "max_drawdown": np.nan,
            "calmar_ratio": np.nan,
            "volatility": np.nan,
            "downside_deviation": np.nan,
            "win_rate": np.nan,
        }

    clean_returns = returns.dropna()

    if len(clean_returns) < 2:
        return {
            "sharpe_ratio": np.nan,
            "sortino_ratio": np.nan,
            "max_drawdown": np.nan,
            "calmar_ratio": np.nan,
            "volatility": np.nan,
            "downside_deviation": np.nan,
            "win_rate": np.nan,
        }

    # Basic metrics
    mean_ret = clean_returns.mean()
    std_ret = clean_returns.std()
    volatility = std_ret * np.sqrt(freq)

    # Sharpe ratio (assuming zero risk-free rate)
    sharpe = mean_ret / std_ret * np.sqrt(freq) if std_ret > 0 else np.nan

    # Sortino ratio (downside deviation)
    downside_returns = clean_returns[clean_returns < 0]
    downside_dev = downside_returns.std() * np.sqrt(freq) if len(downside_returns) > 0 else np.nan
    sortino = mean_ret * freq / downside_dev if downside_dev and downside_dev > 0 else np.nan

    # Maximum drawdown
    cum_returns = (1 + clean_returns).cumprod()
    rolling_max = cum_returns.expanding().max()
    drawdown = (cum_returns - rolling_max) / rolling_max
    max_dd = drawdown.min()

    # Calmar ratio
    calmar = mean_ret * freq / abs(max_dd) if max_dd != 0 else np.nan

    # Win rate
    win_rate = (clean_returns > 0).mean()

    return {
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "max_drawdown": max_dd,
        "calmar_ratio": calmar,
        "volatility": volatility,
        "downside_deviation": downside_dev,
        "win_rate": win_rate,
        "mean_daily_return": mean_ret,
        "annualized_return": mean_ret * freq,
    }


def run_baseline_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run evaluation for all three signals and return summary comparison.

    Includes statistical significance tests and risk-adjusted metrics.

    Args:
        df: DataFrame with all signal columns and future_return_5d.

    Returns:
        Summary DataFrame with one row per signal.
    """
    results = []

    for signal_col in SIGNAL_COLS:
        if signal_col not in df.columns:
            print(f"Warning: {signal_col} not found in DataFrame")
            continue

        spread_series = compute_daily_spread(df, signal_col)
        ic_series = compute_daily_ic(df, signal_col)
        metrics = summarize_metrics(spread_series, ic_series)

        # Statistical significance for IC
        ic_stats = compute_ic_statistics(ic_series)

        # Risk-adjusted metrics for spread (treat as strategy returns)
        risk_metrics = compute_risk_adjusted_metrics(spread_series)

        row = {
            "signal": signal_col,
            "n_days": len(spread_series),
            "mean_spread": metrics["mean_spread"],
            "spread_hit_rate": metrics["spread_hit_rate"],
            "mean_ic": metrics["mean_ic"],
            "ic_hit_rate": metrics["ic_hit_rate"],
            # Statistical significance
            "ic_t_statistic": ic_stats["t_statistic"],
            "ic_p_value": ic_stats["p_value"],
            "annualized_ir": ic_stats["annualized_ir"],
            "significant_5pct": ic_stats["significant_5pct"],
            "ic_skewness": ic_stats["skewness"],
            "ic_kurtosis": ic_stats["kurtosis"],
            # Risk metrics
            "sharpe_ratio": risk_metrics["sharpe_ratio"],
            "sortino_ratio": risk_metrics["sortino_ratio"],
            "max_drawdown": risk_metrics["max_drawdown"],
            "calmar_ratio": risk_metrics["calmar_ratio"],
            "volatility": risk_metrics["volatility"],
        }

        results.append(row)

    return pd.DataFrame(results)


if __name__ == "__main__":
    # Example usage
    from config import PROCESSED_PATH

    df = pd.read_csv(PROCESSED_PATH)
    df["date"] = pd.to_datetime(df["date"])

    print("Running baseline comparison...")
    summary = run_baseline_comparison(df)
    print(summary)
