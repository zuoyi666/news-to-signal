from __future__ import annotations

import pandas as pd

from src import evaluation, walkforward


def run_baseline(df: pd.DataFrame):
    return evaluation.run_baseline_comparison(df)


def run_walkforward(df: pd.DataFrame, train_days: int, val_days: int, test_days: int):
    wf = walkforward.run_walkforward_analysis(
        df,
        train_days=train_days,
        val_days=val_days,
        test_days=test_days,
    )
    robustness = walkforward.check_robustness(wf)
    return wf, robustness
