from __future__ import annotations

import pandas as pd

from src import feature_engineering


def run_feature_engineering(df: pd.DataFrame | None = None):
    if df is None:
        return feature_engineering.main()
    return feature_engineering.main(df=df)
