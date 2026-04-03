from __future__ import annotations

from src import signal_construction


def build_signals(feature_df=None):
    return signal_construction.main(feature_df)
