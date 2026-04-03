from __future__ import annotations

from typing import Any, Dict

from src import preprocess, preprocess_v2


def run_preprocess_v2() -> Dict[str, Any]:
    return {
        "method": "v2_multi_source",
        "dataframe": preprocess_v2.main_v2(),
    }


def run_preprocess_legacy() -> Dict[str, Any]:
    return {
        "method": "v1_yahoo_only",
        "dataframe": preprocess.main(),
    }
