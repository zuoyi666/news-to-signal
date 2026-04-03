# config.py
# Central configuration for news-to-signal.
# All pipeline parameters are defined here to keep src modules clean and reusable.

import os
from pathlib import Path

# Load environment variables from .env file (if it exists)
# This allows secure API key management without committing keys to GitHub
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        print(f"Loaded environment from {env_path}")
except ImportError:
    pass  # python-dotenv not installed, use system env vars only

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))

# Universe selection: "original" (50), "sp500" (110), or "russell1000" (180)
UNIVERSE_TYPE   = "russell1000"  # Phase 1: Expanded universe

UNIVERSE_PATHS  = {
    "original": os.path.join(BASE_DIR, "data", "reference", "universe.csv"),
    "sp500": os.path.join(BASE_DIR, "data", "reference", "sp500_universe.csv"),
    "russell1000": os.path.join(BASE_DIR, "data", "reference", "russell1000_universe.csv"),
}

UNIVERSE_PATH   = UNIVERSE_PATHS.get(UNIVERSE_TYPE, UNIVERSE_PATHS["original"])
LMD_DICT_PATH   = os.path.join(BASE_DIR, "data", "reference", "loughran_mcdonald.csv")
RAW_DATA_PATH   = os.path.join(BASE_DIR, "data", "raw", "news_sample.csv")
PROCESSED_PATH  = os.path.join(BASE_DIR, "data", "processed", "news_features.csv")
ARTIFACT_ROOT = os.path.join(BASE_DIR, "artifacts", "runs")


def _resolve_notebook_run_dir() -> str:
    """Resolve notebook output directory with environment override support.

    Notebook workflows write exploratory outputs to an existing experiment run folder
    under artifacts/runs so they are aligned with CLI run artifacts.
    """
    explicit_run = os.getenv("NEWS_TO_SIGNAL_ARTIFACT_RUN_DIR")
    if explicit_run:
        return explicit_run

    run_root = Path(ARTIFACT_ROOT)
    candidates = sorted(run_root.glob("exp_*"))
    if candidates:
        return str(candidates[-1])

    # Safe fallback for first-run workflow before any experiment has completed.
    return os.path.join(ARTIFACT_ROOT, "manual")


_notebook_run_dir = _resolve_notebook_run_dir()
FIGURES_DIR     = os.path.join(_notebook_run_dir, "figures")
TABLES_DIR      = os.path.join(_notebook_run_dir, "tables")

# ── Data Sources (Phase 1: Multi-source aggregation) ────────────────────────────
DATA_SOURCE_PRIORITY = ["finnhub", "fmp", "yahoo_rss"]  # Priority order
ENABLE_Finnhub = bool(os.getenv("FINNHUB_API_KEY"))
ENABLE_FMP = bool(os.getenv("FMP_API_KEY"))

# Rate limits (calls per minute)
RATE_LIMITS = {
    "finnhub": 60,   # Free tier: 60 calls/minute
    "fmp": 250,      # Free tier: 250 calls/day
    "yahoo_rss": 100,  # Unofficial, be conservative
}

# ── Data collection ───────────────────────────────────────────────────────────
LOOKBACK_DAYS    = 730   # Phase 1: Extended to 2 years
HOLDING_HORIZON  = 5     # number of trading days for future_return_5d
MAX_NEWS_PER_TICKER = 100  # Limit to prevent API overuse

# ── Grouping fallback rules ────────────────────────────────────────────────────
# Applied per cross-section date based on number of valid observations N
MIN_SAMPLE_DROP     = 9    # N < 9:  drop date entirely
MIN_SAMPLE_TERCILE  = 15   # 9 <= N < 15: use tercile; N >= 15: use quintile

# ── Feature engineering ────────────────────────────────────────────────────────
EVENT_KEYWORDS = [
    "earnings", "guidance", "downgrade", "upgrade",
    "acquisition", "lawsuit", "partnership"
]

# ── Signal columns ────────────────────────────────────────────────────────────
SIGNAL_COLS = [
    "signal_sentiment_only",
    "signal_sentiment_minus_uncertainty",
    "signal_full"
]
