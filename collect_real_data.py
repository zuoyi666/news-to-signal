#!/usr/bin/env python3
"""
collect_real_data.py

Collect real financial news data using working Finnhub API.
This creates a high-quality dataset for Phase 1.
"""

import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv

# Load API key
load_dotenv('.env')
FINNHUB_KEY = os.getenv('FINNHUB_API_KEY')

if not FINNHUB_KEY:
    print("ERROR: FINNHUB_API_KEY not found in .env")
    sys.exit(1)

print(f"Using Finnhub API: {FINNHUB_KEY[:15]}...")


def fetch_finnhub_news(ticker: str, days_back: int = 365) -> list:
    """Fetch news from Finnhub API."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    url = "https://finnhub.io/api/v1/company-news"
    params = {
        "symbol": ticker,
        "from": start_date.strftime("%Y-%m-%d"),
        "to": end_date.strftime("%Y-%m-%d"),
        "token": FINNHUB_KEY
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            return data if isinstance(data, list) else []
        else:
            print(f"  Error {response.status_code}: {response.text[:100]}")
            return []
    except Exception as e:
        print(f"  Exception: {e}")
        return []


def main():
    # Load universe
    from config import UNIVERSE_PATH, RAW_DATA_PATH

    universe = pd.read_csv(UNIVERSE_PATH)
    tickers = universe['ticker'].tolist()

    print(f"\nCollecting real news data for {len(tickers)} tickers...")
    print("=" * 60)

    all_news = []

    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] {ticker}...", end=" ")

        news = fetch_finnhub_news(ticker, days_back=730)  # 2 years

        if news:
            for item in news:
                all_news.append({
                    'ticker': ticker,
                    'headline': item.get('headline', ''),
                    'date': datetime.fromtimestamp(item.get('datetime', 0)),
                    'source': item.get('source', 'Finnhub'),
                    'url': item.get('url', ''),
                })
            print(f"✓ {len(news)} items")
        else:
            print("✗ 0 items")

        # Rate limit: 60 calls/minute = 1 call/second
        time.sleep(1.0)

    print("=" * 60)

    if not all_news:
        print("ERROR: No news collected!")
        return

    # Create DataFrame
    df = pd.DataFrame(all_news)
    df['date'] = pd.to_datetime(df['date'])

    print(f"\nCollected {len(df)} total news items")
    print(f"Coverage: {df['ticker'].nunique()}/{len(tickers)} tickers")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")

    # Add required columns
    df['headline_count'] = 1
    df['future_return_5d'] = None

    # Save
    os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)
    df.to_csv(RAW_DATA_PATH, index=False)
    print(f"\nSaved to {RAW_DATA_PATH}")

    print("\nNext step: Compute forward returns")
    print("  python -c \"from src.preprocess_v2 import compute_forward_returns_v2; ...\"")


if __name__ == "__main__":
    main()
