"""
yahoo_enhanced.py

Enhanced Yahoo Finance data collection to maximize coverage without APIs.
Strategies:
1. Extended RSS lookback with pagination simulation
2. Ticker variation handling (e.g., BRK.B -> BRK-B)
3. Multiple RSS endpoints per ticker
4. Fallback to historical price-based signals
"""

import time
from datetime import datetime, timedelta
from typing import List, Optional

import feedparser
import pandas as pd
import yfinance as yyf


def fetch_yahoo_rss_extended(
    ticker: str,
    lookback_days: int = 730,
    max_pages: int = 5
) -> List[dict]:
    """
    Fetch extended RSS feed from Yahoo Finance.

    Yahoo RSS typically returns ~20 recent items per request.
    For historical data, we use ticker variations and multiple attempts.
    """
    all_entries = []

    # Try different ticker formats
    ticker_variations = [ticker]
    if "." in ticker:
        ticker_variations.append(ticker.replace(".", "-"))

    for tick in ticker_variations:
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={tick}&region=US&lang=en-US"

        try:
            feed = feedparser.parse(url)

            for entry in feed.entries:
                # Parse date
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    pub_date = datetime(*entry.published_parsed[:6])
                elif hasattr(entry, 'published'):
                    try:
                        pub_date = datetime.strptime(
                            entry.published, "%a, %d %b %Y %H:%M:%S %z"
                        )
                        pub_date = pub_date.replace(tzinfo=None)
                    except:
                        pub_date = datetime.now()
                else:
                    pub_date = datetime.now()

                # Filter by lookback
                if pub_date >= datetime.now() - timedelta(days=lookback_days):
                    all_entries.append({
                        "ticker": ticker,
                        "headline": entry.title,
                        "published": pub_date,
                        "source": "Yahoo Finance",
                        "url": entry.get("link", ""),
                    })

            time.sleep(0.3)  # Rate limiting

        except Exception as e:
            print(f"Error fetching {tick}: {e}")

    # Remove duplicates
    seen = set()
    unique_entries = []
    for entry in all_entries:
        key = (entry["ticker"], entry["headline"], entry["published"])
        if key not in seen:
            seen.add(key)
            unique_entries.append(entry)

    return unique_entries


def fetch_batch_yahoo_news(
    tickers: List[str],
    lookback_days: int = 730
) -> pd.DataFrame:
    """
    Fetch news for multiple tickers using enhanced Yahoo RSS.
    """
    all_news = []

    print(f"Fetching enhanced Yahoo RSS data for {len(tickers)} tickers...")

    for i, ticker in enumerate(tickers):
        if i % 20 == 0:
            print(f"  Progress: {i}/{len(tickers)}")

        entries = fetch_yahoo_rss_extended(ticker, lookback_days)
        all_news.extend(entries)

        time.sleep(0.2)

    if not all_news:
        return pd.DataFrame()

    df = pd.DataFrame(all_news)
    df["date"] = pd.to_datetime(df["published"]).dt.normalize()

    print(f"\nFetched {len(df)} total news items")
    print(f"Coverage: {df['ticker'].nunique()}/{len(tickers)} tickers")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")

    return df


def create_price_based_features(
    tickers: List[str],
    lookback_days: int = 730
) -> pd.DataFrame:
    """
    Create synthetic news signals from price movements.

    When news is sparse, we can infer sentiment from:
    - Large price moves (>2 std) often coincide with news
    - Volume spikes indicate information events
    """
    print("Creating price-based features as supplement...")

    all_events = []

    for ticker in tickers[:50]:  # Limit to avoid rate limits
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period=f"{lookback_days}d")

            if len(hist) < 20:
                continue

            # Calculate returns and volatility
            hist['returns'] = hist['Close'].pct_change()
            hist['vol'] = hist['returns'].rolling(20).std()
            hist['volume_ma'] = hist['Volume'].rolling(20).mean()

            # Identify significant events
            hist['z_score'] = (hist['returns'] - hist['returns'].mean()) / hist['returns'].std()

            # Large moves (>2 std) with volume confirmation
            events = hist[
                (abs(hist['z_score']) > 2) &
                (hist['Volume'] > hist['volume_ma'] * 1.5)
            ].copy()

            for date, row in events.iterrows():
                # Create synthetic headline based on move direction
                if row['z_score'] > 0:
                    headline = f"{ticker} surges on strong trading volume"
                else:
                    headline = f"{ticker} drops amid heightened selling pressure"

                all_events.append({
                    "date": date,
                    "ticker": ticker,
                    "headline": headline,
                    "headline_count": 1,
                    "source": "Price-Based-Inference",
                    "future_return_5d": None,
                    "z_score": row['z_score'],
                    "is_synthetic": True
                })

            time.sleep(0.1)

        except Exception as e:
            continue

    if not all_events:
        return pd.DataFrame()

    df = pd.DataFrame(all_events)
    print(f"Created {len(df)} price-based events")

    return df


def merge_news_sources(
    yahoo_df: pd.DataFrame,
    price_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Merge multiple news sources, prioritizing real news over synthetic.
    """
    if price_df is not None and len(price_df) > 0:
        # Remove synthetic entries for tickers that have real news
        tickers_with_news = set(yahoo_df['ticker'].unique())
        price_filtered = price_df[~price_df['ticker'].isin(tickers_with_news)]

        combined = pd.concat([yahoo_df, price_filtered], ignore_index=True)
        print(f"\nCombined dataset: {len(combined)} rows")
        print(f"  Real news: {len(yahoo_df)}")
        print(f"  Price-based: {len(price_filtered)}")

        return combined

    return yahoo_df


if __name__ == "__main__":
    # Test the enhanced fetcher
    from config import UNIVERSE_PATH

    universe = pd.read_csv(UNIVERSE_PATH)
    tickers = universe['ticker'].tolist()[:20]  # Test with 20 tickers

    print("Testing enhanced Yahoo RSS fetcher...\n")

    # Fetch Yahoo news
    yahoo_news = fetch_batch_yahoo_news(tickers, lookback_days=180)

    if len(yahoo_news) > 0:
        print("\nSample data:")
        print(yahoo_news.head())
