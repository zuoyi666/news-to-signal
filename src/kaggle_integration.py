"""
kaggle_integration.py

Download and integrate real historical financial news from Kaggle.
Recommended datasets:
1. "Massive Stock News Analysis DB for NLP" (miguelaenlle)
2. "Daily Financial News for 6000+ Stocks" (athoillah)
3. "Stock Market News with Sentiment" (evangower)

Installation:
    pip install kaggle
    # Set up Kaggle API: https://github.com/Kaggle/kaggle-api

Usage:
    python src/kaggle_integration.py --download --dataset miguelaenlle/massive-stock-news-analysis-db-for-nlpbacktests
"""

import os
import subprocess
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from config import BASE_DIR, RAW_DATA_PATH

KAGGLE_DATASETS = {
    "massive_news": {
        "name": "miguelaenlle/massive-stock-news-analysis-db-for-nlpbacktests",
        "description": "4.5M news items with sentiment labels",
        "size": "~500MB",
    },
    "daily_news_6000": {
        "name": "athoillah/daily-financial-news-for-6000-stocks",
        "description": "Daily news for 6000+ stocks",
        "size": "~200MB",
    },
    "stock_sentiment": {
        "name": "evangower/stock-market-news-with-sentiment",
        "description": "News with pre-computed sentiment",
        "size": "~50MB",
    },
}


def download_kaggle_dataset(dataset_key: str, output_dir: str = "data/external"):
    """
    Download a Kaggle dataset.

    Args:
        dataset_key: Key from KAGGLE_DATASETS dict
        output_dir: Where to extract the dataset
    """
    if dataset_key not in KAGGLE_DATASETS:
        print(f"Unknown dataset: {dataset_key}")
        print(f"Available: {list(KAGGLE_DATASETS.keys())}")
        return False

    dataset_info = KAGGLE_DATASETS[dataset_key]
    dataset_name = dataset_info["name"]

    output_path = Path(BASE_DIR) / output_dir
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"Downloading: {dataset_info['description']}")
    print(f"Size: {dataset_info['size']}")
    print(f"This may take a few minutes...")

    try:
        # Download using kaggle CLI
        subprocess.run(
            ["kaggle", "datasets", "download", "-d", dataset_name, "-p", str(output_path)],
            check=True,
            capture_output=True,
        )

        # Extract zip
        zip_file = output_path / f"{dataset_name.split('/')[-1]}.zip"
        if zip_file.exists():
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(output_path / dataset_key)
            zip_file.unlink()  # Remove zip after extraction
            print(f"✓ Downloaded and extracted to {output_path / dataset_key}")
            return True

    except subprocess.CalledProcessError as e:
        print(f"✗ Download failed: {e}")
        print("\nMake sure you have:")
        print("1. Installed kaggle: pip install kaggle")
        print("2. Set up API credentials: ~/.kaggle/kaggle.json")
        print("   (Get token from https://www.kaggle.com/account)")
        return False

    return False


def load_massive_news_dataset(data_dir: str = "data/external/massive_news") -> pd.DataFrame:
    """
    Load and format the 'Massive Stock News' dataset.

    Expected files:
    - raw_partner_headlines.csv
    - processed_news.csv
    """
    data_path = Path(BASE_DIR) / data_dir

    # Try to find CSV files
    csv_files = list(data_path.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {data_path}")
        return pd.DataFrame()

    # Load the main headlines file
    headlines_file = None
    for f in csv_files:
        if "headline" in f.name.lower() or "news" in f.name.lower():
            headlines_file = f
            break

    if not headlines_file:
        headlines_file = csv_files[0]

    print(f"Loading: {headlines_file}")

    try:
        df = pd.read_csv(headlines_file)
        print(f"Loaded {len(df)} rows")
        print(f"Columns: {df.columns.tolist()}")

        # Standardize column names
        column_map = {
            'ticker': 'ticker',
            'stock': 'ticker',
            'symbol': 'ticker',
            'headline': 'headline',
            'title': 'headline',
            'text': 'headline',
            'date': 'date',
            'datetime': 'date',
            'published': 'date',
            'url': 'url',
            'source': 'source',
        }

        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

        # Ensure required columns
        if 'ticker' not in df.columns or 'headline' not in df.columns:
            print("ERROR: Dataset missing required columns (ticker, headline)")
            return pd.DataFrame()

        # Parse dates
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        else:
            df['date'] = datetime.now()

        # Add required columns for pipeline
        if 'headline_count' not in df.columns:
            df['headline_count'] = 1
        if 'source' not in df.columns:
            df['source'] = 'Kaggle'
        if 'future_return_5d' not in df.columns:
            df['future_return_5d'] = None

        # Reorder
        cols = ['date', 'ticker', 'headline', 'headline_count', 'source', 'future_return_5d']
        df = df[[c for c in cols if c in df.columns]]

        print(f"\nFormatted dataset:")
        print(f"  Rows: {len(df)}")
        print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
        print(f"  Tickers: {df['ticker'].nunique()}")

        return df

    except Exception as e:
        print(f"Error loading dataset: {e}")
        return pd.DataFrame()


def merge_with_prices_kaggle(
    news_df: pd.DataFrame,
    lookback_days: int = 730
) -> pd.DataFrame:
    """
    Fetch prices and compute forward returns for Kaggle news data.

    Note: For historical Kaggle data, we use yfinance to get corresponding prices.
    This may miss some delisted stocks.
    """
    import yfinance as yf
    import numpy as np

    if news_df.empty:
        return news_df

    print("\nFetching prices for Kaggle news data...")

    news_df = news_df.copy()
    news_df['future_return_5d'] = np.nan

    tickers = news_df['ticker'].unique()
    print(f"Processing {len(tickers)} tickers...")

    for i, ticker in enumerate(tickers):
        if i % 50 == 0:
            print(f"  {i}/{len(tickers)}")

        try:
            # Download price data
            stock = yf.Ticker(ticker)
            hist = stock.history(period="2y")

            if len(hist) < 30:
                continue

            # Calculate forward returns
            hist['future_return_5d'] = np.log(
                hist['Close'].shift(-6) / hist['Close'].shift(-1)
            )

            # Merge with news
            ticker_news = news_df[news_df['ticker'] == ticker].copy()

            for idx, row in ticker_news.iterrows():
                news_date = pd.Timestamp(row['date'])

                # Find closest trading day
                mask = hist.index >= news_date
                if mask.any():
                    future_ret = hist.loc[mask, 'future_return_5d'].iloc[0]
                    if not np.isnan(future_ret):
                        news_df.loc[idx, 'future_return_5d'] = future_ret

        except Exception as e:
            continue

    coverage = news_df['future_return_5d'].notna().mean()
    print(f"\nForward return coverage: {coverage:.1%}")

    return news_df


def prepare_pipeline_data(
    dataset_key: str = "massive_news",
    sample_size: int = 100000
) -> pd.DataFrame:
    """
    Prepare Kaggle data for the pipeline.

    Args:
        dataset_key: Which Kaggle dataset to use
        sample_size: Limit to N rows for faster processing (None for all)

    Returns:
        DataFrame ready for feature engineering
    """
    data_dir = f"data/external/{dataset_key}"

    # Check if already downloaded
    if not (Path(BASE_DIR) / data_dir).exists():
        print(f"Dataset not found. Downloading...")
        success = download_kaggle_dataset(dataset_key)
        if not success:
            print("Download failed. Please download manually from Kaggle.")
            return pd.DataFrame()

    # Load dataset
    if dataset_key == "massive_news":
        df = load_massive_news_dataset(data_dir)
    else:
        print(f"Loader for {dataset_key} not implemented yet")
        return pd.DataFrame()

    if df.empty:
        return df

    # Sample if too large
    if sample_size and len(df) > sample_size:
        print(f"\nSampling {sample_size:,} rows from {len(df):,}")
        df = df.sample(n=sample_size, random_state=42)

    # Add forward returns
    df = merge_with_prices_kaggle(df)

    # Filter to rows with valid returns
    df_valid = df[df['future_return_5d'].notna()].copy()

    print(f"\nFinal dataset: {len(df_valid)} rows with valid returns")

    # Save to raw data path
    os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)
    df_valid.to_csv(RAW_DATA_PATH, index=False)
    print(f"Saved to {RAW_DATA_PATH}")

    return df_valid


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--download", action="store_true", help="Download dataset")
    parser.add_argument("--dataset", default="massive_news", choices=list(KAGGLE_DATASETS.keys()))
    parser.add_argument("--prepare", action="store_true", help="Prepare pipeline data")
    parser.add_argument("--sample", type=int, default=50000, help="Sample size")
    args = parser.parse_args()

    if args.download:
        download_kaggle_dataset(args.dataset)

    if args.prepare:
        df = prepare_pipeline_data(args.dataset, sample_size=args.sample)
        if not df.empty:
            print("\n✓ Data ready for pipeline!")
            print(f"Run: python run_phase1.py --skip-preprocess")
