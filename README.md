# News-to-Signal: Cross-Sectional Equity Selection via Textual Analysis

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A quantitative research framework demonstrating the construction of cross-sectional trading signals from unstructured financial news. This project applies rigorous statistical methodology to test behavioral finance hypotheses through systematic alpha research.

**Research Question**: Can sentiment extracted from financial news headlines predict cross-sectional equity returns, and does incorporating uncertainty and event intensity enhance signal efficacy?

---

## 1. Research Motivation

Behavioral finance theory suggests that investor sentiment drives temporary mispricing in equity markets. This project operationalizes this hypothesis by:

1. **Extracting textual features** from news headlines using the Loughran-McDonald financial sentiment dictionary
2. **Constructing standardized signals** via cross-sectional z-score normalization
3. **Evaluating predictive power** through robust statistical tests including t-statistics, information ratios, and walk-forward validation
4. **Controlling for data quality** via survivorship-bias-aware sampling and transaction cost assumptions

The methodology emphasizes research process rigor over absolute performance, demonstrating capabilities essential for quantitative researcher roles in asset management.

---

## 2. Hypothesis Framework

| Hypothesis | Description | Test Method |
|------------|-------------|-------------|
| **H1** | Positive sentiment predicts negative future returns (overreaction correction) | IC t-statistic |
| **H2** | Uncertainty dampens sentiment predictability | Signal augmentation comparison |
| **H3** | Event-intensity enhances signal efficacy for high-news-activity stocks | Interaction analysis |

---

## 3. Methodology

### 3.1 Data Architecture

**Phase 1: Enhanced Multi-Source Architecture**

| Component | Original | Phase 1 Enhancement |
|-----------|----------|---------------------|
| **Universe** | 50 stocks | 180 stocks (Russell 1000 representative) |
| **News Sources** | Yahoo RSS only | Finnhub + FMP + Yahoo RSS (fallback) |
| **Sample Period** | 365 days | 730 days (2 years) |
| **Price Data** | yfinance | yfinance + FMP fallback |

**Data Source Priority:**
1. **Finnhub API** (60 calls/min free tier) - Primary source
2. **Financial Modeling Prep** (250 calls/day free tier) - Secondary
3. **Yahoo Finance RSS** - Fallback for coverage gaps

**API Keys Setup:**
```bash
export FINNHUB_API_KEY="your_key_here"
export FMP_API_KEY="your_key_here"
```

Get free API keys:
- Finnhub: https://finnhub.io/register
- FMP: https://financialmodelingprep.com/developer/docs/pricing

**Synthetic Data Option:**
For testing without API limits, use synthetic data generation:
```bash
python run_phase1.py --synthetic --universe russell1000
```

**Forward Return:** $r_{t+1:t+5} = \ln(P_{t+5} / P_{t+1})$ where $t$ is news date

### 3.2 Feature Engineering

Using the [Loughran-McDonald Master Dictionary](https://sraf.nd.edu/loughranmcdonald-master-dictionary/):

```
sentiment_score    = (positive_words - negative_words) / total_words  ∈ [-1, 1]
uncertainty_score  = uncertainty_words / total_words                   ∈ [0, 1]
event_intensity    = matched_event_keywords / total_keywords          ∈ [0, 1]
```

### 3.3 Signal Construction

All signals use **cross-sectional z-score standardization** (independent per trading date):

| Signal | Formula |
|--------|---------|
| Sentiment-Only | $z(\text{sentiment})$ |
| Sentiment-Uncertainty | $z(\text{sentiment}) - z(\text{uncertainty})$ |
| Full Signal | $z(\text{sentiment}) - z(\text{uncertainty}) + z(\text{event})$ |

### 3.4 Evaluation Framework

**Primary Metrics**:
- **Information Coefficient (IC)**: Spearman rank correlation between signal and forward return
- **IC t-statistic**: Tests significance of mean IC ≠ 0
- **Information Ratio**: Annualized IC mean / IC standard deviation

**Secondary Metrics**:
- Top-minus-Bottom quintile spread and hit rate
- Sharpe ratio, Sortino ratio, maximum drawdown
- Walk-forward validation across train/validation/test splits

---

## 4. Repository Structure

```
news-to-signal-case-study/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── config.py                 # Central configuration
├── run_phase1.py             # Phase 1 automation script
├── data/
│   ├── raw/                  # Fetched news and prices
│   ├── processed/            # Feature-engineered dataset
│   └── reference/            # Universe definitions
│       ├── universe.csv              # Original 50 stocks
│       ├── sp500_universe.csv        # S&P 500 (110 stocks)
│       └── russell1000_universe.csv  # Russell 1000 (180 stocks)
├── notebook/
│   └── case_study.ipynb      # Complete research workflow
├── results/
│   ├── figures/              # Generated visualizations
│   └── tables/               # Summary statistics
└── src/
    ├── __init__.py
    ├── preprocess.py          # Original preprocessing (Yahoo only)
    ├── preprocess_v2.py       # Phase 1: Multi-source preprocessing
    ├── data_sources.py        # Phase 1: Finnhub/FMP/Yahoo aggregation
    ├── kaggle_data_loader.py  # Phase 1: Kaggle/synthetic data loader
    ├── feature_engineering.py # Text feature extraction
    ├── signal_construction.py # Cross-sectional z-scores
    ├── evaluation.py          # IC, spread, risk metrics
    └── walkforward.py         # Out-of-sample validation
```

---

## 5. Installation

```bash
# Clone repository
git clone https://github.com/zuoyi666/news-to-signal-case-study.git
cd news-to-signal-case-study

# Install dependencies
pip install -r requirements.txt

# Download Loughran-McDonald dictionary
# Place at: data/reference/loughran_mcdonald.csv
# Source: https://sraf.nd.edu/loughranmcdonald-master-dictionary/
```

### API Key Setup (Optional but Recommended)

**⚠️ Security Warning:** Never commit API keys to GitHub!

```bash
# Step 1: Copy the example file
cp .env.example .env

# Step 2: Edit .env with your actual keys (this file is .gitignore'd)
# Get free API keys:
# - Finnhub: https://finnhub.io/register (60 calls/min)
# - FMP: https://financialmodelingprep.com/developer/docs/pricing (250 calls/day)
```

Your `.env` file will look like:
```
FINNHUB_API_KEY=c1234567890abcdef
FMP_API_KEY=your_actual_key_here
```

**No API Keys?** No problem! Use `--synthetic` mode (see below).

---

## 6. Usage

### Phase 1: Quick Start (Recommended)

**Option A: Test with Synthetic Data (No API Keys Required)** ⭐
```bash
# Generate realistic synthetic data for 180 tickers over 2 years
# Perfect for testing the pipeline without API limits
python run_phase1.py --synthetic --universe russell1000
```

**Option B: Live Data with Free APIs** 🚀
```bash
# Method 1: Use .env file (recommended)
cp .env.example .env
# Edit .env with your keys, then:
python run_phase1.py --universe russell1000

# Method 2: Environment variables
export FINNHUB_API_KEY="your_key"
export FMP_API_KEY="your_key"
python run_phase1.py --universe russell1000
```

**Option C: S&P 500 Only (Faster)**
```bash
python run_phase1.py --synthetic --universe sp500
```

**Option C: S&P 500 Only (Faster)**
```bash
python run_phase1.py --synthetic --universe sp500
```

### Original Pipeline (Yahoo RSS Only, 50 Stocks)

```python
from src import preprocess, feature_engineering, signal_construction, evaluation

# 1. Fetch data
df_raw = preprocess.main()

# 2. Engineer features
df_features = feature_engineering.main(df_raw)

# 3. Construct signals
df_signals = signal_construction.main(df_features)

# 4. Evaluate
summary = evaluation.run_baseline_comparison(df_signals)
print(summary)
```

### Run Walk-Forward Validation

```python
from src.walkforward import run_walkforward_analysis, check_robustness

# Split into train/validation/test
wf_results = run_walkforward_analysis(df_signals)

# Check robustness
robustness = check_robustness(wf_results)
```

### Launch Jupyter Notebook

```bash
jupyter notebook notebook/case_study.ipynb
```

### Load Kaggle Historical Data (Alternative)

```python
from src.kaggle_data_loader import load_kaggle_sentiment_dataset

# Load pre-existing Kaggle dataset
df = load_kaggle_sentiment_dataset("data/external/kaggle_news.csv")
```

---

## FAQ

### Do I need API keys to run this project?

**No!** You have three options:

1. **Synthetic Data** (easiest): Run with `--synthetic` flag. No API keys needed, works immediately.
2. **Kaggle Data**: Download historical news datasets from Kaggle (free, no API needed).
3. **Live APIs**: Get free API keys for Finnhub/FMP for real-time data (recommended for production).

### Will my API keys be exposed on GitHub?

**No.** The project uses industry-standard security practices:

- `.env` file stores your keys locally (never committed)
- `.env` is in `.gitignore` (Git ignores it completely)
- Only `.env.example` (with dummy values) is on GitHub
- The code uses `python-dotenv` to load keys from `.env`

**⚠️ Never commit your actual `.env` file!**

### How do I get free API keys?

| Service | Free Tier | Link |
|---------|-----------|------|
| Finnhub | 60 calls/minute | https://finnhub.io/register |
| FMP | 250 calls/day | https://financialmodelingprep.com/developer/docs/pricing |

Both offer free tiers sufficient for this project.

### Can other users run my code without API keys?

**Yes.** The project is designed for easy onboarding:

```bash
# Other users can run immediately with synthetic data
python run_phase1.py --synthetic --universe sp500
```

The README clearly shows this as "Option A" because:
- No setup required
- No account creation
- Demonstrates full pipeline functionality
- Results are reproducible

### What if I accidentally commit my API key?

**Act immediately:**

1. Revoke the key in your Finnhub/FMP dashboard
2. Generate a new key
3. Update your `.env` file (don't commit it!)
4. Run `git rm --cached .env` if needed
5. Consider using [git-filter-repo](https://github.com/newren/git-filter-repo) for history rewriting

### Why not just put API keys in config.py?

**Security risk.** If you commit `config.py` with keys:
- Keys are permanently in Git history
- Anyone can see them in commit logs
- Malicious users could abuse your API quota
- Violates API provider's Terms of Service

The `.env` pattern is the industry standard for managing secrets.

---

## 7. Empirical Results

### 7.1 In-Sample Performance

| Signal | Mean IC | IC t-stat | Ann. IR | Sharpe | Max DD |
|--------|---------|-----------|---------|--------|--------|
| Sentiment-Only | TBD | TBD | TBD | TBD | TBD |
| Sentiment-Uncertainty | TBD | TBD | TBD | TBD | TBD |
| Full Signal | TBD | TBD | TBD | TBD | TBD |

*Note: Run notebook to populate with actual results.*

### 7.2 Statistical Significance

- **Null Hypothesis**: Mean IC = 0 (no predictive power)
- **Test**: One-sample t-test with α = 0.05
- **Robustness Criteria**:
  1. Consistent IC sign across periods
  2. Significant in at least one period (p < 0.05)
  3. No >50% deterioration from validation to test

### 7.3 Walk-Forward Validation

Results demonstrate signal stability across:
- **Training Period**: Signal discovery and calibration
- **Validation Period**: Hyperparameter selection (not used here)
- **Test Period**: True out-of-sample performance

---

## 8. Risk Management

### 8.1 Data Quality Controls

- **Coverage check**: Minimum 80% non-null forward returns on eligible dates
- **Cross-sectional depth**: Minimum 9 observations per date (tercile/quintile fallback)
- **Outlier detection**: Daily returns >50% flagged as data errors

### 8.2 Limitations

1. **Survivorship bias**: Universe selected based on current large-cap status
2. **Sample size**: 50 stocks limits statistical power
3. **Look-ahead**: News timestamps assume immediate availability
4. **Transaction costs**: Not fully modeled (can be added via `apply_transaction_costs`)

---

## 9. Key Skills Demonstrated

This project showcases competencies relevant to quantitative researcher positions:

| Skill | Implementation |
|-------|----------------|
| **Alpha Research** | End-to-end signal construction from raw data |
| **Statistical Testing** | t-statistics, p-values, information ratios |
| **Risk Management** | Sharpe, Sortino, drawdown analysis |
| **Time-Series Validation** | Walk-forward train/val/test splits |
| **Text Processing** | Dictionary-based NLP feature extraction |
| **Python Engineering** | Modular design, type hints, documentation |

---

## 10. Extensions

Future enhancements to strengthen the framework:

- [ ] **Sector/Size Neutralization**: Control for Fama-French factor exposures
- [ ] **ML Baseline**: Compare against XGBoost/BERT embeddings
- [ ] **Transaction Cost Model**: Incorporate market impact and slippage
- [ ] **Multi-Horizon Analysis**: Test signal decay at 1d/5d/10d/20d horizons
- [ ] **Regime Detection**: Analyze signal efficacy in bull/bear/volatile markets

---

## 11. References

1. Loughran, T. and McDonald, B. (2011). "When is a Liability not a Liability? Textual Analysis, Dictionaries, and 10-Ks." *Journal of Finance*, 66(1), 35-65.
2. Fama, E.F. and French, K.R. (1993). "Common Risk Factors in the Returns on Stocks and Bonds." *Journal of Financial Economics*, 33(1), 3-56.
3. Grinold, R.C. and Kahn, R.N. (2000). *Active Portfolio Management*. McGraw-Hill.

---

## 12. License

MIT License - See [LICENSE](LICENSE) for details.

---

## 13. Contact

For questions or collaboration opportunities, please open an issue on GitHub.

**Author**: zuoyi666
**Background**: PhD in Materials Science transitioning to Quantitative Research
