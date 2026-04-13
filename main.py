import logging
import warnings
from typing import cast

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np 

from src import (
    FinancialAnalyzer,
    fundamentals_to_dataframe,
    compute_keyword_frequency,
    analyze_article_sentiments,
    MarketDataFetcher,
    FundamentalsScraper,
    NewsScraper
)
from src.scrapers.base import ScrapedRow, ScrapedArticle

plt.style.use("seaborn-v0_8-whitegrid")
sns.set_palette("husl")
print("Environment ready.")

market = MarketDataFetcher(ticker="NVDA")
prices = market.fetch_daily_prices(start="2020-01-01")
metrics = market.fetch_info()

print(f"Prices: {prices.shape}")
print("\nKey Metrics:")
for k, v in metrics.items():
    print(f"  {k}: {v}")

with FundamentalsScraper(ticker="NVDA") as fin_scraper:
    raw_fin = cast(list[ScrapedRow], fin_scraper.scrape())

fundamentals = fundamentals_to_dataframe(raw_fin)
print(f"Fundamentals: {fundamentals.shape}")

with NewsScraper(ticker="NVDA") as news_scraper:
    articles = cast(list[ScrapedArticle], news_scraper.scrape())

finviz_articles = [a for a in articles if a.source == "Finviz"]
ir_articles = [a for a in articles if a.source == "Nvidia IR"]

print(f"Total articles: {len(articles)}")
print(f"  Finviz headlines: {len(finviz_articles)}")
print(f"  Nvidia IR press releases: {len(ir_articles)}")

display_cols = [c for c in ["fyear", "revt", "ni", "xrd", "roe", "rd_intensity", "profit_margin"] if c in fundamentals.columns]

sentiment_df = analyze_article_sentiments(articles)
print(f"Sentiment analysed: {len(sentiment_df)} articles")

kw_df = compute_keyword_frequency(articles)

fig, ax = plt.subplots(figsize=(14, 5))

ax.plot(prices.index, prices["Close"], color="#76B900", lw=1.5, label="NVDA Close")
ax.fill_between(
    prices.index,
    prices["Close"].to_numpy(),
    float(prices["Close"].min()),
    alpha=0.08,
    color="#76B900"
)

if len(prices) >= 200:
    ax.plot(prices.index, prices["Close"].rolling(50).mean(), "--", color="#FF6F00", lw=1, alpha=0.8, label="50d MA")
    ax.plot(prices.index, prices["Close"].rolling(200).mean(), "--", color="#D32F2F", lw=1, alpha=0.8, label="200d MA")

ax.set_title("NVDA Daily Close (2020–Present)", fontsize=13, fontweight="bold")
ax.set_xlabel("Date")
ax.set_ylabel("Price (USD)")
ax.legend()
plt.tight_layout()
plt.show()

available_ratios = [c for c in ["roe", "rd_intensity", "profit_margin"] if c in fundamentals.columns]

if available_ratios:
    fig, axes = plt.subplots(1, len(available_ratios), figsize=(5 * len(available_ratios), 5))
    if len(available_ratios) == 1:
        axes = [axes]

    titles = {"roe": "Return on Equity (%)", "rd_intensity": "R&D Intensity (%)", "profit_margin": "Profit Margin (%)"}
    for ax, ratio in zip(axes, available_ratios):
        vals = fundamentals[ratio].dropna().astype(float).to_numpy() * 100
        years = fundamentals["fyear"].astype(str).values[:len(vals)]
        ax.bar(years, vals, color="#1565C0", alpha=0.85)
        ax.set_title(titles.get(ratio, ratio), fontweight="bold")
        ax.set_xlabel("Fiscal Year")
        ax.tick_params(axis="x", rotation=45)

    plt.suptitle("Nvidia Key Financial Ratios", fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.show()
else:
    print("No derived ratios available — check fundamentals DataFrame.")

if "revt" in fundamentals.columns and "ni" in fundamentals.columns:
    fig, ax1 = plt.subplots(figsize=(10, 5))

    years = fundamentals["fyear"].astype(str)
    x = np.arange(len(years))
    width = 0.35

    ax1.bar(x - width/2, fundamentals["revt"] / 1e9, width, label="Revenue", color="#76B900", alpha=0.85)
    ax1.bar(x + width/2, fundamentals["ni"] / 1e9, width, label="Net Income", color="#FF6F00", alpha=0.85)
    ax1.set_ylabel("USD (Billions)")
    ax1.set_xticks(x)
    ax1.set_xticklabels(years, rotation=45)
    ax1.legend(loc="upper left")

    if "profit_margin" in fundamentals.columns:
        ax2 = ax1.twinx()
        pm = fundamentals["profit_margin"].dropna().astype(float).to_numpy() * 100
        ax2.plot(x[:len(pm)], pm, "o-", color="#D32F2F", lw=2, markersize=8, label="Profit Margin")
        ax2.set_ylabel("Profit Margin (%)", color="#D32F2F")
        ax2.legend(loc="upper right")

    ax1.set_title("Revenue, Net Income & Profit Margin", fontsize=13, fontweight="bold")
    plt.tight_layout()
    plt.show()
else:
    print("Revenue / Net Income columns not available.")

fig, ax = plt.subplots(figsize=(11, 6))
top = kw_df.head(15)
bars = ax.barh(top["keyword"], top["total_mentions"], color="#1565C0", alpha=0.85)
ax.invert_yaxis()
ax.set_title("Top Keywords in Nvidia Corporate Narrative", fontsize=13, fontweight="bold")
ax.set_xlabel("Total Mentions")
for bar in bars:
    w = bar.get_width()
    ax.text(w + 0.3, bar.get_y() + bar.get_height()/2, str(int(w)), va="center", fontsize=10)
plt.tight_layout()
plt.show()

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

ax = axes[0]
if not sentiment_df.empty:
    for src in sentiment_df["source"].unique():
        subset = sentiment_df[sentiment_df["source"] == src]
        ax.hist(subset["polarity"], bins=25, alpha=0.5, label=src, edgecolor="white")
    ax.axvline(0, color="red", ls="--", lw=1, label="Neutral")
    ax.set_title("Sentiment Polarity by Source", fontweight="bold")
    ax.set_xlabel("Polarity (-1 to +1)")
    ax.set_ylabel("Frequency")
    ax.legend()

ax = axes[1]
if not sentiment_df.empty:
    sc = ax.scatter(
        sentiment_df["polarity"], sentiment_df["subjectivity"],
        c=sentiment_df["word_count"], cmap="viridis", alpha=0.7, s=50,
    )
    plt.colorbar(sc, ax=ax, label="Word Count")
    ax.axvline(0, color="red", ls="--", lw=1)
    ax.set_title("Polarity vs Subjectivity", fontweight="bold")
    ax.set_xlabel("Polarity")
    ax.set_ylabel("Subjectivity")

plt.tight_layout()
plt.show()

analyzer = FinancialAnalyzer(fundamentals, prices, sentiment_df)
corr = analyzer.compute_correlation_matrix()

cols = [c for c in ["Close", "Volume", "roe", "rd_intensity", "profit_margin",
                     "avg_sentiment_polarity", "avg_sentiment_subjectivity"]
        if c in corr.columns]

if cols:
    fig, ax = plt.subplots(figsize=(9, 7))
    sns.heatmap(
        corr[cols].loc[cols], annot=True, fmt=".2f",
        cmap="RdBu_r", center=0, square=True, linewidths=0.5, ax=ax,
    )
    ax.set_title("Correlation: Financial Metrics × Sentiment", fontsize=13, fontweight="bold")
    plt.tight_layout()
    plt.show()
else:
    print("Insufficient data for correlation matrix.")