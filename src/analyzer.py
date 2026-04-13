"""
Data analysis module, implementing Natural Language Processing (NLP) sentiment scoring, data transformation, and correlation matrix logic.
"""
from __future__ import annotations

import logging
import re
from collections import Counter
from typing import Optional

import pandas as pd
from textblob import TextBlob

from .scrapers import ScrapedArticle, ScrapedRow

logger = logging.getLogger(__name__)

def compute_sentiment(text: str) -> dict[str, float]:
    """Return polarity (-1…+1) and subjectivity (0…1) for *text*."""
    blob = TextBlob(text)
    return {
        "polarity": round(blob.sentiment.polarity, 4),
        "subjectivity": round(blob.sentiment.subjectivity, 4),
    }


def compute_keyword_frequency(
    articles: list[ScrapedArticle],
    keywords: Optional[list[str]] = None,
) -> pd.DataFrame:
    """Count keyword occurrences across articles.

    Parameters
    ----------
    articles : list[ScrapedArticle]
        Corpus to scan.
    keywords : list[str] | None
        Terms to track (defaults to a curated AI/hardware list).

    Returns
    -------
    pd.DataFrame
        ``['keyword', 'total_mentions', 'article_count']`` sorted descending.
    """
    if keywords is None:
        keywords = [
            "AI", "artificial intelligence", "GPU", "datacenter", "data center",
            "Blackwell", "Hopper", "H100", "revenue", "growth", "autonomous",
            "automotive", "gaming", "cloud", "semiconductor", "chip",
            "neural", "inference", "training", "supercomputer",
        ]

    pattern = re.compile(
        r"\b(?:{})\b".format("|".join(re.escape(k) for k in keywords)),
        re.IGNORECASE,
    )

    counts: Counter[str] = Counter()
    per_article: Counter[str] = Counter()

    for article in articles:
        text = f"{article.title} {article.body_text}".lower()
        found = {m.group().lower() for m in pattern.finditer(text)}
        for kw in found:
            counts[kw] += text.count(kw)
            per_article[kw] += 1

    rows = [
        {
            "keyword": kw,
            "total_mentions": counts.get(kw.lower(), 0),
            "article_count": per_article.get(kw.lower(), 0),
        }
        for kw in keywords
    ]
    return pd.DataFrame(rows).sort_values("total_mentions", ascending=False).reset_index(drop=True)


def analyze_article_sentiments(articles: list[ScrapedArticle]) -> pd.DataFrame:
    """Run sentiment analysis on each article.

    Returns
    -------
    pd.DataFrame
        ``['title', 'source', 'publish_date', 'word_count', 'polarity', 'subjectivity']``.
    """
    rows: list[dict[str, object]] = []
    for a in articles:
        sent = compute_sentiment(f"{a.title} {a.body_text}")
        rows.append({
            "title": a.title,
            "source": a.source,
            "publish_date": a.publish_date,
            "word_count": a.word_count,
            **sent,
        })
    logger.info("Sentiment analysis complete for %d articles.", len(articles))
    return pd.DataFrame(rows)

def fundamentals_to_dataframe(rows: list[ScrapedRow]) -> pd.DataFrame:
    """Pivot a flat list of ``ScrapedRow`` into a wide DataFrame.

    Parameters
    ----------
    rows : list[ScrapedRow]
        Rows as returned by ``FundamentalsScraper.scrape()``.

    Returns
    -------
    pd.DataFrame
        One row per year, columns for each financial metric.
    """
    if not rows:
        return pd.DataFrame()

    records = [
        {"period": r.period, r.label: r.value}
        for r in rows
        if r.period != "unknown"
    ]

    df = pd.DataFrame(records).groupby("period", sort=True).first().reset_index()
    df.rename(columns={"period": "fyear"}, inplace=True)

    # Derived ratios (where columns exist)
    for src, dst in [("ni", "roe"), ("ni", "profit_margin"), ("xrd", "rd_intensity")]:
        if src not in df.columns:
            continue
        denom = "ceq" if dst == "roe" else "revt"
        if denom in df.columns:
            df[dst] = df[src] / df[denom].replace(0, pd.NA)

    logger.info("Fundamentals pivoted: %d rows × %d columns.", len(df), len(df.columns))
    return df

class FinancialAnalyzer:
    """Merges structured and unstructured data into a unified time-series.

    Parameters
    ----------
    fundamentals : pd.DataFrame
        Annual fundamentals (from ``fundamentals_to_dataframe``).
    prices : pd.DataFrame
        Daily OHLCV from ``MarketDataFetcher``.
    sentiment_df : pd.DataFrame
        Article-level sentiment from ``analyze_article_sentiments``.
    """

    def __init__(
        self,
        fundamentals: pd.DataFrame,
        prices: pd.DataFrame,
        sentiment_df: pd.DataFrame,
    ) -> None:
        self.fundamentals = fundamentals.copy()
        self.prices = prices.copy()
        self.sentiment_df = sentiment_df.copy()

    def _broadcast_annual_to_daily(self, fund: pd.DataFrame) -> pd.DataFrame:
        """Forward-fill annual fundamentals to daily frequency, capped at year-end."""
        df = fund.copy()
        df["date"] = pd.to_datetime(df["fyear"].astype(str) + "-01-01")
        df = df.set_index("date")

        # Select numeric columns only
        numeric = df.select_dtypes(include="number")
        daily = numeric.resample("D").ffill()

        # Cap forward-fill at each fiscal year boundary
        for _, row in df.iterrows():
            year = int(row["fyear"]) if isinstance(row["fyear"], (int, float)) else int(str(row["fyear"])[:4])
            cutoff = pd.Timestamp(year=year, month=12, day=31)
            mask = daily.index > cutoff
            if mask.any():
                daily.loc[mask] = pd.NA

        return daily

    def build_merged_dataset(self) -> pd.DataFrame:
        """Produce the final merged dataset.

        Returns
        -------
        pd.DataFrame
            Daily-frequency DataFrame with price, fundamental, and sentiment columns.
        """
        logger.info("Building merged dataset …")

        # Broadcast fundamentals
        if not self.fundamentals.empty:
            daily_fund = self._broadcast_annual_to_daily(self.fundamentals)
        else:
            daily_fund = pd.DataFrame()

        # Prepare prices
        prices = self.prices.copy()
        prices.index = pd.to_datetime(prices.index).tz_localize(None)
        price_cols = [c for c in ["Close", "Volume"] if c in prices.columns]

        # Merge on date index
        merged = prices[price_cols]
        if not daily_fund.empty:
            merged = merged.join(daily_fund, how="inner")

        # Overlay average sentiment
        if not self.sentiment_df.empty and "polarity" in self.sentiment_df.columns:
            merged["avg_sentiment_polarity"] = self.sentiment_df["polarity"].mean()
            merged["avg_sentiment_subjectivity"] = self.sentiment_df["subjectivity"].mean()

        logger.info("Merged dataset: %d rows × %d columns.", len(merged), len(merged.columns))
        return merged

    def compute_correlation_matrix(self) -> pd.DataFrame:
        """Pearson correlations between all numeric columns."""
        merged = self.build_merged_dataset()
        numeric = merged.select_dtypes(include="number")
        corr = numeric.corr()
        logger.info("Correlation matrix: %d × %d.", *corr.shape)
        return corr
