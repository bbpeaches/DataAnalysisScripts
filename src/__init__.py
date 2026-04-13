"""
Core source package, providing interfaces for market data retrieval, web scraping, and financial analysis.
"""

from .scrapers import FundamentalsScraper, NewsScraper
from .market_data import MarketDataFetcher
from .analyzer import (
    FinancialAnalyzer,
    fundamentals_to_dataframe,
    compute_keyword_frequency,
    analyze_article_sentiments,
)

__all__ = [
    "FundamentalsScraper",
    "NewsScraper",
    "MarketDataFetcher",
    "FinancialAnalyzer",
    "fundamentals_to_dataframe",
    "compute_keyword_frequency",
    "analyze_article_sentiments",
]