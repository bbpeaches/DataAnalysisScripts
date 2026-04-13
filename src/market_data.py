"""
Market data acquisition module, implementing the retrieval of historical OHLCV prices and key financial metrics via the yfinance API.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

class MarketDataFetcher:
    """Fetches equity market data from Yahoo Finance."""

    def __init__(self, ticker: str = "NVDA") -> None:
        self.ticker: str = ticker.upper()
        self._t: Optional[yf.Ticker] = None

    @property
    def _tick(self) -> yf.Ticker:
        """Lazy-initialise the ``yf.Ticker`` instance."""
        if self._t is None:
            self._t = yf.Ticker(self.ticker)
            logger.debug("Initialised yf.Ticker for %s.", self.ticker)
        return self._t

    def fetch_daily_prices(
        self,
        start: str = "2020-01-01",
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """Download daily OHLCV price history."""
        end = end or datetime.now().strftime("%Y-%m-%d")
        logger.info("Fetching %s daily prices (%s → %s) …", self.ticker, start, end)

        hist: pd.DataFrame = self._tick.history(start=start, end=end, auto_adjust=True)

        if hist.empty:
            logger.warning("yfinance returned no data for %s in [%s, %s].", self.ticker, start, end)
        else:
            logger.info("Retrieved %d rows for %s.", len(hist), self.ticker)

        return hist

    def fetch_info(self) -> dict[str, object]:
        """Retrieve snapshot valuation and profitability metrics."""
        logger.info("Fetching info snapshot for %s …", self.ticker)
        raw: dict[str, object] = self._tick.info

        keys = [
            "trailingPE", "forwardPE", "priceToBook", "marketCap",
            "fiftyDayAverage", "twoHundredDayAverage", "dividendYield",
            "returnOnEquity", "revenueGrowth", "earningsGrowth",
            "grossMargins", "operatingMargins",
        ]

        info = {k: raw[k] for k in keys if raw.get(k) is not None}
        logger.info("Retrieved %d metrics for %s.", len(info), self.ticker)
        return info