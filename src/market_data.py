"""
Market data acquisition module, implementing the retrieval of historical OHLCV prices and key financial metrics.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def _yfinance_available() -> bool:
    """Return True if the yfinance backend can reach Yahoo Finance."""
    try:
        import yfinance as yf
        t = yf.Ticker("NVDA")
        h = t.history(period="5d")
        return not h.empty
    except Exception:
        return False


def _akshare_available() -> bool:
    """Return True if the akshare backend can fetch US stock data."""
    try:
        import akshare as ak
        df = ak.stock_us_daily(symbol="NVDA", adjust="qfq")
        return not df.empty
    except Exception:
        return False


class MarketDataFetcher:
    """Fetches equity market data.

    Tries yfinance first; automatically falls back to akshare when Yahoo Finance
    is unreachable (common on geo-restricted or rate-limited networks).
    """

    def __init__(self, ticker: str = "NVDA") -> None:
        self.ticker: str = ticker.upper()
        self._backend: str = "yfinance"
        self._t = None  # lazy yf.Ticker

        # Detect the best available backend
        if not _yfinance_available():
            if _akshare_available():
                logger.info(
                    "yfinance unavailable for %s — switching to akshare backend.",
                    self.ticker,
                )
                self._backend = "akshare"
            else:
                logger.warning(
                    "Neither yfinance nor akshare can fetch data for %s.",
                    self.ticker,
                )

    @property
    def _tick(self):
        """Lazy-initialise the ``yf.Ticker`` instance."""
        if self._t is None:
            import yfinance as yf
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
        logger.info(
            "Fetching %s daily prices (%s → %s) via %s …",
            self.ticker, start, end, self._backend,
        )

        if self._backend == "akshare":
            return self._fetch_daily_akshare(start, end)

        return self._fetch_daily_yfinance(start, end)

    def fetch_info(self) -> dict[str, object]:
        """Retrieve snapshot valuation and profitability metrics."""
        logger.info("Fetching info snapshot for %s via %s …", self.ticker, self._backend)

        if self._backend == "akshare":
            return self._fetch_info_akshare()

        return self._fetch_info_yfinance()

    def _fetch_daily_yfinance(self, start: str, end: str) -> pd.DataFrame:
        import yfinance as yf

        hist: pd.DataFrame = self._tick.history(start=start, end=end, auto_adjust=True)

        if hist.empty:
            logger.warning(
                "yfinance returned no data for %s in [%s, %s].",
                self.ticker, start, end,
            )
        else:
            logger.info("Retrieved %d rows for %s.", len(hist), self.ticker)

        return hist

    def _fetch_info_yfinance(self) -> dict[str, object]:
        raw: dict = self._tick.info

        keys = [
            "trailingPE", "forwardPE", "priceToBook", "marketCap",
            "fiftyDayAverage", "twoHundredDayAverage", "dividendYield",
            "returnOnEquity", "revenueGrowth", "earningsGrowth",
            "grossMargins", "operatingMargins",
        ]

        info = {k: raw[k] for k in keys if raw.get(k) is not None}
        logger.info("Retrieved %d metrics for %s.", len(info), self.ticker)
        return info

    def _fetch_daily_akshare(self, start: str, end: str) -> pd.DataFrame:
        import akshare as ak

        df = ak.stock_us_daily(symbol=self.ticker, adjust="qfq")

        if df.empty:
            logger.warning("akshare returned no data for %s.", self.ticker)
            return df

        df["date"] = pd.to_datetime(df["date"])
        df = df[(df["date"] >= start) & (df["date"] <= end)]

        df = df.rename(columns={
            "open": "Open", "high": "High", "low": "Low",
            "close": "Close", "volume": "Volume",
        })
        df = df.set_index("date").sort_index()
        df = df[["Open", "High", "Low", "Close", "Volume"]]

        logger.info("Retrieved %d rows for %s (akshare).", len(df), self.ticker)
        return df

    def _fetch_info_akshare(self) -> dict[str, object]:
        import akshare as ak

        info: dict[str, object] = {}

        # Derive basic metrics from recent price data
        try:
            df = ak.stock_us_daily(symbol=self.ticker, adjust="qfq")
            if not df.empty:
                latest = df.iloc[-1]
                info["latestClose"] = float(latest["close"])
                info["latestVolume"] = float(latest["volume"])
                info["latestDate"] = str(latest["date"])
        except Exception as exc:
            logger.debug("akshare daily endpoint unavailable: %s", exc)

        info = {k: v for k, v in info.items() if v is not None}
        logger.info("Retrieved %d metrics for %s (akshare).", len(info), self.ticker)
        return info
