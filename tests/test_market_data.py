"""
Unit tests for src/market_data.py.
"""
from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.market_data import MarketDataFetcher


def _force_yfinance_backend():
    """Patch availability helpers to force the yfinance backend."""
    ctx = ExitStack()
    ctx.enter_context(patch("src.market_data._yfinance_available", return_value=True))
    ctx.enter_context(patch("src.market_data._akshare_available", return_value=False))
    return ctx


class TestMarketDataFetcher:
    def test_init_normalises_ticker(self) -> None:
        with _force_yfinance_backend():
            f = MarketDataFetcher(ticker="nvda")
        assert f.ticker == "NVDA"

    @patch("yfinance.Ticker")
    def test_lazy_ticker_init(self, mock_cls: MagicMock) -> None:
        with _force_yfinance_backend():
            f = MarketDataFetcher(ticker="NVDA")
        assert f._t is None
        _ = f._tick
        mock_cls.assert_called_once_with("NVDA")
        _ = f._tick  # second access — no additional call
        mock_cls.assert_called_once()

    @patch("yfinance.Ticker")
    def test_fetch_daily_prices(self, mock_cls: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame(
            {
                "Open": [100.0, 102.0],
                "High": [103.0, 105.0],
                "Low": [99.0, 101.0],
                "Close": [102.0, 104.0],
                "Volume": [1_000_000, 1_200_000],
            },
            index=pd.date_range("2024-01-01", periods=2),
        )
        mock_cls.return_value = mock_ticker

        with _force_yfinance_backend():
            f = MarketDataFetcher(ticker="NVDA")
            df = f.fetch_daily_prices(start="2024-01-01", end="2024-01-02")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]

    @patch("yfinance.Ticker")
    def test_fetch_daily_prices_empty(self, mock_cls: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame()
        mock_cls.return_value = mock_ticker

        with _force_yfinance_backend():
            f = MarketDataFetcher(ticker="NVDA")
            df = f.fetch_daily_prices()

        assert df.empty

    @patch("yfinance.Ticker")
    def test_fetch_info(self, mock_cls: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker.info = {
            "trailingPE": 65.0,
            "forwardPE": 55.0,
            "priceToBook": 42.0,
            "marketCap": 3e12,
            "returnOnEquity": 0.82,
            "revenueGrowth": 1.22,
            "earningsGrowth": 5.81,
            "grossMargins": 0.78,
            "operatingMargins": 0.65,
            "irrelevantField": "filtered",
        }
        mock_cls.return_value = mock_ticker

        with _force_yfinance_backend():
            f = MarketDataFetcher(ticker="NVDA")
            info = f.fetch_info()

        assert info["trailingPE"] == 65.0
        assert info["marketCap"] == 3e12
        assert "irrelevantField" not in info

    @patch("yfinance.Ticker")
    def test_fetch_info_filters_none(self, mock_cls: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker.info = {"trailingPE": None, "marketCap": 3e12}
        mock_cls.return_value = mock_ticker

        with _force_yfinance_backend():
            f = MarketDataFetcher(ticker="NVDA")
            info = f.fetch_info()

        assert "trailingPE" not in info
        assert "marketCap" in info


class TestMarketDataFetcherAkshareFallback:
    """Tests for the akshare fallback backend."""

    @patch("src.market_data._yfinance_available", return_value=False)
    @patch("src.market_data._akshare_available", return_value=True)
    @patch("akshare.stock_us_daily")
    def test_fetch_daily_prices_akshare(
        self, mock_daily: MagicMock, _mock_ak: MagicMock, _mock_yf: MagicMock
    ) -> None:
        mock_daily.return_value = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "open": [100.0, 102.0],
                "high": [103.0, 105.0],
                "low": [99.0, 101.0],
                "close": [102.0, 104.0],
                "volume": [1_000_000, 1_200_000],
            }
        )

        f = MarketDataFetcher(ticker="NVDA")
        assert f._backend == "akshare"
        df = f.fetch_daily_prices(start="2024-01-01", end="2024-01-02")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]

    @patch("src.market_data._yfinance_available", return_value=False)
    @patch("src.market_data._akshare_available", return_value=True)
    @patch("akshare.stock_us_daily")
    def test_fetch_info_akshare(
        self, mock_daily: MagicMock, _mock_ak: MagicMock, _mock_yf: MagicMock
    ) -> None:
        mock_daily.return_value = pd.DataFrame(
            {
                "date": ["2024-01-02"],
                "open": [100.0],
                "high": [103.0],
                "low": [99.0],
                "close": [102.0],
                "volume": [1_000_000],
            }
        )

        f = MarketDataFetcher(ticker="NVDA")
        assert f._backend == "akshare"
        info = f.fetch_info()

        assert info["latestClose"] == 102.0
        assert info["latestVolume"] == 1_000_000
