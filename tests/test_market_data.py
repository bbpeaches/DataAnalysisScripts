"""
Unit tests for src/market_data.py.

All yfinance API calls are mocked via unittest.mock.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.market_data import MarketDataFetcher


class TestMarketDataFetcher:
    def test_init_normalises_ticker(self) -> None:
        f = MarketDataFetcher(ticker="nvda")
        assert f.ticker == "NVDA"

    @patch("src.market_data.yf.Ticker")
    def test_lazy_ticker_init(self, mock_cls: MagicMock) -> None:
        f = MarketDataFetcher(ticker="NVDA")
        assert f._t is None
        _ = f._tick
        mock_cls.assert_called_once_with("NVDA")
        _ = f._tick  # second access — no additional call
        mock_cls.assert_called_once()

    @patch("src.market_data.yf.Ticker")
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

        f = MarketDataFetcher(ticker="NVDA")
        df = f.fetch_daily_prices(start="2024-01-01", end="2024-01-02")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]

    @patch("src.market_data.yf.Ticker")
    def test_fetch_daily_prices_empty(self, mock_cls: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame()
        mock_cls.return_value = mock_ticker

        f = MarketDataFetcher(ticker="NVDA")
        df = f.fetch_daily_prices()

        assert df.empty

    @patch("src.market_data.yf.Ticker")
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
            "irrelevantField": "filtered",
        }
        mock_cls.return_value = mock_ticker

        f = MarketDataFetcher(ticker="NVDA")
        info = f.fetch_info()

        assert info["trailingPE"] == 65.0
        assert info["marketCap"] == 3e12
        assert "irrelevantField" not in info

    @patch("src.market_data.yf.Ticker")
    def test_fetch_info_filters_none(self, mock_cls: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker.info = {"trailingPE": None, "marketCap": 3e12}
        mock_cls.return_value = mock_ticker

        f = MarketDataFetcher(ticker="NVDA")
        info = f.fetch_info()

        assert "trailingPE" not in info
        assert "marketCap" in info
