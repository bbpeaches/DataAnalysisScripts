"""
Unit tests for the scraper hierarchy.

Updated to test the yfinance Adapter and Google News RSS implementations.
All external calls are mocked to run swiftly and completely offline.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from src.scrapers.base import BaseScraper, ScrapedArticle, ScrapedRow
from src.scrapers.fundamentals import FundamentalsScraper
from src.scrapers.news import NewsScraper

class TestScrapedArticle:
    def test_word_count(self) -> None:
        a = ScrapedArticle(title="T", url="http://x", publish_date="2024", source="S", body_text="one two three four")
        assert a.word_count == 4

    def test_word_count_empty(self) -> None:
        a = ScrapedArticle(title="T", url="http://x", publish_date="2024", source="S", body_text="")
        assert a.word_count == 0

    def test_frozen(self) -> None:
        a = ScrapedArticle(title="T", url="http://x", publish_date="2024", source="S", body_text="hi")
        with pytest.raises(AttributeError):
            a.title = "new"  # type: ignore[misc]


class TestScrapedRow:
    def test_fields(self) -> None:
        r = ScrapedRow(label="revt", value=26_974_000_000.0, period="2023", source="Test")
        assert r.label == "revt"
        assert r.value == 26_974_000_000.0

class _DummyScraper(BaseScraper):
    @property
    def name(self) -> str:
        return "DummyScraper"

    @property
    def target_urls(self) -> list[str]:
        return ["https://example.com/page1"]

    def parse(self, soup: BeautifulSoup, url: str) -> list[ScrapedArticle | ScrapedRow]:
        return [ScrapedArticle(title="Test", url=url, publish_date="2024", source="Dummy", body_text="body")]

class TestBaseScraper:
    def test_cannot_instantiate_abc(self) -> None:
        with pytest.raises(TypeError):
            BaseScraper()  # type: ignore[abstract]

    def test_build_headers_has_ua(self) -> None:
        s = _DummyScraper()
        h = s._build_headers()
        assert "User-Agent" in h

    @patch("src.scrapers.base.time.sleep")
    def test_rate_limit(self, mock_sleep: MagicMock) -> None:
        s = _DummyScraper(min_delay=1.0, max_delay=1.0)
        s._rate_limit()
        mock_sleep.assert_called_once_with(1.0)

    def test_fetch_returns_none_on_all_retries(self) -> None:
        import requests as req
        s = _DummyScraper(max_retries=2)
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.raise_for_status.side_effect = req.exceptions.HTTPError("403")

        with patch.object(s._session, "get", return_value=mock_resp):
            with patch.object(s, "_rate_limit"):
                assert s._fetch("https://bad.com") is None

class TestFundamentalsScraper:
    def test_name_and_urls(self) -> None:
        s = FundamentalsScraper(ticker="NVDA")
        assert s.name == "FundamentalsScraper (yfinance Adapter)"
        assert len(s.target_urls) == 1
        assert "api://yfinance" in s.target_urls[0]

    @patch("src.scrapers.fundamentals.yf.Ticker")
    def test_scrape_success(self, mock_ticker_cls: MagicMock) -> None:
        mock_ticker = MagicMock()
        
        # Mock transposed DataFrames for financials and balance_sheet
        mock_ticker.financials.T = pd.DataFrame(
            {"Total Revenue": [100.0], "Net Income": [10.0], "Research And Development": [5.0]},
            index=[pd.Timestamp("2023-12-31")]
        )
        mock_ticker.balance_sheet.T = pd.DataFrame(
            {"Total Assets": [500.0], "Stockholders Equity": [200.0]},
            index=[pd.Timestamp("2023-12-31")]
        )
        mock_ticker_cls.return_value = mock_ticker

        s = FundamentalsScraper(ticker="NVDA")
        rows = s.scrape()

        # Should extract revt, ni, xrd, at, ceq (5 items)
        assert len(rows) == 5
        labels = {r.label for r in rows}
        assert "revt" in labels
        assert "ni" in labels
        assert "ceq" in labels
        assert rows[0].source == "Yahoo Finance API"

    @patch("src.scrapers.fundamentals.yf.Ticker")
    def test_scrape_exception(self, mock_ticker_cls: MagicMock) -> None:
        mock_ticker_cls.side_effect = Exception("API down")
        s = FundamentalsScraper(ticker="NVDA")
        rows = s.scrape()
        assert rows == []

_GOOGLE_RSS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Nvidia AI Chip Launch</title>
      <link>https://news.google.com/1</link>
      <pubDate>Mon, 01 Jan 2024 10:00:00 GMT</pubDate>
      <description>Nvidia launches new AI chips for datacenter.</description>
    </item>
    <item>
      <title>NVDA Stock Surges</title>
      <link>https://news.google.com/2</link>
      <pubDate>Tue, 02 Jan 2024 10:00:00 GMT</pubDate>
      <description>&lt;a href="test"&gt;HTML in description&lt;/a&gt; is parsed out.</description>
    </item>
  </channel>
</rss>
"""

class TestNewsScraper:
    def test_name_and_urls(self) -> None:
        s = NewsScraper(ticker="NVDA")
        assert s.name == "NewsScraper (Google RSS)"
        assert len(s.target_urls) == 1
        assert "news.google.com" in s.target_urls[0]

    def test_parse_empty_feed(self) -> None:
        s = NewsScraper()
        soup = BeautifulSoup("<rss></rss>", "html.parser")
        assert s.parse(soup, "http://test") == []

    def test_scrape_end_to_end(self) -> None:
        s = NewsScraper(ticker="NVDA")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = _GOOGLE_RSS_XML
        mock_resp.raise_for_status = MagicMock()

        with patch.object(s._session, "get", return_value=mock_resp):
            with patch.object(s, "_rate_limit"):
                results = s.scrape()

        assert len(results) == 2
        assert results[0].title == "Nvidia AI Chip Launch"
        assert results[0].source == "Google News"
        assert "datacenter" in results[0].body_text
        
        # Check that HTML inside the RSS description was parsed correctly
        assert "HTML in description is parsed out." in results[1].body_text