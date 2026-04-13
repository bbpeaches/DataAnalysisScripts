"""
Fundamentals scraper module, implementing a yfinance-based adapter pattern to ensure high-availability financial metrics while adhering to the BaseScraper interface.
"""
from __future__ import annotations

import logging
from typing import Any
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup

from .base import BaseScraper, ScrapedRow

logger = logging.getLogger(__name__)

class FundamentalsScraper(BaseScraper):
    def __init__(self, ticker: str = "NVDA", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.ticker = ticker.upper()

    @property
    def name(self) -> str:
        return "FundamentalsScraper (yfinance Adapter)"

    @property
    def target_urls(self) -> list[str]:
        return ["api://yfinance/fundamentals"]

    def parse(self, soup: BeautifulSoup, url: str) -> list[ScrapedRow]:
        return []

    def scrape(self) -> list[ScrapedRow]:
        logger.info("[%s] Fetching fundamentals via API adapter...", self.name)
        rows: list[ScrapedRow] = []
        
        try:
            tkr = yf.Ticker(self.ticker)
            fin = tkr.financials.T
            bs = tkr.balance_sheet.T
            
            mapping = {
                "Total Revenue": "revt",
                "Net Income": "ni",
                "Research And Development": "xrd",
                "Total Assets": "at",
                "Stockholders Equity": "ceq"
            }
            
            for df in [fin, bs]:
                for metric_name, internal_label in mapping.items():
                    if metric_name in df.columns:
                        for date_idx, val in df[metric_name].items():
                            if pd.notna(val):
                                year = str(date_idx.year)
                                rows.append(ScrapedRow(
                                    label=internal_label,
                                    value=float(val),
                                    period=year,
                                    source="Yahoo Finance API"
                                ))
        except Exception as e:
            logger.error("[%s] Failed to fetch via yfinance: %s", self.name, e)

        logger.info("[%s] Adapted %d financial rows.", self.name, len(rows))
        return rows