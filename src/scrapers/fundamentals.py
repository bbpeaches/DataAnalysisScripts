"""
Fundamentals scraper module, implementing a yfinance-based adapter pattern to ensure
high-availability financial metrics while adhering to the BaseScraper interface.

Primary backend: yfinance.  Falls back to akshare when yfinance is
unavailable (e.g. geo-restricted networks).
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup

from .base import BaseScraper, ScrapedRow

logger = logging.getLogger(__name__)


def _yfinance_available() -> bool:
    try:
        import yfinance as yf  # type: ignore[import-untyped]
        t = yf.Ticker("NVDA")
        return t.financials is not None and not t.financials.empty
    except Exception:
        return False


def _akshare_available() -> bool:
    try:
        import akshare as ak  # type: ignore[import-untyped]
        df = ak.stock_financial_us_report_em(
            stock="NVDA", symbol="综合损益表", indicator="年报"
        )
        return not df.empty
    except Exception:
        return False


class FundamentalsScraper(BaseScraper):
    def __init__(self, ticker: str = "NVDA", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.ticker = ticker.upper()
        self._backend: str = "yfinance"

        if not _yfinance_available():
            if _akshare_available():
                logger.info(
                    "[%s] yfinance unavailable — switching to akshare backend.",
                    self.__class__.__name__,
                )
                self._backend = "akshare"
            else:
                logger.warning(
                    "[%s] Neither yfinance nor akshare can fetch fundamentals.",
                    self.__class__.__name__,
                )

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

        if self._backend == "akshare":
            rows = self._scrape_akshare()
        else:
            rows = self._scrape_yfinance()

        logger.info("[%s] Adapted %d financial rows.", self.name, len(rows))
        return rows

    # ------------------------------------------------------------------
    # yfinance implementation
    # ------------------------------------------------------------------

    def _scrape_yfinance(self) -> list[ScrapedRow]:
        import yfinance as yf

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
                "Stockholders Equity": "ceq",
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
                                    source="Yahoo Finance API",
                                ))
        except Exception as e:
            logger.error("[%s] Failed to fetch via yfinance: %s", self.name, e)

        return rows

    # ------------------------------------------------------------------
    # akshare fallback implementation
    # ------------------------------------------------------------------

    def _scrape_akshare(self) -> list[ScrapedRow]:
        import akshare as ak

        rows: list[ScrapedRow] = []

        # Mapping from akshare Chinese item names → internal labels
        income_mapping = {
            "营业收入": "revt",
            "净利润": "ni",
            "研发费用": "xrd",
        }
        balance_mapping = {
            "总资产": "at",
            "股东权益合计": "ceq",
        }

        try:
            # Income statement
            df_income = ak.stock_financial_us_report_em(
                stock=self.ticker, symbol="综合损益表", indicator="年报",
            )
            self._extract_rows(df_income, income_mapping, rows)

            # Balance sheet
            df_balance = ak.stock_financial_us_report_em(
                stock=self.ticker, symbol="资产负债表", indicator="年报",
            )
            self._extract_rows(df_balance, balance_mapping, rows)

        except Exception as e:
            logger.error("[%s] Failed to fetch via akshare: %s", self.name, e)

        return rows

    def _extract_rows(
        self,
        df: pd.DataFrame,
        mapping: dict[str, str],
        rows: list[ScrapedRow],
    ) -> None:
        """Extract matching financial rows from an akshare report DataFrame."""
        for item_name, internal_label in mapping.items():
            matched = df[df["ITEM_NAME"] == item_name]
            if matched.empty:
                continue
            for _, row in matched.iterrows():
                val = row["AMOUNT"]
                if pd.notna(val):
                    # Extract fiscal year from REPORT column (e.g. "2025/FY" → "2025")
                    report = str(row["REPORT"])
                    year = report.split("/")[0]
                    rows.append(ScrapedRow(
                        label=internal_label,
                        value=float(val),
                        period=year,
                        source="EastMoney API",
                    ))
