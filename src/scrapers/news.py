"""
News scraper module, implementing the XML parsing logic for Google News RSS feeds to extract unstructured corporate narrative text reliably.
"""
from __future__ import annotations

import logging
from typing import Any
from bs4 import BeautifulSoup

from .base import BaseScraper, ScrapedArticle

logger = logging.getLogger(__name__)

class NewsScraper(BaseScraper):
    def __init__(self, ticker: str = "NVDA", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.ticker = ticker.upper()

    @property
    def name(self) -> str:
        return "NewsScraper (Google RSS)"

    @property
    def target_urls(self) -> list[str]:
        # Google News RSS is extremely reliable for scraping
        return [f"https://news.google.com/rss/search?q={self.ticker}+stock&hl=en-US&gl=US&ceid=US:en"]

    def parse(self, soup: BeautifulSoup, url: str) -> list[ScrapedArticle]:
        articles: list[ScrapedArticle] = []
        
        items = soup.find_all("item")
        if not items:
            logger.warning("[%s] No items found in RSS feed.", self.name)
            return articles

        for item in items:
            title = item.title.get_text(strip=True) if item.title else "Untitled"
            link = item.link.get_text(strip=True) if item.link else url
            pub_date = item.pubdate.get_text(strip=True) if item.pubdate else "Unknown"
            
            # Google News injects HTML into the description, we parse it out
            desc = ""
            if item.description:
                desc_soup = BeautifulSoup(item.description.text, "html.parser")
                desc = desc_soup.get_text(separator=" ", strip=True)
            if not desc:
                desc = title

            articles.append(ScrapedArticle(
                title=title,
                url=link,
                publish_date=pub_date,
                source="Google News",
                body_text=desc,
            ))

        return articles