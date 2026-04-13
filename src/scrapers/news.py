"""
News scraper module.

Primary backend: Google News RSS feed.  Falls back to akshare
when Google News is unreachable (e.g. geo-restricted or SSL-blocked networks).
"""
from __future__ import annotations

import logging
from typing import Any

from bs4 import BeautifulSoup

from .base import BaseScraper, ScrapedArticle

logger = logging.getLogger(__name__)


def _google_rss_available() -> bool:
    try:
        import requests
        r = requests.get(
            "https://news.google.com/rss/search?q=NVDA&hl=en-US&gl=US&ceid=US:en",
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        return r.status_code == 200 and len(r.text) > 100
    except Exception:
        return False


def _akshare_available() -> bool:
    try:
        import akshare as ak  # type: ignore[import-untyped]
        df = ak.stock_news_em(symbol="NVDA")
        return not df.empty
    except Exception:
        return False


class NewsScraper(BaseScraper):
    def __init__(self, ticker: str = "NVDA", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.ticker = ticker.upper()
        self._backend: str = "google_rss"

        if not _google_rss_available():
            if _akshare_available():
                logger.info(
                    "[%s] Google RSS unavailable — switching to akshare backend.",
                    self.__class__.__name__,
                )
                self._backend = "akshare"
            else:
                logger.warning(
                    "[%s] Neither Google RSS nor akshare can fetch news.",
                    self.__class__.__name__,
                )

    @property
    def name(self) -> str:
        return "NewsScraper (Google RSS)"

    @property
    def target_urls(self) -> list[str]:
        return [
            f"https://news.google.com/rss/search?q={self.ticker}+stock"
            f"&hl=en-US&gl=US&ceid=US:en"
        ]

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

    def scrape(self) -> list[ScrapedArticle]:
        """Execute the scraping cycle using the best available backend."""
        logger.info("[%s] Fetching news …", self.name)

        if self._backend == "akshare":
            return self._scrape_akshare()

        # Default: use BaseScraper's HTTP-based scrape() for Google RSS
        return self._scrape_google_rss()
        
    def _scrape_google_rss(self) -> list[ScrapedArticle]:
        """Scrape via Google News RSS using the parent BaseScraper logic."""
        results: list[ScrapedArticle] = []

        for url in self.target_urls:
            logger.info("[%s] Scraping %s …", self.name, url)
            soup = self._fetch(url)
            if soup is None:
                continue
            try:
                items = self.parse(soup, url)
                results.extend(items)
                logger.info("[%s] Extracted %d items from %s", self.name, len(items), url)
            except Exception as exc:
                logger.error("[%s] Parse error on %s — %s", self.name, url, exc, exc_info=True)
            self._rate_limit()

        logger.info("[%s] Total items scraped: %d", self.name, len(results))
        return results
        
    # Common Chinese financial news sources → English names
    _SOURCE_MAP: dict[str, str] = {
        "东方财富网": "EastMoney",
        "东方财富": "EastMoney",
        "新浪财经": "Sina Finance",
        "同花顺": "THS",
        "证券时报": "Securities Times",
        "中国证券报": "China Securities Journal",
        "上海证券报": "Shanghai Securities News",
        "第一财经": "Yicai",
        "财联社": "Cailian Press",
        "澎湃新闻": "The Paper",
        "每日经济新闻": "National Business Daily",
        "界面新闻": "Jiemian News",
        "21世纪经济报道": "21st Century Business Herald",
        "中国经济网": "China Economy",
        "中证网": "CS",
        "金融界": "JRJ",
        "投资快报": "Investment Express",
        "期货日报": "Futures Daily",
        "财中社": "Caizhongshe",
        "哈富证券": "Hafu Securities",
        "中国经营报": "China Business Journal",
        "上游新闻": "Upstream News",
        "科创板日报": "Sci-Tech Board Daily",
        "经济观察报": "Economic Observer",
        "蓝鲸财经": "Blue Whale Finance",
        "智通财经": "Zhitong Finance",
        "格隆汇": "Gelonghui",
        "搜狐财经": "Sohu Finance",
        "网易财经": "NetEase Finance",
        "腾讯新闻": "Tencent News",
        "新华网": "Xinhua News",
        "人民网": "People's Daily",
        "央广网": "CNR",
        "央视新闻": "CCTV News",
        "光明网": "Guangming Daily",
        "中国青年报": "China Youth Daily",
        "环球时报": "Global Times",
        "参考消息": "Reference News",
    }

    @classmethod
    def _map_source(cls, raw: str) -> str:
        """Translate a Chinese source name to English, with partial matching."""
        if not raw:
            return "EastMoney"
        # Exact match
        if raw in cls._SOURCE_MAP:
            return cls._SOURCE_MAP[raw]
        # Partial match (e.g. "中国证券报·中证网" contains "中国证券报")
        for zh, en in cls._SOURCE_MAP.items():
            if zh in raw:
                return en
        # Fallback: any remaining CJK text → generic label
        if any("\u4e00" <= c <= "\u9fff" for c in raw):
            return "Other"
        return raw

    def _scrape_akshare(self) -> list[ScrapedArticle]:
        import akshare as ak

        articles: list[ScrapedArticle] = []

        try:
            df = ak.stock_news_em(symbol=self.ticker)
            for _, row in df.iterrows():
                title = str(row.get("新闻标题", ""))
                body = str(row.get("新闻内容", ""))
                pub_date = str(row.get("发布时间", "Unknown"))
                source_raw = str(row.get("文章来源", ""))
                source = self._map_source(source_raw)
                url = str(row.get("新闻链接", ""))

                articles.append(ScrapedArticle(
                    title=title,
                    url=url,
                    publish_date=pub_date,
                    source=source,
                    body_text=body if body and body != "nan" else title,
                ))
        except Exception as exc:
            logger.error("[%s] Failed to fetch news via akshare: %s", self.name, exc)

        logger.info("[%s] Total items scraped via akshare: %d", self.name, len(articles))
        return articles
