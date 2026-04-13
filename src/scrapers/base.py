"""
Base scraper module, implementing the Abstract Base Class (ABC), session management, HTTP request retry, and User-Agent rotation logic.
"""
from __future__ import annotations

import abc
import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) "
    "Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 "
    "Edg/124.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

_DEFAULT_HEADERS: dict[str, str] = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

@dataclass(frozen=True)
class ScrapedArticle:
    """Immutable value object representing a single scraped article."""

    title: str
    url: str
    publish_date: str
    source: str
    body_text: str

    @property
    def word_count(self) -> int:
        return len(self.body_text.split())


@dataclass(frozen=True)
class ScrapedRow:
    """Immutable value object for a single structured financial row."""

    label: str
    value: float
    period: str
    source: str

class BaseScraper(abc.ABC):
    """Abstract base scraper with session management, UA rotation, and rate limiting.

    Subclasses must implement ``target_urls`` and ``parse``. The public
    ``scrape()`` method orchestrates the full fetch → parse → validate cycle.

    Parameters
    ----------
    min_delay : float
        Minimum seconds between requests (default ``1.5``).
    max_delay : float
        Maximum seconds between requests (default ``3.5``).
    timeout : int
        HTTP timeout in seconds (default ``15``).
    max_retries : int
        Retry attempts per request (default ``3``).
    """

    def __init__(
        self,
        min_delay: float = 1.5,
        max_delay: float = 3.5,
        timeout: int = 15,
        max_retries: int = 3,
    ) -> None:
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.timeout = timeout
        self.max_retries = max_retries
        self._session = requests.Session()

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Human-readable scraper identifier (e.g. ``'FundamentalsScraper'``)."""

    @property
    @abc.abstractmethod
    def target_urls(self) -> list[str]:
        """List of URLs this scraper targets."""

    def _build_headers(self) -> dict[str, str]:
        """Return request headers with a randomly selected User-Agent."""
        headers = _DEFAULT_HEADERS.copy()
        headers["User-Agent"] = random.choice(_USER_AGENTS)
        return headers

    def _rate_limit(self) -> None:
        """Sleep for a random duration in ``[min_delay, max_delay]``."""
        delay = random.uniform(self.min_delay, self.max_delay)
        logger.debug("[%s] Rate-limiting: %.2fs", self.name, delay)
        time.sleep(delay)

    def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse HTML from *url* with retry logic.

        Returns
        -------
        BeautifulSoup | None
            Parsed HTML, or ``None`` if all retries exhausted.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._session.get(
                    url,
                    headers=self._build_headers(),
                    timeout=self.timeout,
                )
                response.raise_for_status()
                logger.debug("[%s] Fetched %s (status %d)", self.name, url, response.status_code)
                return BeautifulSoup(response.text, "html.parser")

            except requests.exceptions.HTTPError as exc:
                status = getattr(exc.response, "status_code", "???")
                logger.warning(
                    "[%s] HTTP %s on attempt %d/%d for %s",
                    self.name, status, attempt, self.max_retries, url,
                )
                if attempt < self.max_retries:
                    self._rate_limit()

            except requests.exceptions.Timeout:
                logger.warning(
                    "[%s] Timeout on attempt %d/%d for %s",
                    self.name, attempt, self.max_retries, url,
                )
                if attempt < self.max_retries:
                    self._rate_limit()

            except requests.exceptions.RequestException as exc:
                logger.error("[%s] Request failed for %s — %s", self.name, url, exc)
                if attempt < self.max_retries:
                    self._rate_limit()

        logger.error("[%s] All %d retries exhausted for %s", self.name, self.max_retries, url)
        return None

    def close(self) -> None:
        """Close the underlying ``requests.Session``."""
        self._session.close()
        logger.debug("[%s] Session closed.", self.name)

    def __enter__(self) -> "BaseScraper":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    @abc.abstractmethod
    def parse(self, soup: BeautifulSoup, url: str) -> list[ScrapedArticle | ScrapedRow]:
        """Extract structured data from parsed HTML.

        Parameters
        ----------
        soup : BeautifulSoup
            Parsed HTML document.
        url : str
            The source URL (useful for resolving relative links).

        Returns
        -------
        list[ScrapedArticle | ScrapedRow]
            Parsed data items.
        """

    def scrape(self) -> list[ScrapedArticle | ScrapedRow]:
        """Execute the full scraping cycle for all target URLs.

        Returns
        -------
        list[ScrapedArticle | ScrapedRow]
            Aggregated results from all target URLs.
        """
        results: list[ScrapedArticle | ScrapedRow] = []
        visited: set[str] = set()

        for url in self.target_urls:
            if url in visited:
                continue
            visited.add(url)

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
