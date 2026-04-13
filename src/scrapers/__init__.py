"""
Web scraping package, providing interfaces for abstract scraper hierarchy, HTML/XML parsing, and value objects.
"""

from .base import BaseScraper, ScrapedArticle, ScrapedRow
from .fundamentals import FundamentalsScraper
from .news import NewsScraper

__all__ = [
    "BaseScraper", 
    "FundamentalsScraper", 
    "NewsScraper", 
    "ScrapedArticle", 
    "ScrapedRow"
    ]