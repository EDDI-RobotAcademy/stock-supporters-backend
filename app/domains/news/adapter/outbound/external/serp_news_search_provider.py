import httpx

from app.domains.news.application.port.news_search_provider import (
    NewsSearchProvider,
    NewsSearchResult,
)
from app.domains.news.domain.entity.news_article import NewsArticle


class SerpNewsSearchProvider(NewsSearchProvider):
    """SerpAPI Google News 검색 어댑터"""

    BASE_URL = "https://serpapi.com/search"

    def __init__(self, api_key: str):
        self._api_key = api_key

    async def search(
        self, keyword: str, page: int, page_size: int
    ) -> NewsSearchResult:
        start = (page - 1) * page_size

        params = {
            "engine": "google_news",
            "q": keyword,
            "gl": "kr",
            "hl": "ko",
            "start": start,
            "num": page_size,
            "api_key": self._api_key,
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(self.BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

        news_results = data.get("news_results", [])

        articles = [
            NewsArticle(
                title=item.get("title", ""),
                snippet=item.get("snippet", ""),
                source=item.get("source", {}).get("name", "")
                if isinstance(item.get("source"), dict)
                else item.get("source", ""),
                published_at=item.get("date", ""),
                link=item.get("link"),
            )
            for item in news_results
        ]

        total_count = data.get("search_information", {}).get(
            "total_results", len(articles)
        )

        return NewsSearchResult(articles=articles, total_count=total_count)
