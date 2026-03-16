from pydantic import BaseModel


class NewsArticleResponse(BaseModel):
    title: str
    snippet: str
    source: str
    published_at: str
    link: str | None = None


class SearchNewsResponse(BaseModel):
    articles: list[NewsArticleResponse]
    total_count: int
    page: int
    page_size: int
