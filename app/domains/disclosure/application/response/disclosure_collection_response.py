from pydantic import BaseModel


class DisclosureCollectionResponse(BaseModel):
    total_fetched: int
    filtered_count: int
    saved_count: int
    duplicate_skipped: int
    message: str
