from pydantic import BaseModel


class CleanupResponse(BaseModel):
    deleted_disclosures: int
    deleted_jobs: int
    deleted_orphaned_chunks: int
    message: str
