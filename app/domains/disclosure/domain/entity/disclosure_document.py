from datetime import datetime
from typing import Any, Optional


class DisclosureDocument:
    def __init__(
        self,
        rcept_no: str,
        document_type: str,
        raw_text: Optional[str] = None,
        parsed_json: Optional[dict[str, Any]] = None,
        summary_text: Optional[str] = None,
        stored_in_rag: bool = False,
        document_id: Optional[int] = None,
        collected_at: Optional[datetime] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ):
        self.document_id = document_id
        self.rcept_no = rcept_no
        self.document_type = document_type
        self.raw_text = raw_text
        self.parsed_json = parsed_json
        self.summary_text = summary_text
        self.stored_in_rag = stored_in_rag
        self.collected_at = collected_at or datetime.now()
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
