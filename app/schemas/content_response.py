# app/schemas/company_response.py (또는 symbol_response.py)

from datetime import datetime
from pydantic import BaseModel, ConfigDict
from typing import List



class ContentResponse(BaseModel):
    id: int
    symbol: str
    summary: str
    url: str
    title: str
    date: datetime
    model_config = ConfigDict(from_attributes=True)


class PaginatedContentResponse(BaseModel):
    total: int
    page: int
    size: int
    total_pages: int
    has_next: bool
    has_prev: bool
    items: List[ContentResponse]
