from fastapi import APIRouter, Query, Path
from app.services.content_service import get_contents_paginated, get_contents_by_symbol
from app.schemas.content_response import PaginatedContentResponse

router = APIRouter()

@router.get("/contents", response_model=PaginatedContentResponse, summary="콘텐츠 목록 조회 (페이지네이션)", tags=["Content"])
def get_contents(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
):
    return get_contents_paginated(page, size)


@router.get("/contents/{symbol}", response_model=PaginatedContentResponse, summary="심볼 기반 콘텐츠 조회", tags=["Content"])
def get_contents_by_symbol_route(
    symbol: str = Path(..., description="조회할 심볼 예: AAPL"),
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
):
    return get_contents_by_symbol(symbol, page, size)
