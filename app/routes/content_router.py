from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy.orm import Session
from app.database.connection import get_db
from app.models.content import Content
from sqlalchemy import select

router = APIRouter()

@router.get(
    "/contents",
    summary="콘텐츠 목록 조회 (페이지네이션)",
    tags=["Content"],
    responses={
        200: {
            "description": "성공적으로 콘텐츠를 조회했습니다.",
            "content": {
                "application/json": {
                    "example": {
                        "page": 1,
                        "size": 1,
                        "data": [
                            {
                                "id": 177,
                                "symbol": "AAPL",
                                "summary": "Comprehensive up-to-date news coverage, aggregated from sources all over the world by Google News.",
                                "html": "<html>Blocked by Google policy</html>",
                                "crawled_at": "2025-05-13T05:48:16",
                                "is_duplicate": False,
                                "url": "https://news.google.com/rss/articles/CBMif0...",
                                "title": "Google News",
                                "source": "news.google.com",
                                "content_hash": "6b9a6f1e2dcc1c772ef6bc961ff408c0e65ac094c6d3749a0ba6a592f7aecbb2"
                            }
                        ]
                    }
                }
            }
        },
        422: {
            "description": "잘못된 요청 매개변수로 인해 요청이 실패했습니다.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "page"],
                                "msg": "ensure this value is greater than or equal to 1",
                                "type": "value_error.number.not_ge",
                                "ctx": {"limit_value": 1}
                            }
                        ]
                    }
                }
            }
        }
    }
)
def get_contents(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    offset = (page - 1) * size
    stmt = select(Content).offset(offset).limit(size)
    results = db.execute(stmt).scalars().all()
    return {
        "page": page,
        "size": size,
        "data": results
    }


@router.get(
    "/contents/{symbol}",
    summary="심볼 기반 콘텐츠 조회",
    tags=["Content"],
    responses={
        200: {
            "description": "성공적으로 콘텐츠를 조회했습니다.",
            "content": {
                "application/json": {
                    "example": {
                        "page": 1,
                        "size": 2,
                        "data": [
                            {
                                "id": 101,
                                "symbol": "AAPL",
                                "title": "Apple launches new product",
                                "summary": "Apple just launched something big.",
                                "html": "<html>...</html>",
                                "crawled_at": "2025-05-13T05:48:16",
                                "is_duplicate": False,
                                "url": "https://example.com/news1",
                                "source": "example.com",
                                "content_hash": "abc123..."
                            }
                        ]
                    }
                }
            }
        },
        422: {
            "description": "잘못된 요청 매개변수로 인해 요청이 실패했습니다.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "page"],
                                "msg": "ensure this value is greater than or equal to 1",
                                "type": "value_error.number.not_ge",
                                "ctx": {"limit_value": 1}
                            }
                        ]
                    }
                }
            }
        }
    }
)
def get_contents_by_symbol(
    symbol: str = Path(..., description="조회할 심볼 예: AAPL"),
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    offset = (page - 1) * size
    stmt = (
        select(Content)
        .where(Content.symbol == symbol)
        .order_by(Content.crawled_at.desc())
        .offset(offset)
        .limit(size)
    )
    results = db.execute(stmt).scalars().all()
    return {
        "page": page,
        "size": size,
        "data": results
    }
