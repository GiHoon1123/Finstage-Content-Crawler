from sqlalchemy.orm import Session
from sqlalchemy import select, func
from app.models.content import Content
from app.schemas.content_response import ContentResponse, PaginatedContentResponse
from app.database.connection import SessionLocal


def get_contents_paginated(page: int, size: int) -> PaginatedContentResponse:
    session = SessionLocal()
    offset = (page - 1) * size

    total = session.scalar(select(func.count()).select_from(Content))

    stmt = select(Content).order_by(Content.crawled_at.desc()).offset(offset).limit(size)
    contents = session.execute(stmt).scalars().all()

    items = [
        ContentResponse(
            id=content.id,
            symbol=content.symbol,
            summary=content.summary,
            url=content.url,
            title=content.title,
            date=content.crawled_at,  # 매핑
        )
        for content in contents
    ]

    return PaginatedContentResponse(
        total=total,
        page=page,
        size=size,
        total_pages=(total // size + (1 if total % size else 0)),
        has_next=page < (total // size + (1 if total % size else 0)),
        has_prev=page > 1,
        items=items
    )


def get_contents_by_symbol(symbol: str, page: int, size: int) -> PaginatedContentResponse:
    session = SessionLocal()

    offset = (page - 1) * size
    total = session.scalar(
        select(func.count()).select_from(Content).where(Content.symbol == symbol)
    )

    stmt = (
        select(Content)
        .where(Content.symbol == symbol)
        .order_by(Content.crawled_at.desc())
        .offset(offset)
        .limit(size)
    )
    contents = session.execute(stmt).scalars().all()

    items = [
        ContentResponse(
            id=content.id,
            symbol=content.symbol,
            summary=content.summary,
            url=content.url,
            title=content.title,
            date=content.crawled_at
        )
        for content in contents
    ]

    return PaginatedContentResponse(
        total=total,
        page=page,
        size=size,
        total_pages=(total // size + (1 if total % size else 0)),
        has_next=page < (total // size + (1 if total % size else 0)),
        has_prev=page > 1,
        items=items
    )
