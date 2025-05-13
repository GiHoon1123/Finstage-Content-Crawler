# app/routes/content_router.py

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.database.connection import get_db
from app.models.content import Content
from sqlalchemy import select

router = APIRouter(prefix="/contents", tags=["Contents"])


@router.get("/")
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


@router.get("/{content_id}")
def get_content_by_id(content_id: int, db: Session = Depends(get_db)):
    result = db.get(Content, content_id)
    if not result:
        raise HTTPException(status_code=404, detail="콘텐츠를 찾을 수 없습니다.")
    return result
