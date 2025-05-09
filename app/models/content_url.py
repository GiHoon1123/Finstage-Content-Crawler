# app/models/content_url.py

from sqlalchemy import Column, Integer, String, DateTime


from app.models.base import Base


class ContentUrl(Base):
    __tablename__ = "content_urls"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(255), nullable=False)
    url = Column(String(1024), nullable=False)
    source = Column(String(50), nullable=True, comment="수집 출처")

