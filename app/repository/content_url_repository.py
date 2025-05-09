# app/repository/content_url_repository.py
from sqlalchemy.orm import Session
from sqlalchemy import select
from app.database.connection import SessionLocal
from app.models.content_url import ContentUrl

class ContentUrlRepository:

    @staticmethod
    def get_existing_urls_for_symbol(symbol: str) -> set[str]:
        session: Session = SessionLocal()
        try:
            result = session.execute(
                select(ContentUrl.url).where(ContentUrl.symbol == symbol)
            )
            urls = [row[0] for row in result.all()]
            return set(urls)
        except Exception as e:
            print(f"[❌ DB 조회 실패] {symbol} - {e}")
            return set()
        finally:
            session.close()
