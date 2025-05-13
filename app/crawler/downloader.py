from app.database.connection import engine
from app.crawler.deduplicator import is_duplicate_hash
from app.models.content import Content
from app.models.content_url import ContentUrl
from hashlib import sha256
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select


DUMMY_HTML = "<html>Blocked by Google policy</html>"
Session = sessionmaker(bind=engine)

def get_domain(url: str) -> str:
    try:
        return url.split("/")[2]
    except Exception:
        return ""

def is_duplicate_url(db_session, url: str) -> bool:
    result = db_session.execute(
        select(ContentUrl).where(ContentUrl.url == url)
    ).scalar_one_or_none()
    return result is not None

def download_and_process(symbol: str, news_url: str, title, summary, content_hash):
    print(f"🔥 download_and_process 호출됨 → {symbol}, {news_url}")
    html = DUMMY_HTML
    session = Session()
    try:
        if is_duplicate_url(session, news_url):
            print(f"⚠️ 이미 저장된 URL: {news_url}")
            return

        if is_duplicate_hash(session, content_hash):
            print(f"⚠️ 중복 콘텐츠 해시: {title}")
            return

        # ✅ DB 저장
        content = Content(
            symbol=symbol,
            title=title,
            summary=summary,
            url=news_url,
            html=html,
            source=get_domain(news_url),
            content_hash=content_hash,
            is_duplicate=False,
            
        )
        session.add(content)

        content_url = ContentUrl(
            url=news_url,
            symbol=symbol,
            source="google"
        )
        session.add(content_url)

        session.commit()
        print(f"✅ 저장 완료: {title}")

    except Exception as e:
        session.rollback()
        print(f"❌ 저장 실패: {news_url} → {e}")
    finally:
        session.close()


