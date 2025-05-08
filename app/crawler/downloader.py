import asyncio
import aiohttp
from bs4 import BeautifulSoup
from app.database.connection import engine
from app.crawler.deduplicator import is_duplicate_hash
from app.models.content import Content
from hashlib import sha256
from sqlalchemy.orm import sessionmaker
from urllib.parse import urlparse, parse_qs

Session = sessionmaker(bind=engine)


async def resolve_google_news_url(url: str) -> str | None:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    # news.google.com/read? → q 파라미터가 원래 URL
    if "q" in query:
        return query["q"][0]

    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, allow_redirects=True) as resp:
                return str(resp.url)
    except Exception as e:
        print(f"❌ 리디렉션 URL 추출 실패: {url} → {e}")
        return None


def get_domain(url: str) -> str:
    try:
        return url.split("/")[2]
    except Exception:
        return ""


async def download_and_process(symbol: str, google_news_url: str):
    real_url = await resolve_google_news_url(google_news_url)
    if not real_url:
        print(f"❌ HTML 다운로드 실패: {google_news_url}")
        return

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                real_url,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    )
                },
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                html = await response.text()
    except Exception as e:
        print(f"❌ HTML 다운로드 실패: {real_url} → {e}")
        return

    if not html.strip():
        print(f"❌ HTML 비어있음: {real_url}")
        return

    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("title")
    title = title_tag.text.strip() if title_tag else "제목 없음"
    summary = None
    content_hash = sha256(title.encode("utf-8")).hexdigest()

    if is_duplicate_hash(content_hash):
        print(f"⚠️ 중복 콘텐츠: {title}")
        return

    session = Session()
    try:
        content = Content(
            symbol=symbol,
            title=title,
            summary=summary,
            url=real_url,
            source=get_domain(real_url),
            content_hash=content_hash,
            is_duplicate=False,
        )
        session.add(content)
        session.commit()
        print(f"✅ DB 저장 완료: {title}")
    except Exception as e:
        session.rollback()
        print(f"❌ DB 저장 실패: {title} → {e}")
    finally:
        session.close()
