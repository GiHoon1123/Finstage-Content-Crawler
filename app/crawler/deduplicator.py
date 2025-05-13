# app/crawler/deduplicator.py

import hashlib
from urllib.parse import urlparse
from sqlalchemy import select
from app.models.content import Content
from app.models.content_url import ContentUrl

# ✅ 미래에 사용할 수 있는 허용 도메인 목록 (현재는 무시)
ALLOWED_DOMAINS = [
    "news.google.com",
    "www.reuters.com",
    "www.cnbc.com",
    "edition.cnn.com",
    "finance.yahoo.com",
]

# ❌ 다운로드하지 않을 확장자 목록
BLOCKED_EXTENSIONS = [".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".zip", ".exe"]

def is_valid_url(url: str) -> bool:
    """
    유효한 URL인지 검사
    - 현재는 모든 도메인 허용 (단, 확장자만 필터링)
    """
    parsed = urlparse(url)

    if parsed.scheme not in {"http", "https"}:
        return False

    if any(url.lower().endswith(ext) for ext in BLOCKED_EXTENSIONS):
        return False

    # 향후 도메인 제한용 (사용 안함)
    # if parsed.netloc not in ALLOWED_DOMAINS:
    #     return False

    return True

def get_content_hash(content: str) -> str:
    """
    콘텐츠 문자열을 SHA-256 해시로 변환
    """
    return hashlib.sha256(content.encode("utf-8")).hexdigest()

def is_duplicate_hash(session, content_hash: str) -> bool:
    """
    동일한 콘텐츠 해시가 DB에 존재하는지 확인
    """
    result = session.execute(
        select(Content).where(Content.content_hash == content_hash)
    ).scalar_one_or_none()
    return result is not None

def is_duplicate_url(session, url: str) -> bool:
    """
    동일한 URL이 이미 저장돼 있는지 확인
    """
    result = session.execute(
        select(ContentUrl).where(ContentUrl.url == url)
    ).scalar_one_or_none()
    return result is not None

def filter_and_deduplicate(session, urls: list[str]) -> list[str]:
    """
    URL 리스트 중:
    - 유효한 URL인지 검사
    - 이미 저장된 URL이면 제외
    """
    filtered = []
    for url in urls:
        if is_valid_url(url) and not is_duplicate_url(session, url):
            filtered.append(url)
    return filtered
