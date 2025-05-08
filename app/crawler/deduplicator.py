# app/crawler/deduplicator.py

import hashlib
import re
from urllib.parse import urlparse
from app.database.connection import get_db_connection

# ✅ 미래에 사용할 수 있는 허용 도메인 목록 (현재는 무시)
ALLOWED_DOMAINS = [
    "news.google.com",
    "www.reuters.com",
    "www.cnbc.com",
    "edition.cnn.com",
    "finance.yahoo.com",  # ← 야후 파이낸스 추가
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

    # 향후 적용할 도메인 필터 (현재는 주석 처리)
    # if parsed.netloc not in ALLOWED_DOMAINS:
    #     return False

    return True

def get_content_hash(content: str) -> str:
    """
    콘텐츠 문자열을 SHA-256 해시로 변환
    """
    return hashlib.sha256(content.encode("utf-8")).hexdigest()

def is_duplicate_hash(content_hash: str) -> bool:
    """
    동일한 해시가 이미 존재하는지 확인
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM contents WHERE content_hash = %s LIMIT 1", (content_hash,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def is_duplicate_url(url: str) -> bool:
    """
    이미 동일한 URL이 저장돼 있는지 확인
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM contents WHERE url = %s LIMIT 1", (url,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def filter_and_deduplicate(urls: list[str]) -> list[str]:
    """
    URL 리스트 중:
    - 확장자 기반 유효성 검사
    - 중복 URL 제거

    콘텐츠 해시는 본문 다운로드 이후 확인하므로 여기서는 URL만 필터링
    """
    filtered = []
    for url in urls:
        if is_valid_url(url) and not is_duplicate_url(url):
            filtered.append(url)
    return filtered
