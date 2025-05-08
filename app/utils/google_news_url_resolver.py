import aiohttp
from urllib.parse import urlparse, parse_qs

async def resolve_google_news_url(url: str) -> str | None:
    """
    구글 뉴스 링크를 받아 실제 기사 URL로 리디렉션된 주소를 반환
    """
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    
    # 1차 시도: news.google.com/read? → q 파라미터에 원래 URL이 있는 경우
    if "q" in query:
        return query["q"][0]
    
    # 2차 시도: HEAD 요청으로 리디렉션 따라가기
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, allow_redirects=True) as resp:
                return str(resp.url)
    except Exception as e:
        print(f"[resolve_google_news_url] Error resolving URL: {e}")
        return None
