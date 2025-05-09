# app/crawler/bfs_url_extractor.py

from bs4 import BeautifulSoup
import aiohttp
from urllib.parse import urljoin, urlparse
from app.repository.content_url_repository import ContentUrlRepository

MAX_DEPTH = 2  # 탐색 최대 깊이
MAX_URLS_PER_SYMBOL = 10  # 심볼당 최대 URL 수집 수

async def fetch_html(session, url):
    try:
        async with session.get(url, timeout=5) as response:
            return await response.text()
    except Exception as e:
        print(f"❌ 실패: {url} - {e}")
        return None

async def extract_links(html, base_url):
    soup = BeautifulSoup(html, "lxml")
    links = set()
    for tag in soup.find_all("a", href=True):
        href = tag['href']
        absolute_url = urljoin(base_url, href)
        parsed = urlparse(absolute_url)
        if parsed.scheme.startswith("http"):
            links.add(absolute_url)
    return links

async def bfs_extract_urls(symbol: str) -> list[str]:
    print(f"[🔍 START] {symbol} BFS URL 탐색 시작")

    visited = set()
    result = set()

    try:
        existing_urls = ContentUrlRepository.get_existing_urls_for_symbol(symbol)
        print(f"[🧾 DB 조회] {symbol} 기존 URL 수: {len(existing_urls)}")
    except Exception as e:
        print(f"[❌ DB 에러] {symbol} - {e}")
        return []

    queue = [f"https://news.google.com/search?q={symbol}"]
    async with aiohttp.ClientSession() as session:
        depth = 0
        while queue and depth < MAX_DEPTH and len(result) < MAX_URLS_PER_SYMBOL:
            print(f"[🔁 깊이 {depth}] Queue 크기: {len(queue)}")

            next_queue = []
            for url in queue:
                if url in visited:
                    continue
                visited.add(url)

                print(f"[🌐 요청] {url}")
                html = await fetch_html(session, url)
                if not html:
                    continue

                links = await extract_links(html, url)
                print(f"[🔗 추출된 링크 수] {len(links)}")

                for link in links:
                    if "news" in link and link not in visited:
                        if link in existing_urls:
                            print(f"[⚠️ 중복 URL 스킵] {link}")
                            continue

                        result.add(link)
                        next_queue.append(link)

                        print(f"[✅ URL 수집됨] {link}")
                        if len(result) >= MAX_URLS_PER_SYMBOL:
                            break
            queue = next_queue
            depth += 1

    print(f"[🏁 완료] {symbol} → 최종 URL 수: {len(result)}")
    return list(result)