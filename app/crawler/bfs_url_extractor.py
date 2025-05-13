from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
from app.repository.content_url_repository import ContentUrlRepository

MAX_DEPTH = 2
MAX_URLS_PER_SYMBOL = 3

def fetch_html(url):
    try:
        response = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code == 200:
            return response.text
        else:
            print(f"❌ 실패: {url} - 상태코드 {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ 예외: {url} - {e}")
        return None

def extract_links(html, base_url):
    soup = BeautifulSoup(html, "lxml")
    links = set()
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        absolute_url = urljoin(base_url, href)
        parsed = urlparse(absolute_url)
        if parsed.scheme.startswith("http"):
            links.add(absolute_url)
    return links

def extract_title_summary(symbol, html, url):
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.string.strip() if soup.title and soup.title.string else "제목 없음"
    meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
    summary = meta['content'].strip() if meta and meta.get('content') else "요약 없음"
    return {
        "symbol": symbol,
        "title": title,
        "summary": summary,
        "url": url
    }

def get_initial_links_from_rss(symbol: str) -> list[str]:
    """
    RSS에서 뉴스 URL만 추출하여 초기 큐로 사용
    """
    rss_url = f"https://news.google.com/rss/search?q={symbol}"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:
        response = requests.get(rss_url, headers=headers)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        channel = root.find("channel")
        items = channel.findall("item")
        return [item.find("link").text for item in items]
    except Exception as e:
        print(f"[❌ RSS 파싱 실패] {e}")
        return []

def bfs_extract_urls(symbol: str) -> list[dict]:
    print(f"[🔍 START] {symbol} BFS URL 탐색 시작")

    visited = set()
    results = []

    try:
        existing_urls = ContentUrlRepository.get_existing_urls_for_symbol(symbol)
        print(f"[🧾 DB 조회] {symbol} 기존 URL 수: {len(existing_urls)}")
    except Exception as e:
        print(f"[❌ DB 에러] {symbol} - {e}")
        existing_urls = set()

    # ✅ 초기 큐는 RSS에서 추출된 URL들
    rss_links = get_initial_links_from_rss(symbol)
    queue = [(url, 0) for url in rss_links]

    while queue and len(results) < MAX_URLS_PER_SYMBOL:
        url, depth = queue.pop(0)
        if url in visited or depth > MAX_DEPTH:
            continue
        visited.add(url)

        print(f"[🌐 요청] 깊이 {depth} → {url}")
        html = fetch_html(url)
        if not html:
            continue

        try:
            data = extract_title_summary(symbol, html, url)
            if url not in existing_urls:
                results.append(data)
                print(f"[✅ 수집됨] {data['title']} - {url}")
                if len(results) >= MAX_URLS_PER_SYMBOL:
                    break
        except Exception as e:
            print(f"[⚠️ 요약 추출 실패] {url} - {e}")

        # 링크 추출 (BFS 탐색 확장)
        links = extract_links(html, url)
        print(f"[🔗 추출된 링크 수] {len(links)}")
        for link in links:
            if "news" in link and link not in visited and link not in existing_urls:
                queue.append((link, depth + 1))

    print(f"[🏁 완료] {symbol} → 최종 URL 수: {len(results)}")
    return results


if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) < 2:
        print("❌ 사용법: python -m app.crawler.bfs_url_extractor <심볼>")
        sys.exit(1)

    symbol = sys.argv[1]
    results = bfs_extract_urls(symbol)

    print("\n📦 최종 수집 결과:")
    for i, item in enumerate(results, 1):
        print(f"[{i}] {item['title']}")
        print(f"    🔗 URL: {item['url']}")
        print(f"    📝 요약: {item['summary']}\n")

    # JSON 전체 보기 원하면 아래도 가능
    # print(json.dumps(results, indent=2, ensure_ascii=False))
