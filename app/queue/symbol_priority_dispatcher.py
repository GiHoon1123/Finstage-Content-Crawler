import time
import asyncio
from app.priority.symbol_priority_buffer import SymbolPriorityBuffer
from app.queue.url_priority_queue import UrlPriorityQueueManager
from app.crawler.bfs_url_extractor import bfs_extract_urls


class SymbolPriorityDispatcher:
    """
    심볼 우선순위 버퍼에서 조건을 만족한 심볼을 꺼내 URL을 추출하고 큐에 전달
    - 별도 스레드 없이 동기 루프 형태로 주기적으로 실행
    """

    def __init__(
        self,
        priority_buffer: SymbolPriorityBuffer,
        url_queue: UrlPriorityQueueManager,
        interval: float = 1.0
    ):
        self.priority_buffer = priority_buffer
        self.url_queue = url_queue
        self.interval = interval
        self.running = True

    def start(self):
        print("🚀 SymbolPriorityDispatcher 시작됨.")
        while self.running:
            symbol_groups = self.priority_buffer.check_and_flush()

            if symbol_groups:
                print(f"🚀 우선순위 그룹 도출 완료: {symbol_groups}")

                for priority, symbols in symbol_groups.items():
                    for symbol in symbols:
                        try:
                            # asyncio.run() → 반복 호출은 위험 → 하나의 이벤트 루프 유지
                            urls = asyncio.run(bfs_extract_urls(symbol))
                            for url in urls:
                                self.url_queue.put(priority, {"symbol": symbol, "url": url})
                                print(f"✅ URL 추가됨: ({priority}) {symbol} - {url}")
                        except Exception as e:
                            print(f"[❌ URL 추출 실패] {symbol} - {e}")

            time.sleep(self.interval)

    def stop(self):
        self.running = False
