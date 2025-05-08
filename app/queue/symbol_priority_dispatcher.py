import time
import asyncio
from threading import Thread
from app.priority.symbol_priority_buffer import SymbolPriorityBuffer
from app.queue.url_priority_queue import UrlPriorityQueueManager
from app.crawler.bfs_url_extractor import bfs_extract_urls


class SymbolPriorityDispatcher(Thread):
    """
    SymbolPriorityBuffer에 모인 심볼 데이터를 바탕으로
    BFS 방식으로 URL을 수집하고 URL 우선순위 큐에 분배하는 디스패처.
    - 일정 주기마다 버퍼 상태를 확인
    - 조건을 만족한 심볼 그룹에 대해 URL을 추출
    - 각 URL을 URL 우선순위 큐에 삽입
    """

    def __init__(
        self,
        priority_buffer: SymbolPriorityBuffer,
        url_queue: UrlPriorityQueueManager,
        interval: float = 1.0
    ):
        super().__init__(daemon=True)
        self.priority_buffer = priority_buffer
        self.url_queue = url_queue
        self.interval = interval
        self.running = True

    def run(self):
        """
        백그라운드에서 실행되며 주기적으로 SymbolPriorityBuffer 상태를 체크함.
        조건 만족한 심볼 그룹이 있다면 BFS 방식으로 URL을 수집하고
        각 URL을 URL 우선순위 큐에 삽입한다.
        """
        while self.running:
            # 조건을 만족한 심볼 그룹 추출 (예: 2개 이상 or 10초 경과)
            symbol_groups = self.priority_buffer.check_and_flush()
            if symbol_groups:
                print(f"🚀 우선순위 그룹 도출 완료: {symbol_groups}")

                for priority, symbols in symbol_groups.items():
                    for symbol in symbols:
                        try:
                            # BFS 방식으로 뉴스 URL 추출
                            urls = asyncio.run(bfs_extract_urls(symbol))
                            for url in urls:
                                self.url_queue.put(priority, {"symbol": symbol, "url": url})
                                print(f"✅ URL 추가됨: ({priority}) {symbol} - {url}")
                        except Exception as e:
                            print(f"[❌ URL 추출 실패] {symbol} - {e}")

            time.sleep(self.interval)

    def stop(self):
        """
        외부에서 호출 시 디스패처 루프를 중단함
        """
        self.running = False
