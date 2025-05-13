from queue import Empty
import threading
import time
import hashlib
from queue import Queue
from collections import deque
import traceback

from app.crawler.downloader import download_and_process


class ContentWorker(threading.Thread):
    """
    URL 큐에서 메시지를 꺼내 HTML 수집 후 DB에 저장하는 작업 스레드
    병렬로 작동하며 큐가 비면 대기함
    """

    def __init__(self, url_queue, db_session_factory, use_threadsafe_queue=False):
        """
        :param url_queue: 처리할 URL 큐 (Queue 또는 deque)
        :param db_session_factory: SQLAlchemy 세션 팩토리
        :param use_threadsafe_queue: thread-safe 큐 여부
        """
        super().__init__(daemon=True)
        self.url_queue = url_queue
        self.db_session_factory = db_session_factory
        self.use_threadsafe_queue = use_threadsafe_queue
        self.running = True

    def stop(self):
        """외부에서 스레드를 안전하게 종료하기 위한 메서드"""
        self.running = False

    def run(self):
        print(f"🧵 [Worker-{self.name}] 시작됨.")
        while self.running:
            try:
                if self.use_threadsafe_queue:
                    try:
                        message = self.url_queue.get(timeout=1)
                    except Empty:
                        continue
                else:
                    if not self.url_queue:
                        time.sleep(1)
                        continue
                    message = self.url_queue.popleft()

                self._process(message)

                if self.use_threadsafe_queue:
                    self.url_queue.task_done()

            except Exception as e:
                print(f"[❌ 처리 중 예외] {e}")
                import traceback
                traceback.print_exc()
                time.sleep(1)

    def _process(self, message: dict):
        """
        HTML 수집 및 DB 저장 처리

        :param message: {
            "symbol": "AAPL",
            "title": "...",
            "summary": "...",
            "url": "..."
        }
        """
        symbol = message.get("symbol")
        title = message.get("title")
        summary = message.get("summary")
        url = message.get("url")

        # 중복 체크를 위한 해시 생성
        raw = f"{url}"
        content_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()

        # HTML 다운로드 및 처리
        download_and_process(symbol, url, title, summary, content_hash)
