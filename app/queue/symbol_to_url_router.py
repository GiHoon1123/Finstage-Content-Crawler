import time
from queue import Queue
from collections import deque
from app.crawler.bfs_url_extractor import bfs_extract_urls
from app.ranking.symbol_priority_classifier import SymbolMessage

# URL 큐 하나당 최대 크기
MAX_URL_QUEUE_SIZE = 30


class SymbolToUrlQueueRouter:
    """
    전면 큐 라우터
    - 심볼 우선순위 큐에서 하나씩 꺼내 BFS를 통해 URL 목록 수집
    - 수집한 URL들을 우선순위에 맞는 URL 큐에 삽입
    - URL 큐가 가득 차면 대기 (심볼은 다시 큐에 보관하지 않음, 단순 대기)
    """

    def __init__(
        self,
        symbol_queue_top,
        symbol_queue_mid,
        symbol_queue_bot,
        url_queue_top,
        url_queue_mid,
        url_queue_bot,
        use_threadsafe_queue=False
    ):
        """
        전면 큐 라우터 초기화

        :param symbol_queue_*: 심볼 우선순위 큐
        :param url_queue_*: URL 우선순위 큐
        :param use_threadsafe_queue: Queue 사용 여부
        """
        self.symbol_queue_top = symbol_queue_top
        self.symbol_queue_mid = symbol_queue_mid
        self.symbol_queue_bot = symbol_queue_bot

        self.url_queue_top = url_queue_top
        self.url_queue_mid = url_queue_mid
        self.url_queue_bot = url_queue_bot

        self.use_threadsafe_queue = use_threadsafe_queue

        self.queue_map = {
            "TOP": (self.symbol_queue_top, self.url_queue_top),
            "MID": (self.symbol_queue_mid, self.url_queue_mid),
            "BOT": (self.symbol_queue_bot, self.url_queue_bot),
        }

    def start(self):
        """
        전면 큐 라우터 실행 메서드 (무한 루프)
        """
        print("🚀 [전면 큐 라우터] 시작됨.")

        while True:
            processed = False

            for priority in ["TOP", "MID", "BOT"]:
                if self._process(priority):
                    processed = True
                    break

            if not processed:
                # print("[⏳ 대기] 모든 심볼 큐가 비어 있음")
                time.sleep(1)

    def _process(self, priority: str) -> bool:
        """
        지정된 우선순위 큐에서 심볼을 꺼내 URL 수집 후 URL 큐에 삽입

        :param priority: "TOP", "MID", "BOT"
        :return: 처리되었으면 True
        """
        symbol_queue, url_queue = self.queue_map[priority]

        try:
            if self.use_threadsafe_queue:
                if symbol_queue.empty():
                    return False

                if url_queue.qsize() >= MAX_URL_QUEUE_SIZE:
                    print(f"[⛔ 큐 FULL] {priority} URL 큐가 가득 참 → 대기")
                    time.sleep(1)
                    return True

                symbol_msg: SymbolMessage = symbol_queue.get_nowait()

            else:
                if not symbol_queue:
                    return False

                if len(url_queue) >= MAX_URL_QUEUE_SIZE:
                    print(f"[⛔ 큐 FULL] {priority} URL 큐가 가득 참 → 대기")
                    time.sleep(1)
                    return True

                symbol_msg: SymbolMessage = symbol_queue.popleft()

        except Exception as e:
            print(f"[❌ 큐 처리 실패] {priority} - {e}")
            return False

        symbol = symbol_msg.symbol
        print(f"[🔍 심볼 처리] {priority} 큐 → {symbol}")

        try:
            url_list = bfs_extract_urls(symbol)
            print(f"[✅ URL 수집 완료] {symbol} → {len(url_list)}개")
        except Exception as e:
            print(f"[❌ URL 수집 실패] {symbol} - {e}")
            return True

        for url in url_list:
            try:
                if self.use_threadsafe_queue:
                    if url_queue.qsize() >= MAX_URL_QUEUE_SIZE:
                        print(f"[⚠️ 중단] {priority} URL 큐 가득 참 → 일부 URL 누락")
                        break
                    url_queue.put_nowait(url)
                else:
                    if len(url_queue) >= MAX_URL_QUEUE_SIZE:
                        print(f"[⚠️ 중단] {priority} URL 큐 가득 참 → 일부 URL 누락")
                        break
                    url_queue.append(url)

                print(f"[📥 삽입 → {priority}] {url['url']}")
            except Exception as e:
                print(f"[❌ URL 삽입 실패] {url['url']} - {e}")

        return True
