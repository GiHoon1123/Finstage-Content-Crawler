from collections import defaultdict
from datetime import datetime, timedelta

class SymbolPriorityBuffer:
    """
    심볼 + 우선순위 조합으로 메시지를 잠시 보관했다가
    - 개수(threshold) 이상
    - 시간(timeout) 초 경과
    조건을 만족하면 flush하여 dispatcher_callback을 호출한다.
    """

    def __init__(self, dispatcher_callback, threshold=2, timeout=10):
        self.buffer = defaultdict(list)
        self.threshold = threshold
        self.timeout = timeout
        self.dispatcher_callback = dispatcher_callback  # 큐로 push하는 콜백 함수

    def add(self, priority: str, symbol: str, score: int):
        """
        버퍼에 새 메시지를 추가
        """
        now = datetime.now()
        self.buffer[priority].append((symbol, score, now))

    def check_and_flush(self) -> dict:
        """
        조건을 만족한 심볼 그룹을 찾아 flush하고 큐로 push
        반환 형태: { "top": ["TSLA", "NVDA"], "mid": [...], ... }
        """
        now = datetime.now()
        flushed = {}

        for priority, items in list(self.buffer.items()):
            if not items:
                continue  # ✅ 빈 항목 방지 (IndexError 대비)

            # 조건: 개수 ≥ threshold 또는 시간 초과
            if len(items) >= self.threshold or (now - items[0][2]) >= timedelta(seconds=self.timeout):
                flushed[priority] = [sym for sym, _, _ in items]

                # dispatcher_callback 호출
                for sym, score, _ in items:
                    self.dispatcher_callback(priority, score, {'symbol': sym, 'score': score})

                self.buffer[priority].clear()

        return flushed
