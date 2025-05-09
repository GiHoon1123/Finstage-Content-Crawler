from collections import defaultdict
from datetime import datetime, timedelta

class SymbolPriorityBuffer:
    """
    심볼 + 우선순위 조합으로 메시지를 잠시 보관했다가
    - 개수(threshold) 이상
    - 시간(timeout) 초 경과
    조건을 만족하면 flush하여 dispatcher_callback을 호출한다.
    """

    VALID_PRIORITIES = {"top", "mid", "bottom"}

    def __init__(self, dispatcher_callback, threshold=2, timeout=10):
        self.buffer = defaultdict(list)
        self.threshold = threshold
        self.timeout = timeout
        self.dispatcher_callback = dispatcher_callback  # 큐로 push하는 콜백 함수

    def add(self, priority: str, symbol: str, score: int):
        """
        버퍼에 새 메시지를 추가
        """
        if priority not in self.VALID_PRIORITIES:
            print(f"[❌ 잘못된 priority] {priority} → 무시됨")
            return

        now = datetime.now()
        self.buffer[priority].append((symbol, score, now))

    def check_and_flush(self) -> dict:
        now = datetime.now()
        flushed = {}

        for priority, items in list(self.buffer.items()):
            if not items:
                continue

            if len(items) >= self.threshold or (now - items[0][2]) >= timedelta(seconds=self.timeout):
                flushed[priority] = [sym for sym, _, _ in items]

                for sym, score, _ in items:
                    print(f"[🚨 dispatcher_callback 호출 직전] priority={priority}, symbol={sym}, score={score}")
                    self.dispatcher_callback(priority, score, {'symbol': sym, 'score': score})

                self.buffer[priority].clear()

        return flushed
