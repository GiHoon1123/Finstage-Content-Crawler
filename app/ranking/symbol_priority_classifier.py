import time
from queue import Queue
from collections import deque
from dataclasses import dataclass
from typing import Optional

# 각 큐에 들어갈 수 있는 최대 메시지 수
MAX_QUEUE_SIZE = 10


@dataclass
class SymbolMessage:
    symbol: str
    score: int
    received_at: float


class SymbolPriorityClassifier:
    def __init__(self, queue_top, queue_mid, queue_bot, use_threadsafe_queue: bool = False):
        self.queue_top = queue_top
        self.queue_mid = queue_mid
        self.queue_bot = queue_bot
        self.use_threadsafe_queue = use_threadsafe_queue

        self.buffer: list[SymbolMessage] = []
        self.first_received_at: Optional[float] = None

    def receive(self, symbol: str, score: int):
        now = time.time()
        message = SymbolMessage(symbol, score, now)
        self.buffer.append(message)

        if not self.first_received_at:
            self.first_received_at = now

        self.try_flush()

    def try_flush(self):
        now = time.time()

        if len(self.buffer) >= 1 and (now - self.first_received_at >= 1):
            self._flush_batch()
            self.first_received_at = None
        elif 0 < len(self.buffer) < 3:
            self._flush_individual()

    def _flush_batch(self):
        print("[⚙️ BATCH 처리] 조건 충족 → 큐 분배 시도")

        batch = self.buffer[:3]
        self.buffer = self.buffer[3:]

        sorted_batch = sorted(batch, key=lambda m: m.score, reverse=True)
        top_msg = sorted_batch[0]
        mid_msg = sorted_batch[2]
        bot_msg = sorted_batch[1]

        success_top = self._enqueue(self.queue_top, top_msg, "TOP")
        success_mid = self._enqueue(self.queue_mid, bot_msg, "MID")
        success_bot = self._enqueue(self.queue_bot, mid_msg, "BOT")

        for success, msg in zip([success_top, success_mid, success_bot], [top_msg, bot_msg, mid_msg]):
            if not success:
                self.buffer.insert(0, msg)

    def _flush_individual(self):
        print("[⚙️ 개별 처리] 큐 여유 공간에 메시지 삽입 시도")

        rebuffer = []
        for msg in self.buffer:
            if self._enqueue(self.queue_top, msg, "TOP"):
                continue
            if self._enqueue(self.queue_mid, msg, "MID"):
                continue
            if self._enqueue(self.queue_bot, msg, "BOT"):
                continue
            rebuffer.append(msg)

        self.buffer = rebuffer
        self.first_received_at = time.time() if self.buffer else None

    def _enqueue(self, queue, message: SymbolMessage, label: str) -> bool:
        if self.use_threadsafe_queue:
            if queue.qsize() < MAX_QUEUE_SIZE:
                try:
                    queue.put_nowait(message)
                    print(f"[📥 삽입 → {label}] {message.symbol} (score={message.score})")
                    return True
                except Exception as e:
                    print(f"[❌ 삽입 실패 → {label}] {message.symbol} - {e}")
                    return False
            else:
                print(f"[⛔ 큐 FULL → {label}] {message.symbol} (score={message.score})")
                return False
        else:
            if len(queue) < MAX_QUEUE_SIZE:
                queue.append(message)
                print(f"[📥 삽입 → {label}] {message.symbol} (score={message.score})")
                return True
            else:
                print(f"[⛔ 큐 FULL → {label}] {message.symbol} (score={message.score})")
                return False
