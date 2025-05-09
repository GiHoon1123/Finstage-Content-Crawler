from kafka import KafkaConsumer
import json
import threading

from app.priority.symbol_priority_buffer import SymbolPriorityBuffer
from app.queue.symbol_priority_dispatcher import SymbolPriorityDispatcher
from app.queue.url_priority_dispatcher import UrlQueueDispatcher
from app.queue.url_priority_queue import UrlPriorityQueueManager
from app.worker.worker_pool import WorkerPool
from app.queue.symbol_priority_queue import SymbolPriorityQueueManager


KAFKA_TOPIC = "symbol.crawl.priority"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "finstage-crawler-group"


class KafkaSymbolConsumer(threading.Thread):
    def __init__(self, priority_buffer: SymbolPriorityBuffer):
        super().__init__(daemon=True)
        self.priority_buffer = priority_buffer

        self.consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True
        )

    def run(self):
        print("✅ Kafka 심볼 소비자 시작됨.")
        for message in self.consumer:
            try:
                print(f"[📦 원본 메시지] {message.value}")

                data = message.value
                symbol = data.get("symbol")
                score = data.get("score")
                priority = data.get("priority")

                print(f"[🔍 파싱 결과] symbol={symbol}, score={score}, priority={priority}")

                if not symbol or not isinstance(score, int) or priority not in ["top", "mid", "bottom"]:
                    print(f"[⚠️ 무시됨] 잘못된 메시지: {data}")
                    continue

                print(f"📥 Kafka 수신: {priority.upper()} - {symbol} (score: {score})")

                # 👉 버퍼에 추가
                self.priority_buffer.add(priority, symbol, score)
                print(f"[➡️ 버퍼에 전달] priority={priority}, symbol={symbol}, score={score}")

            except Exception as e:
                print(f"[❌ Kafka 메시지 처리 실패] {e}")


def start_consumer():
    print("🚀 크롤링 서비스 초기화 시작...")

    url_queue = UrlPriorityQueueManager()
    worker_pool = WorkerPool(max_workers=10)
    url_dispatcher = UrlQueueDispatcher(url_queue, worker_pool)
    url_dispatcher.start()

    symbol_queue = SymbolPriorityQueueManager()

    def push_to_symbol_queue(priority: str, score: int, data: dict):
        print(f"[💡 dispatcher_callback 호출됨] priority={priority}, score={score}, data={data}")
        symbol_queue._push(priority, score, data)

    priority_buffer = SymbolPriorityBuffer(dispatcher_callback=push_to_symbol_queue)
    symbol_dispatcher = SymbolPriorityDispatcher(priority_buffer, url_queue)
    symbol_dispatcher.start()

    consumer = KafkaSymbolConsumer(priority_buffer)
    consumer.start()

    print("✅ 모든 컴포넌트 초기화 완료. Kafka 소비 시작됨.")
    consumer.join()

    symbol_dispatcher.stop()
    url_dispatcher.stop()


if __name__ == "__main__":
    start_consumer()
