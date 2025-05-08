# app/kafka/consumer.py

from kafka import KafkaConsumer
import json
import threading

from app.priority.symbol_priority_buffer import SymbolPriorityBuffer  # ✅ 이름 변경된 우선순위 버퍼
from app.queue.symbol_priority_dispatcher import SymbolPriorityDispatcher
from app.queue.url_priority_dispatcher import UrlQueueDispatcher
from app.queue.url_priority_queue import UrlPriorityQueueManager
from app.worker.worker_pool import WorkerPool
from app.queue.symbol_priority_queue import SymbolPriorityQueueManager



# Kafka 설정
KAFKA_TOPIC = "symbol.crawl.priority"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "finstage-crawler-group"


class KafkaSymbolConsumer(threading.Thread):
    """
    Kafka로부터 심볼 데이터를 실시간으로 소비하는 클래스
    - 메시지 형식: {"symbol": "TSLA", "score": 91, "priority": "top"}
    - 메시지 수신 시 SymbolPriorityBuffer로 전달
    """

    def __init__(self, priority_buffer: SymbolPriorityBuffer):
        super().__init__(daemon=True)
        self.priority_buffer = priority_buffer

        # KafkaConsumer 객체 생성
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
                data = message.value  # Kafka 메시지에서 value 추출
                symbol = data.get("symbol")
                score = data.get("score")
                priority = data.get("priority")

                # 필수 필드 검증
                if not symbol or not isinstance(score, int) or priority not in ["top", "mid", "bottom"]:
                    print(f"[⚠️ 무시됨] 잘못된 메시지: {data}")
                    continue

                print(f"📥 Kafka 수신: {priority.upper()} - {symbol} (score: {score})")

                # ✅ 우선순위 버퍼에 전달 (조건 만족 시 자동으로 큐로 넘어감)
                self.priority_buffer.add(priority, symbol, score)

            except Exception as e:
                print(f"[❌ Kafka 메시지 처리 실패] {e}")


def start_consumer():
    """
    크롤링 시스템 전체 초기화 및 시작 진입점
    1. 우선순위 큐들 생성
    2. 워커풀 및 디스패처들 실행
    3. Kafka 소비자 실행
    """
    print("🚀 크롤링 서비스 초기화 시작...")

    # 1. URL 우선순위 큐
    url_queue = UrlPriorityQueueManager()

    # 2. 워커 풀 (최대 동시 스레드 수: 10)
    worker_pool = WorkerPool(max_workers=10)

    # 3. URL 큐 디스패처: URL 작업을 워커에게 전달
    url_dispatcher = UrlQueueDispatcher(url_queue, worker_pool)
    url_dispatcher.start()

    # 4. 심볼 우선순위 큐
    symbol_queue = SymbolPriorityQueueManager()

    # ✅ 5. 심볼 버퍼 생성 (조건 만족 시 symbol_queue._push 실행)
    def push_to_symbol_queue(priority: str, score: int, data: dict):
        """
        SymbolPriorityBuffer 내부 조건이 만족되면 호출되는 콜백
        - symbol_queue의 우선순위 큐에 데이터 삽입
        """
        symbol_queue._push(priority, score, data)

    priority_buffer = SymbolPriorityBuffer(dispatcher_callback=push_to_symbol_queue)

    # 6. 심볼 → URL 추출 디스패처 실행
    symbol_dispatcher = SymbolPriorityDispatcher(priority_buffer, url_queue)
    symbol_dispatcher.start()

    # 7. Kafka 컨슈머 시작 (→ symbol_priority_buffer로 수신 데이터 전달)
    consumer = KafkaSymbolConsumer(priority_buffer)
    consumer.start()

    print("✅ 모든 컴포넌트 초기화 완료. Kafka 소비 시작됨.")

    # 메인 스레드는 Kafka 소비자와 함께 대기
    consumer.join()

    # 종료 시 디스패처도 함께 중단
    symbol_dispatcher.stop()
    url_dispatcher.stop()


# python consumer.py로 직접 실행하는 경우
if __name__ == "__main__":
    start_consumer()
