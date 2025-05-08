# app/main.py

from fastapi import FastAPI
from app.database.connection import engine
from app.models.content import Base

from app.kafka.consumer import KafkaSymbolConsumer
from app.queue.url_priority_queue import UrlPriorityQueueManager
from app.worker.worker_pool import WorkerPool
from app.queue.url_priority_dispatcher import UrlQueueDispatcher
from app.queue.symbol_priority_dispatcher import SymbolPriorityDispatcher
from app.queue.symbol_priority_queue import SymbolPriorityQueueManager
from app.priority.symbol_priority_buffer import SymbolPriorityBuffer

app = FastAPI(
    title="Finstage Content Crawler",
    version="1.0.0",
    description="우선순위 기반 기업 뉴스 콘텐츠 크롤링 서버"
)

# ✅ 서버 실행 시 테이블 자동 생성
Base.metadata.create_all(bind=engine)


@app.on_event("startup")
def startup_event():
    """
    FastAPI 앱 시작 시 Kafka Consumer 및 Dispatcher들을 백그라운드에서 실행
    """

    print("🚀 크롤링 시스템 초기화 중...")

    # 1. URL 우선순위 큐
    url_queue = UrlPriorityQueueManager()

    # 2. 워커 풀 생성
    worker_pool = WorkerPool(max_workers=10)

    # 3. URL 큐 디스패처 시작
    url_dispatcher = UrlQueueDispatcher(url_queue, worker_pool)
    url_dispatcher.start()

    # 4. 심볼 우선순위 큐 및 버퍼 생성
    symbol_queue = SymbolPriorityQueueManager()

    def push_to_symbol_queue(priority: str, score: int, data: dict):
        symbol_queue._push(priority, score, data)

    symbol_buffer = SymbolPriorityBuffer(dispatcher_callback=push_to_symbol_queue)

    # 5. 심볼 디스패처 시작
    symbol_dispatcher = SymbolPriorityDispatcher(symbol_buffer, url_queue)
    symbol_dispatcher.start()

    # 6. Kafka Consumer 시작
    consumer = KafkaSymbolConsumer(symbol_buffer)
    consumer.start()

    print("✅ 백그라운드 Kafka consumer 및 dispatcher 구동 완료.")
