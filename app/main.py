from fastapi import FastAPI
from app.database.connection import engine
from app.models.base import Base
from app.kafka.consumer import KafkaSymbolConsumer
from app.queue.url_priority_queue import UrlPriorityQueueManager
from app.worker.worker_pool import WorkerPool
from app.queue.url_priority_dispatcher import UrlQueueDispatcher
from app.queue.symbol_priority_queue import SymbolPriorityQueueManager
from app.priority.symbol_priority_buffer import SymbolPriorityBuffer
from app.queue.symbol_priority_dispatcher import SymbolPriorityDispatcher
import threading
import time

app = FastAPI(
    title="Finstage Content Crawler",
    version="1.0.0",
    description="우선순위 기반 기업 뉴스 콘텐츠 크롤링 서버"
)

# ✅ 서버 실행 시 테이블 자동 생성
Base.metadata.create_all(bind=engine)

# 전역 큐 및 버퍼 객체
url_queue = UrlPriorityQueueManager()
worker_pool = WorkerPool(max_workers=10)
symbol_queue = SymbolPriorityQueueManager()


def push_to_symbol_queue(priority: str, score: int, data: dict):
    symbol_queue._push(priority, score, data)

symbol_buffer = SymbolPriorityBuffer(dispatcher_callback=push_to_symbol_queue)


def run_symbol_dispatcher():
    """
    SymbolPriorityDispatcher를 별도 스레드 없이 주기적으로 실행
    """
    dispatcher = SymbolPriorityDispatcher(symbol_buffer, url_queue)
    while True:
        dispatcher.start()
        time.sleep(1)


@app.on_event("startup")
def startup_event():
    print("🚀 크롤링 시스템 초기화 중...")

    # URL 큐 디스패처 시작
    url_dispatcher = UrlQueueDispatcher(url_queue, worker_pool)
    url_dispatcher.start()

    # Symbol → URL 디스패처 실행 (별도 스레드로 flush 루프만 돌림)
    threading.Thread(target=run_symbol_dispatcher, daemon=True).start()

    # Kafka Consumer 시작
    consumer = KafkaSymbolConsumer(symbol_buffer)
    consumer.start()

    print("✅ 백그라운드 Kafka consumer 및 dispatcher 구동 완료.")
