# uvicorn app.main:app --host 0.0.0.0 --port 8082 -- reload
from fastapi import FastAPI
from app.models.base import Base
from app.database.connection import engine
from sqlalchemy.orm import sessionmaker
from app.routes import content_router  
from app.kafka.kafka_simple_consumer import KafkaSimpleConsumer
from app.ranking.symbol_priority_classifier import SymbolPriorityClassifier
from app.queue.symbol_to_url_router import SymbolToUrlQueueRouter
from app.worker.content_worker_pool import ContentWorkerPool

from queue import Queue  # ✅ 교체 포인트: thread-safe queue 사용
import threading

app = FastAPI(
    title="Finstage Content Crawler",
    version="1.0.0",
    description="우선순위 기반 기업 뉴스 콘텐츠 크롤링 서버"
)

app.include_router(content_router.router)

# ✅ 테이블 자동 생성
Base.metadata.create_all(bind=engine)

# ✅ 큐 구성 (thread-safe)
symbol_queue_top = Queue()
symbol_queue_mid = Queue()
symbol_queue_bot = Queue()

url_queue_top = Queue()
url_queue_mid = Queue()
url_queue_bot = Queue()

# ✅ 심볼 분류기 (점수 기반으로 우선순위 큐에 배정)
symbol_classifier = SymbolPriorityClassifier(
    queue_top=symbol_queue_top,
    queue_mid=symbol_queue_mid,
    queue_bot=symbol_queue_bot,
    use_threadsafe_queue=True
)

# ✅ URL 라우터 (심볼 큐 → bfs → URL 큐)
symbol_router = SymbolToUrlQueueRouter(
    symbol_queue_top=symbol_queue_top,
    symbol_queue_mid=symbol_queue_mid,
    symbol_queue_bot=symbol_queue_bot,
    url_queue_top=url_queue_top,
    url_queue_mid=url_queue_mid,
    url_queue_bot=url_queue_bot,
    use_threadsafe_queue=True
)

# ✅ 워커 풀 (URL 큐 → HTML 수집 → DB 저장)
SessionFactory = sessionmaker(bind=engine)
content_worker_pool = ContentWorkerPool(
    db_session_factory=SessionFactory,
    url_queue_top=url_queue_top,
    url_queue_mid=url_queue_mid,
    url_queue_bot=url_queue_bot,
    use_threadsafe_queue=True
)

@app.on_event("startup")
def startup_event():
    print("크롤링 시스템 초기화 시작...")

    # 1. 전면 큐 라우터 실행
    threading.Thread(target=symbol_router.start, daemon=True).start()

    # 2. 워커 풀 실행
    content_worker_pool.start()

    # 3. Kafka Consumer 실행 (classifier만 넘김)
    consumer = KafkaSimpleConsumer(symbol_classifier)
    consumer.start()

    print("✅ Kafka consumer + 라우터 + 워커 실행 완료")


@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok", "message": "Finstage Crawler is running"}

