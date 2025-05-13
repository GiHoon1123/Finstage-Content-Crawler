from app.worker.content_worker import ContentWorker
from queue import Queue
from collections import deque

class ContentWorkerPool:
    """
    콘텐츠 워커 스레드 풀
    - URL 우선순위 큐별로 지정된 수의 워커를 병렬 실행
    - 각 워커는 큐에서 URL 메시지를 가져와 HTML 수집 및 DB 저장 수행
    """

    def __init__(self, db_session_factory, url_queue_top, url_queue_mid, url_queue_bot, use_threadsafe_queue=False):
        """
        :param db_session_factory: SQLAlchemy 세션 팩토리
        :param url_queue_top: 높은 우선순위 URL 큐
        :param url_queue_mid: 중간 우선순위 URL 큐
        :param url_queue_bot: 낮은 우선순위 URL 큐
        :param use_threadsafe_queue: thread-safe 큐(Queue) 사용 여부
        """
        self.db_session_factory = db_session_factory
        self.use_threadsafe_queue = use_threadsafe_queue

        if use_threadsafe_queue:
            assert isinstance(url_queue_top, Queue)
            assert isinstance(url_queue_mid, Queue)
            assert isinstance(url_queue_bot, Queue)
        else:
            assert isinstance(url_queue_top, deque)
            assert isinstance(url_queue_mid, deque)
            assert isinstance(url_queue_bot, deque)

        # 스레드 할당 비율: TOP=5, MID=3, BOT=2
        self.worker_configs = [
            (url_queue_top, 5),
            (url_queue_mid, 3),
            (url_queue_bot, 2),
        ]

        self.workers = []

    def start(self):
        """
        모든 워커 스레드를 생성 및 시작
        """
        print("🚀 [WorkerPool] 콘텐츠 워커 스레드 실행 시작")

        for queue, count in self.worker_configs:
            for _ in range(count):
                worker = ContentWorker(queue, self.db_session_factory, self.use_threadsafe_queue)
                worker.start()
                self.workers.append(worker)

        print(f"✅ 총 {len(self.workers)}개 워커 스레드 실행 완료")

    def stop(self):
        """
        모든 워커 스레드 종료 요청
        """
        print("🛑 [WorkerPool] 종료 요청")
        for worker in self.workers:
            worker.stop()
