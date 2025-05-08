# app/queue/url_priority_dispatcher.py

import time
from threading import Thread
from app.queue.url_priority_queue import UrlPriorityQueueManager
from app.worker.worker_pool import WorkerPool


class UrlQueueDispatcher(Thread):
    """
    URL 큐 디스패처
    - 우선순위 큐에서 URL 작업을 꺼내 워커 스레드에게 할당
    - top → mid → bottom 순으로 우선순위 처리
    - 일정 주기로 동작 (기본 1초)
    """

    def __init__(self, url_queue: UrlPriorityQueueManager, worker_pool: WorkerPool, interval: float = 1.0):
        super().__init__(daemon=True)
        self.url_queue = url_queue
        self.worker_pool = worker_pool
        self.interval = interval
        self.running = True

    def run(self):
        while self.running:
            for level in ['top', 'mid', 'bottom']:
                task = self.url_queue.get(level)
                if task:
                    self.worker_pool.assign(task)
            time.sleep(self.interval)

    def stop(self):
        self.running = False
