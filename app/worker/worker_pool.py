from threading import Thread
from typing import List
import asyncio
from app.crawler.downloader import download_and_process


class Worker(Thread):
    """
    개별 작업 스레드 클래스
    - 주어진 URL에 대해 HTML을 다운로드하고 처리합니다.
    """

    def __init__(self, task_data: dict):
        super().__init__(daemon=True)
        self.task_data = task_data

    def run(self):
        symbol = self.task_data.get("symbol")
        url = self.task_data.get("url")

        if symbol and url:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(download_and_process(symbol, url))
            except Exception as e:
                print(f"❌ 다운로드 처리 중 예외 발생: {e}")
            finally:
                loop.close()
        else:
            print(f"[❌ 잘못된 작업 데이터] {self.task_data}")


class WorkerPool:
    """
    작업 스레드 풀
    - 외부에서 assign(task_data) 호출 시 새로운 스레드를 생성해 작업 처리
    """

    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.active_workers: List[Thread] = []

    def assign(self, task_data: dict):
        self.cleanup_finished_threads()

        if len(self.active_workers) < self.max_workers:
            worker = Worker(task_data)
            worker.start()
            self.active_workers.append(worker)
        else:
            print("⚠️ 최대 스레드 수 초과로 작업 대기 중...")

    def cleanup_finished_threads(self):
        self.active_workers = [t for t in self.active_workers if t.is_alive()]
