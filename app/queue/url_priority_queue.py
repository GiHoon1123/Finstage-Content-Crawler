import heapq
import uuid  # 🔥 dict끼리 비교 방지용 UUID 사용


class UrlPriorityQueueManager:
    """
    URL 기반 우선순위 큐 클래스
    - 심볼로부터 추출된 URL을 저장
    - top / mid / bottom 세 단계로 나뉜 우선순위 큐 관리
    """

    def __init__(self):
        self.queues = {
            'top': [],
            'mid': [],
            'bottom': []
        }

    def put(self, priority: str, url_task: dict):
        """
        우선순위 큐에 URL 작업 추가
        - heapq는 튜플의 앞 요소부터 정렬
        - (우선순위 정수, UUID, 데이터) 형식으로 넣어 dict 비교 방지
        """
        heapq.heappush(
            self.queues[priority],
            (0, uuid.uuid4(), url_task)  # UUID 추가로 dict끼리 비교 못하게 막음
        )

    def get(self, priority: str):
        """
        우선순위 큐에서 하나 꺼냄
        - 없는 경우 None 반환
        """
        if self.queues[priority]:
            _, _, task = heapq.heappop(self.queues[priority])
            return task
        return None

    def size(self, priority: str):
        return len(self.queues[priority])

    def has_items(self):
        """
        모든 우선순위 큐 중 하나라도 비어있지 않으면 True
        """
        return any(self.queues[level] for level in ['top', 'mid', 'bottom'])
