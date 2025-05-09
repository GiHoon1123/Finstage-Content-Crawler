import heapq
from collections import defaultdict
from datetime import datetime, timedelta
import itertools


class SymbolPriorityQueueManager:
    def __init__(self):
        self.queues = {
            1: [],
            2: [],
            3: []
        }
        self.priority_mapping = {
            'top': 1,
            'mid': 2,
            'bottom': 3
        }
        self.counter = itertools.count()

    def _push(self, priority, score, item):
        if isinstance(priority, str):
            priority = self.priority_mapping.get(priority.lower())
            if priority is None:
                raise ValueError(f"Invalid priority string: {priority}")

        count = next(self.counter)
        heapq.heappush(self.queues[priority], (-score, count, item))

    def pop(self, priority):
        if isinstance(priority, str):
            priority = self.priority_mapping.get(priority.lower())
        return heapq.heappop(self.queues[priority])[2] if self.queues[priority] else None

    def is_empty(self, priority):
        if isinstance(priority, str):
            priority = self.priority_mapping.get(priority.lower())
        return not self.queues[priority]
