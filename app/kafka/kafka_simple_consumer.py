# kafka_simple_consumer.py

import json
import threading
from kafka import KafkaConsumer

KAFKA_TOPIC = "symbol.crawl.priority"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "finstage-crawler-group"

class KafkaSimpleConsumer(threading.Thread):
    def __init__(self, classifier):
        super().__init__(daemon=True)
        self.classifier = classifier

    def run(self):
        print("✅ Kafka consumer 시작됨.")
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True
        )

        for message in consumer:
            data = message.value
            print(f"[📦 Kafka 수신] {data}")

            symbol = data.get("symbol")
            score = data.get("score")

            if not symbol or not isinstance(score, int):
                print(f"[⚠️ 무시됨] 잘못된 메시지: {data}")
                continue

            self.classifier.receive(symbol, score)
