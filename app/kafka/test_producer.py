# app/kafka/test_producer.py

from kafka import KafkaProducer
import json
import time
import random

KAFKA_TOPIC = "symbol.crawl.priority"
BOOTSTRAP_SERVERS = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# 테스트용 심볼 및 우선순위 목록
symbols = ["AAPL", "TSLA", "MSFT", "GOOGL", "NVDA"]
priorities = ["top", "mid", "bottom"]

for _ in range(10):
    symbol = random.choice(symbols)
    score = random.randint(1, 100)
    priority = random.choice(priorities)

    message = {
        "symbol": symbol,
        "score": score,
        "priority": priority
    }

    producer.send(KAFKA_TOPIC, value=message)
    print(f"📤 보냄: {message}")
    time.sleep(1)  # 1초 간격으로 발행

producer.flush()
producer.close()
print("✅ 테스트 메시지 전송 완료")
