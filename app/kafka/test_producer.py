# test_producer.py

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_TOPIC = "symbol.crawl.priority"
BOOTSTRAP_SERVERS = "localhost:9092"

# 테스트용 심볼 목록
# SYMBOLS = ["AAPL", "TSLA", "GOOG", "AMZN", "MSFT", "NVDA", "NFLX", "META"]
SYMBOLS = ["AAPL"]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def generate_test_message():
    symbol = random.choice(SYMBOLS)
    score = random.randint(30, 100)  # 30 ~ 100 사이 점수
    created_at = datetime.utcnow().isoformat()

    return {
        "symbol": symbol,
        "score": score,
        "createdAt": created_at
    }

if __name__ == "__main__":
    print("🚀 테스트 메시지 전송 시작")

    for _ in range(1):  # 메시지 10개 전송
        msg = generate_test_message()
        producer.send(KAFKA_TOPIC, value=msg)
        print(f"[📤 전송됨] {msg}")
        time.sleep(1)  # 1초 간격으로 전송

    producer.flush()
    print("✅ 전송 완료")
