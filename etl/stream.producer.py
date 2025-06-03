# stream_producer.py

import asyncio
import json
import os
from kafka import KafkaProducer
from extractor import BinanceExtractor

def get_kafka_config():
    return {
        'bootstrap_servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'value_serializer': lambda v: json.dumps(v).encode('utf-8')
    }

# Fungsi callback (opsional)
def on_success_log(timestamp):
    print(f"[Kafka Producer] Sent data at {timestamp}")

async def main():
    queue = asyncio.Queue()
    extractor = BinanceExtractor(symbol='BTC/USDT')
    
    try:
        producer = KafkaProducer(**get_kafka_config())
        print(f"[Kafka Producer] Connected to {os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
        
        async def send_to_kafka():
            while True:
                data = await queue.get()
                topic = os.environ.get('KAFKA_TOPIC', 'stream-data')
                producer.send(topic, value=data)
                on_success_log(data["timestamp"])
                queue.task_done()

        await asyncio.gather(
            extractor.extract_loop(queue, interval=1, on_success=on_success_log),
            send_to_kafka()
        )
    except Exception as e:
        print(f"[Kafka Producer Error] {e}")
    finally:
        if 'producer' in locals():
            producer.close()
        await extractor.close()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Kafka Producer] Stopped by user.")