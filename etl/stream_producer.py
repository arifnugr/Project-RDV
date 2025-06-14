# stream_producer.py

import asyncio
import json
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
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
    # Gunakan data dummy jika API Binance bermasalah
    use_dummy = os.environ.get('USE_DUMMY_DATA', 'false').lower() == 'true'
    extractor = BinanceExtractor(symbol='BTC/USDT', use_dummy_data=use_dummy)
    
    if use_dummy:
        print("[Kafka Producer] Using DUMMY DATA mode")
    
    # Retry logic untuk koneksi Kafka
    max_retries = 5
    retry_delay = 5  # detik
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(**get_kafka_config())
            print(f"[Kafka Producer] Connected to {os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
            break
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f"[Kafka Producer] No brokers available, retrying in {retry_delay} seconds... (attempt {attempt+1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                raise
    
    try:
        async def send_to_kafka():
            while True:
                try:
                    data = await queue.get()
                    topic = os.environ.get('KAFKA_TOPIC', 'stream-data')
                    producer.send(topic, value=data)
                    on_success_log(data["timestamp"])
                    queue.task_done()
                except Exception as e:
                    print(f"[Kafka Producer Send Error] {e}")
                    await asyncio.sleep(1)  # Pause briefly on error

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