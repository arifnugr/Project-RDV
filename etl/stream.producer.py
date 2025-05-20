# stream_producer.py

import asyncio
import json
from kafka import KafkaProducer
from extractor import BinanceExtractor

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Fungsi callback (opsional)
def on_success_log(timestamp):
    print(f"[Kafka Producer] Sent data at {timestamp}")

async def main():
    queue = asyncio.Queue()
    extractor = BinanceExtractor(symbol='BTC/USDT')

    async def send_to_kafka():
        while True:
            data = await queue.get()
            producer.send('stream-data', value=data)
            on_success_log(data["timestamp"])
            queue.task_done()

    await asyncio.gather(
        extractor.extract_loop(queue, interval=1, on_success=on_success_log),
        send_to_kafka()
    )

if __name__ == '__main__':
    asyncio.run(main())
