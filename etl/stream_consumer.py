import asyncio
import json
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv
from transformer import Transformer
from loader import Loader

load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def log_success(ts):
    print(f"[Consumer] Inserted data at {ts}")

raw_queue = asyncio.Queue()
transformed_queue = asyncio.Queue()

transformer = Transformer()
loader = Loader()

async def receive_messages():
    # Consumer Kafka sinkron, dibungkus async
    loop = asyncio.get_event_loop()
    def blocking_consume():
        for message in consumer:
            # Masukkan ke async queue secara thread-safe
            asyncio.run_coroutine_threadsafe(raw_queue.put(message.value), loop)
    await loop.run_in_executor(None, blocking_consume)

async def run_transformer():
    # Consume raw_queue, output ke transformed_queue
    await transformer.transform_loop(raw_queue, transformed_queue)

async def run_loader():
    # Consume transformed_queue dan insert ke DB
    await loader.load_loop(transformed_queue, on_success=log_success)

async def main():
    await asyncio.gather(
        receive_messages(),
        run_transformer(),
        run_loader(),
    )

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        loader.close()
        print("\n[Consumer] Stopped by user.")
