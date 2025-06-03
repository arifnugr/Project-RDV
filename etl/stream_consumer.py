import asyncio
import json
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv
from transformer import Transformer
from loader import Loader

load_dotenv()

def get_kafka_config():
    return {
        'bootstrap_servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
        'auto_offset_reset': 'latest',
        'group_id': 'market-data-consumer'
    }

def log_success(ts):
    print(f"[Consumer] Inserted data at {ts}")

async def main():
    raw_queue = asyncio.Queue()
    transformed_queue = asyncio.Queue()

    transformer = Transformer()
    loader = Loader()
    
    try:
        print(f"[Kafka Consumer] Connecting to {os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
        consumer = KafkaConsumer(
            os.environ.get('KAFKA_TOPIC', 'stream-data'),
            **get_kafka_config()
        )
        print(f"[Kafka Consumer] Connected and subscribed to {os.environ.get('KAFKA_TOPIC', 'stream-data')}")
        
        # Consumer Kafka sinkron, dibungkus async
        loop = asyncio.get_event_loop()
        
        async def receive_messages():
            def blocking_consume():
                for message in consumer:
                    # Masukkan ke async queue secara thread-safe
                    asyncio.run_coroutine_threadsafe(raw_queue.put(message.value), loop)
                    print(f"[Kafka Consumer] Received message with timestamp: {message.value.get('timestamp', 'unknown')}")
            await loop.run_in_executor(None, blocking_consume)

        async def run_transformer():
            # Consume raw_queue, output ke transformed_queue
            while True:
                raw_data = await raw_queue.get()
                transformed = transformer.transform(raw_data)
                await transformed_queue.put(transformed)
                print(f"[Transformer] Processed data with timestamp: {transformed.get('timestamp', 'unknown')}")
                raw_queue.task_done()

        async def run_loader():
            # Consume transformed_queue dan insert ke DB
            while True:
                data = await transformed_queue.get()
                loader.insert(data)
                log_success(data.get("timestamp", "unknown"))
                transformed_queue.task_done()

        await asyncio.gather(
            receive_messages(),
            run_transformer(),
            run_loader(),
        )
    except Exception as e:
        print(f"[Kafka Consumer Error] {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
        loader.close()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Consumer] Stopped by user.")