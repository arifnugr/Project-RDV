#!/usr/bin/env python3
# stream_consumer_thread.py - Thread-based implementation to avoid event loop issues

import json
import os
import sys
import time
import threading
import queue
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from transformer import Transformer
from loader import Loader

# Konfigurasi logging yang lebih jelas
def log(message):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)
    sys.stdout.flush()

# Log startup message
log("CONSUMER STARTING - THREAD BASED IMPLEMENTATION")

# Load environment variables
load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
USE_MONGODB = os.getenv("USE_MONGODB", "true").lower() == "true"
bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

log(f"Configuration: bootstrap_servers={bootstrap_servers}, topic={KAFKA_TOPIC}, mongodb={USE_MONGODB}")

# Create queues for inter-thread communication
raw_queue = queue.Queue(maxsize=100)
transformed_queue = queue.Queue(maxsize=100)

# Initialize components
log("Initializing transformer and loader")
transformer = Transformer()
loader = Loader(use_mongodb=USE_MONGODB)

# Callback for successful inserts
def log_success(ts):
    log(f"Inserted data at {ts}")

# Thread function for Kafka consumer
def kafka_consumer_thread():
    log("Starting Kafka consumer thread")
    
    # Retry logic for Kafka connection
    max_retries = 10
    retry_delay = 5  # seconds
    consumer = None
    
    for attempt in range(max_retries):
        try:
            log(f"Attempting to connect to Kafka at {bootstrap_servers}, topic: {KAFKA_TOPIC} (attempt {attempt+1}/{max_retries})")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='market_data_consumer_group'
            )
            log(f"Successfully connected to Kafka")
            break
        except NoBrokersAvailable:
            log(f"No brokers available, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
        except Exception as e:
            log(f"Error connecting to Kafka: {str(e)}")
            time.sleep(retry_delay)
    
    if consumer is None:
        log("Failed to initialize Kafka consumer after multiple attempts")
        return
    
    # Consume messages
    try:
        log("Starting to consume messages from Kafka")
        for message in consumer:
            try:
                log(f"Received message from Kafka: {message.topic}, partition: {message.partition}, offset: {message.offset}")
                raw_queue.put(message.value)
            except Exception as e:
                log(f"Error processing message: {str(e)}")
    except Exception as e:
        log(f"Error in Kafka consumer: {str(e)}")
    finally:
        if consumer:
            consumer.close()
            log("Kafka consumer closed")

# Thread function for transformer
def transformer_thread():
    log("Starting transformer thread")
    while True:
        try:
            # Get data from raw queue
            raw_data = raw_queue.get(timeout=1.0)
            
            # Transform data
            log("Transforming data")
            transformed_data = transformer.transform(raw_data)
            
            # Put transformed data into transformed queue
            transformed_queue.put(transformed_data)
            
            # Mark task as done
            raw_queue.task_done()
        except queue.Empty:
            # No data available, just continue
            continue
        except Exception as e:
            log(f"Error in transformer: {str(e)}")
            import traceback
            traceback.print_exc()
            # Mark task as done even if there was an error
            if not raw_queue.empty():
                raw_queue.task_done()

# Thread function for loader
def loader_thread():
    log("Starting loader thread")
    while True:
        try:
            # Get data from transformed queue
            transformed_data = transformed_queue.get(timeout=1.0)
            
            # Load data
            log("Loading data to database")
            loader.insert(transformed_data)  # Menggunakan insert() bukan load()
            
            # Call success callback
            log_success(transformed_data.get("timestamp", "unknown"))
            
            # Mark task as done
            transformed_queue.task_done()
        except queue.Empty:
            # No data available, just continue
            continue
        except Exception as e:
            log(f"Error in loader: {str(e)}")
            import traceback
            traceback.print_exc()
            # Mark task as done even if there was an error
            if not transformed_queue.empty():
                transformed_queue.task_done()

def main():
    try:
        log("Starting main function")
        # Create and start threads
        threads = [
            threading.Thread(target=kafka_consumer_thread, daemon=True),
            threading.Thread(target=transformer_thread, daemon=True),
            threading.Thread(target=loader_thread, daemon=True)
        ]
        
        for thread in threads:
            thread.start()
            log(f"Started thread {thread.name}")
        
        # Keep main thread alive with periodic heartbeat
        while True:
            log("Consumer heartbeat - still running")
            time.sleep(60)  # Log heartbeat every minute
            
    except KeyboardInterrupt:
        log("Stopped by user")
    except Exception as e:
        log(f"Error in main: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        loader.close()
        log("Shutdown complete")

if __name__ == "__main__":
    log("Starting consumer application")
    main()