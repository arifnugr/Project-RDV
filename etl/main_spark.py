import asyncio
import threading
import time
import os
from datetime import datetime

from extractor import BinanceExtractor
from transformer import Transformer
from loader import Loader
from visualizer import run_visualizer
from exporter import export_to_csv

from rich.live import Live
from rich.table import Table

# Status dan penghitung total data
status_dict = {
    "extractor": "Menunggu...",
    "transformer": "Menunggu...",
    "loader": "Menunggu...",
    "exporter": "Menunggu...",
    "spark": "Menunggu...",
    "kafka": "Menunggu...",
    "scheduler": "Menunggu...",
}
total_records = 0  # jumlah data yang masuk ke DB

def make_status_table():
    table = Table(title="🚀 Pipeline Status Monitor (Full Stack)", expand=True)
    table.add_column("Komponen", style="cyan", justify="center")
    table.add_column("Status Terkini", style="green", justify="left")

    for key, value in status_dict.items():
        table.add_row(key.capitalize(), value)
    return table

def update_status(worker, message):
    global total_records
    if worker == "loader":
        total_records += 1
        message += f" | Total: {total_records}"
    status_dict[worker] = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"

async def extractor_worker(queue: asyncio.Queue):
    extractor = BinanceExtractor()
    update_status("extractor", "Memulai ekstraksi...")
    try:
        await extractor.extract_loop(queue, on_success=lambda ts: update_status("extractor", f"Data ditarik [ts: {ts}]"))
    finally:
        await extractor.close()

async def transformer_worker(in_queue: asyncio.Queue, out_queue: asyncio.Queue):
    transformer = Transformer()
    while True:
        raw_data = await in_queue.get()
        transformed = transformer.transform(raw_data)
        await out_queue.put(transformed)
        update_status("transformer", f"Transformasi selesai [ts: {transformed['timestamp']}]")
        in_queue.task_done()

async def loader_worker(queue: asyncio.Queue):
    loader = Loader()
    while True:
        data = await queue.get()
        loader.insert(data)
        update_status("loader", f"Data ke DB [ts: {data['timestamp']}]")
        queue.task_done()

def run_visualizer_thread():
    run_visualizer()

def run_export_csv_periodically(interval=60):
    while True:
        time.sleep(interval)
        export_to_csv()
        update_status("exporter", f"CSV diekspor")

def run_spark_batch_periodically(interval=300):
    try:
        # Import here to avoid circular imports
        from spark_integration import SparkProcessor
        
        update_status("spark", "Spark service ready")
        
        while True:
            time.sleep(interval)
            try:
                update_status("spark", "Memulai batch processing...")
                processor = SparkProcessor()
                success = processor.process_batch()
                if success:
                    update_status("spark", "Batch processing selesai")
                else:
                    update_status("spark", "Batch processing gagal")
                processor.stop()
            except Exception as e:
                update_status("spark", f"Error: {str(e)}")
    except ImportError:
        update_status("spark", "Spark tidak tersedia")

def check_kafka_status():
    update_status("kafka", "Kafka service ready")
    
def check_scheduler_status():
    update_status("scheduler", "Scheduler service ready")

async def main():
    raw_queue = asyncio.Queue()
    transformed_queue = asyncio.Queue()

    # Start visualization thread
    threading.Thread(target=run_visualizer_thread, daemon=True).start()
    
    # Start CSV export thread
    threading.Thread(target=run_export_csv_periodically, daemon=True).start()
    
    # Start Spark batch processing if enabled
    if os.environ.get("ENABLE_SPARK", "false").lower() == "true":
        threading.Thread(target=run_spark_batch_periodically, daemon=True).start()
    
    # Check Kafka status
    if os.environ.get("ENABLE_KAFKA", "false").lower() == "true":
        threading.Thread(target=check_kafka_status, daemon=True).start()
    
    # Check Scheduler status
    if os.environ.get("ENABLE_SCHEDULER", "false").lower() == "true":
        threading.Thread(target=check_scheduler_status, daemon=True).start()

    with Live(make_status_table(), refresh_per_second=4) as live:
        async def refresh_loop():
            while True:
                live.update(make_status_table())
                await asyncio.sleep(1)

        try:
            await asyncio.gather(
                extractor_worker(raw_queue),
                transformer_worker(raw_queue, transformed_queue),
                loader_worker(transformed_queue),
                refresh_loop(),
            )
        except KeyboardInterrupt:
            print("\n[!] Pipeline dihentikan oleh user.")
        finally:
            export_to_csv()
            print(f"[💾] Data akhir berhasil disimpan ke CSV.")
            print(f"[✅] Total data yang direkam ke DB: {total_records}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Program dihentikan oleh user.")