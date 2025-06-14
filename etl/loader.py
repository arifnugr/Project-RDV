import os
import asyncio
from pymongo import MongoClient
import sqlite3

class Loader:
    def __init__(self, db_path=None, use_mongodb=True):
        # Gunakan path absolut ke direktori root proyek
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(current_dir)
        
        # MongoDB connection
        self.use_mongodb = use_mongodb
        if self.use_mongodb:
            try:
                # Gunakan nama host 'mongodb' untuk Docker atau 'localhost' untuk development
                mongo_host = os.environ.get('MONGODB_HOST', 'mongodb')
                self.mongo_client = MongoClient(f"mongodb://{mongo_host}:27017/")
                self.db = self.mongo_client["market_data"]
                self.collection = self.db["market_data"]
                print(f"[Loader] Connected to MongoDB at {mongo_host}")
            except Exception as e:
                print(f"[Loader Error] Failed to connect to MongoDB: {e}")
                print("[Loader] Falling back to SQLite")
                self.use_mongodb = False
        
        # SQLite fallback
        if not self.use_mongodb:
            if db_path is None:
                db_path = os.path.join(project_dir, 'data', 'market_data.db')
            
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            self.conn = sqlite3.connect(db_path)
            self._create_sqlite_table()
            print(f"[Loader] Using SQLite at {db_path}")

    def _create_sqlite_table(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS market_data (
                timestamp TEXT,
                last_price REAL,
                bid_price REAL,
                ask_price REAL,
                bid_volume REAL,
                ask_volume REAL,
                volume_24h REAL,
                high_24h REAL,
                low_24h REAL,
                change_24h REAL,
                price_change_24h REAL,
                bid_ask_spread REAL,
                change_24h_normalized REAL,
                spread_percentage REAL,
                trend TEXT
            )
        ''')
        self.conn.commit()

    def insert(self, data: dict):
        if self.use_mongodb:
            try:
                # Insert into MongoDB
                self.collection.insert_one(data)
            except Exception as e:
                print(f"[Loader Error] MongoDB insert failed: {e}")
                # Fallback to SQLite if MongoDB fails
                if not hasattr(self, 'conn'):
                    current_dir = os.path.dirname(os.path.abspath(__file__))
                    project_dir = os.path.dirname(current_dir)
                    db_path = os.path.join(project_dir, 'data', 'market_data.db')
                    os.makedirs(os.path.dirname(db_path), exist_ok=True)
                    self.conn = sqlite3.connect(db_path)
                    self._create_sqlite_table()
                
                self._insert_sqlite(data)
        else:
            # Use SQLite directly
            self._insert_sqlite(data)

    def _insert_sqlite(self, data: dict):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO market_data (
                timestamp,
                last_price,
                bid_price,
                ask_price,
                bid_volume,
                ask_volume,
                volume_24h,
                high_24h,
                low_24h,
                change_24h,
                price_change_24h,
                bid_ask_spread,
                change_24h_normalized,
                spread_percentage,
                trend
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get('timestamp'),
            data.get('last_price'),
            data.get('bid_price'),
            data.get('ask_price'),
            data.get('bid_volume'),
            data.get('ask_volume'),
            data.get('volume_24h'),
            data.get('high_24h'),
            data.get('low_24h'),
            data.get('change_24h'),
            data.get('price_change_24h'),
            data.get('bid_ask_spread'),
            data.get('change_24h_normalized'),
            data.get('spread_percentage'),
            data.get('trend'),
        ))
        self.conn.commit()

    async def load_loop(self, in_queue: asyncio.Queue, on_success=None):
        while True:
            data = await in_queue.get()
            self.insert(data)
            in_queue.task_done()
            if on_success:
                on_success(data.get("timestamp", "-"))

    def close(self):
        if self.use_mongodb:
            self.mongo_client.close()
        else:
            self.conn.close()