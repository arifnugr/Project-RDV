import sqlite3
import pandas as pd
import os

def export_to_csv(db_path='data/market_data.db', csv_path='data/market_data.csv'):
    # Pastikan folder tujuan CSV ada
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM market_data", conn)
        df.to_csv(csv_path, index=False)
    except Exception as e:
        print(f"[Exporter Error] {e}")
    finally:
        conn.close()
