# visualizer.py

import matplotlib.pyplot as plt
import matplotlib.animation as animation
import sqlite3

DB_PATH = 'data/market_data.db'

def animate(i):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT timestamp, last_price FROM market_data ORDER BY timestamp DESC LIMIT 50")
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return

    timestamps = [row[0] for row in reversed(rows)]
    prices = [row[1] for row in reversed(rows)]

    latest_price = prices[-1] if prices else None

    plt.cla()
    plt.plot(timestamps, prices, marker='o', linestyle='-')
    plt.xticks(rotation=45, ha='right')
    plt.xlabel('Waktu')
    plt.ylabel('Harga USD')
    plt.title(f'BTC/USDT Harga Terakhir (Live): ${latest_price:.2f}' if latest_price else "BTC/USDT Harga Terakhir (Live)")
    plt.tight_layout()

def run_visualizer():
    fig = plt.figure()
    ani = animation.FuncAnimation(fig, animate, interval=2000)
    plt.show()
