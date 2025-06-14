# extractor.py

import ccxt.async_support as ccxt
import asyncio
import time
import random
from datetime import datetime

class BinanceExtractor:
    def __init__(self, symbol='BTC/USDT', use_dummy_data=False):
        self.exchange = ccxt.binance({
            'enableRateLimit': True,  # Aktifkan rate limiting otomatis
            'timeout': 30000,         # Timeout 30 detik
            'options': {
                'defaultType': 'spot'  # Gunakan spot market
            }
        })
        self.symbol = symbol
        self.use_dummy_data = use_dummy_data
        self.retry_count = 0
        self.max_retries = 5
        self.base_delay = 5  # Delay dasar 5 detik

    async def get_dummy_data(self):
        """Generate dummy data untuk testing"""
        last_price = 30000 + random.uniform(-500, 500)
        bid_price = last_price - random.uniform(1, 10)
        ask_price = last_price + random.uniform(1, 10)
        
        return {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'last_price': last_price,
            'bid_price': bid_price,
            'ask_price': ask_price,
            'bid_volume': random.uniform(1, 5),
            'ask_volume': random.uniform(1, 5),
            'volume_24h': random.uniform(10000, 50000),
            'high_24h': last_price + random.uniform(100, 1000),
            'low_24h': last_price - random.uniform(100, 1000),
            'change_24h': random.uniform(-5, 5),
            'price_change_24h': random.uniform(-500, 500),
            'bid_ask_spread': ask_price - bid_price,
            'change_24h_normalized': random.uniform(-0.05, 0.05),
            'spread_percentage': random.uniform(0.01, 0.5),
            'trend': random.choice(['bullish', 'bearish', 'neutral'])
        }

    async def get_binance_data(self):
        # Gunakan data dummy jika diaktifkan
        if self.use_dummy_data:
            return await self.get_dummy_data()
            
        # Exponential backoff jika ada retry
        if self.retry_count > 0:
            delay = self.base_delay * (2 ** (self.retry_count - 1))
            print(f"[Extractor] Retry #{self.retry_count} after {delay}s delay")
            await asyncio.sleep(delay)
        
        try:
            ticker = await self.exchange.fetch_ticker(self.symbol)
            orderbook = await self.exchange.fetch_order_book(self.symbol)
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Reset retry counter setelah berhasil
            if self.retry_count > 0:
                print(f"[Extractor] Successfully connected to Binance after {self.retry_count} retries")
                self.retry_count = 0

            data = {
                'timestamp': now,
                'last_price': ticker['last'],
                'bid_price': orderbook['bids'][0][0] if orderbook['bids'] else None,
                'ask_price': orderbook['asks'][0][0] if orderbook['asks'] else None,
                'bid_volume': orderbook['bids'][0][1] if orderbook['bids'] else None,
                'ask_volume': orderbook['asks'][0][1] if orderbook['asks'] else None,
                'volume_24h': ticker['quoteVolume'],
                'high_24h': ticker['high'],
                'low_24h': ticker['low'],
                'change_24h': ticker['percentage'],
                'open_price': ticker['open'],
                'close_price': ticker['close'],
                'price_change_24h': ticker['change'],
                'bid_ask_spread': (
                    orderbook['asks'][0][0] - orderbook['bids'][0][0]
                    if orderbook['bids'] and orderbook['asks'] else None
                ),
                'change_24h_normalized': ticker['percentage'] / 100 if ticker['percentage'] else 0,
                'spread_percentage': (
                    (orderbook['asks'][0][0] - orderbook['bids'][0][0]) / orderbook['bids'][0][0]
                    if orderbook['bids'] and orderbook['asks'] and orderbook['bids'][0][0] > 0 else 0
                ),
                'trend': 'bullish' if ticker['percentage'] > 0 else 'bearish' if ticker['percentage'] < 0 else 'neutral'
            }

            return data
        except Exception as e:
            print(f"[Extractor Error] {e}")
            
            # Increment retry counter dan coba lagi jika belum mencapai batas
            self.retry_count += 1
            if self.retry_count <= self.max_retries:
                print(f"[Extractor] Will retry ({self.retry_count}/{self.max_retries})")
                return await self.get_binance_data()
            else:
                print(f"[Extractor] Max retries reached, falling back to dummy data")
                self.retry_count = 0
                return await self.get_dummy_data()  # Fallback ke dummy data

    async def extract_loop(self, queue: asyncio.Queue, interval=1, on_success=None):
        while True:
            try:
                data = await self.get_binance_data()
                if data:
                    await queue.put(data)
                    if on_success:
                        on_success(data["timestamp"])
                
                # Tambahkan jitter untuk menghindari thundering herd
                jitter = random.uniform(0, 1)
                await asyncio.sleep(interval + jitter)
            except Exception as e:
                print(f"[Extractor Loop Error] {e}")
                # Tetap lanjutkan loop meskipun ada error
                await asyncio.sleep(interval)

    async def close(self):
        try:
            await self.exchange.close()
        except Exception as e:
            print(f"[Extractor Close Error] {e}")
