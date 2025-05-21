# extractor.py

import ccxt.async_support as ccxt
import asyncio
from datetime import datetime

class BinanceExtractor:
    def __init__(self, symbol='BTC/USDT'):
        self.exchange = ccxt.binance()
        self.symbol = symbol

    async def get_binance_data(self):
        try:
            ticker = await self.exchange.fetch_ticker(self.symbol)
            orderbook = await self.exchange.fetch_order_book(self.symbol)
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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
                'market_cap': ticker['info'].get('market_cap', 'N/A'),
                'price_change_24h': ticker['change'],
                'bid_ask_spread': (
                    orderbook['bids'][0][0] - orderbook['asks'][0][0]
                    if orderbook['bids'] and orderbook['asks'] else None
                )
            }

            return data
        except Exception as e:
            print(f"[Extractor Error] {e}")
            return None

    async def extract_loop(self, queue: asyncio.Queue, interval=1, on_success=None):
        while True:
            data = await self.get_binance_data()
            if data:
                await queue.put(data)
                if on_success:
                    on_success(data["timestamp"])
            await asyncio.sleep(interval)

    async def close(self):
        await self.exchange.close()
