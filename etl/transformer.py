import asyncio

class Transformer:
    def __init__(self):
        pass

    def transform(self, data: dict) -> dict:
        for col in ['market_cap', 'open_price', 'close_price']:
            data.pop(col, None)

        numeric_keys = ['last_price', 'bid_price', 'ask_price', 'bid_volume',
                        'ask_volume', 'volume_24h', 'high_24h', 'low_24h',
                        'change_24h', 'price_change_24h', 'bid_ask_spread']
        for key in numeric_keys:
            if data.get(key) is not None:
                try:
                    data[key] = float(data[key])
                except ValueError:
                    data[key] = None

        if data.get("change_24h") is not None:
            data["change_24h_normalized"] = max(-1.0, min(1.0, data["change_24h"] / 100.0))

        if data.get("bid_ask_spread") and data.get("last_price"):
            data["spread_percentage"] = (data["bid_ask_spread"] / data["last_price"]) * 100
        else:
            data["spread_percentage"] = None

        if data.get("change_24h") is not None:
            data["trend"] = "bullish" if data["change_24h"] > 0 else "bearish"
        else:
            data["trend"] = "unknown"

        return data

    async def transform_loop(self, in_queue: asyncio.Queue, out_queue: asyncio.Queue, on_success=None):
        while True:
            raw_data = await in_queue.get()
            transformed = self.transform(raw_data)
            await out_queue.put(transformed)
            in_queue.task_done()
            if on_success:
                on_success(transformed.get("timestamp", "-"))
