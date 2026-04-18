import random
import time

from kafka_demo.common.envelope import new_event
from kafka_demo.common.event_bus import publish_event
from kafka_demo.common.kafka_client import build_producer
from kafka_demo.common.topics import MARKET_RAW


SYMBOLS = ["AAPL", "MSFT", "TSLA", "NVDA"]



def run() -> None:
    producer = build_producer()
    prices = {"AAPL": 190.0, "MSFT": 410.0, "TSLA": 175.0, "NVDA": 920.0}

    while True:
        for symbol in SYMBOLS:
            drift = random.uniform(-0.8, 0.8)
            prices[symbol] = max(1, prices[symbol] + drift)
            mid = round(prices[symbol], 4)
            event = new_event(
                event_type="market.tick.raw",
                topic=MARKET_RAW,
                key=symbol,
                payload={
                    "symbol": symbol,
                    "bid": round(mid - 0.05, 4),
                    "ask": round(mid + 0.05, 4),
                    "source": "simulator",
                },
            )
            publish_event(producer, event)
        time.sleep(1)


if __name__ == "__main__":
    run()
