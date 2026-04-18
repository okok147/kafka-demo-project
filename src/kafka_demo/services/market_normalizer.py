from kafka_demo.common.envelope import new_event
from kafka_demo.common.event_bus import publish_deadletter, publish_event
from kafka_demo.common.kafka_client import build_consumer, build_producer
from kafka_demo.common.serde import deserialize_envelope
from kafka_demo.common.topics import MARKET_NORMALIZED, MARKET_RAW



def run() -> None:
    consumer = build_consumer("market-normalizer", [MARKET_RAW])
    producer = build_producer()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                event = deserialize_envelope(msg.value())
                symbol = event.payload["symbol"].upper()
                bid = float(event.payload["bid"])
                ask = float(event.payload["ask"])
                normalized = new_event(
                    event_type="market.tick.normalized",
                    topic=MARKET_NORMALIZED,
                    key=symbol,
                    payload={"symbol": symbol, "bid": bid, "ask": ask},
                    metadata={"source_event_id": event.event_id},
                )
                publish_event(producer, normalized)
            except Exception as exc:
                publish_deadletter(
                    producer=producer,
                    source_service="market-normalizer",
                    original_topic=MARKET_RAW,
                    original_key=msg.key().decode("utf-8") if msg.key() else "",
                    raw_payload=msg.value().decode("utf-8"),
                    error_message=str(exc),
                )
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
