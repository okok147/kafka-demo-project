import json

from kafka_demo.common.db import get_conn
from kafka_demo.common.dlq_payload import normalize_deadletter_payload
from kafka_demo.common.kafka_client import build_consumer
from kafka_demo.common.serde import deserialize_envelope
from kafka_demo.common.topics import DEADLETTER_EVENTS



def run() -> None:
    consumer = build_consumer("deadletter-handler", [DEADLETTER_EVENTS])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            event = deserialize_envelope(msg.value())
            payload = event.payload
            normalized = normalize_deadletter_payload(payload.get("event_payload"))

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO deadletter_events(source_service, original_topic, event_key, event_payload, error_message)
                        VALUES (%s,%s,%s,%s::jsonb,%s)
                        """,
                        (
                            payload["source_service"],
                            payload["original_topic"],
                            payload.get("event_key"),
                            json.dumps(normalized, separators=(",", ":")),
                            payload["error_message"],
                        ),
                    )
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
