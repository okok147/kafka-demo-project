import argparse
import os

import psycopg2
from confluent_kafka import Producer


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay one deadletter event")
    parser.add_argument("--deadletter-event-id", type=int, required=True)
    args = parser.parse_args()

    dsn = os.environ.get("POSTGRES_DSN", os.environ.get("DATABASE_URL", ""))
    if dsn:
        conn = psycopg2.connect(dsn)
    else:
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=os.environ.get("POSTGRES_PORT", "5432"),
            dbname=os.environ.get("POSTGRES_DB", "kafkademo"),
            user=os.environ.get("POSTGRES_USER", "kafka"),
            password=os.environ.get("POSTGRES_PASSWORD", "kafka"),
        )

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT original_topic, event_key, event_payload::text FROM deadletter_events WHERE id=%s",
                (args.deadletter_event_id,),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("deadletter event not found")

    topic, key, payload = row
    producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")})
    producer.produce(topic, key=key, value=payload.encode("utf-8"))
    producer.flush(10)

    with conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE deadletter_events SET replayed=true WHERE id=%s", (args.deadletter_event_id,))

    conn.close()
    print(f"replayed deadletter event {args.deadletter_event_id}")


if __name__ == "__main__":
    main()
