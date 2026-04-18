import json

from kafka_demo.common.db import get_conn
from kafka_demo.common.event_bus import publish_deadletter
from kafka_demo.common.kafka_client import build_consumer, build_producer
from kafka_demo.common.serde import deserialize_envelope
from kafka_demo.common.topics import REPLAY_JOBS



def _run_job(payload: dict, producer) -> None:
    job_id = payload["job_id"]
    deadletter_id = payload.get("deadletter_event_id")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE replay_jobs SET status='RUNNING' WHERE job_id=%s", (job_id,))

            if deadletter_id:
                cur.execute(
                    "SELECT id, original_topic, event_key, event_payload::text FROM deadletter_events WHERE id=%s AND replayed=false",
                    (deadletter_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT id, original_topic, event_key, event_payload::text
                    FROM deadletter_events
                    WHERE replayed=false
                    ORDER BY id ASC
                    LIMIT 200
                    """
                )

            rows = cur.fetchall()

            for row in rows:
                evt_id, topic, key, payload_txt = row
                producer.produce(topic, key=key, value=payload_txt.encode("utf-8"))
                cur.execute("UPDATE deadletter_events SET replayed=true WHERE id=%s", (evt_id,))

            producer.flush(10)
            cur.execute(
                "UPDATE replay_jobs SET status='DONE', finished_at=NOW(), error=NULL WHERE job_id=%s",
                (job_id,),
            )



def run() -> None:
    consumer = build_consumer("replay-runner", [REPLAY_JOBS])
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
                _run_job(event.payload, producer)
            except Exception as exc:
                try:
                    payload_obj = json.loads(msg.value().decode("utf-8"))
                    job_id = payload_obj.get("payload", {}).get("job_id")
                except Exception:
                    job_id = None

                if job_id:
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE replay_jobs SET status='FAILED', finished_at=NOW(), error=%s WHERE job_id=%s",
                                (str(exc), job_id),
                            )

                publish_deadletter(
                    producer=producer,
                    source_service="replay-runner",
                    original_topic=msg.topic(),
                    original_key=msg.key().decode("utf-8") if msg.key() else "",
                    raw_payload=msg.value().decode("utf-8"),
                    error_message=str(exc),
                )
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
