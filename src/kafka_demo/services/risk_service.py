from decimal import Decimal

from kafka_demo.common.db import get_conn
from kafka_demo.common.envelope import EventEnvelope, new_event
from kafka_demo.common.event_bus import publish_deadletter, publish_event
from kafka_demo.common.kafka_client import build_consumer, build_producer
from kafka_demo.common.serde import deserialize_envelope
from kafka_demo.common.topics import ORDER_COMMANDS, RISK_ACCEPTED, RISK_REJECTED



def evaluate_new_order(order: dict) -> tuple[bool, str]:
    account_id = order["account_id"]
    qty = Decimal(order["quantity"])
    price = Decimal(order["price"] or "0")
    notional = qty * price

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max_notional, max_open_orders FROM account_limits WHERE account_id=%s",
                (account_id,),
            )
            limit = cur.fetchone()
            if not limit:
                return False, "NO_ACCOUNT_LIMIT"

            max_notional, max_open_orders = Decimal(limit[0]), int(limit[1])
            cur.execute("SELECT COALESCE(COUNT(1),0) FROM open_orders WHERE account_id=%s", (account_id,))
            open_count = int(cur.fetchone()[0])
            cur.execute(
                "SELECT COALESCE(SUM(remaining_qty * COALESCE(price, 0)), 0) FROM open_orders WHERE account_id=%s",
                (account_id,),
            )
            open_notional = Decimal(cur.fetchone()[0])

    if open_count >= max_open_orders:
        return False, "OPEN_ORDER_LIMIT"
    if open_notional + notional > max_notional:
        return False, "NOTIONAL_LIMIT"
    return True, "ACCEPTED"



def _update_order_status(order_id: str, status: str, event_id: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE orders SET status=%s, updated_at=NOW(), last_event_id=%s WHERE order_id=%s",
                (status, event_id, order_id),
            )



def _handle_order_new(event: EventEnvelope, producer) -> None:
    accepted, reason = evaluate_new_order(event.payload)
    target_topic = RISK_ACCEPTED if accepted else RISK_REJECTED
    event_type = "risk.accepted" if accepted else "risk.rejected"

    outcome = new_event(
        event_type=event_type,
        topic=target_topic,
        key=event.key,
        payload={**event.payload, "reason": reason, "command_type": "NEW"},
        metadata={"source_event_id": event.event_id},
    )
    publish_event(producer, outcome)
    _update_order_status(event.payload["order_id"], "RISK_ACCEPTED" if accepted else "RISK_REJECTED", outcome.event_id)



def _handle_order_cancel(event: EventEnvelope, producer) -> None:
    accepted = new_event(
        event_type="risk.accepted",
        topic=RISK_ACCEPTED,
        key=event.key,
        payload={"order_id": event.payload["order_id"], "reason": event.payload.get("reason"), "command_type": "CANCEL"},
        metadata={"source_event_id": event.event_id},
    )
    publish_event(producer, accepted)



def run() -> None:
    consumer = build_consumer("risk-service", [ORDER_COMMANDS])
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
                if event.event_type == "order.new.requested":
                    _handle_order_new(event, producer)
                elif event.event_type == "order.cancel.requested":
                    _handle_order_cancel(event, producer)
            except Exception as exc:
                publish_deadletter(
                    producer=producer,
                    source_service="risk-service",
                    original_topic=ORDER_COMMANDS,
                    original_key=msg.key().decode("utf-8") if msg.key() else "",
                    raw_payload=msg.value().decode("utf-8"),
                    error_message=str(exc),
                )
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
