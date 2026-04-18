from decimal import Decimal
import uuid

from kafka_demo.common.db import get_conn
from kafka_demo.common.envelope import EventEnvelope, new_event
from kafka_demo.common.event_bus import publish_deadletter, publish_event
from kafka_demo.common.kafka_client import build_consumer, build_producer
from kafka_demo.common.serde import deserialize_envelope
from kafka_demo.common.topics import EXECUTION_EVENTS, MARKET_NORMALIZED, RISK_ACCEPTED



def _update_quote(symbol: str, bid: Decimal, ask: Decimal) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quote_cache(symbol, bid, ask, last_updated)
                VALUES (%s,%s,%s,NOW())
                ON CONFLICT(symbol) DO UPDATE
                SET bid=EXCLUDED.bid, ask=EXCLUDED.ask, last_updated=NOW()
                """,
                (symbol, bid, ask),
            )



def _place_open_order(order: dict) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO open_orders(order_id, account_id, symbol, side, remaining_qty, price, status)
                VALUES (%s,%s,%s,%s,%s,%s,'OPEN')
                ON CONFLICT(order_id) DO NOTHING
                """,
                (
                    order["order_id"],
                    order["account_id"],
                    order["symbol"],
                    order["side"],
                    Decimal(order["quantity"]),
                    Decimal(order["price"]) if order["price"] else None,
                ),
            )
            cur.execute(
                "UPDATE orders SET status='OPEN', updated_at=NOW() WHERE order_id=%s",
                (order["order_id"],),
            )



def _consume_open_order(order_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT order_id, account_id, symbol, side, remaining_qty, price FROM open_orders WHERE order_id=%s",
                (order_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cur.execute("DELETE FROM open_orders WHERE order_id=%s", (order_id,))
            cur.execute("UPDATE orders SET status='FILLED', updated_at=NOW() WHERE order_id=%s", (order_id,))
            return row



def _cancel_open_order(order_id: str) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM open_orders WHERE order_id=%s", (order_id,))
            deleted = cur.rowcount > 0
            cur.execute(
                "UPDATE orders SET status=%s, updated_at=NOW() WHERE order_id=%s",
                ("CANCELLED" if deleted else "FILLED", order_id),
            )
            return deleted



def _get_quote(symbol: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT bid, ask FROM quote_cache WHERE symbol=%s", (symbol,))
            return cur.fetchone()



def _should_fill(side: str, limit_price, bid: Decimal, ask: Decimal) -> tuple[bool, Decimal]:
    if side == "BUY":
        fill_price = ask
        if limit_price is None:
            return True, fill_price
        return Decimal(limit_price) >= ask, fill_price

    fill_price = bid
    if limit_price is None:
        return True, fill_price
    return Decimal(limit_price) <= bid, fill_price



def _try_fill(order_id: str, producer) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT order_id, account_id, symbol, side, remaining_qty, price FROM open_orders WHERE order_id=%s",
                (order_id,),
            )
            row = cur.fetchone()

    if not row:
        return

    _, account_id, symbol, side, qty, limit_price = row
    quote = _get_quote(symbol)
    if not quote:
        return

    bid, ask = Decimal(quote[0]), Decimal(quote[1])
    should_fill, fill_price = _should_fill(side, limit_price, bid, ask)
    if not should_fill:
        return

    consumed = _consume_open_order(order_id)
    if not consumed:
        return

    fill_event = new_event(
        event_type="execution.order.filled",
        topic=EXECUTION_EVENTS,
        key=order_id,
        payload={
            "fill_id": str(uuid.uuid4()),
            "order_id": order_id,
            "account_id": account_id,
            "symbol": symbol,
            "side": side,
            "quantity": str(qty),
            "price": str(fill_price),
            "notional": str(Decimal(qty) * Decimal(fill_price)),
        },
    )
    publish_event(producer, fill_event)



def _try_fill_symbol(symbol: str, producer) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_id
                FROM open_orders
                WHERE symbol=%s
                ORDER BY created_at ASC
                """,
                (symbol,),
            )
            rows = cur.fetchall()

    for row in rows:
        _try_fill(row[0], producer)


def run() -> None:
    consumer = build_consumer("execution-service", [RISK_ACCEPTED, MARKET_NORMALIZED])
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
                if event.topic == MARKET_NORMALIZED:
                    _update_quote(
                        symbol=event.payload["symbol"],
                        bid=Decimal(str(event.payload["bid"])),
                        ask=Decimal(str(event.payload["ask"])),
                    )
                    _try_fill_symbol(event.payload["symbol"], producer)
                    continue

                if event.payload.get("command_type") == "NEW":
                    _place_open_order(event.payload)
                    ack = new_event(
                        event_type="execution.order.opened",
                        topic=EXECUTION_EVENTS,
                        key=event.payload["order_id"],
                        payload={"order_id": event.payload["order_id"], "status": "OPEN"},
                    )
                    publish_event(producer, ack)
                    _try_fill(event.payload["order_id"], producer)

                if event.payload.get("command_type") == "CANCEL":
                    order_id = event.payload["order_id"]
                    cancelled = _cancel_open_order(order_id)
                    out = new_event(
                        event_type="execution.order.cancelled" if cancelled else "execution.order.cancel_after_fill",
                        topic=EXECUTION_EVENTS,
                        key=order_id,
                        payload={"order_id": order_id, "cancelled": cancelled},
                    )
                    publish_event(producer, out)
            except Exception as exc:
                publish_deadletter(
                    producer=producer,
                    source_service="execution-service",
                    original_topic=msg.topic(),
                    original_key=msg.key().decode("utf-8") if msg.key() else "",
                    raw_payload=msg.value().decode("utf-8"),
                    error_message=str(exc),
                )
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
