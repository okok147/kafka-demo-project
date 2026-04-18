from decimal import Decimal

from kafka_demo.common.db import get_conn
from kafka_demo.common.envelope import new_event
from kafka_demo.common.event_bus import publish_deadletter, publish_event
from kafka_demo.common.kafka_client import build_consumer, build_producer
from kafka_demo.common.serde import deserialize_envelope
from kafka_demo.common.topics import EXECUTION_EVENTS, MARKET_NORMALIZED, PORTFOLIO_EVENTS



def _record_fill(fill: dict) -> None:
    qty = Decimal(fill["quantity"])
    price = Decimal(fill["price"])
    notional = Decimal(fill["notional"])
    side = fill["side"]
    cash_delta = -notional if side == "BUY" else notional

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fill_ledger(fill_id, order_id, account_id, symbol, side, quantity, price, notional, occurred_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT(fill_id) DO NOTHING
                """,
                (
                    fill["fill_id"],
                    fill["order_id"],
                    fill["account_id"],
                    fill["symbol"],
                    side,
                    qty,
                    price,
                    notional,
                ),
            )

            cur.execute(
                """
                INSERT INTO account_balances(account_id, balance, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT(account_id) DO UPDATE
                SET balance = account_balances.balance + EXCLUDED.balance,
                    updated_at = NOW()
                RETURNING balance
                """,
                (fill["account_id"], cash_delta),
            )
            new_balance = Decimal(cur.fetchone()[0])

            cur.execute(
                "INSERT INTO cash_ledger(account_id, delta, balance, event_id) VALUES (%s,%s,%s,%s)",
                (fill["account_id"], cash_delta, new_balance, fill["fill_id"]),
            )

            signed_qty = qty if side == "BUY" else -qty
            cur.execute(
                "SELECT quantity, avg_price, realized_pnl FROM positions WHERE account_id=%s AND symbol=%s",
                (fill["account_id"], fill["symbol"]),
            )
            pos = cur.fetchone()

            if not pos:
                cur.execute(
                    """
                    INSERT INTO positions(account_id, symbol, quantity, avg_price, realized_pnl, unrealized_pnl, updated_at)
                    VALUES (%s,%s,%s,%s,0,0,NOW())
                    """,
                    (fill["account_id"], fill["symbol"], signed_qty, price),
                )
            else:
                old_qty = Decimal(pos[0])
                old_avg = Decimal(pos[1])
                old_realized = Decimal(pos[2])
                new_qty = old_qty + signed_qty

                realized = old_realized
                new_avg = old_avg

                if old_qty == 0 or (old_qty > 0 and signed_qty > 0) or (old_qty < 0 and signed_qty < 0):
                    abs_total = abs(old_qty) + abs(signed_qty)
                    new_avg = ((abs(old_qty) * old_avg) + (abs(signed_qty) * price)) / abs_total
                else:
                    closed_qty = min(abs(old_qty), abs(signed_qty))
                    if old_qty > 0:
                        realized += closed_qty * (price - old_avg)
                    else:
                        realized += closed_qty * (old_avg - price)
                    if new_qty == 0:
                        new_avg = Decimal("0")
                    elif abs(signed_qty) > abs(old_qty):
                        new_avg = price

                cur.execute(
                    """
                    UPDATE positions
                    SET quantity=%s, avg_price=%s, realized_pnl=%s, updated_at=NOW()
                    WHERE account_id=%s AND symbol=%s
                    """,
                    (new_qty, new_avg, realized, fill["account_id"], fill["symbol"]),
                )



def _update_unrealized(symbol: str, mid: Decimal) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT account_id, quantity, avg_price, realized_pnl FROM positions WHERE symbol=%s",
                (symbol,),
            )
            rows = cur.fetchall()

            affected_accounts: set[str] = set()
            for account_id, qty, avg_price, _ in rows:
                qty_d = Decimal(qty)
                unrealized = qty_d * (mid - Decimal(avg_price))
                cur.execute(
                    "UPDATE positions SET unrealized_pnl=%s, updated_at=NOW() WHERE account_id=%s AND symbol=%s",
                    (unrealized, account_id, symbol),
                )
                affected_accounts.add(account_id)

            for account_id in affected_accounts:
                cur.execute(
                    "SELECT COALESCE(SUM(realized_pnl),0), COALESCE(SUM(unrealized_pnl),0) FROM positions WHERE account_id=%s",
                    (account_id,),
                )
                realized, unrealized = cur.fetchone()
                realized_d = Decimal(realized)
                unrealized_d = Decimal(unrealized)
                cur.execute(
                    "INSERT INTO pnl_snapshots(account_id, realized, unrealized, total) VALUES (%s,%s,%s,%s)",
                    (account_id, realized_d, unrealized_d, realized_d + unrealized_d),
                )



def run() -> None:
    consumer = build_consumer("portfolio-service", [EXECUTION_EVENTS, MARKET_NORMALIZED])
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
                if event.topic == EXECUTION_EVENTS and event.event_type == "execution.order.filled":
                    _record_fill(event.payload)
                    out = new_event(
                        event_type="portfolio.fill.recorded",
                        topic=PORTFOLIO_EVENTS,
                        key=event.key,
                        payload={"order_id": event.payload["order_id"], "fill_id": event.payload["fill_id"]},
                    )
                    publish_event(producer, out)
                elif event.topic == MARKET_NORMALIZED:
                    mid = (Decimal(str(event.payload["bid"])) + Decimal(str(event.payload["ask"]))) / Decimal("2")
                    _update_unrealized(event.payload["symbol"], mid)
            except Exception as exc:
                publish_deadletter(
                    producer=producer,
                    source_service="portfolio-service",
                    original_topic=msg.topic(),
                    original_key=msg.key().decode("utf-8") if msg.key() else "",
                    raw_payload=msg.value().decode("utf-8"),
                    error_message=str(exc),
                )
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
