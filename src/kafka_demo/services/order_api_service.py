from __future__ import annotations

from decimal import Decimal
import uuid

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from kafka_demo.common.db import get_conn
from kafka_demo.common.envelope import new_event
from kafka_demo.common.event_bus import publish_event
from kafka_demo.common.kafka_client import build_producer
from kafka_demo.common.topics import ORDER_COMMANDS


class CreateOrderRequest(BaseModel):
    account_id: str
    symbol: str
    side: str = Field(pattern="^(BUY|SELL)$")
    quantity: Decimal = Field(gt=0)
    price: Decimal | None = Field(default=None, gt=0)


class CancelOrderRequest(BaseModel):
    reason: str = "user_requested"


app = FastAPI(title="order-api-service")
producer = build_producer()


@app.post("/orders")
def create_order(payload: CreateOrderRequest, idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")):
    order_id = uuid.uuid4()
    response = {"order_id": str(order_id), "status": "PENDING_RISK"}

    with get_conn() as conn:
        with conn.cursor() as cur:
            if idempotency_key:
                cur.execute("SELECT response_json::text FROM idempotency_keys WHERE idempotency_key=%s", (idempotency_key,))
                existing = cur.fetchone()
                if existing:
                    import json

                    return json.loads(existing[0])

            cur.execute(
                """
                INSERT INTO orders(order_id, account_id, symbol, side, quantity, price, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    str(order_id),
                    payload.account_id,
                    payload.symbol.upper(),
                    payload.side,
                    payload.quantity,
                    payload.price,
                    "PENDING_RISK",
                ),
            )

            command = new_event(
                event_type="order.new.requested",
                topic=ORDER_COMMANDS,
                key=str(order_id),
                payload={
                    "order_id": str(order_id),
                    "account_id": payload.account_id,
                    "symbol": payload.symbol.upper(),
                    "side": payload.side,
                    "quantity": str(payload.quantity),
                    "price": str(payload.price) if payload.price is not None else None,
                },
            )
            publish_event(producer, command)

            if idempotency_key:
                cur.execute(
                    "INSERT INTO idempotency_keys(idempotency_key, order_id, response_json) VALUES (%s,%s,%s::jsonb)",
                    (idempotency_key, str(order_id), __import__("json").dumps(response)),
                )

    return response


@app.post("/orders/{order_id}/cancel")
def cancel_order(order_id: str, payload: CancelOrderRequest):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM orders WHERE order_id=%s", (order_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="order not found")

    command = new_event(
        event_type="order.cancel.requested",
        topic=ORDER_COMMANDS,
        key=order_id,
        payload={"order_id": order_id, "reason": payload.reason},
    )
    publish_event(producer, command)
    return {"order_id": order_id, "status": "CANCEL_PENDING"}


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
