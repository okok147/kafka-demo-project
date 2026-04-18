from __future__ import annotations

import json
import os
import threading
import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
import requests

from kafka_demo.common.config import settings
from kafka_demo.common.db import fetch_all, fetch_one, get_conn
from kafka_demo.common.envelope import new_event
from kafka_demo.common.event_bus import publish_deadletter, publish_event
from kafka_demo.common.kafka_client import build_consumer, build_producer
from kafka_demo.common.serde import deserialize_envelope
from kafka_demo.common.topics import EXECUTION_EVENTS, ORDER_COMMANDS, PORTFOLIO_EVENTS, REPLAY_JOBS, RISK_ACCEPTED, RISK_REJECTED
from kafka_demo.services.order_api_service import CancelOrderRequest, CreateOrderRequest


PROJECTOR_TOPICS = [ORDER_COMMANDS, RISK_ACCEPTED, RISK_REJECTED, EXECUTION_EVENTS, PORTFOLIO_EVENTS]

app = FastAPI(title="query-projector")
producer = build_producer()
order_api_internal_url = os.environ.get("ORDER_API_INTERNAL_URL", f"http://127.0.0.1:{os.environ.get('ORDER_API_PORT', '8000')}")



def _append_timeline(order_id: str | None, event_type: str, event_id: str, payload: dict) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO order_timeline(order_id, event_type, event_id, payload) VALUES (%s,%s,%s,%s::jsonb)",
                (order_id, event_type, event_id, json.dumps(payload)),
            )



def _projector_loop() -> None:
    consumer = build_consumer("query-projector", PROJECTOR_TOPICS)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                event = deserialize_envelope(msg.value())
                order_id = event.payload.get("order_id")
                _append_timeline(order_id, event.event_type, event.event_id, event.payload)
            except Exception as exc:
                publish_deadletter(
                    producer=producer,
                    source_service="query-projector",
                    original_topic=msg.topic(),
                    original_key=msg.key().decode("utf-8") if msg.key() else "",
                    raw_payload=msg.value().decode("utf-8"),
                    error_message=str(exc),
                )
    finally:
        consumer.close()


@app.on_event("startup")
def _startup() -> None:
    t = threading.Thread(target=_projector_loop, daemon=True)
    t.start()


@app.get("/")
def index():
    return FileResponse(Path(__file__).resolve().parent.parent / "web" / "index.html")


def _forward_json(method: str, path: str, payload: dict | None = None, headers: dict | None = None) -> JSONResponse:
    url = f"{order_api_internal_url}{path}"
    try:
        resp = requests.request(method=method, url=url, json=payload, headers=headers, timeout=20)
    except requests.RequestException as exc:
        raise HTTPException(status_code=503, detail=f"order api unavailable: {exc}") from exc

    try:
        body = resp.json()
    except ValueError:
        body = {"detail": resp.text}
    return JSONResponse(status_code=resp.status_code, content=body)


@app.post("/orders")
def create_order(payload: CreateOrderRequest, request: Request):
    headers = {}
    idem_key = request.headers.get("Idempotency-Key")
    if idem_key:
        headers["Idempotency-Key"] = idem_key
    body = payload.model_dump(mode="json")
    return _forward_json("POST", "/orders", payload=body, headers=headers)


@app.post("/orders/{order_id}/cancel")
def cancel_order(order_id: str, payload: CancelOrderRequest):
    body = payload.model_dump(mode="json")
    return _forward_json("POST", f"/orders/{order_id}/cancel", payload=body, headers=None)


@app.get("/views/orders")
def orders_view(limit: int = 50):
    rows = fetch_all(
        """
        SELECT order_id::text, account_id, symbol, side, quantity::text, COALESCE(price::text, '') AS price,
               status, updated_at::text
        FROM orders
        ORDER BY updated_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    return rows


@app.get("/views/orders/{order_id}/timeline")
def timeline_view(order_id: str):
    rows = fetch_all(
        """
        SELECT event_type, event_id, payload, created_at::text
        FROM order_timeline
        WHERE order_id=%s
        ORDER BY id ASC
        """,
        (order_id,),
    )
    return rows


@app.get("/views/positions")
def positions_view():
    rows = fetch_all(
        """
        SELECT account_id, symbol, quantity::text, avg_price::text, realized_pnl::text,
               unrealized_pnl::text, updated_at::text
        FROM positions
        ORDER BY account_id, symbol
        """
    )
    return rows


@app.get("/views/pnl")
def pnl_view(account_id: str | None = None):
    if account_id:
        rows = fetch_all(
            """
            SELECT account_id, realized::text, unrealized::text, total::text, updated_at::text
            FROM pnl_snapshots
            WHERE account_id=%s
            ORDER BY id DESC
            LIMIT 100
            """,
            (account_id,),
        )
    else:
        rows = fetch_all(
            """
            SELECT account_id, realized::text, unrealized::text, total::text, updated_at::text
            FROM pnl_snapshots
            ORDER BY id DESC
            LIMIT 100
            """
        )
    return rows


@app.get("/dashboard")
def dashboard():
    total_orders = fetch_one("SELECT COUNT(1) AS c FROM orders")["c"]
    open_orders = fetch_one("SELECT COUNT(1) AS c FROM open_orders")["c"]
    fills = fetch_one("SELECT COUNT(1) AS c FROM fill_ledger")["c"]
    pnl = fetch_one("SELECT COALESCE(total,0) AS t FROM pnl_snapshots ORDER BY id DESC LIMIT 1")
    return {
        "total_orders": total_orders,
        "open_orders": open_orders,
        "total_fills": fills,
        "latest_total_pnl": str(pnl["t"] if pnl else 0),
    }


@app.post("/replay/jobs")
def create_replay_job(deadletter_event_id: int | None = None, requested_by: str = "manual"):
    job_id = str(uuid.uuid4())
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO replay_jobs(job_id, status, requested_by, deadletter_event_id) VALUES (%s,'PENDING',%s,%s)",
                (job_id, requested_by, deadletter_event_id),
            )

    evt = new_event(
        event_type="replay.job.created",
        topic=REPLAY_JOBS,
        key=job_id,
        payload={"job_id": job_id, "deadletter_event_id": deadletter_event_id, "requested_by": requested_by},
    )
    publish_event(producer, evt)
    return {"job_id": job_id, "status": "PENDING"}


@app.get("/replay/jobs")
def list_replay_jobs(limit: int = 100):
    rows = fetch_all(
        """
        SELECT job_id::text, status, requested_by, deadletter_event_id, created_at::text, finished_at::text, COALESCE(error, '') AS error
        FROM replay_jobs
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    return rows


@app.get("/replay/deadletters")
def list_deadletters(limit: int = 100, replayed: bool | None = None):
    if replayed is None:
        rows = fetch_all(
            """
            SELECT id, source_service, original_topic, event_key, replayed, error_message, created_at::text
            FROM deadletter_events
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        )
    else:
        rows = fetch_all(
            """
            SELECT id, source_service, original_topic, event_key, replayed, error_message, created_at::text
            FROM deadletter_events
            WHERE replayed=%s
            ORDER BY id DESC
            LIMIT %s
            """,
            (replayed, limit),
        )
    return rows


@app.post("/replay/deadletters/{deadletter_event_id}")
def manual_replay_deadletter(deadletter_event_id: int, requested_by: str = "manual"):
    return create_replay_job(deadletter_event_id=deadletter_event_id, requested_by=requested_by)
