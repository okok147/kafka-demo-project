from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from kafka_demo.sim.engine import SimulationEngine


class CreateOrderRequest(BaseModel):
    account_id: str
    symbol: str
    side: str = Field(pattern="^(BUY|SELL)$")
    quantity: float = Field(gt=0)
    price: float | None = Field(default=None, gt=0)


class CancelOrderRequest(BaseModel):
    reason: str = "user_requested"


engine = SimulationEngine(enable_market_simulator=True)
app = FastAPI(title="kafka-demo-sim-server")


@app.on_event("startup")
async def _startup() -> None:
    await engine.start()


@app.on_event("shutdown")
async def _shutdown() -> None:
    await engine.close()


@app.get("/")
async def index():
    return FileResponse(Path(__file__).resolve().parent.parent / "web" / "index.html")


@app.post("/orders")
async def create_order(payload: CreateOrderRequest, idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")):
    return await engine.create_order(payload.model_dump(mode="python"), idempotency_key=idempotency_key)


@app.post("/orders/{order_id}/cancel")
async def cancel_order(order_id: str, payload: CancelOrderRequest):
    try:
        return await engine.cancel_order(order_id, payload.reason)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="order not found") from exc


@app.get("/views/orders")
async def orders_view(limit: int = 50):
    return await engine.orders_view(limit=limit)


@app.get("/views/orders/{order_id}/timeline")
async def timeline_view(order_id: str):
    return await engine.timeline_view(order_id=order_id)


@app.get("/views/positions")
async def positions_view():
    return await engine.positions_view()


@app.get("/views/pnl")
async def pnl_view(account_id: str | None = None):
    return await engine.pnl_view(account_id=account_id)


@app.get("/dashboard")
async def dashboard():
    return await engine.dashboard()


@app.get("/replay/jobs")
async def list_replay_jobs(limit: int = 100):
    return await engine.replay_jobs_view(limit=limit)


@app.get("/replay/deadletters")
async def list_deadletters(limit: int = 100, replayed: bool | None = None):
    return await engine.deadletters_view(limit=limit, replayed=replayed)


@app.post("/replay/jobs")
async def create_replay_job(deadletter_event_id: int | None = None, requested_by: str = "manual"):
    return await engine.create_replay_job(deadletter_event_id=deadletter_event_id, requested_by=requested_by)


@app.post("/replay/deadletters/{deadletter_event_id}")
async def manual_replay_deadletter(deadletter_event_id: int, requested_by: str = "manual"):
    return await engine.create_replay_job(deadletter_event_id=deadletter_event_id, requested_by=requested_by)


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    return {"status": "ok", "mode": "simulation"}
