from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
import random
from typing import Any
import uuid

from kafka_demo.common.envelope import EventEnvelope, new_event
from kafka_demo.common.topics import DEADLETTER_EVENTS, EXECUTION_EVENTS, MARKET_NORMALIZED, MARKET_RAW, ORDER_COMMANDS, PORTFOLIO_EVENTS, REPLAY_JOBS, RISK_ACCEPTED, RISK_REJECTED
from kafka_demo.services.logic import risk_decision, should_fill
from kafka_demo.sim.broker import SimulatedKafkaBroker
from kafka_demo.sim.state import Position, SimulationState


PROJECTOR_TOPICS = {ORDER_COMMANDS, RISK_ACCEPTED, RISK_REJECTED, EXECUTION_EVENTS, PORTFOLIO_EVENTS}


class SimulationEngine:
    def __init__(self, *, enable_market_simulator: bool = True) -> None:
        self.state = SimulationState()
        self.broker = SimulatedKafkaBroker()
        self._lock = asyncio.Lock()
        self._market_task: asyncio.Task | None = None
        self._enable_market_simulator = enable_market_simulator
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        self.broker.subscribe(topic=MARKET_RAW, consumer_name="market-normalizer", handler=self._on_market_raw, on_error=self._on_consumer_error)
        self.broker.subscribe(topic=MARKET_NORMALIZED, consumer_name="execution-quote", handler=self._on_market_normalized_for_execution, on_error=self._on_consumer_error)
        self.broker.subscribe(topic=MARKET_NORMALIZED, consumer_name="portfolio-mark", handler=self._on_market_normalized_for_portfolio, on_error=self._on_consumer_error)
        self.broker.subscribe(topic=ORDER_COMMANDS, consumer_name="risk-service", handler=self._on_order_command, on_error=self._on_consumer_error)
        self.broker.subscribe(topic=RISK_ACCEPTED, consumer_name="execution-service", handler=self._on_risk_accepted, on_error=self._on_consumer_error)
        self.broker.subscribe(topic=EXECUTION_EVENTS, consumer_name="portfolio-service", handler=self._on_execution_event, on_error=self._on_consumer_error)
        self.broker.subscribe(topic=REPLAY_JOBS, consumer_name="replay-runner", handler=self._on_replay_job, on_error=self._on_consumer_error)
        self.broker.subscribe(topic=DEADLETTER_EVENTS, consumer_name="deadletter-handler", handler=self._on_deadletter_event, on_error=None)

        for topic in PROJECTOR_TOPICS:
            self.broker.subscribe(topic=topic, consumer_name=f"query-projector-{topic}", handler=self._on_projector_event, on_error=self._on_consumer_error)

        if self._enable_market_simulator:
            self._market_task = asyncio.create_task(self._market_loop())

    async def close(self) -> None:
        if self._market_task:
            self._market_task.cancel()
            await asyncio.gather(self._market_task, return_exceptions=True)
        await self.broker.close()

    async def create_order(self, payload: dict[str, Any], idempotency_key: str | None) -> dict[str, Any]:
        async with self._lock:
            if idempotency_key and idempotency_key in self.state.idempotency_keys:
                return self.state.idempotency_keys[idempotency_key]

            order_id = str(uuid.uuid4())
            resp = {"order_id": order_id, "status": "PENDING_RISK"}
            self.state.orders[order_id] = {
                "order_id": order_id,
                "account_id": payload["account_id"],
                "symbol": payload["symbol"].upper(),
                "side": payload["side"],
                "quantity": str(payload["quantity"]),
                "price": str(payload["price"]) if payload.get("price") is not None else "",
                "status": "PENDING_RISK",
                "created_at": _utc_now_str(),
                "updated_at": _utc_now_str(),
                "last_event_id": None,
            }
            if idempotency_key:
                self.state.idempotency_keys[idempotency_key] = resp

        cmd = new_event(
            event_type="order.new.requested",
            topic=ORDER_COMMANDS,
            key=resp["order_id"],
            payload={
                "order_id": resp["order_id"],
                "account_id": payload["account_id"],
                "symbol": payload["symbol"].upper(),
                "side": payload["side"],
                "quantity": str(payload["quantity"]),
                "price": str(payload["price"]) if payload.get("price") is not None else None,
            },
        )
        await self.broker.produce(cmd)
        return resp

    async def cancel_order(self, order_id: str, reason: str) -> dict[str, Any]:
        async with self._lock:
            if order_id not in self.state.orders:
                raise KeyError("order not found")
        cmd = new_event(
            event_type="order.cancel.requested",
            topic=ORDER_COMMANDS,
            key=order_id,
            payload={"order_id": order_id, "reason": reason},
        )
        await self.broker.produce(cmd)
        return {"order_id": order_id, "status": "CANCEL_PENDING"}

    async def create_replay_job(self, deadletter_event_id: int | None, requested_by: str) -> dict[str, Any]:
        job_id = str(uuid.uuid4())
        async with self._lock:
            self.state.replay_jobs[job_id] = {
                "job_id": job_id,
                "status": "PENDING",
                "requested_by": requested_by,
                "deadletter_event_id": deadletter_event_id,
                "created_at": _utc_now_str(),
                "finished_at": None,
                "error": "",
            }
        evt = new_event(
            event_type="replay.job.created",
            topic=REPLAY_JOBS,
            key=job_id,
            payload={"job_id": job_id, "deadletter_event_id": deadletter_event_id, "requested_by": requested_by},
        )
        await self.broker.produce(evt)
        return {"job_id": job_id, "status": "PENDING"}

    async def orders_view(self, limit: int = 50) -> list[dict[str, Any]]:
        async with self._lock:
            rows = sorted(self.state.orders.values(), key=lambda x: x["updated_at"], reverse=True)
            return rows[:limit]

    async def timeline_view(self, order_id: str) -> list[dict[str, Any]]:
        async with self._lock:
            return [x for x in self.state.order_timeline if x.get("order_id") == order_id]

    async def positions_view(self) -> list[dict[str, Any]]:
        async with self._lock:
            out: list[dict[str, Any]] = []
            for pos in self.state.positions.values():
                out.append(
                    {
                        "account_id": pos.account_id,
                        "symbol": pos.symbol,
                        "quantity": str(pos.quantity),
                        "avg_price": str(pos.avg_price),
                        "realized_pnl": str(pos.realized_pnl),
                        "unrealized_pnl": str(pos.unrealized_pnl),
                        "updated_at": _utc_now_str(),
                    }
                )
            out.sort(key=lambda x: (x["account_id"], x["symbol"]))
            return out

    async def pnl_view(self, account_id: str | None) -> list[dict[str, Any]]:
        async with self._lock:
            rows = list(self.state.pnl_snapshots)
            if account_id:
                rows = [x for x in rows if x["account_id"] == account_id]
            return list(reversed(rows[-100:]))

    async def dashboard(self) -> dict[str, Any]:
        async with self._lock:
            latest_total = self.state.pnl_snapshots[-1]["total"] if self.state.pnl_snapshots else Decimal("0")
            return {
                "total_orders": len(self.state.orders),
                "open_orders": len(self.state.open_orders),
                "total_fills": len(self.state.fill_ledger),
                "latest_total_pnl": str(latest_total),
            }

    async def replay_jobs_view(self, limit: int = 100) -> list[dict[str, Any]]:
        async with self._lock:
            rows = sorted(self.state.replay_jobs.values(), key=lambda x: x["created_at"], reverse=True)
            return rows[:limit]

    async def deadletters_view(self, limit: int = 100, replayed: bool | None = None) -> list[dict[str, Any]]:
        async with self._lock:
            rows = list(reversed(self.state.deadletter_events))
            if replayed is not None:
                rows = [x for x in rows if x["replayed"] == replayed]
            return rows[:limit]

    async def _market_loop(self) -> None:
        prices = {"AAPL": 190.0, "MSFT": 410.0, "TSLA": 175.0, "NVDA": 920.0}
        while True:
            for symbol in list(prices.keys()):
                prices[symbol] = max(1.0, prices[symbol] + random.uniform(-0.8, 0.8))
                mid = round(prices[symbol], 4)
                event = new_event(
                    event_type="market.tick.raw",
                    topic=MARKET_RAW,
                    key=symbol,
                    payload={"symbol": symbol, "bid": round(mid - 0.05, 4), "ask": round(mid + 0.05, 4), "source": "simulator"},
                )
                await self.broker.produce(event)
            await asyncio.sleep(1.0)

    async def _on_consumer_error(self, topic: str, consumer_name: str, event: EventEnvelope, exc: Exception) -> None:
        deadletter = new_event(
            event_type="deadletter.created",
            topic=DEADLETTER_EVENTS,
            key=event.key or "unknown",
            payload={
                "source_service": consumer_name,
                "original_topic": topic,
                "event_key": event.key,
                "event_payload": asdict(event),
                "error_message": str(exc),
            },
        )
        await self.broker.produce(deadletter)

    async def _on_market_raw(self, event: EventEnvelope) -> None:
        symbol = event.payload["symbol"].upper()
        normalized = new_event(
            event_type="market.tick.normalized",
            topic=MARKET_NORMALIZED,
            key=symbol,
            payload={"symbol": symbol, "bid": float(event.payload["bid"]), "ask": float(event.payload["ask"])},
            metadata={"source_event_id": event.event_id},
        )
        await self.broker.produce(normalized)

    async def _on_market_normalized_for_execution(self, event: EventEnvelope) -> None:
        symbol = event.payload["symbol"]
        async with self._lock:
            self.state.quote_cache[symbol] = {
                "bid": Decimal(str(event.payload["bid"])),
                "ask": Decimal(str(event.payload["ask"])),
            }
            order_ids = [oid for oid, o in self.state.open_orders.items() if o["symbol"] == symbol]

        for order_id in order_ids:
            await self._try_fill(order_id)

    async def _on_market_normalized_for_portfolio(self, event: EventEnvelope) -> None:
        symbol = event.payload["symbol"]
        mid = (Decimal(str(event.payload["bid"])) + Decimal(str(event.payload["ask"]))) / Decimal("2")
        async with self._lock:
            accounts: set[str] = set()
            for key, pos in self.state.positions.items():
                if key[1] != symbol:
                    continue
                pos.unrealized_pnl = pos.quantity * (mid - pos.avg_price)
                accounts.add(pos.account_id)
            for account_id in accounts:
                self._append_pnl_snapshot(account_id)

    async def _on_order_command(self, event: EventEnvelope) -> None:
        if event.event_type == "order.new.requested":
            await self._handle_order_new(event)
            return
        if event.event_type == "order.cancel.requested":
            accepted = new_event(
                event_type="risk.accepted",
                topic=RISK_ACCEPTED,
                key=event.key,
                payload={"order_id": event.payload["order_id"], "reason": event.payload.get("reason"), "command_type": "CANCEL"},
                metadata={"source_event_id": event.event_id},
            )
            await self.broker.produce(accepted)

    async def _handle_order_new(self, event: EventEnvelope) -> None:
        order = event.payload
        account_id = order["account_id"]
        qty = Decimal(str(order["quantity"]))
        price = Decimal(str(order["price"] or "0"))

        async with self._lock:
            limit = self.state.account_limits.get(account_id)
            if not limit:
                accepted = False
                reason = "NO_ACCOUNT_LIMIT"
            else:
                open_orders = [x for x in self.state.open_orders.values() if x["account_id"] == account_id]
                open_notional = sum((Decimal(str(x["remaining_qty"])) * Decimal(str(x["price"] or "0")) for x in open_orders), Decimal("0"))
                accepted, reason = risk_decision(
                    max_notional=Decimal(str(limit["max_notional"])),
                    max_open_orders=int(limit["max_open_orders"]),
                    open_notional=open_notional,
                    open_order_count=len(open_orders),
                    qty=qty,
                    price=price,
                )

        topic = RISK_ACCEPTED if accepted else RISK_REJECTED
        event_type = "risk.accepted" if accepted else "risk.rejected"
        outcome = new_event(
            event_type=event_type,
            topic=topic,
            key=event.key,
            payload={**order, "reason": reason, "command_type": "NEW"},
            metadata={"source_event_id": event.event_id},
        )
        async with self._lock:
            target = self.state.orders.get(order["order_id"])
            if target:
                target["status"] = "RISK_ACCEPTED" if accepted else "RISK_REJECTED"
                target["updated_at"] = _utc_now_str()
                target["last_event_id"] = outcome.event_id
        await self.broker.produce(outcome)

    async def _on_risk_accepted(self, event: EventEnvelope) -> None:
        command_type = event.payload.get("command_type")
        if command_type == "NEW":
            await self._open_order(event.payload)
            opened = new_event(
                event_type="execution.order.opened",
                topic=EXECUTION_EVENTS,
                key=event.payload["order_id"],
                payload={"order_id": event.payload["order_id"], "status": "OPEN"},
            )
            await self.broker.produce(opened)
            await self._try_fill(event.payload["order_id"])
            return

        if command_type == "CANCEL":
            order_id = event.payload["order_id"]
            async with self._lock:
                cancelled = order_id in self.state.open_orders
                if cancelled:
                    self.state.open_orders.pop(order_id, None)
                order = self.state.orders.get(order_id)
                if order:
                    order["status"] = "CANCELLED" if cancelled else "FILLED"
                    order["updated_at"] = _utc_now_str()
            out = new_event(
                event_type="execution.order.cancelled" if cancelled else "execution.order.cancel_after_fill",
                topic=EXECUTION_EVENTS,
                key=order_id,
                payload={"order_id": order_id, "cancelled": cancelled},
            )
            await self.broker.produce(out)

    async def _open_order(self, order: dict[str, Any]) -> None:
        async with self._lock:
            self.state.open_orders[order["order_id"]] = {
                "order_id": order["order_id"],
                "account_id": order["account_id"],
                "symbol": order["symbol"],
                "side": order["side"],
                "remaining_qty": Decimal(str(order["quantity"])),
                "price": Decimal(str(order["price"])) if order.get("price") else None,
                "status": "OPEN",
            }
            target = self.state.orders.get(order["order_id"])
            if target:
                target["status"] = "OPEN"
                target["updated_at"] = _utc_now_str()

    async def _try_fill(self, order_id: str) -> None:
        async with self._lock:
            o = self.state.open_orders.get(order_id)
            if not o:
                return
            quote = self.state.quote_cache.get(o["symbol"])
            if not quote:
                return
            bid, ask = quote["bid"], quote["ask"]
            limit_price = o["price"]
            fillable, fill_px = should_fill(o["side"], limit_price, bid, ask)
            if not fillable:
                return
            fill_qty = o["remaining_qty"]
            self.state.open_orders.pop(order_id, None)
            order = self.state.orders.get(order_id)
            if order:
                order["status"] = "FILLED"
                order["updated_at"] = _utc_now_str()

        fill = new_event(
            event_type="execution.order.filled",
            topic=EXECUTION_EVENTS,
            key=order_id,
            payload={
                "fill_id": str(uuid.uuid4()),
                "order_id": order_id,
                "account_id": o["account_id"],
                "symbol": o["symbol"],
                "side": o["side"],
                "quantity": str(fill_qty),
                "price": str(fill_px),
                "notional": str(fill_qty * fill_px),
            },
        )
        await self.broker.produce(fill)

    async def _on_execution_event(self, event: EventEnvelope) -> None:
        if event.event_type != "execution.order.filled":
            return
        fill = event.payload
        qty = Decimal(str(fill["quantity"]))
        price = Decimal(str(fill["price"]))
        notional = Decimal(str(fill["notional"]))
        side = fill["side"]
        account_id = fill["account_id"]
        symbol = fill["symbol"]

        async with self._lock:
            self.state.fill_ledger.append(
                {
                    "fill_id": fill["fill_id"],
                    "order_id": fill["order_id"],
                    "account_id": account_id,
                    "symbol": symbol,
                    "side": side,
                    "quantity": str(qty),
                    "price": str(price),
                    "notional": str(notional),
                    "occurred_at": _utc_now_str(),
                }
            )

            cash_delta = -notional if side == "BUY" else notional
            balance = self.state.account_balances.get(account_id, Decimal("0")) + cash_delta
            self.state.account_balances[account_id] = balance
            self.state.cash_ledger.append(
                {"account_id": account_id, "delta": str(cash_delta), "balance": str(balance), "event_id": fill["fill_id"], "created_at": _utc_now_str()}
            )

            key = (account_id, symbol)
            pos = self.state.positions.get(key)
            signed_qty = qty if side == "BUY" else -qty
            if not pos:
                self.state.positions[key] = Position(
                    account_id=account_id,
                    symbol=symbol,
                    quantity=signed_qty,
                    avg_price=price,
                    realized_pnl=Decimal("0"),
                    unrealized_pnl=Decimal("0"),
                )
            else:
                old_qty = pos.quantity
                old_avg = pos.avg_price
                old_real = pos.realized_pnl
                new_qty = old_qty + signed_qty

                realized = old_real
                new_avg = old_avg
                if old_qty == 0 or (old_qty > 0 and signed_qty > 0) or (old_qty < 0 and signed_qty < 0):
                    total_abs = abs(old_qty) + abs(signed_qty)
                    new_avg = ((abs(old_qty) * old_avg) + (abs(signed_qty) * price)) / total_abs
                else:
                    close_qty = min(abs(old_qty), abs(signed_qty))
                    if old_qty > 0:
                        realized += close_qty * (price - old_avg)
                    else:
                        realized += close_qty * (old_avg - price)
                    if new_qty == 0:
                        new_avg = Decimal("0")
                    elif abs(signed_qty) > abs(old_qty):
                        new_avg = price

                pos.quantity = new_qty
                pos.avg_price = new_avg
                pos.realized_pnl = realized
            self._append_pnl_snapshot(account_id)

        out = new_event(
            event_type="portfolio.fill.recorded",
            topic=PORTFOLIO_EVENTS,
            key=event.key,
            payload={"order_id": fill["order_id"], "fill_id": fill["fill_id"]},
        )
        await self.broker.produce(out)

    async def _on_replay_job(self, event: EventEnvelope) -> None:
        payload = event.payload
        job_id = payload["job_id"]
        deadletter_id = payload.get("deadletter_event_id")
        async with self._lock:
            job = self.state.replay_jobs[job_id]
            job["status"] = "RUNNING"
            candidates = [x for x in self.state.deadletter_events if not x["replayed"]]
            if deadletter_id is not None:
                candidates = [x for x in candidates if x["id"] == deadletter_id]

        try:
            for d in candidates:
                replay_payload = d["event_payload"]
                if isinstance(replay_payload, dict) and "topic" in replay_payload and "event_type" in replay_payload:
                    replay_event = EventEnvelope(**replay_payload)
                else:
                    replay_event = new_event(
                        event_type="replay.raw",
                        topic=d["original_topic"],
                        key=d.get("event_key") or "replay",
                        payload={"raw": replay_payload},
                    )
                await self.broker.produce(replay_event)
                async with self._lock:
                    d["replayed"] = True
            async with self._lock:
                job["status"] = "DONE"
                job["finished_at"] = _utc_now_str()
                job["error"] = ""
        except Exception as exc:
            async with self._lock:
                job["status"] = "FAILED"
                job["finished_at"] = _utc_now_str()
                job["error"] = str(exc)

    async def _on_deadletter_event(self, event: EventEnvelope) -> None:
        payload = event.payload
        async with self._lock:
            d_id = len(self.state.deadletter_events) + 1
            self.state.deadletter_events.append(
                {
                    "id": d_id,
                    "source_service": payload["source_service"],
                    "original_topic": payload["original_topic"],
                    "event_key": payload.get("event_key"),
                    "event_payload": payload.get("event_payload"),
                    "error_message": payload["error_message"],
                    "replayed": False,
                    "created_at": _utc_now_str(),
                }
            )

    async def _on_projector_event(self, event: EventEnvelope) -> None:
        async with self._lock:
            self.state.order_timeline.append(
                {
                    "order_id": event.payload.get("order_id"),
                    "event_type": event.event_type,
                    "event_id": event.event_id,
                    "payload": event.payload,
                    "created_at": _utc_now_str(),
                }
            )

    def _append_pnl_snapshot(self, account_id: str) -> None:
        realized = Decimal("0")
        unrealized = Decimal("0")
        for pos in self.state.positions.values():
            if pos.account_id != account_id:
                continue
            realized += pos.realized_pnl
            unrealized += pos.unrealized_pnl
        self.state.pnl_snapshots.append(
            {"account_id": account_id, "realized": realized, "unrealized": unrealized, "total": realized + unrealized, "updated_at": _utc_now_str()}
        )


def _utc_now_str() -> str:
    return datetime.now(timezone.utc).isoformat()
